//! Small DFS control-plane records stored in `sinomo-kv`.
//!
//! This module intentionally does not store per-file or per-object metadata.
//! `sinomo-kv` is for compact, cluster-wide configuration such as DfsFileSystem
//! roots and metadata-shard placement. File/object namespace metadata belongs
//! in the DFS metadata service and its sharded local stores.

use crate::DfsFileSystemId;
use crate::namespace::{SIFS_SERVICES_PATH, SIFS_USERS_PATH};
use crate::{DfsFileSystem, DfsSubtreePolicy, sifs_file_system_id};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::DfsObjectPolicy;
use sinomo_core::kv::{
    InProcessKvStore, KvCompare, KvEntry, KvError, KvOp, KvRevision, KvTxn, KvTxnResponse,
};
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

const DFS_KV_PREFIX: &str = "/dfs";
const METADATA_HEAD_WATERMARK_PUBLISH_RETRIES: usize = 4;
const METADATA_PRIMARY_TRANSFER_RETRIES: usize = 4;

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

/// Compact Raft-backed write barrier used to prove that a metadata-shard
/// assignment was current at a particular point in the cluster KV log.
///
/// This lives outside `metadata-shards/` so shard-prefix scans continue to
/// contain only [`DfsMetadataShardConfig`] values.
pub fn metadata_shard_authority_barrier_key(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-authority-barriers/{}",
        normalize_segment(shard_id)
    )
}

/// Quorum-committed durable head for one metadata-shard authority epoch.
pub fn metadata_shard_head_watermark_key(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-head-watermarks/{}",
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

impl From<DfsFileSystemConfig> for DfsFileSystem {
    fn from(config: DfsFileSystemConfig) -> Self {
        Self {
            file_system_id: config.file_system_id,
            display_name: config.display_name,
            owner_node_id: config.owner_node_id,
            subtree_policies: config.subtree_policies,
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

/// Highest metadata journal prefix known to be durable on a configured
/// majority while the exact shard assignment and authority epoch were current.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardHeadWatermark {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    pub primary_node_id: NodeId,
    pub epoch: u64,
    pub journal_sequence: u64,
    /// Exact complete materialized-state identity acknowledged at
    /// `journal_sequence`. A sequence alone cannot distinguish table-only
    /// recovery/job state at the same journal head.
    #[serde(default)]
    pub state_fingerprint: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acknowledged_node_ids: Vec<NodeId>,
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
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError>;
    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError>;
    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError>;
    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError>;

    /// Compare a control-plane value through the client's linearizable write
    /// path. The fresh barrier write is intentional: a compare-only or
    /// same-value transaction can be answered by the cluster's semantic
    /// no-effect fast path without committing a new Raft entry.
    fn linearizable_compare_value(
        &mut self,
        key: String,
        expected_value: Vec<u8>,
        barrier_key: String,
    ) -> Result<bool, KvError> {
        let barrier_value =
            sinomo_core::kv::encode_kv_value(&sinomo_core::Id::generate().to_string())?;
        let response = self.txn(KvTxn {
            compares: vec![KvCompare::ValueEquals {
                key,
                value: expected_value,
            }],
            success: vec![KvOp::Put {
                key: barrier_key,
                value: barrier_value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.succeeded)
    }
}

pub type DfsControlPlaneKvFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, KvError>> + Send + 'a>>;

pub trait AsyncDfsControlPlaneKvClient: std::fmt::Debug + Send {
    fn refresh_latest_async<'a>(&'a mut self) -> DfsControlPlaneKvFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn get_async<'a>(&'a self, key: &'a str) -> DfsControlPlaneKvFuture<'a, Option<KvEntry>>;

    fn range_prefix_async<'a>(
        &'a self,
        prefix: &'a str,
    ) -> DfsControlPlaneKvFuture<'a, Vec<KvEntry>>;

    fn put_async<'a>(
        &'a mut self,
        key: String,
        value: Vec<u8>,
    ) -> DfsControlPlaneKvFuture<'a, KvRevision>;

    fn txn_async<'a>(&'a mut self, txn: KvTxn) -> DfsControlPlaneKvFuture<'a, KvTxnResponse>;

    /// Async counterpart of
    /// [`DfsControlPlaneKvClient::linearizable_compare_value`].
    fn linearizable_compare_value_async<'a>(
        &'a mut self,
        key: String,
        expected_value: Vec<u8>,
        barrier_key: String,
    ) -> DfsControlPlaneKvFuture<'a, bool> {
        Box::pin(async move {
            let barrier_value =
                sinomo_core::kv::encode_kv_value(&sinomo_core::Id::generate().to_string())?;
            let response = self
                .txn_async(KvTxn {
                    compares: vec![KvCompare::ValueEquals {
                        key,
                        value: expected_value,
                    }],
                    success: vec![KvOp::Put {
                        key: barrier_key,
                        value: barrier_value,
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                })
                .await?;
            Ok(response.succeeded)
        })
    }
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
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .refresh_latest()
    }

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

impl DfsControlPlaneKvClient for InProcessKvStore {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        Ok(self.get(key).cloned())
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.range_prefix(prefix)
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        InProcessKvStore::put(self, key, value, None)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        InProcessKvStore::txn(self, txn)
    }
}

impl AsyncDfsControlPlaneKvClient for InProcessKvStore {
    fn get_async<'a>(&'a self, key: &'a str) -> DfsControlPlaneKvFuture<'a, Option<KvEntry>> {
        Box::pin(async move { DfsControlPlaneKvClient::get(self, key) })
    }

    fn range_prefix_async<'a>(
        &'a self,
        prefix: &'a str,
    ) -> DfsControlPlaneKvFuture<'a, Vec<KvEntry>> {
        Box::pin(async move { DfsControlPlaneKvClient::range_prefix(self, prefix) })
    }

    fn put_async<'a>(
        &'a mut self,
        key: String,
        value: Vec<u8>,
    ) -> DfsControlPlaneKvFuture<'a, KvRevision> {
        Box::pin(async move { InProcessKvStore::put(self, key, value, None) })
    }

    fn txn_async<'a>(&'a mut self, txn: KvTxn) -> DfsControlPlaneKvFuture<'a, KvTxnResponse> {
        Box::pin(async move { InProcessKvStore::txn(self, txn) })
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

    /// Replace exactly `expected` (or create an absent shard when `None`) with
    /// `desired`. The compare uses the stored bytes after logical decoding so
    /// legacy encodings remain replaceable without allowing a stale overwrite.
    pub fn compare_and_put_metadata_shard(
        &mut self,
        expected: Option<&DfsMetadataShardConfig>,
        desired: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        validate_metadata_shard_cas_identity(expected, desired)?;
        let key = metadata_shard_key(&desired.file_system_id, &desired.shard_id);
        let current_entry = self.kv.get(&key)?;
        let compare = match (expected, current_entry.as_ref()) {
            (None, None) => KvCompare::Missing { key: key.clone() },
            (Some(expected), Some(entry))
                if entry.decode::<DfsMetadataShardConfig>()? == *expected =>
            {
                KvCompare::ValueEquals {
                    key: key.clone(),
                    value: entry.value.clone(),
                }
            }
            _ => return Ok(false),
        };
        Ok(self
            .kv
            .txn(KvTxn {
                compares: vec![compare],
                success: vec![KvOp::put_serde(key, desired, None)?],
                failure: Vec::new(),
            })?
            .succeeded)
    }

    pub fn get_metadata_shard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        self.get_serde(&metadata_shard_key(file_system_id, shard_id))
    }

    pub fn get_metadata_shard_head_watermark(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardHeadWatermark>, KvError> {
        self.get_serde(&metadata_shard_head_watermark_key(file_system_id, shard_id))
    }

    /// Publish a quorum-durable metadata head only while `expected_shard` is
    /// still the exact committed shard assignment.
    ///
    /// The previous watermark participates in the compare so concurrent syncs
    /// cannot regress the durable head. A fresh authority-barrier write keeps
    /// repeated identical publication on the Raft quorum path instead of the
    /// semantic no-effect fast path.
    pub fn publish_metadata_shard_head_watermark(
        &mut self,
        expected_shard: &DfsMetadataShardConfig,
        watermark: &DfsMetadataShardHeadWatermark,
    ) -> Result<bool, KvError> {
        let watermark = normalize_metadata_shard_head_watermark(expected_shard, watermark)?;
        let shard_key =
            metadata_shard_key(&expected_shard.file_system_id, &expected_shard.shard_id);
        let watermark_key = metadata_shard_head_watermark_key(
            &expected_shard.file_system_id,
            &expected_shard.shard_id,
        );

        for _ in 0..METADATA_HEAD_WATERMARK_PUBLISH_RETRIES {
            let Some(shard_entry) = self.kv.get(&shard_key)? else {
                return Ok(false);
            };
            if shard_entry.decode::<DfsMetadataShardConfig>()? != *expected_shard {
                return Ok(false);
            }

            let previous_entry = self.kv.get(&watermark_key)?;
            let published = merged_metadata_shard_head_watermark(
                expected_shard,
                previous_entry.as_ref(),
                &watermark,
            )?;
            let mut compares = vec![KvCompare::ValueEquals {
                key: shard_key.clone(),
                value: shard_entry.value,
            }];
            compares.push(match previous_entry.as_ref() {
                Some(entry) => KvCompare::ValueEquals {
                    key: watermark_key.clone(),
                    value: entry.value.clone(),
                },
                None => KvCompare::Missing {
                    key: watermark_key.clone(),
                },
            });
            let barrier_value =
                sinomo_core::kv::encode_kv_value(&sinomo_core::Id::generate().to_string())?;
            let response = self.kv.txn(KvTxn {
                compares,
                success: vec![
                    KvOp::put_serde(watermark_key.clone(), &published, None)?,
                    KvOp::Put {
                        key: metadata_shard_authority_barrier_key(
                            &expected_shard.file_system_id,
                            &expected_shard.shard_id,
                        ),
                        value: barrier_value,
                        lease_id: None,
                    },
                ],
                failure: Vec::new(),
            })?;
            if response.succeeded {
                return Ok(true);
            }
        }
        Ok(false)
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
        select_metadata_shard_config_for_path(
            file_system_id,
            &self.list_metadata_shards(file_system_id)?,
            path,
        )
    }

    pub fn select_metadata_shard_for_path_strict_default_sifs_split(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        select_metadata_shard_config_for_path_strict_default_sifs_split(
            file_system_id,
            &self.list_metadata_shards(file_system_id)?,
            path,
        )
    }

    /// Prove through a Raft-backed write barrier that `expected` is still the
    /// exact committed metadata-shard assignment.
    ///
    /// The preliminary read preserves compatibility with older encodings: the
    /// barrier compares the bytes already stored in KV after verifying that
    /// they decode to the expected logical value.
    pub fn certify_metadata_shard_authority(
        &mut self,
        expected: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        let key = metadata_shard_key(&expected.file_system_id, &expected.shard_id);
        let Some(entry) = self.kv.get(&key)? else {
            return Ok(false);
        };
        if entry.decode::<DfsMetadataShardConfig>()? != *expected {
            return Ok(false);
        }
        self.kv.linearizable_compare_value(
            key,
            entry.value,
            metadata_shard_authority_barrier_key(&expected.file_system_id, &expected.shard_id),
        )
    }

    pub fn transfer_metadata_shard_primary(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        expected_epoch: u64,
        new_primary_node_id: NodeId,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        let shard_key = metadata_shard_key(file_system_id, shard_id);
        let watermark_key = metadata_shard_head_watermark_key(file_system_id, shard_id);
        for _ in 0..METADATA_PRIMARY_TRANSFER_RETRIES {
            let Some(shard_entry) = self.kv.get(&shard_key)? else {
                return Ok(None);
            };
            let current: DfsMetadataShardConfig = shard_entry.decode()?;
            if current.epoch != expected_epoch {
                return Ok(None);
            }
            if current.primary_node_id == new_primary_node_id {
                return Ok(Some(current));
            }
            let Some(watermark_entry) = self.kv.get(&watermark_key)? else {
                return Ok(None);
            };
            let watermark = watermark_entry.decode::<DfsMetadataShardHeadWatermark>()?;
            let Some((transferred, transferred_watermark)) =
                prepare_metadata_shard_primary_transfer(
                    &current,
                    &watermark,
                    &new_primary_node_id,
                )?
            else {
                return Ok(None);
            };
            let response = self.kv.txn(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: shard_key.clone(),
                        value: shard_entry.value,
                    },
                    KvCompare::ValueEquals {
                        key: watermark_key.clone(),
                        value: watermark_entry.value,
                    },
                ],
                success: vec![
                    KvOp::put_serde(shard_key.clone(), &transferred, None)?,
                    KvOp::put_serde(watermark_key.clone(), &transferred_watermark, None)?,
                ],
                failure: Vec::new(),
            })?;
            if response.succeeded {
                return Ok(Some(transferred));
            }
        }
        Ok(None)
    }

    fn put_serde<T>(&mut self, key: &str, value: &T) -> Result<(), KvError>
    where
        T: Serialize,
    {
        self.kv
            .put(key.to_string(), sinomo_core::kv::encode_kv_value(value)?)?;
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

pub struct DfsControlPlaneAsyncStore<'a> {
    pub(crate) kv: &'a mut dyn AsyncDfsControlPlaneKvClient,
}

impl<'a> DfsControlPlaneAsyncStore<'a> {
    pub fn new(kv: &'a mut dyn AsyncDfsControlPlaneKvClient) -> Self {
        Self { kv }
    }

    pub async fn put_file_system_config(
        &mut self,
        config: &DfsFileSystemConfig,
    ) -> Result<(), KvError> {
        self.put_serde(&file_system_config_key(&config.file_system_id), config)
            .await
    }

    pub async fn get_file_system_config(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystemConfig>, KvError> {
        self.get_serde(&file_system_config_key(file_system_id))
            .await
    }

    pub async fn put_metadata_shard(
        &mut self,
        shard: &DfsMetadataShardConfig,
    ) -> Result<(), KvError> {
        self.put_serde(
            &metadata_shard_key(&shard.file_system_id, &shard.shard_id),
            shard,
        )
        .await
    }

    /// Async exact-value compare-and-put for metadata-shard bootstrap.
    pub async fn compare_and_put_metadata_shard(
        &mut self,
        expected: Option<&DfsMetadataShardConfig>,
        desired: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        validate_metadata_shard_cas_identity(expected, desired)?;
        let key = metadata_shard_key(&desired.file_system_id, &desired.shard_id);
        let current_entry = self.kv.get_async(&key).await?;
        let compare = match (expected, current_entry.as_ref()) {
            (None, None) => KvCompare::Missing { key: key.clone() },
            (Some(expected), Some(entry))
                if entry.decode::<DfsMetadataShardConfig>()? == *expected =>
            {
                KvCompare::ValueEquals {
                    key: key.clone(),
                    value: entry.value.clone(),
                }
            }
            _ => return Ok(false),
        };
        Ok(self
            .kv
            .txn_async(KvTxn {
                compares: vec![compare],
                success: vec![KvOp::put_serde(key, desired, None)?],
                failure: Vec::new(),
            })
            .await?
            .succeeded)
    }

    pub async fn get_metadata_shard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        self.get_serde(&metadata_shard_key(file_system_id, shard_id))
            .await
    }

    pub async fn get_metadata_shard_head_watermark(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardHeadWatermark>, KvError> {
        let key = metadata_shard_head_watermark_key(file_system_id, shard_id);
        let Some(entry) = self.kv.get_async(&key).await? else {
            return Ok(None);
        };
        entry.decode().map(Some)
    }

    /// Async exact-authority publication of a quorum-durable metadata head.
    pub async fn publish_metadata_shard_head_watermark(
        &mut self,
        expected_shard: &DfsMetadataShardConfig,
        watermark: &DfsMetadataShardHeadWatermark,
    ) -> Result<bool, KvError> {
        let watermark = normalize_metadata_shard_head_watermark(expected_shard, watermark)?;
        let shard_key =
            metadata_shard_key(&expected_shard.file_system_id, &expected_shard.shard_id);
        let watermark_key = metadata_shard_head_watermark_key(
            &expected_shard.file_system_id,
            &expected_shard.shard_id,
        );

        for _ in 0..METADATA_HEAD_WATERMARK_PUBLISH_RETRIES {
            let Some(shard_entry) = self.kv.get_async(&shard_key).await? else {
                return Ok(false);
            };
            if shard_entry.decode::<DfsMetadataShardConfig>()? != *expected_shard {
                return Ok(false);
            }

            let previous_entry = self.kv.get_async(&watermark_key).await?;
            let published = merged_metadata_shard_head_watermark(
                expected_shard,
                previous_entry.as_ref(),
                &watermark,
            )?;
            let mut compares = vec![KvCompare::ValueEquals {
                key: shard_key.clone(),
                value: shard_entry.value,
            }];
            compares.push(match previous_entry.as_ref() {
                Some(entry) => KvCompare::ValueEquals {
                    key: watermark_key.clone(),
                    value: entry.value.clone(),
                },
                None => KvCompare::Missing {
                    key: watermark_key.clone(),
                },
            });
            let barrier_value =
                sinomo_core::kv::encode_kv_value(&sinomo_core::Id::generate().to_string())?;
            let response = self
                .kv
                .txn_async(KvTxn {
                    compares,
                    success: vec![
                        KvOp::put_serde(watermark_key.clone(), &published, None)?,
                        KvOp::Put {
                            key: metadata_shard_authority_barrier_key(
                                &expected_shard.file_system_id,
                                &expected_shard.shard_id,
                            ),
                            value: barrier_value,
                            lease_id: None,
                        },
                    ],
                    failure: Vec::new(),
                })
                .await?;
            if response.succeeded {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn list_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardConfig>, KvError> {
        let mut shards: Vec<DfsMetadataShardConfig> = self
            .kv
            .range_prefix_async(&format!("{}/", metadata_shards_prefix(file_system_id)))
            .await?
            .into_iter()
            .map(|entry| entry.decode())
            .collect::<Result<Vec<_>, _>>()?;
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        Ok(shards)
    }

    pub async fn select_metadata_shard_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        select_metadata_shard_config_for_path(
            file_system_id,
            &self.list_metadata_shards(file_system_id).await?,
            path,
        )
    }

    pub async fn select_metadata_shard_for_path_strict_default_sifs_split(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        select_metadata_shard_config_for_path_strict_default_sifs_split(
            file_system_id,
            &self.list_metadata_shards(file_system_id).await?,
            path,
        )
    }

    /// Async Raft-backed proof that `expected` remains the exact committed
    /// metadata-shard assignment.
    pub async fn certify_metadata_shard_authority(
        &mut self,
        expected: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        let key = metadata_shard_key(&expected.file_system_id, &expected.shard_id);
        let Some(entry) = self.kv.get_async(&key).await? else {
            return Ok(false);
        };
        if entry.decode::<DfsMetadataShardConfig>()? != *expected {
            return Ok(false);
        }
        self.kv
            .linearizable_compare_value_async(
                key,
                entry.value,
                metadata_shard_authority_barrier_key(&expected.file_system_id, &expected.shard_id),
            )
            .await
    }

    pub async fn transfer_metadata_shard_primary(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        expected_epoch: u64,
        new_primary_node_id: NodeId,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        let shard_key = metadata_shard_key(file_system_id, shard_id);
        let watermark_key = metadata_shard_head_watermark_key(file_system_id, shard_id);
        for _ in 0..METADATA_PRIMARY_TRANSFER_RETRIES {
            let Some(shard_entry) = self.kv.get_async(&shard_key).await? else {
                return Ok(None);
            };
            let current: DfsMetadataShardConfig = shard_entry.decode()?;
            if current.epoch != expected_epoch {
                return Ok(None);
            }
            if current.primary_node_id == new_primary_node_id {
                return Ok(Some(current));
            }
            let Some(watermark_entry) = self.kv.get_async(&watermark_key).await? else {
                return Ok(None);
            };
            let watermark = watermark_entry.decode::<DfsMetadataShardHeadWatermark>()?;
            let Some((transferred, transferred_watermark)) =
                prepare_metadata_shard_primary_transfer(
                    &current,
                    &watermark,
                    &new_primary_node_id,
                )?
            else {
                return Ok(None);
            };
            let response = self
                .kv
                .txn_async(KvTxn {
                    compares: vec![
                        KvCompare::ValueEquals {
                            key: shard_key.clone(),
                            value: shard_entry.value,
                        },
                        KvCompare::ValueEquals {
                            key: watermark_key.clone(),
                            value: watermark_entry.value,
                        },
                    ],
                    success: vec![
                        KvOp::put_serde(shard_key.clone(), &transferred, None)?,
                        KvOp::put_serde(watermark_key.clone(), &transferred_watermark, None)?,
                    ],
                    failure: Vec::new(),
                })
                .await?;
            if response.succeeded {
                return Ok(Some(transferred));
            }
        }
        Ok(None)
    }

    async fn put_serde<T>(&mut self, key: &str, value: &T) -> Result<(), KvError>
    where
        T: Serialize,
    {
        self.kv
            .put_async(key.to_string(), sinomo_core::kv::encode_kv_value(value)?)
            .await?;
        Ok(())
    }

    async fn get_serde<T>(&self, key: &str) -> Result<Option<T>, KvError>
    where
        T: DeserializeOwned,
    {
        let Some(entry) = self.kv.get_async(key).await? else {
            return Ok(None);
        };
        entry.decode().map(Some)
    }
}

fn validate_metadata_shard_cas_identity(
    expected: Option<&DfsMetadataShardConfig>,
    desired: &DfsMetadataShardConfig,
) -> Result<(), KvError> {
    if expected.is_some_and(|expected| {
        expected.file_system_id != desired.file_system_id || expected.shard_id != desired.shard_id
    }) {
        return Err(KvError::Serialization {
            detail: "metadata-shard compare-and-put cannot change shard identity".to_string(),
        });
    }
    Ok(())
}

/// Rotate the existing ordered metadata membership around `new_primary` while
/// preserving exactly the same unique configured nodes.
///
/// Keeping the old primary in the replica set is load-bearing: merely replacing
/// `primary_node_id` leaves the new primary duplicated in `replica_node_ids` and
/// silently shrinks the effective quorum after replica normalization.
pub(crate) fn topology_preserving_metadata_shard_replicas(
    current: &DfsMetadataShardConfig,
    new_primary: &NodeId,
) -> Option<Vec<NodeId>> {
    let mut members = vec![current.primary_node_id.clone()];
    for node_id in &current.replica_node_ids {
        if !members.contains(node_id) {
            members.push(node_id.clone());
        }
    }
    let primary_index = members.iter().position(|node_id| node_id == new_primary)?;
    if members.len() < 2 {
        return None;
    }
    Some(
        (1..members.len())
            .map(|offset| members[(primary_index + offset) % members.len()].clone())
            .collect(),
    )
}

fn prepare_metadata_shard_primary_transfer(
    current: &DfsMetadataShardConfig,
    watermark: &DfsMetadataShardHeadWatermark,
    new_primary: &NodeId,
) -> Result<Option<(DfsMetadataShardConfig, DfsMetadataShardHeadWatermark)>, KvError> {
    let Some(replica_node_ids) = topology_preserving_metadata_shard_replicas(current, new_primary)
    else {
        return Ok(None);
    };
    let watermark = normalize_metadata_shard_head_watermark(current, watermark)?;
    if watermark.journal_sequence == 0
        || !watermark
            .acknowledged_node_ids
            .iter()
            .any(|node_id| node_id == new_primary)
    {
        return Ok(None);
    }
    let Some(next_epoch) = current.epoch.checked_add(1) else {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} authority epoch is exhausted",
                current.shard_id
            ),
        });
    };
    let transferred = DfsMetadataShardConfig {
        primary_node_id: new_primary.clone(),
        replica_node_ids,
        epoch: next_epoch,
        ..current.clone()
    };
    let transferred_watermark = normalize_metadata_shard_head_watermark(
        &transferred,
        &DfsMetadataShardHeadWatermark {
            primary_node_id: new_primary.clone(),
            epoch: next_epoch,
            ..watermark
        },
    )?;
    Ok(Some((transferred, transferred_watermark)))
}

fn normalize_metadata_shard_head_watermark(
    expected_shard: &DfsMetadataShardConfig,
    watermark: &DfsMetadataShardHeadWatermark,
) -> Result<DfsMetadataShardHeadWatermark, KvError> {
    if watermark.file_system_id != expected_shard.file_system_id
        || watermark.shard_id != expected_shard.shard_id
        || watermark.primary_node_id != expected_shard.primary_node_id
        || watermark.epoch != expected_shard.epoch
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata-head watermark authority does not match shard {} epoch {}",
                expected_shard.shard_id, expected_shard.epoch
            ),
        });
    }

    let configured_node_ids = std::iter::once(expected_shard.primary_node_id.clone())
        .chain(expected_shard.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let acknowledged_node_ids = watermark
        .acknowledged_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if !acknowledged_node_ids.contains(&expected_shard.primary_node_id) {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata-head watermark for shard {} does not acknowledge primary {}",
                expected_shard.shard_id, expected_shard.primary_node_id
            ),
        });
    }
    if let Some(unconfigured) = acknowledged_node_ids
        .iter()
        .find(|node_id| !configured_node_ids.contains(*node_id))
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata-head watermark for shard {} acknowledges unconfigured node {}",
                expected_shard.shard_id, unconfigured
            ),
        });
    }
    let quorum_size = configured_node_ids.len() / 2 + 1;
    if acknowledged_node_ids.len() < quorum_size {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata-head watermark for shard {} has {} acknowledgements; quorum is {}",
                expected_shard.shard_id,
                acknowledged_node_ids.len(),
                quorum_size
            ),
        });
    }
    if watermark.state_fingerprint.is_empty() {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata-head watermark for shard {} has no exact state fingerprint",
                expected_shard.shard_id
            ),
        });
    }

    Ok(DfsMetadataShardHeadWatermark {
        file_system_id: watermark.file_system_id.clone(),
        shard_id: watermark.shard_id.clone(),
        primary_node_id: watermark.primary_node_id.clone(),
        epoch: watermark.epoch,
        journal_sequence: watermark.journal_sequence,
        state_fingerprint: watermark.state_fingerprint.clone(),
        acknowledged_node_ids: acknowledged_node_ids.into_iter().collect(),
    })
}

fn merged_metadata_shard_head_watermark(
    expected_shard: &DfsMetadataShardConfig,
    previous_entry: Option<&KvEntry>,
    candidate: &DfsMetadataShardHeadWatermark,
) -> Result<DfsMetadataShardHeadWatermark, KvError> {
    let Some(previous_entry) = previous_entry else {
        return Ok(candidate.clone());
    };
    let previous = previous_entry.decode::<DfsMetadataShardHeadWatermark>()?;
    if previous.file_system_id != candidate.file_system_id
        || previous.shard_id != candidate.shard_id
        || previous.primary_node_id != candidate.primary_node_id
        || previous.epoch != candidate.epoch
    {
        return Ok(candidate.clone());
    }
    let Ok(mut previous) = normalize_metadata_shard_head_watermark(expected_shard, &previous)
    else {
        return Ok(candidate.clone());
    };

    match previous.journal_sequence.cmp(&candidate.journal_sequence) {
        std::cmp::Ordering::Greater => Ok(previous),
        std::cmp::Ordering::Less => Ok(candidate.clone()),
        std::cmp::Ordering::Equal => {
            if previous.state_fingerprint != candidate.state_fingerprint {
                // The candidate itself already carries a complete fresh
                // quorum. Never union acknowledgements for different states
                // that happen to share a journal sequence.
                return Ok(candidate.clone());
            }
            previous
                .acknowledged_node_ids
                .extend(candidate.acknowledged_node_ids.iter().cloned());
            normalize_metadata_shard_head_watermark(expected_shard, &previous)
        }
    }
}

fn normalize_segment(segment: &str) -> String {
    segment.trim_matches('/').replace('/', "_")
}

pub(crate) fn select_metadata_shard_config_for_path(
    file_system_id: &DfsFileSystemId,
    shards: &[DfsMetadataShardConfig],
    path: &str,
) -> Result<Option<DfsMetadataShardConfig>, KvError> {
    select_metadata_shard_config_for_path_with_options(file_system_id, shards, path, false)
}

pub(crate) fn select_metadata_shard_config_for_path_strict_default_sifs_split(
    file_system_id: &DfsFileSystemId,
    shards: &[DfsMetadataShardConfig],
    path: &str,
) -> Result<Option<DfsMetadataShardConfig>, KvError> {
    select_metadata_shard_config_for_path_with_options(file_system_id, shards, path, true)
}

fn select_metadata_shard_config_for_path_with_options(
    file_system_id: &DfsFileSystemId,
    shards: &[DfsMetadataShardConfig],
    path: &str,
    reject_incomplete_default_sifs_split: bool,
) -> Result<Option<DfsMetadataShardConfig>, KvError> {
    let path = normalize_metadata_selection_path(path)?;
    let path_hash = metadata_name_hash(&path);
    let mut root = None::<&DfsMetadataShardConfig>;
    let mut prefix_match = None::<(usize, String, &DfsMetadataShardConfig)>;
    let mut hash_match = None::<(u128, String, &DfsMetadataShardConfig)>;
    let mut saw_default_sifs_split_shard = false;

    for shard in shards {
        match &shard.assignment {
            DfsMetadataShardAssignment::Root => {
                if root
                    .as_ref()
                    .is_none_or(|current| shard.shard_id < current.shard_id)
                {
                    root = Some(shard);
                }
            }
            DfsMetadataShardAssignment::PathPrefix { prefix } => {
                let prefix = normalize_metadata_selection_path(prefix)?;
                if matches!(prefix.as_str(), SIFS_SERVICES_PATH | SIFS_USERS_PATH) {
                    saw_default_sifs_split_shard = true;
                }
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
                    if hash_match.as_ref().is_none_or(|current| {
                        candidate.0 < current.0
                            || (candidate.0 == current.0 && candidate.1 < current.1)
                    }) {
                        hash_match = Some(candidate);
                    }
                }
            }
        }
    }

    if reject_incomplete_default_sifs_split
        && prefix_match.is_none()
        && hash_match.is_none()
        && root.is_some()
        && file_system_id == &sifs_file_system_id()
        && saw_default_sifs_split_shard
        && let Some((shard_id, prefix)) = default_sifs_split_shard_for_path(&path)
    {
        return Err(KvError::Busy {
            path: format!(
                "metadata shard config incomplete for {path}: missing default {shard_id} shard for {prefix}"
            ),
        });
    }

    Ok(prefix_match
        .map(|(_, _, shard)| shard)
        .or_else(|| hash_match.map(|(_, _, shard)| shard))
        .or(root)
        .cloned())
}

fn default_sifs_split_shard_for_path(path: &str) -> Option<(&'static str, &'static str)> {
    if metadata_path_matches_prefix(path, SIFS_SERVICES_PATH) {
        Some(("services", SIFS_SERVICES_PATH))
    } else if metadata_path_matches_prefix(path, SIFS_USERS_PATH) {
        Some(("users", SIFS_USERS_PATH))
    } else {
        None
    }
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

    fn publish_transfer_watermark(
        kv: &mut InProcessKvStore,
        shard: &DfsMetadataShardConfig,
        journal_sequence: u64,
    ) {
        let acknowledged_node_ids = std::iter::once(shard.primary_node_id.clone())
            .chain(shard.replica_node_ids.iter().cloned())
            .collect();
        DfsControlPlaneStore::new(kv)
            .publish_metadata_shard_head_watermark(
                shard,
                &DfsMetadataShardHeadWatermark {
                    file_system_id: shard.file_system_id.clone(),
                    shard_id: shard.shard_id.clone(),
                    primary_node_id: shard.primary_node_id.clone(),
                    epoch: shard.epoch,
                    journal_sequence,
                    state_fingerprint: format!("blake3:test-{journal_sequence}"),
                    acknowledged_node_ids,
                },
            )
            .expect("publish transfer watermark");
    }

    #[derive(Debug)]
    struct WatermarkRacingKv {
        inner: InProcessKvStore,
        watermark_key: String,
        replacement_watermark: Option<Vec<u8>>,
    }

    impl DfsControlPlaneKvClient for WatermarkRacingKv {
        fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
            Ok(self.inner.get(key).cloned())
        }

        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            self.inner.range_prefix(prefix)
        }

        fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
            self.inner.put(key, value, None)
        }

        fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            let compares_watermark = txn.compares.iter().any(|compare| match compare {
                KvCompare::Missing { key }
                | KvCompare::Exists { key }
                | KvCompare::ValueEquals { key, .. }
                | KvCompare::VersionEquals { key, .. }
                | KvCompare::CreateRevisionEquals { key, .. }
                | KvCompare::ModifyRevisionEquals { key, .. }
                | KvCompare::LeaseEquals { key, .. } => key == &self.watermark_key,
            });
            if compares_watermark && let Some(value) = self.replacement_watermark.take() {
                self.inner.put(self.watermark_key.clone(), value, None)?;
            }
            self.inner.txn(txn)
        }
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
        assert_eq!(
            metadata_shard_authority_barrier_key(&file_system_id, "root/frag"),
            format!("/dfs/filesystems/{file_system_id}/metadata-authority-barriers/root_frag")
        );
        assert_eq!(
            metadata_shard_head_watermark_key(&file_system_id, "root/frag"),
            format!("/dfs/filesystems/{file_system_id}/metadata-head-watermarks/root_frag")
        );
    }

    #[test]
    fn store_puts_and_gets_only_control_plane_records() {
        let node_owner = node("node_owner");
        let node_replica = node("node_replica");
        let mut kv = InProcessKvStore::new();
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
        let mut kv = InProcessKvStore::new();
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
        publish_transfer_watermark(&mut kv, &shard, 5);

        let transferred = DfsControlPlaneStore::new(&mut kv)
            .transfer_metadata_shard_primary(&file_system_id, "root", 7, node_replica.clone())
            .expect("transfer")
            .expect("epoch match");
        assert_eq!(transferred.primary_node_id, node_replica.clone());
        assert_eq!(transferred.replica_node_ids, vec![node_owner.clone()]);
        assert_eq!(transferred.epoch, 8);
        let transferred_watermark = DfsControlPlaneStore::new(&mut kv)
            .get_metadata_shard_head_watermark(&file_system_id, "root")
            .expect("read transferred watermark")
            .expect("transferred watermark");
        assert_eq!(transferred_watermark.primary_node_id, node_replica);
        assert_eq!(transferred_watermark.epoch, 8);
        assert_eq!(transferred_watermark.journal_sequence, 5);
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .transfer_metadata_shard_primary(&file_system_id, "root", 7, node_owner)
                .expect("stale transfer")
                .is_none()
        );
    }

    #[test]
    fn metadata_shard_primary_transfer_retries_an_exact_watermark_change() {
        let owner = node("node_transfer_race_owner");
        let replica_a = node("node_transfer_race_replica_a");
        let replica_b = node("node_transfer_race_replica_b");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replica_a.clone(), replica_b.clone()],
            epoch: 3,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let first_watermark = DfsMetadataShardHeadWatermark {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            epoch: shard.epoch,
            journal_sequence: 10,
            state_fingerprint: "blake3:first".to_string(),
            acknowledged_node_ids: vec![owner.clone(), replica_b.clone()],
        };
        let replacement_watermark = DfsMetadataShardHeadWatermark {
            journal_sequence: 11,
            acknowledged_node_ids: vec![owner, replica_a.clone(), replica_b.clone()],
            ..first_watermark.clone()
        };
        let mut inner = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut inner)
            .put_metadata_shard(&shard)
            .expect("put shard");
        assert!(
            DfsControlPlaneStore::new(&mut inner)
                .publish_metadata_shard_head_watermark(&shard, &first_watermark)
                .expect("publish initial watermark")
        );
        let watermark_key = metadata_shard_head_watermark_key(&file_system_id, "root");
        let mut racing = WatermarkRacingKv {
            inner,
            watermark_key,
            replacement_watermark: Some(
                sinomo_core::kv::encode_kv_value(&replacement_watermark)
                    .expect("encode replacement watermark"),
            ),
        };

        let transferred = DfsControlPlaneStore::new(&mut racing)
            .transfer_metadata_shard_primary(
                &file_system_id,
                "root",
                shard.epoch,
                replica_b.clone(),
            )
            .expect("transfer after watermark race")
            .expect("retry reaches certified replacement watermark");
        assert_eq!(transferred.primary_node_id, replica_b.clone());
        assert_eq!(
            transferred.replica_node_ids,
            vec![shard.primary_node_id.clone(), replica_a]
        );
        let stored_watermark = DfsControlPlaneStore::new(&mut racing)
            .get_metadata_shard_head_watermark(&file_system_id, "root")
            .expect("read transferred watermark")
            .expect("transferred watermark");
        assert_eq!(stored_watermark.primary_node_id, replica_b);
        assert_eq!(stored_watermark.epoch, shard.epoch + 1);
        assert_eq!(stored_watermark.journal_sequence, 11);
    }

    #[test]
    fn metadata_shard_primary_transfer_rejects_uncertified_target() {
        let owner = node("node_transfer_uncertified_owner");
        let certified_replica = node("node_transfer_certified_replica");
        let uncertified_replica = node("node_transfer_uncertified_replica");
        let outsider = node("node_transfer_unconfigured_outsider");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![certified_replica.clone(), uncertified_replica.clone()],
            epoch: 9,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&shard)
            .expect("put shard");
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .publish_metadata_shard_head_watermark(
                    &shard,
                    &DfsMetadataShardHeadWatermark {
                        file_system_id: file_system_id.clone(),
                        shard_id: "root".to_string(),
                        primary_node_id: owner,
                        epoch: shard.epoch,
                        journal_sequence: 4,
                        state_fingerprint: "blake3:transfer".to_string(),
                        acknowledged_node_ids: vec![
                            shard.primary_node_id.clone(),
                            certified_replica,
                        ],
                    },
                )
                .expect("publish quorum watermark")
        );

        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .transfer_metadata_shard_primary(
                    &file_system_id,
                    "root",
                    shard.epoch,
                    uncertified_replica,
                )
                .expect("reject uncertified target")
                .is_none()
        );
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .transfer_metadata_shard_primary(&file_system_id, "root", shard.epoch, outsider,)
                .expect("reject unconfigured target")
                .is_none()
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read unchanged shard"),
            Some(shard)
        );
    }

    #[test]
    fn metadata_shard_primary_transfer_rejects_epoch_exhaustion() {
        let owner = node("node_transfer_exhausted_owner");
        let replica = node("node_transfer_exhausted_replica");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: vec![replica.clone()],
            epoch: u64::MAX,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&shard)
            .expect("put shard");
        publish_transfer_watermark(&mut kv, &shard, 1);

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).transfer_metadata_shard_primary(
                &file_system_id,
                "root",
                u64::MAX,
                replica,
            ),
            Err(KvError::Serialization { detail }) if detail.contains("epoch is exhausted")
        ));
    }

    #[tokio::test]
    async fn async_metadata_shard_primary_transfer_rotates_certified_topology() {
        let owner = node("node_transfer_async_owner");
        let replica = node("node_transfer_async_replica");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replica.clone()],
            epoch: 12,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&shard)
            .expect("put shard");
        publish_transfer_watermark(&mut kv, &shard, 8);

        let transferred = DfsControlPlaneAsyncStore::new(&mut kv)
            .transfer_metadata_shard_primary(&file_system_id, "root", shard.epoch, replica.clone())
            .await
            .expect("async transfer")
            .expect("certified async transfer");
        assert_eq!(transferred.primary_node_id, replica.clone());
        assert_eq!(transferred.replica_node_ids, vec![owner]);
        assert_eq!(transferred.epoch, 13);
        assert_eq!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .get_metadata_shard_head_watermark(&file_system_id, "root")
                .await
                .expect("read async watermark")
                .expect("async watermark")
                .primary_node_id,
            replica
        );
    }

    #[test]
    fn metadata_shard_compare_and_put_rejects_stale_expected_value() {
        let owner = node("node_shard_cas_owner");
        let replacement = node("node_shard_cas_replacement");
        let file_system_id = file_system_id();
        let initial = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replacement.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            primary_node_id: replacement.clone(),
            epoch: 2,
            ..initial.clone()
        };
        let mut kv = InProcessKvStore::new();
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .compare_and_put_metadata_shard(None, &initial)
                .expect("create missing shard")
        );
        publish_transfer_watermark(&mut kv, &initial, 1);
        DfsControlPlaneStore::new(&mut kv)
            .transfer_metadata_shard_primary(&file_system_id, "root", 1, replacement.clone())
            .expect("transfer")
            .expect("matching epoch");

        assert!(
            !DfsControlPlaneStore::new(&mut kv)
                .compare_and_put_metadata_shard(Some(&initial), &desired)
                .expect("stale compare")
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read current shard")
                .expect("shard")
                .primary_node_id,
            replacement
        );
    }

    #[test]
    fn metadata_shard_authority_certification_uses_fresh_write_barriers() {
        let owner = node("node_authority_barrier_owner");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: Vec::new(),
            epoch: 3,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let barrier_key = metadata_shard_authority_barrier_key(&file_system_id, "root");
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&shard)
            .expect("put shard");

        let before_first = kv.revision();
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .certify_metadata_shard_authority(&shard)
                .expect("first authority certification")
        );
        let first_revision = kv.revision();
        let first_barrier = kv.get(&barrier_key).expect("first barrier").value.clone();
        assert!(first_revision > before_first);

        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .certify_metadata_shard_authority(&shard)
                .expect("second authority certification")
        );
        let second_revision = kv.revision();
        let second_barrier = kv.get(&barrier_key).expect("second barrier").value.clone();
        assert!(second_revision > first_revision);
        assert_ne!(second_barrier, first_barrier);
    }

    #[test]
    fn metadata_shard_authority_certification_rejects_stale_assignment() {
        let owner = node("node_authority_stale_owner");
        let replacement = node("node_authority_stale_replacement");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: vec![replacement.clone()],
            epoch: 4,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut kv = InProcessKvStore::new();
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control.put_metadata_shard(&shard).expect("put shard");
        publish_transfer_watermark(&mut kv, &shard, 1);
        DfsControlPlaneStore::new(&mut kv)
            .transfer_metadata_shard_primary(&file_system_id, "root", 4, replacement)
            .expect("transfer")
            .expect("matching epoch");

        let revision_before = kv.revision();
        assert!(
            !DfsControlPlaneStore::new(&mut kv)
                .certify_metadata_shard_authority(&shard)
                .expect("stale authority certification")
        );
        assert_eq!(kv.revision(), revision_before);
    }

    #[test]
    fn metadata_head_watermark_publish_is_quorum_guarded_and_monotonic() {
        let owner = node("node_head_watermark_owner");
        let replica_a = node("node_head_watermark_replica_a");
        let replica_b = node("node_head_watermark_replica_b");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replica_b, replica_a.clone()],
            epoch: 6,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let watermark = DfsMetadataShardHeadWatermark {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            epoch: 6,
            journal_sequence: 12,
            state_fingerprint: "blake3:watermark-12".to_string(),
            acknowledged_node_ids: vec![replica_a.clone(), owner.clone()],
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&shard)
            .expect("put shard");

        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .publish_metadata_shard_head_watermark(&shard, &watermark)
                .expect("publish watermark")
        );
        let first_revision = kv.revision();
        let stored = DfsControlPlaneStore::new(&mut kv)
            .get_metadata_shard_head_watermark(&file_system_id, "root")
            .expect("read watermark")
            .expect("watermark");
        assert_eq!(stored.journal_sequence, 12);
        let mut expected_acknowledgements = vec![owner.clone(), replica_a.clone()];
        expected_acknowledgements.sort();
        assert_eq!(stored.acknowledged_node_ids, expected_acknowledgements);

        let lower = DfsMetadataShardHeadWatermark {
            journal_sequence: 7,
            ..watermark.clone()
        };
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .publish_metadata_shard_head_watermark(&shard, &lower)
                .expect("repeat lower watermark through fresh barrier")
        );
        assert!(kv.revision() > first_revision);
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard_head_watermark(&file_system_id, "root")
                .expect("read monotonic watermark")
                .expect("watermark")
                .journal_sequence,
            12
        );

        DfsControlPlaneStore::new(&mut kv)
            .transfer_metadata_shard_primary(&file_system_id, "root", 6, replica_a)
            .expect("transfer")
            .expect("matching epoch");
        assert!(
            !DfsControlPlaneStore::new(&mut kv)
                .publish_metadata_shard_head_watermark(&shard, &watermark)
                .expect("stale authority rejected")
        );
    }

    #[test]
    fn metadata_head_watermark_rejects_non_quorum_acknowledgements() {
        let owner = node("node_head_watermark_quorum_owner");
        let replica_a = node("node_head_watermark_quorum_replica_a");
        let replica_b = node("node_head_watermark_quorum_replica_b");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replica_a, replica_b],
            epoch: 2,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let watermark = DfsMetadataShardHeadWatermark {
            file_system_id,
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            epoch: 2,
            journal_sequence: 3,
            state_fingerprint: "blake3:non-quorum".to_string(),
            acknowledged_node_ids: vec![owner],
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&shard)
            .expect("put shard");

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv)
                .publish_metadata_shard_head_watermark(&shard, &watermark),
            Err(KvError::Serialization { detail }) if detail.contains("quorum is 2")
        ));
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard_head_watermark(&shard.file_system_id, "root")
                .expect("read missing watermark")
                .is_none()
        );
    }

    #[tokio::test]
    async fn async_metadata_head_watermark_publish_uses_exact_authority() {
        let owner = node("node_head_watermark_async_owner");
        let replica = node("node_head_watermark_async_replica");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replica.clone()],
            epoch: 9,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let watermark = DfsMetadataShardHeadWatermark {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            epoch: 9,
            journal_sequence: 21,
            state_fingerprint: "blake3:async-21".to_string(),
            acknowledged_node_ids: vec![replica, shard.primary_node_id.clone()],
        };
        let mut kv = InProcessKvStore::new();
        assert!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .compare_and_put_metadata_shard(None, &shard)
                .await
                .expect("create shard with async CAS")
        );
        assert!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .publish_metadata_shard_head_watermark(&shard, &watermark)
                .await
                .expect("publish watermark")
        );
        let mut expected_acknowledgements = vec![
            shard.primary_node_id.clone(),
            shard.replica_node_ids[0].clone(),
        ];
        expected_acknowledgements.sort();
        assert_eq!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .get_metadata_shard_head_watermark(&file_system_id, "root")
                .await
                .expect("read watermark"),
            Some(DfsMetadataShardHeadWatermark {
                acknowledged_node_ids: expected_acknowledgements,
                ..watermark
            })
        );
    }

    #[test]
    fn metadata_shard_selection_prefers_longest_path_prefix() {
        let node_owner = node("node_shard_select_owner");
        let node_replica = node("node_shard_select_replica");
        let mut kv = InProcessKvStore::new();
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
    fn metadata_shard_selection_rejects_partial_default_sifs_split_view() {
        let node_root = node("node_shard_partial_root");
        let node_services = node("node_shard_partial_services");
        let node_users = node("node_shard_partial_users");
        let mut kv = InProcessKvStore::new();
        let file_system_id = sifs_file_system_id();
        let mut control = DfsControlPlaneStore::new(&mut kv);
        let root_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_root.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        control
            .put_metadata_shard(&root_shard)
            .expect("put root shard");
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services/api")
                .expect("root-only SIFS view remains usable")
                .map(|shard| shard.shard_id),
            Some("root".to_string())
        );

        let users_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "users".to_string(),
            primary_node_id: node_users,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_USERS_PATH.to_string(),
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        control
            .put_metadata_shard(&users_shard)
            .expect("put users shard");
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services/api")
                .expect("generic selector keeps custom partial SIFS views usable")
                .map(|shard| shard.shard_id),
            Some("root".to_string())
        );
        let error = control
            .select_metadata_shard_for_path_strict_default_sifs_split(
                &file_system_id,
                "/services/api",
            )
            .expect_err("partial split view must not route /services to root");
        assert!(
            matches!(&error, KvError::Busy { path } if path.contains("missing default services shard")),
            "unexpected error: {error:?}"
        );
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/users/alice")
                .expect("visible users shard remains selected")
                .map(|shard| shard.shard_id),
            Some("users".to_string())
        );

        let services_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: node_services,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        control
            .put_metadata_shard(&services_shard)
            .expect("put services shard");
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services/api")
                .expect("complete split view selects services")
                .map(|shard| shard.shard_id),
            Some("services".to_string())
        );
    }

    #[test]
    fn metadata_shard_selection_uses_hash_range_before_root() {
        let node_owner = node("node_shard_hash_owner");
        let node_replica = node("node_shard_hash_replica");
        let mut kv = InProcessKvStore::new();
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
    fn metadata_shard_selection_prefers_narrower_hash_range() {
        let node_owner = node("node_shard_hash_width_owner");
        let mut kv = InProcessKvStore::new();
        let file_system_id = file_system_id();
        let selected_path = "/users/bob/project";
        let hash = metadata_name_hash(selected_path);
        let mut control = DfsControlPlaneStore::new(&mut kv);
        for shard in [
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "aaa-broad".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::NameHashRange {
                    start_inclusive: hash.wrapping_sub(100),
                    end_exclusive: hash.wrapping_add(101),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "zzz-narrow".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::NameHashRange {
                    start_inclusive: hash,
                    end_exclusive: hash.wrapping_add(1),
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
            Some("zzz-narrow".to_string())
        );
    }

    #[test]
    fn metadata_shard_selection_rejects_nul_paths() {
        let node_owner = node("node_shard_invalid_owner");
        let mut kv = InProcessKvStore::new();
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
