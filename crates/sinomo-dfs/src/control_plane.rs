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
const METADATA_OPERATION_FLOOR_PUBLISH_RETRIES: usize = 4;
const METADATA_MEMBERSHIP_HANDOFF_RETRIES: usize = 4;

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

/// Bounded quorum-committed floor that primary transfer must preserve.
///
/// This is deliberately separate from [`metadata_shard_authority_barrier_key`]:
/// ordinary authority checks overwrite that generic barrier, while an
/// operation permit must remain effective until a later prepared operation
/// advances this single per-shard floor.
fn metadata_shard_operation_floor_key(file_system_id: &DfsFileSystemId, shard_id: &str) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-operation-floors/{}",
        normalize_segment(shard_id)
    )
}

/// One bounded, quorum-committed fence for a same-primary membership handoff.
///
/// The active shard configuration remains at [`metadata_shard_key`] until the
/// retiring assignment has transferred an exact DFS metadata state to every
/// member of both assignments. Keeping this outside `metadata-shards/` ensures
/// ordinary placement scans continue to decode only shard configurations.
pub fn metadata_shard_membership_handoff_key(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-membership-handoffs/{}",
        normalize_segment(shard_id)
    )
}

/// Last completed membership-handoff proof for one shard.
///
/// This is bounded audit/failover evidence. A later handoff overwrites it; DFS
/// file and object metadata never enters cluster KV.
pub fn metadata_shard_membership_handoff_proof_key(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-membership-handoff-proofs/{}",
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
struct DfsMetadataShardOperationFloor {
    operation_digest: String,
    prepared_sequence: u64,
    authority_epoch: u64,
    permit_id: String,
}

/// Quorum-committed fence separating a retiring metadata assignment from a
/// desired same-primary assignment.
///
/// While this record exists, the retiring configuration remains stored at the
/// normal shard key but may not authorize a new canonical publication. A data
/// plane coordinator must catch up the union of the retiring and desired
/// members and then provide [`DfsMetadataShardMembershipHandoffProof`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardMembershipHandoff {
    pub handoff_id: String,
    pub retiring_shard: DfsMetadataShardConfig,
    pub desired_shard: DfsMetadataShardConfig,
    pub required_prepared_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_head_watermark: Option<DfsMetadataShardHeadWatermark>,
    /// Exact retiring-primary state captured only after that primary has
    /// observed this durable pending handoff under its publication guard.
    /// A quorum-side `begin` may precede this acknowledgement while the old
    /// primary is partitioned, so activation must never infer a fence merely
    /// from the existence of the handoff record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retiring_primary_fence: Option<DfsMetadataShardMembershipHandoffFence>,
}

/// Retiring-primary acknowledgement that closes the cross-process
/// validate-to-publish race for a membership handoff.
///
/// This is bounded authority evidence, not DFS file metadata. The retiring
/// primary captures it under the same process-wide publication guard held by
/// canonical commits, after refreshing and verifying the durable handoff.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardMembershipHandoffFence {
    pub handoff_id: String,
    pub primary_node_id: NodeId,
    pub journal_sequence: u64,
    pub state_fingerprint: String,
}

/// Exact DFS-state evidence consumed to activate a fenced membership change.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardMembershipHandoffProof {
    pub handoff_id: String,
    pub retiring_shard: DfsMetadataShardConfig,
    pub desired_shard: DfsMetadataShardConfig,
    pub journal_sequence: u64,
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

    /// Create an absent shard or idempotently replace the exact same value.
    ///
    /// A changed same-primary assignment is deliberately *not* activated here.
    /// It must use [`Self::begin_metadata_shard_membership_handoff`] followed by
    /// [`Self::complete_metadata_shard_membership_handoff`] so a committed
    /// membership decision cannot outrun the retiring DFS metadata state.
    pub fn compare_and_put_metadata_shard(
        &mut self,
        expected: Option<&DfsMetadataShardConfig>,
        desired: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        validate_metadata_shard_cas_identity(expected, desired)?;
        let key = metadata_shard_key(&desired.file_system_id, &desired.shard_id);
        let current_entry = self.kv.get(&key)?;
        let shard_compare = match (expected, current_entry.as_ref()) {
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
        let compares = vec![shard_compare];
        if let Some(expected) = expected
            && expected != desired
        {
            validate_metadata_shard_cas_reconfiguration(expected, desired)?;
            return Ok(false);
        }
        Ok(self
            .kv
            .txn(KvTxn {
                compares,
                success: vec![KvOp::put_serde(key, desired, None)?],
                failure: Vec::new(),
            })?
            .succeeded)
    }

    /// Record a pending same-primary membership decision before catch-up.
    ///
    /// The shard configuration itself is intentionally left unchanged. The
    /// returned record binds the retiring and desired assignments to a fresh
    /// id and to the highest quorum-permitted prepare sequence observed in the
    /// same Raft transaction. Because a quorum-side caller may commit this
    /// while the retiring primary is partitioned, this record alone is not the
    /// retiring-primary publication fence; that is attached later by
    /// [`Self::acknowledge_metadata_shard_membership_handoff_fence`].
    pub fn begin_metadata_shard_membership_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        validate_metadata_shard_cas_identity(Some(retiring), desired)?;
        if retiring == desired {
            return Err(KvError::Serialization {
                detail: format!(
                    "metadata shard {} membership handoff does not change the assignment",
                    retiring.shard_id
                ),
            });
        }
        validate_metadata_shard_cas_reconfiguration(retiring, desired)?;
        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);
        for _ in 0..METADATA_MEMBERSHIP_HANDOFF_RETRIES {
            let Some(shard_entry) = self.kv.get(&shard_key)? else {
                return Ok(None);
            };
            if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
                return Ok(None);
            }
            if let Some(entry) = self.kv.get(&handoff_key)? {
                let handoff = entry.decode::<DfsMetadataShardMembershipHandoff>()?;
                return Ok((handoff.retiring_shard == *retiring
                    && handoff.desired_shard == *desired)
                    .then_some(handoff));
            }

            let floor_entry = self.kv.get(&floor_key)?;
            let floor_sequence = match floor_entry.as_ref() {
                Some(entry) => {
                    let floor = entry.decode::<DfsMetadataShardOperationFloor>()?;
                    if floor.authority_epoch > retiring.epoch {
                        return Ok(None);
                    }
                    floor.prepared_sequence
                }
                None => 0,
            };
            let watermark_entry = self.kv.get(&watermark_key)?;
            let required_head_watermark = watermark_entry
                .as_ref()
                .map(|entry| {
                    normalize_metadata_shard_head_watermark(
                        retiring,
                        &entry.decode::<DfsMetadataShardHeadWatermark>()?,
                    )
                })
                .transpose()?;
            let required_prepared_sequence = required_head_watermark
                .as_ref()
                .map(|watermark| watermark.journal_sequence)
                .unwrap_or(0)
                .max(floor_sequence);
            let handoff = DfsMetadataShardMembershipHandoff {
                handoff_id: sinomo_core::Id::generate().to_string(),
                retiring_shard: retiring.clone(),
                desired_shard: desired.clone(),
                required_prepared_sequence,
                required_head_watermark,
                retiring_primary_fence: None,
            };
            let floor_compare = match floor_entry {
                Some(entry) => KvCompare::ValueEquals {
                    key: floor_key.clone(),
                    value: entry.value,
                },
                None => KvCompare::Missing {
                    key: floor_key.clone(),
                },
            };
            let watermark_compare = match watermark_entry {
                Some(entry) => KvCompare::ValueEquals {
                    key: watermark_key.clone(),
                    value: entry.value,
                },
                None => KvCompare::Missing {
                    key: watermark_key.clone(),
                },
            };
            let response = self.kv.txn(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: shard_key.clone(),
                        value: shard_entry.value,
                    },
                    floor_compare,
                    watermark_compare,
                    KvCompare::Missing {
                        key: handoff_key.clone(),
                    },
                ],
                success: vec![KvOp::put_serde(handoff_key.clone(), &handoff, None)?],
                failure: Vec::new(),
            })?;
            if response.succeeded {
                return Ok(Some(handoff));
            }
        }
        Ok(None)
    }

    /// Attach the exact post-fence retiring-primary head to a pending handoff.
    ///
    /// The caller must be the retiring-primary service and must capture
    /// `fence` while holding its process-wide publication guard after a fresh
    /// control-plane read. This transaction makes that observation durable and
    /// immutable before any data-plane catch-up can activate the desired set.
    pub fn acknowledge_metadata_shard_membership_handoff_fence(
        &mut self,
        expected_handoff: &DfsMetadataShardMembershipHandoff,
        fence: &DfsMetadataShardMembershipHandoffFence,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        let retiring = &expected_handoff.retiring_shard;
        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);

        let Some(shard_entry) = self.kv.get(&shard_key)? else {
            return Ok(None);
        };
        if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
            return Ok(None);
        }
        let Some(handoff_entry) = self.kv.get(&handoff_key)? else {
            return Ok(None);
        };
        let handoff = handoff_entry.decode::<DfsMetadataShardMembershipHandoff>()?;
        if !metadata_shard_membership_handoff_identity_matches(&handoff, expected_handoff) {
            return Ok(None);
        }
        if let Some(existing) = handoff.retiring_primary_fence.as_ref() {
            return Ok((existing == fence).then_some(handoff));
        }

        let floor_entry = self.kv.get(&floor_key)?;
        let current_floor = floor_entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardOperationFloor>())
            .transpose()?;
        let fence = normalize_metadata_shard_membership_handoff_fence(
            &handoff,
            fence,
            current_floor.as_ref(),
        )?;
        let watermark_entry = self.kv.get(&watermark_key)?;
        if !metadata_shard_membership_handoff_watermark_is_exact(
            &handoff,
            watermark_entry.as_ref(),
        )? {
            return Ok(None);
        }
        let floor_compare = match floor_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: floor_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: floor_key },
        };
        let watermark_compare = match watermark_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: watermark_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: watermark_key },
        };
        let mut acknowledged = handoff;
        acknowledged.retiring_primary_fence = Some(fence);
        Ok(self
            .kv
            .txn(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: shard_key,
                        value: shard_entry.value,
                    },
                    KvCompare::ValueEquals {
                        key: handoff_key.clone(),
                        value: handoff_entry.value,
                    },
                    floor_compare,
                    watermark_compare,
                ],
                success: vec![KvOp::put_serde(handoff_key, &acknowledged, None)?],
                failure: Vec::new(),
            })?
            .succeeded
            .then_some(acknowledged))
    }

    /// Activate a desired same-primary assignment only after exact retiring
    /// state has been acknowledged by every member of both assignments.
    pub fn complete_metadata_shard_membership_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
        proof: &DfsMetadataShardMembershipHandoffProof,
    ) -> Result<bool, KvError> {
        validate_metadata_shard_cas_identity(Some(retiring), desired)?;
        validate_metadata_shard_cas_reconfiguration(retiring, desired)?;
        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);
        let proof_key = metadata_shard_membership_handoff_proof_key(
            &retiring.file_system_id,
            &retiring.shard_id,
        );

        let Some(shard_entry) = self.kv.get(&shard_key)? else {
            return Ok(false);
        };
        if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
            return Ok(false);
        }
        let Some(handoff_entry) = self.kv.get(&handoff_key)? else {
            return Ok(false);
        };
        let handoff = handoff_entry.decode::<DfsMetadataShardMembershipHandoff>()?;
        if handoff.retiring_shard != *retiring || handoff.desired_shard != *desired {
            return Ok(false);
        }
        let floor_entry = self.kv.get(&floor_key)?;
        let current_floor = floor_entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardOperationFloor>())
            .transpose()?;
        let proof = normalize_metadata_shard_membership_handoff_proof(
            &handoff,
            proof,
            current_floor.as_ref(),
        )?;
        let activated_watermark =
            metadata_shard_membership_handoff_activated_watermark(&handoff, &proof)?;
        let watermark_entry = self.kv.get(&watermark_key)?;
        if !metadata_shard_membership_handoff_watermark_is_exact(
            &handoff,
            watermark_entry.as_ref(),
        )? {
            return Ok(false);
        }
        let floor_compare = match floor_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: floor_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: floor_key },
        };
        let watermark_compare = match watermark_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: watermark_key.clone(),
                value: entry.value,
            },
            None => KvCompare::Missing {
                key: watermark_key.clone(),
            },
        };
        Ok(self
            .kv
            .txn(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: shard_key.clone(),
                        value: shard_entry.value,
                    },
                    KvCompare::ValueEquals {
                        key: handoff_key.clone(),
                        value: handoff_entry.value,
                    },
                    floor_compare,
                    watermark_compare,
                ],
                success: vec![
                    KvOp::put_serde(shard_key, desired, None)?,
                    KvOp::put_serde(watermark_key, &activated_watermark, None)?,
                    KvOp::put_serde(proof_key, &proof, None)?,
                    KvOp::Delete { key: handoff_key },
                ],
                failure: Vec::new(),
            })?
            .succeeded)
    }

    pub fn get_metadata_shard_membership_handoff(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        self.get_serde(&metadata_shard_membership_handoff_key(
            file_system_id,
            shard_id,
        ))
    }

    pub fn get_metadata_shard_membership_handoff_proof(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardMembershipHandoffProof>, KvError> {
        self.get_serde(&metadata_shard_membership_handoff_proof_key(
            file_system_id,
            shard_id,
        ))
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
        let handoff_key = metadata_shard_membership_handoff_key(
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
            if self.kv.get(&handoff_key)?.is_some() {
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
            compares.push(KvCompare::Missing {
                key: handoff_key.clone(),
            });
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
        let handoff_key =
            metadata_shard_membership_handoff_key(&expected.file_system_id, &expected.shard_id);
        let Some(entry) = self.kv.get(&key)? else {
            return Ok(false);
        };
        if entry.decode::<DfsMetadataShardConfig>()? != *expected {
            return Ok(false);
        }
        if self.kv.get(&handoff_key)?.is_some() {
            return Ok(false);
        }
        Ok(self
            .kv
            .txn(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key,
                        value: entry.value,
                    },
                    KvCompare::Missing { key: handoff_key },
                ],
                success: vec![KvOp::Put {
                    key: metadata_shard_authority_barrier_key(
                        &expected.file_system_id,
                        &expected.shard_id,
                    ),
                    value: sinomo_core::kv::encode_kv_value(
                        &sinomo_core::Id::generate().to_string(),
                    )?,
                    lease_id: None,
                }],
                failure: Vec::new(),
            })?
            .succeeded)
    }

    /// Obtain an operation-bound quorum permit while the exact shard
    /// assignment is still committed. This is the partition fallback for a
    /// canonical DFS mutation. The fixed per-shard record also advances the
    /// minimum prepared journal sequence a future primary must have replayed;
    /// it is bounded control state, not per-file metadata or an operation log.
    pub fn certify_metadata_shard_operation(
        &mut self,
        expected: &DfsMetadataShardConfig,
        operation_digest: &str,
        prepared_sequence: u64,
    ) -> Result<Option<String>, KvError> {
        if operation_digest.is_empty() || prepared_sequence == 0 {
            return Ok(None);
        }
        let shard_key = metadata_shard_key(&expected.file_system_id, &expected.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&expected.file_system_id, &expected.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&expected.file_system_id, &expected.shard_id);
        for _ in 0..METADATA_OPERATION_FLOOR_PUBLISH_RETRIES {
            let Some(shard_entry) = self.kv.get(&shard_key)? else {
                return Ok(None);
            };
            if shard_entry.decode::<DfsMetadataShardConfig>()? != *expected {
                return Ok(None);
            }
            if self.kv.get(&handoff_key)?.is_some() {
                return Ok(None);
            }

            let floor_entry = self.kv.get(&floor_key)?;
            if let Some(previous_entry) = floor_entry.as_ref() {
                let previous = previous_entry.decode::<DfsMetadataShardOperationFloor>()?;
                if previous.authority_epoch > expected.epoch
                    || previous.prepared_sequence > prepared_sequence
                    || (previous.prepared_sequence == prepared_sequence
                        && previous.operation_digest != operation_digest)
                {
                    return Ok(None);
                }
            }

            let permit_id = sinomo_core::Id::generate().to_string();
            let floor = DfsMetadataShardOperationFloor {
                operation_digest: operation_digest.to_string(),
                prepared_sequence,
                authority_epoch: expected.epoch,
                permit_id: permit_id.clone(),
            };
            let floor_compare = match floor_entry.as_ref() {
                Some(entry) => KvCompare::ValueEquals {
                    key: floor_key.clone(),
                    value: entry.value.clone(),
                },
                None => KvCompare::Missing {
                    key: floor_key.clone(),
                },
            };
            let response = self.kv.txn(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: shard_key.clone(),
                        value: shard_entry.value,
                    },
                    floor_compare,
                    KvCompare::Missing {
                        key: handoff_key.clone(),
                    },
                ],
                success: vec![KvOp::put_serde(floor_key.clone(), &floor, None)?],
                failure: Vec::new(),
            })?;
            if response.succeeded {
                return Ok(Some(permit_id));
            }
        }
        Ok(None)
    }

    /// Return the unchanged shard for an idempotent request, but fail closed
    /// for an actual primary change.
    ///
    /// Cluster KV cannot certify the current DFS journal head because healthy
    /// full-shard writes intentionally do not update a Raft watermark. A future
    /// proof-bearing data-plane handoff must freeze the retiring primary and
    /// prove that exact prefix on the promoted replica before this surface can
    /// activate a different primary.
    pub fn transfer_metadata_shard_primary(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        expected_epoch: u64,
        new_primary_node_id: NodeId,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        let shard_key = metadata_shard_key(file_system_id, shard_id);
        let handoff_key = metadata_shard_membership_handoff_key(file_system_id, shard_id);
        if self.kv.get(&handoff_key)?.is_some() {
            return Ok(None);
        }
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
        reject_kv_only_metadata_shard_primary_transfer(&current, &new_primary_node_id)
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

    /// Async create/idempotent compare-and-put. Changed assignments require the
    /// explicit two-phase membership-handoff API.
    pub async fn compare_and_put_metadata_shard(
        &mut self,
        expected: Option<&DfsMetadataShardConfig>,
        desired: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        validate_metadata_shard_cas_identity(expected, desired)?;
        let key = metadata_shard_key(&desired.file_system_id, &desired.shard_id);
        let current_entry = self.kv.get_async(&key).await?;
        let shard_compare = match (expected, current_entry.as_ref()) {
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
        let compares = vec![shard_compare];
        if let Some(expected) = expected
            && expected != desired
        {
            validate_metadata_shard_cas_reconfiguration(expected, desired)?;
            return Ok(false);
        }
        Ok(self
            .kv
            .txn_async(KvTxn {
                compares,
                success: vec![KvOp::put_serde(key, desired, None)?],
                failure: Vec::new(),
            })
            .await?
            .succeeded)
    }

    pub async fn begin_metadata_shard_membership_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        validate_metadata_shard_cas_identity(Some(retiring), desired)?;
        if retiring == desired {
            return Err(KvError::Serialization {
                detail: format!(
                    "metadata shard {} membership handoff does not change the assignment",
                    retiring.shard_id
                ),
            });
        }
        validate_metadata_shard_cas_reconfiguration(retiring, desired)?;
        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);
        for _ in 0..METADATA_MEMBERSHIP_HANDOFF_RETRIES {
            let Some(shard_entry) = self.kv.get_async(&shard_key).await? else {
                return Ok(None);
            };
            if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
                return Ok(None);
            }
            if let Some(entry) = self.kv.get_async(&handoff_key).await? {
                let handoff = entry.decode::<DfsMetadataShardMembershipHandoff>()?;
                return Ok((handoff.retiring_shard == *retiring
                    && handoff.desired_shard == *desired)
                    .then_some(handoff));
            }

            let floor_entry = self.kv.get_async(&floor_key).await?;
            let floor_sequence = match floor_entry.as_ref() {
                Some(entry) => {
                    let floor = entry.decode::<DfsMetadataShardOperationFloor>()?;
                    if floor.authority_epoch > retiring.epoch {
                        return Ok(None);
                    }
                    floor.prepared_sequence
                }
                None => 0,
            };
            let watermark_entry = self.kv.get_async(&watermark_key).await?;
            let required_head_watermark = watermark_entry
                .as_ref()
                .map(|entry| {
                    normalize_metadata_shard_head_watermark(
                        retiring,
                        &entry.decode::<DfsMetadataShardHeadWatermark>()?,
                    )
                })
                .transpose()?;
            let required_prepared_sequence = required_head_watermark
                .as_ref()
                .map(|watermark| watermark.journal_sequence)
                .unwrap_or(0)
                .max(floor_sequence);
            let handoff = DfsMetadataShardMembershipHandoff {
                handoff_id: sinomo_core::Id::generate().to_string(),
                retiring_shard: retiring.clone(),
                desired_shard: desired.clone(),
                required_prepared_sequence,
                required_head_watermark,
                retiring_primary_fence: None,
            };
            let floor_compare = match floor_entry {
                Some(entry) => KvCompare::ValueEquals {
                    key: floor_key.clone(),
                    value: entry.value,
                },
                None => KvCompare::Missing {
                    key: floor_key.clone(),
                },
            };
            let watermark_compare = match watermark_entry {
                Some(entry) => KvCompare::ValueEquals {
                    key: watermark_key.clone(),
                    value: entry.value,
                },
                None => KvCompare::Missing {
                    key: watermark_key.clone(),
                },
            };
            let response = self
                .kv
                .txn_async(KvTxn {
                    compares: vec![
                        KvCompare::ValueEquals {
                            key: shard_key.clone(),
                            value: shard_entry.value,
                        },
                        floor_compare,
                        watermark_compare,
                        KvCompare::Missing {
                            key: handoff_key.clone(),
                        },
                    ],
                    success: vec![KvOp::put_serde(handoff_key.clone(), &handoff, None)?],
                    failure: Vec::new(),
                })
                .await?;
            if response.succeeded {
                return Ok(Some(handoff));
            }
        }
        Ok(None)
    }

    pub async fn acknowledge_metadata_shard_membership_handoff_fence(
        &mut self,
        expected_handoff: &DfsMetadataShardMembershipHandoff,
        fence: &DfsMetadataShardMembershipHandoffFence,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        let retiring = &expected_handoff.retiring_shard;
        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);

        let Some(shard_entry) = self.kv.get_async(&shard_key).await? else {
            return Ok(None);
        };
        if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
            return Ok(None);
        }
        let Some(handoff_entry) = self.kv.get_async(&handoff_key).await? else {
            return Ok(None);
        };
        let handoff = handoff_entry.decode::<DfsMetadataShardMembershipHandoff>()?;
        if !metadata_shard_membership_handoff_identity_matches(&handoff, expected_handoff) {
            return Ok(None);
        }
        if let Some(existing) = handoff.retiring_primary_fence.as_ref() {
            return Ok((existing == fence).then_some(handoff));
        }

        let floor_entry = self.kv.get_async(&floor_key).await?;
        let current_floor = floor_entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardOperationFloor>())
            .transpose()?;
        let fence = normalize_metadata_shard_membership_handoff_fence(
            &handoff,
            fence,
            current_floor.as_ref(),
        )?;
        let watermark_entry = self.kv.get_async(&watermark_key).await?;
        if !metadata_shard_membership_handoff_watermark_is_exact(
            &handoff,
            watermark_entry.as_ref(),
        )? {
            return Ok(None);
        }
        let floor_compare = match floor_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: floor_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: floor_key },
        };
        let watermark_compare = match watermark_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: watermark_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: watermark_key },
        };
        let mut acknowledged = handoff;
        acknowledged.retiring_primary_fence = Some(fence);
        Ok(self
            .kv
            .txn_async(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: shard_key,
                        value: shard_entry.value,
                    },
                    KvCompare::ValueEquals {
                        key: handoff_key.clone(),
                        value: handoff_entry.value,
                    },
                    floor_compare,
                    watermark_compare,
                ],
                success: vec![KvOp::put_serde(handoff_key, &acknowledged, None)?],
                failure: Vec::new(),
            })
            .await?
            .succeeded
            .then_some(acknowledged))
    }

    pub async fn complete_metadata_shard_membership_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
        proof: &DfsMetadataShardMembershipHandoffProof,
    ) -> Result<bool, KvError> {
        validate_metadata_shard_cas_identity(Some(retiring), desired)?;
        validate_metadata_shard_cas_reconfiguration(retiring, desired)?;
        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);
        let proof_key = metadata_shard_membership_handoff_proof_key(
            &retiring.file_system_id,
            &retiring.shard_id,
        );

        let Some(shard_entry) = self.kv.get_async(&shard_key).await? else {
            return Ok(false);
        };
        if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
            return Ok(false);
        }
        let Some(handoff_entry) = self.kv.get_async(&handoff_key).await? else {
            return Ok(false);
        };
        let handoff = handoff_entry.decode::<DfsMetadataShardMembershipHandoff>()?;
        if handoff.retiring_shard != *retiring || handoff.desired_shard != *desired {
            return Ok(false);
        }
        let floor_entry = self.kv.get_async(&floor_key).await?;
        let current_floor = floor_entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardOperationFloor>())
            .transpose()?;
        let proof = normalize_metadata_shard_membership_handoff_proof(
            &handoff,
            proof,
            current_floor.as_ref(),
        )?;
        let activated_watermark =
            metadata_shard_membership_handoff_activated_watermark(&handoff, &proof)?;
        let watermark_entry = self.kv.get_async(&watermark_key).await?;
        if !metadata_shard_membership_handoff_watermark_is_exact(
            &handoff,
            watermark_entry.as_ref(),
        )? {
            return Ok(false);
        }
        let floor_compare = match floor_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: floor_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: floor_key },
        };
        let watermark_compare = match watermark_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: watermark_key.clone(),
                value: entry.value,
            },
            None => KvCompare::Missing {
                key: watermark_key.clone(),
            },
        };
        Ok(self
            .kv
            .txn_async(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: shard_key.clone(),
                        value: shard_entry.value,
                    },
                    KvCompare::ValueEquals {
                        key: handoff_key.clone(),
                        value: handoff_entry.value,
                    },
                    floor_compare,
                    watermark_compare,
                ],
                success: vec![
                    KvOp::put_serde(shard_key, desired, None)?,
                    KvOp::put_serde(watermark_key, &activated_watermark, None)?,
                    KvOp::put_serde(proof_key, &proof, None)?,
                    KvOp::Delete { key: handoff_key },
                ],
                failure: Vec::new(),
            })
            .await?
            .succeeded)
    }

    pub async fn get_metadata_shard_membership_handoff(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        self.get_serde(&metadata_shard_membership_handoff_key(
            file_system_id,
            shard_id,
        ))
        .await
    }

    pub async fn get_metadata_shard_membership_handoff_proof(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardMembershipHandoffProof>, KvError> {
        self.get_serde(&metadata_shard_membership_handoff_proof_key(
            file_system_id,
            shard_id,
        ))
        .await
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
        let handoff_key = metadata_shard_membership_handoff_key(
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
            if self.kv.get_async(&handoff_key).await?.is_some() {
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
            compares.push(KvCompare::Missing {
                key: handoff_key.clone(),
            });
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
        let handoff_key =
            metadata_shard_membership_handoff_key(&expected.file_system_id, &expected.shard_id);
        let Some(entry) = self.kv.get_async(&key).await? else {
            return Ok(false);
        };
        if entry.decode::<DfsMetadataShardConfig>()? != *expected {
            return Ok(false);
        }
        if self.kv.get_async(&handoff_key).await?.is_some() {
            return Ok(false);
        }
        Ok(self
            .kv
            .txn_async(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key,
                        value: entry.value,
                    },
                    KvCompare::Missing { key: handoff_key },
                ],
                success: vec![KvOp::Put {
                    key: metadata_shard_authority_barrier_key(
                        &expected.file_system_id,
                        &expected.shard_id,
                    ),
                    value: sinomo_core::kv::encode_kv_value(
                        &sinomo_core::Id::generate().to_string(),
                    )?,
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
            .await?
            .succeeded)
    }

    /// Async counterpart of [`DfsControlPlaneStore::transfer_metadata_shard_primary`].
    /// Actual primary changes fail closed until a proof-bearing DFS data-plane
    /// handoff supplies the retiring-primary fence and promoted-replica proof.
    pub async fn transfer_metadata_shard_primary(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        expected_epoch: u64,
        new_primary_node_id: NodeId,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        let shard_key = metadata_shard_key(file_system_id, shard_id);
        let handoff_key = metadata_shard_membership_handoff_key(file_system_id, shard_id);
        if self.kv.get_async(&handoff_key).await?.is_some() {
            return Ok(None);
        }
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
        reject_kv_only_metadata_shard_primary_transfer(&current, &new_primary_node_id)
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

/// Validate the only configuration delta a same-primary membership handoff may
/// carry. Assignment and policy changes are separate administrative decisions,
/// and an epoch bump without an actual replica-set change has no state-transfer
/// justification. Primary movement requires a proof-carrying DFS data-plane
/// handoff and cannot use a generic or KV-only transaction.
fn validate_metadata_shard_cas_reconfiguration(
    expected: &DfsMetadataShardConfig,
    desired: &DfsMetadataShardConfig,
) -> Result<(), KvError> {
    if expected.primary_node_id != desired.primary_node_id {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} primary changes require a current operation-bound DFS prefix fence and exact promoted-replica acknowledgement",
                expected.shard_id
            ),
        });
    }
    if expected.assignment != desired.assignment
        || expected.default_policy != desired.default_policy
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff cannot change assignment or default policy",
                expected.shard_id
            ),
        });
    }
    if metadata_shard_configured_node_ids(expected) == metadata_shard_configured_node_ids(desired) {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff must change the configured replica set",
                expected.shard_id
            ),
        });
    }
    let Some(next_epoch) = expected.epoch.checked_add(1) else {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} authority epoch is exhausted",
                expected.shard_id
            ),
        });
    };
    if desired.epoch != next_epoch {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} reconfiguration must advance epoch {} to {}",
                expected.shard_id, expected.epoch, next_epoch
            ),
        });
    }
    Ok(())
}

fn metadata_shard_configured_node_ids(shard: &DfsMetadataShardConfig) -> BTreeSet<NodeId> {
    std::iter::once(shard.primary_node_id.clone())
        .chain(shard.replica_node_ids.iter().cloned())
        .collect()
}

fn metadata_shard_membership_handoff_watermark_is_exact(
    handoff: &DfsMetadataShardMembershipHandoff,
    current_entry: Option<&KvEntry>,
) -> Result<bool, KvError> {
    match (&handoff.required_head_watermark, current_entry) {
        (None, None) => Ok(true),
        (Some(required), Some(entry)) => Ok(normalize_metadata_shard_head_watermark(
            &handoff.retiring_shard,
            &entry.decode::<DfsMetadataShardHeadWatermark>()?,
        )? == *required),
        _ => Ok(false),
    }
}

fn metadata_shard_membership_handoff_activated_watermark(
    handoff: &DfsMetadataShardMembershipHandoff,
    proof: &DfsMetadataShardMembershipHandoffProof,
) -> Result<DfsMetadataShardHeadWatermark, KvError> {
    normalize_metadata_shard_head_watermark(
        &handoff.desired_shard,
        &DfsMetadataShardHeadWatermark {
            file_system_id: handoff.desired_shard.file_system_id.clone(),
            shard_id: handoff.desired_shard.shard_id.clone(),
            primary_node_id: handoff.desired_shard.primary_node_id.clone(),
            epoch: handoff.desired_shard.epoch,
            journal_sequence: proof.journal_sequence,
            state_fingerprint: proof.state_fingerprint.clone(),
            acknowledged_node_ids: metadata_shard_configured_node_ids(&handoff.desired_shard)
                .into_iter()
                .collect(),
        },
    )
}

fn metadata_shard_membership_handoff_identity_matches(
    current: &DfsMetadataShardMembershipHandoff,
    expected: &DfsMetadataShardMembershipHandoff,
) -> bool {
    current.handoff_id == expected.handoff_id
        && current.retiring_shard == expected.retiring_shard
        && current.desired_shard == expected.desired_shard
        && current.required_prepared_sequence == expected.required_prepared_sequence
        && current.required_head_watermark == expected.required_head_watermark
}

fn normalize_metadata_shard_membership_handoff_fence(
    handoff: &DfsMetadataShardMembershipHandoff,
    fence: &DfsMetadataShardMembershipHandoffFence,
    current_floor: Option<&DfsMetadataShardOperationFloor>,
) -> Result<DfsMetadataShardMembershipHandoffFence, KvError> {
    if fence.handoff_id != handoff.handoff_id
        || fence.primary_node_id != handoff.retiring_shard.primary_node_id
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} retiring-primary fence is not bound to handoff {} and primary {}",
                handoff.retiring_shard.shard_id,
                handoff.handoff_id,
                handoff.retiring_shard.primary_node_id
            ),
        });
    }
    if fence.state_fingerprint.is_empty() {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} retiring-primary fence has no exact state fingerprint",
                handoff.retiring_shard.shard_id
            ),
        });
    }
    if current_floor.is_some_and(|floor| floor.authority_epoch > handoff.retiring_shard.epoch) {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} retiring-primary fence was superseded by a newer authority epoch",
                handoff.retiring_shard.shard_id
            ),
        });
    }
    let required_sequence = handoff.required_prepared_sequence.max(
        current_floor
            .map(|floor| floor.prepared_sequence)
            .unwrap_or(0),
    );
    if fence.journal_sequence < required_sequence {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} retiring-primary fence sequence {} is below required prepared floor {}",
                handoff.retiring_shard.shard_id, fence.journal_sequence, required_sequence
            ),
        });
    }
    if let Some(required_head) = handoff.required_head_watermark.as_ref()
        && fence.journal_sequence == required_head.journal_sequence
        && fence.state_fingerprint != required_head.state_fingerprint
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} retiring-primary fence at sequence {} does not match its required exact head",
                handoff.retiring_shard.shard_id, fence.journal_sequence
            ),
        });
    }
    Ok(fence.clone())
}

fn normalize_metadata_shard_membership_handoff_proof(
    handoff: &DfsMetadataShardMembershipHandoff,
    proof: &DfsMetadataShardMembershipHandoffProof,
    current_floor: Option<&DfsMetadataShardOperationFloor>,
) -> Result<DfsMetadataShardMembershipHandoffProof, KvError> {
    if handoff.handoff_id.is_empty()
        || proof.handoff_id != handoff.handoff_id
        || proof.retiring_shard != handoff.retiring_shard
        || proof.desired_shard != handoff.desired_shard
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof is not bound to the pending retiring and desired assignments",
                handoff.retiring_shard.shard_id
            ),
        });
    }
    if proof.state_fingerprint.is_empty() {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof has no exact state fingerprint",
                handoff.retiring_shard.shard_id
            ),
        });
    }
    let Some(retiring_fence) = handoff.retiring_primary_fence.as_ref() else {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff has no retiring-primary fence acknowledgement",
                handoff.retiring_shard.shard_id
            ),
        });
    };
    if proof.journal_sequence != retiring_fence.journal_sequence
        || proof.state_fingerprint != retiring_fence.state_fingerprint
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof does not carry the exact retiring-primary fenced head",
                handoff.retiring_shard.shard_id
            ),
        });
    }
    if current_floor.is_some_and(|floor| floor.authority_epoch > handoff.retiring_shard.epoch) {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff was superseded by authority epoch {}",
                handoff.retiring_shard.shard_id,
                current_floor
                    .expect("checked current floor")
                    .authority_epoch
            ),
        });
    }
    let required_prepared_sequence = current_floor
        .map(|floor| floor.prepared_sequence)
        .unwrap_or(0)
        .max(handoff.required_prepared_sequence);
    if proof.journal_sequence < required_prepared_sequence {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof sequence {} is below required prepared floor {}",
                handoff.retiring_shard.shard_id, proof.journal_sequence, required_prepared_sequence
            ),
        });
    }

    if let Some(required_head_watermark) = handoff.required_head_watermark.as_ref()
        && proof.journal_sequence == required_head_watermark.journal_sequence
        && proof.state_fingerprint != required_head_watermark.state_fingerprint
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof at retiring head sequence {} does not match its exact state fingerprint",
                handoff.retiring_shard.shard_id, proof.journal_sequence
            ),
        });
    }

    let retiring_node_ids = metadata_shard_configured_node_ids(&handoff.retiring_shard);
    let desired_node_ids = metadata_shard_configured_node_ids(&handoff.desired_shard);
    let required_node_ids = retiring_node_ids
        .union(&desired_node_ids)
        .cloned()
        .collect::<BTreeSet<_>>();
    let acknowledged_node_ids = proof
        .acknowledged_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if acknowledged_node_ids != required_node_ids {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof acknowledges {:?}; exact retiring and desired union is {:?}",
                handoff.retiring_shard.shard_id, acknowledged_node_ids, required_node_ids
            ),
        });
    }

    Ok(DfsMetadataShardMembershipHandoffProof {
        handoff_id: proof.handoff_id.clone(),
        retiring_shard: proof.retiring_shard.clone(),
        desired_shard: proof.desired_shard.clone(),
        journal_sequence: proof.journal_sequence,
        state_fingerprint: proof.state_fingerprint.clone(),
        acknowledged_node_ids: acknowledged_node_ids.into_iter().collect(),
    })
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

/// Reject a primary change that has only control-plane state available.
///
/// Healthy full-shard writes intentionally do not publish their metadata head
/// to cluster KV. Consequently even a valid, quorum-acknowledged KV watermark
/// can lag the retiring primary's durable DFS journal and cannot safely fence a
/// promotion. Primary movement must be completed by a data-plane handoff that
/// freezes the retiring primary under its publication guard and proves the
/// exact resulting prefix on the promoted replica.
fn reject_kv_only_metadata_shard_primary_transfer(
    current: &DfsMetadataShardConfig,
    new_primary: &NodeId,
) -> Result<Option<DfsMetadataShardConfig>, KvError> {
    if topology_preserving_metadata_shard_replicas(current, new_primary).is_none() {
        return Ok(None);
    }
    if current.epoch == u64::MAX {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} authority epoch is exhausted",
                current.shard_id
            ),
        });
    }
    Err(KvError::Serialization {
        detail: format!(
            "metadata shard {} primary transfer from {} to {} requires a current operation-bound DFS prefix fence and exact promoted-replica acknowledgement; a cluster-KV head watermark cannot prove the latest healthy full-shard write",
            current.shard_id, current.primary_node_id, new_primary
        ),
    })
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

    fn membership_handoff_proof(
        handoff: &DfsMetadataShardMembershipHandoff,
        journal_sequence: u64,
    ) -> DfsMetadataShardMembershipHandoffProof {
        let retiring_node_ids = metadata_shard_configured_node_ids(&handoff.retiring_shard);
        let desired_node_ids = metadata_shard_configured_node_ids(&handoff.desired_shard);
        let acknowledged_node_ids = retiring_node_ids
            .union(&desired_node_ids)
            .cloned()
            .collect();
        let state_fingerprint = handoff
            .retiring_primary_fence
            .as_ref()
            .filter(|fence| fence.journal_sequence == journal_sequence)
            .map(|fence| fence.state_fingerprint.clone())
            .or_else(|| {
                handoff
                    .required_head_watermark
                    .as_ref()
                    .filter(|watermark| watermark.journal_sequence == journal_sequence)
                    .map(|watermark| watermark.state_fingerprint.clone())
            })
            .unwrap_or_else(|| format!("blake3:membership-handoff-{journal_sequence}"));
        DfsMetadataShardMembershipHandoffProof {
            handoff_id: handoff.handoff_id.clone(),
            retiring_shard: handoff.retiring_shard.clone(),
            desired_shard: handoff.desired_shard.clone(),
            journal_sequence,
            state_fingerprint,
            acknowledged_node_ids,
        }
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

    #[derive(Debug)]
    struct OperationFloorRacingKv {
        inner: InProcessKvStore,
        floor_key: String,
        replacement_floor: Option<Vec<u8>>,
    }

    impl DfsControlPlaneKvClient for OperationFloorRacingKv {
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
            let compares_floor = txn.compares.iter().any(|compare| match compare {
                KvCompare::Missing { key }
                | KvCompare::Exists { key }
                | KvCompare::ValueEquals { key, .. }
                | KvCompare::VersionEquals { key, .. }
                | KvCompare::CreateRevisionEquals { key, .. }
                | KvCompare::ModifyRevisionEquals { key, .. }
                | KvCompare::LeaseEquals { key, .. } => key == &self.floor_key,
            });
            if compares_floor && let Some(value) = self.replacement_floor.take() {
                self.inner.put(self.floor_key.clone(), value, None)?;
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
            metadata_shard_operation_floor_key(&file_system_id, "root/frag"),
            format!("/dfs/filesystems/{file_system_id}/metadata-operation-floors/root_frag")
        );
        assert_eq!(
            metadata_shard_membership_handoff_key(&file_system_id, "root/frag"),
            format!("/dfs/filesystems/{file_system_id}/metadata-membership-handoffs/root_frag")
        );
        assert_eq!(
            metadata_shard_membership_handoff_proof_key(&file_system_id, "root/frag"),
            format!(
                "/dfs/filesystems/{file_system_id}/metadata-membership-handoff-proofs/root_frag"
            )
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
    fn metadata_shard_primary_transfer_preserves_epoch_compare_and_idempotence() {
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
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .transfer_metadata_shard_primary(&file_system_id, "root", 6, node_replica.clone(),)
                .expect("stale transfer")
                .is_none()
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .transfer_metadata_shard_primary(&file_system_id, "root", 7, node_owner)
                .expect("idempotent transfer"),
            Some(shard.clone())
        );
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).transfer_metadata_shard_primary(
                &file_system_id,
                "root",
                7,
                node_replica,
            ),
            Err(KvError::Serialization { detail })
                if detail.contains("current operation-bound DFS prefix fence")
        ));
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read unchanged shard"),
            Some(shard)
        );
    }

    #[test]
    fn metadata_shard_primary_transfer_rejects_watermark_below_operation_floor() {
        let owner = node("node_transfer_floor_owner");
        let replica = node("node_transfer_floor_replica");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: vec![replica.clone()],
            epoch: 21,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&shard)
            .expect("put shard");
        publish_transfer_watermark(&mut kv, &shard, 5);

        let permit_id = DfsControlPlaneStore::new(&mut kv)
            .certify_metadata_shard_operation(&shard, "blake3:prepared-write", 6)
            .expect("certify prepared operation")
            .expect("operation permit");
        let floor = kv
            .get(&metadata_shard_operation_floor_key(&file_system_id, "root"))
            .expect("stored operation floor")
            .decode::<DfsMetadataShardOperationFloor>()
            .expect("decode operation floor");
        assert_eq!(floor.operation_digest, "blake3:prepared-write");
        assert_eq!(floor.prepared_sequence, 6);
        assert_eq!(floor.authority_epoch, shard.epoch);
        assert_eq!(floor.permit_id, permit_id);

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).transfer_metadata_shard_primary(
                &file_system_id,
                "root",
                shard.epoch,
                replica,
            ),
            Err(KvError::Serialization { detail })
                if detail.contains("cluster-KV head watermark cannot prove")
        ));
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read unchanged shard"),
            Some(shard)
        );
    }

    #[test]
    fn metadata_shard_primary_transfer_rejects_even_caught_up_kv_watermark() {
        let owner = node("node_transfer_floor_caught_up_owner");
        let replica = node("node_transfer_floor_caught_up_replica");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replica.clone()],
            epoch: 22,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&shard)
            .expect("put shard");
        publish_transfer_watermark(&mut kv, &shard, 5);
        DfsControlPlaneStore::new(&mut kv)
            .certify_metadata_shard_operation(&shard, "blake3:caught-up-write", 6)
            .expect("certify prepared operation")
            .expect("operation permit");
        publish_transfer_watermark(&mut kv, &shard, 6);

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).transfer_metadata_shard_primary(
                &file_system_id,
                "root",
                shard.epoch,
                replica,
            ),
            Err(KvError::Serialization { detail })
                if detail.contains("current operation-bound DFS prefix fence")
                    && detail.contains("latest healthy full-shard write")
        ));
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read unchanged shard"),
            Some(shard)
        );
    }

    #[test]
    fn metadata_shard_primary_transfer_does_not_consume_a_racing_kv_watermark() {
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

        assert!(matches!(
            DfsControlPlaneStore::new(&mut racing).transfer_metadata_shard_primary(
                &file_system_id,
                "root",
                shard.epoch,
                replica_b.clone(),
            ),
            Err(KvError::Serialization { detail })
                if detail.contains("cluster-KV head watermark cannot prove")
        ));
        assert!(racing.replacement_watermark.is_some());
        let stored_watermark = DfsControlPlaneStore::new(&mut racing)
            .get_metadata_shard_head_watermark(&file_system_id, "root")
            .expect("read unchanged watermark")
            .expect("unchanged watermark");
        assert_eq!(stored_watermark, first_watermark);
        assert_eq!(
            DfsControlPlaneStore::new(&mut racing)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read unchanged shard"),
            Some(shard)
        );
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

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).transfer_metadata_shard_primary(
                &file_system_id,
                "root",
                shard.epoch,
                uncertified_replica,
            ),
            Err(KvError::Serialization { detail })
                if detail.contains("exact promoted-replica acknowledgement")
        ));
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
    async fn async_metadata_shard_primary_transfer_rejects_kv_only_proof() {
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

        assert!(matches!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .transfer_metadata_shard_primary(
                    &file_system_id,
                    "root",
                    shard.epoch,
                    replica,
                )
                .await,
            Err(KvError::Serialization { detail })
                if detail.contains("current operation-bound DFS prefix fence")
        ));
        assert_eq!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .await
                .expect("read unchanged async shard"),
            Some(shard)
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
        // Install the concurrent successor directly as fixture state. The
        // public KV-only primary-transfer surface deliberately cannot create
        // this transition without a proof-bearing DFS handoff.
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&desired)
            .expect("install concurrent successor fixture");

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
    fn metadata_shard_compare_and_put_rejects_direct_primary_change() {
        let owner = node("node_shard_direct_primary_owner");
        let replacement = node("node_shard_direct_primary_replacement");
        let file_system_id = file_system_id();
        let current = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replacement.clone()],
            epoch: 40,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            primary_node_id: replacement,
            replica_node_ids: vec![owner],
            epoch: 41,
            ..current.clone()
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&current)
            .expect("put current shard");

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv)
                .compare_and_put_metadata_shard(Some(&current), &desired),
            Err(KvError::Serialization { detail })
                if detail.contains("current operation-bound DFS prefix fence")
        ));
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read unchanged shard"),
            Some(current)
        );
    }

    #[test]
    fn metadata_shard_membership_change_requires_exact_retiring_handoff_before_activation() {
        let owner = node("node_membership_owner");
        let old_replica = node("node_membership_old_replica");
        let new_replica = node("node_membership_new_replica");
        let file_system_id = file_system_id();
        let current = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![old_replica],
            epoch: 50,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![new_replica.clone()],
            epoch: 51,
            ..current.clone()
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&current)
            .expect("put current shard");
        publish_transfer_watermark(&mut kv, &current, 6);
        DfsControlPlaneStore::new(&mut kv)
            .certify_metadata_shard_operation(&current, "blake3:membership-floor", 6)
            .expect("certify pending operation")
            .expect("operation permit");

        let wrong_epoch = DfsMetadataShardConfig {
            epoch: current.epoch,
            ..desired.clone()
        };
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv)
                .compare_and_put_metadata_shard(Some(&current), &wrong_epoch),
            Err(KvError::Serialization { detail })
                if detail.contains("must advance epoch")
        ));
        let changed_assignment = DfsMetadataShardConfig {
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/not-a-membership-change".to_string(),
            },
            ..desired.clone()
        };
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv)
                .begin_metadata_shard_membership_handoff(&current, &changed_assignment),
            Err(KvError::Serialization { detail })
                if detail.contains("cannot change assignment or default policy")
        ));
        let changed_policy = DfsMetadataShardConfig {
            default_policy: DfsObjectPolicy::ReplicatedConfirmed,
            ..desired.clone()
        };
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv)
                .begin_metadata_shard_membership_handoff(&current, &changed_policy),
            Err(KvError::Serialization { detail })
                if detail.contains("cannot change assignment or default policy")
        ));
        let no_replica_change = DfsMetadataShardConfig {
            epoch: current.epoch + 1,
            ..current.clone()
        };
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv)
                .begin_metadata_shard_membership_handoff(&current, &no_replica_change),
            Err(KvError::Serialization { detail })
                if detail.contains("must change the configured replica set")
        ));
        assert!(
            !DfsControlPlaneStore::new(&mut kv)
                .compare_and_put_metadata_shard(Some(&current), &desired)
                .expect("bare membership activation must fail closed")
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read retiring assignment"),
            Some(current.clone())
        );

        let handoff = DfsControlPlaneStore::new(&mut kv)
            .begin_metadata_shard_membership_handoff(&current, &desired)
            .expect("begin handoff")
            .expect("exact retiring assignment");
        assert_eq!(handoff.required_prepared_sequence, 6);
        assert_eq!(
            handoff.required_head_watermark.as_ref().map(|watermark| (
                watermark.journal_sequence,
                watermark.state_fingerprint.as_str()
            )),
            Some((6, "blake3:test-6"))
        );
        assert_eq!(handoff.retiring_shard, current);
        assert_eq!(handoff.desired_shard, desired);
        assert_eq!(handoff.retiring_primary_fence, None);

        // Installing the pending record fences quorum-side authority records.
        // The retiring primary must still acknowledge its exact post-guard
        // head before this membership can activate.
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .certify_metadata_shard_operation(&current, "blake3:after-fence", 7)
                .expect("fenced permit check")
                .is_none()
        );
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).publish_metadata_shard_head_watermark(
                &current,
                &DfsMetadataShardHeadWatermark {
                    file_system_id: file_system_id.clone(),
                    shard_id: "root".to_string(),
                    primary_node_id: owner.clone(),
                    epoch: current.epoch,
                    journal_sequence: 7,
                    state_fingerprint: "blake3:fenced-head".to_string(),
                    acknowledged_node_ids: vec![
                        current.primary_node_id.clone(),
                        current.replica_node_ids[0].clone(),
                    ],
                },
            ),
            Ok(false)
        ));

        let proof_before_primary_fence = membership_handoff_proof(&handoff, 6);
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).complete_metadata_shard_membership_handoff(
                &current,
                &desired,
                &proof_before_primary_fence,
            ),
            Err(KvError::Serialization { detail })
                if detail.contains("no retiring-primary fence acknowledgement")
        ));
        let below_floor_fence = DfsMetadataShardMembershipHandoffFence {
            handoff_id: handoff.handoff_id.clone(),
            primary_node_id: owner.clone(),
            journal_sequence: 5,
            state_fingerprint: "blake3:membership-handoff-5".to_string(),
        };
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv)
                .acknowledge_metadata_shard_membership_handoff_fence(
                    &handoff,
                    &below_floor_fence,
                ),
            Err(KvError::Serialization { detail })
                if detail.contains("below required prepared floor")
        ));
        let retiring_fence = DfsMetadataShardMembershipHandoffFence {
            handoff_id: handoff.handoff_id.clone(),
            primary_node_id: owner.clone(),
            journal_sequence: 6,
            state_fingerprint: "blake3:test-6".to_string(),
        };
        let handoff = DfsControlPlaneStore::new(&mut kv)
            .acknowledge_metadata_shard_membership_handoff_fence(&handoff, &retiring_fence)
            .expect("acknowledge retiring-primary fence")
            .expect("pending handoff remains exact");
        assert_eq!(
            handoff.retiring_primary_fence.as_ref(),
            Some(&retiring_fence)
        );

        let mut wrong_exact_state = membership_handoff_proof(&handoff, 6);
        wrong_exact_state.state_fingerprint = "blake3:not-the-retiring-head".to_string();
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).complete_metadata_shard_membership_handoff(
                &current,
                &desired,
                &wrong_exact_state,
            ),
            Err(KvError::Serialization { detail })
                if detail.contains("exact retiring-primary fenced head")
        ));

        let mut incomplete = membership_handoff_proof(&handoff, 6);
        incomplete
            .acknowledged_node_ids
            .retain(|node_id| node_id != &new_replica);
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv)
                .complete_metadata_shard_membership_handoff(&current, &desired, &incomplete),
            Err(KvError::Serialization { detail }) if detail.contains("exact retiring and desired union")
        ));
        let proof = membership_handoff_proof(&handoff, 6);
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .complete_metadata_shard_membership_handoff(&current, &desired, &proof)
                .expect("activate exact caught-up membership")
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read active assignment"),
            Some(desired.clone())
        );
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard_membership_handoff(&file_system_id, "root")
                .expect("read consumed handoff")
                .is_none()
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard_membership_handoff_proof(&file_system_id, "root")
                .expect("read durable proof"),
            Some(proof)
        );

        // Activation atomically advances the authority-bound watermark from
        // the exact carried state; no stale retiring-epoch window remains.
        let activated_watermark = DfsControlPlaneStore::new(&mut kv)
            .get_metadata_shard_head_watermark(&file_system_id, "root")
            .expect("read activated watermark")
            .expect("activated watermark");
        assert_eq!(activated_watermark.primary_node_id, owner);
        assert_eq!(activated_watermark.epoch, desired.epoch);
        assert_eq!(activated_watermark.journal_sequence, 6);
        let mut desired_acknowledgements =
            vec![desired.primary_node_id.clone(), new_replica.clone()];
        desired_acknowledgements.sort();
        assert_eq!(
            activated_watermark.acknowledged_node_ids,
            desired_acknowledgements
        );

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).transfer_metadata_shard_primary(
                &file_system_id,
                "root",
                desired.epoch,
                new_replica.clone(),
            ),
            Err(KvError::Serialization { detail })
                if detail.contains("current operation-bound DFS prefix fence")
        ));
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("membership assignment remains active"),
            Some(desired)
        );
    }

    #[test]
    fn metadata_shard_membership_change_compares_concurrent_operation_floor() {
        let owner = node("node_membership_floor_race_owner");
        let old_replica = node("node_membership_floor_race_old");
        let new_replica = node("node_membership_floor_race_new");
        let file_system_id = file_system_id();
        let current = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: vec![old_replica],
            epoch: 60,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![new_replica],
            epoch: 61,
            ..current.clone()
        };
        let mut inner = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut inner)
            .put_metadata_shard(&current)
            .expect("put current shard");
        let handoff = DfsControlPlaneStore::new(&mut inner)
            .begin_metadata_shard_membership_handoff(&current, &desired)
            .expect("begin membership handoff")
            .expect("retiring assignment is exact");
        let retiring_fence = DfsMetadataShardMembershipHandoffFence {
            handoff_id: handoff.handoff_id.clone(),
            primary_node_id: current.primary_node_id.clone(),
            journal_sequence: 9,
            state_fingerprint: "blake3:membership-handoff-9".to_string(),
        };
        let handoff = DfsControlPlaneStore::new(&mut inner)
            .acknowledge_metadata_shard_membership_handoff_fence(&handoff, &retiring_fence)
            .expect("acknowledge retiring-primary fence")
            .expect("pending handoff remains exact");
        let proof = membership_handoff_proof(&handoff, 9);
        let floor_key = metadata_shard_operation_floor_key(&file_system_id, "root");
        let concurrent_floor = DfsMetadataShardOperationFloor {
            operation_digest: "blake3:concurrent-prepare".to_string(),
            prepared_sequence: 9,
            authority_epoch: current.epoch,
            permit_id: "permit-concurrent".to_string(),
        };
        let mut racing = OperationFloorRacingKv {
            inner,
            floor_key,
            replacement_floor: Some(
                sinomo_core::kv::encode_kv_value(&concurrent_floor)
                    .expect("encode concurrent floor"),
            ),
        };

        assert!(
            !DfsControlPlaneStore::new(&mut racing)
                .complete_metadata_shard_membership_handoff(&current, &desired, &proof)
                .expect("floor race aborts membership transition")
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut racing)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read unchanged shard"),
            Some(current.clone())
        );
        assert!(
            DfsControlPlaneStore::new(&mut racing)
                .complete_metadata_shard_membership_handoff(&current, &desired, &proof)
                .expect("retry observes stable floor")
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut racing)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read activated shard"),
            Some(desired)
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
        let transferred = DfsMetadataShardConfig {
            primary_node_id: replacement,
            replica_node_ids: vec![shard.primary_node_id.clone()],
            epoch: shard.epoch + 1,
            ..shard.clone()
        };
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&transferred)
            .expect("install successor fixture");

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

        let successor = DfsMetadataShardConfig {
            primary_node_id: replica_a.clone(),
            replica_node_ids: topology_preserving_metadata_shard_replicas(&shard, &replica_a)
                .expect("configured successor"),
            epoch: shard.epoch + 1,
            ..shard.clone()
        };
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&successor)
            .expect("install successor fixture");
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
