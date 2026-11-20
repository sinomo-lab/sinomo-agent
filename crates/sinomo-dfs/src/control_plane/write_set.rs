//! Shard metadata write sets.
//!
//! A shard on the write-set protocol keeps its configuration at
//! [`metadata_shard_key`] and adds one record at
//! [`metadata_shard_write_set_key`]. Every change to the write set, the
//! configured members or the primary is one guarded cluster-KV transaction that
//! compares the exact bytes of both keys, so the pair behaves as one
//! compare-and-swap register. The shard configuration keeps its schema, so a
//! node that does not implement the protocol still decodes it and routes by
//! primary and epoch; such a node never reads the write-set key.
//!
//! Every transaction carries a stable request identity. The write set's
//! generation advances with every change and the transaction compares the whole
//! record, so a transaction built from an earlier record can never apply once
//! the record has changed. A takeover swap is settled from its own outcome:
//! resubmitting the identical request returns the committed response or
//! commits it once.

use super::*;
use sinomo_core::node::DFS_METADATA_WRITE_SET_V1_CAPABILITY;

mod bounds;
mod keeping;
mod takeover;
pub use bounds::{
    DfsMetadataShardBounds, DfsMetadataWriteSetCandidate, DfsMetadataWriteSetCandidateKind,
    metadata_commit_write_set_floor,
    metadata_shard_effective_n, metadata_shard_replication_bounds, select_metadata_shard_members,
};

pub(crate) use keeping::{
    DfsWriteSetKeepingTxn, DfsWriteSetKeepingView, DfsWriteSetMemberFacts, DfsWriteSetMemberReach,
    DfsWriteSetRemoveCause, DfsWriteSetShrinkCause, metadata_write_set_reduction,
    plan_metadata_write_set_keeping,
};
pub(crate) use takeover::{
    DfsTakeoverCause, DfsTakeoverHandOffDecision, DfsTakeoverPlan, DfsTakeoverRoundEntry,
    DfsTakeoverRoundOutcome, DfsTakeoverStanding, DfsTakeoverView,
    decide_metadata_takeover_hand_off, decide_metadata_takeover_round, plan_metadata_takeover,
};

#[cfg(test)]
mod budget_native_tests;
#[cfg(test)]
mod commit_native_tests;
#[cfg(test)]
mod crossing_native_tests;
#[cfg(test)]
mod follow_native_tests;
#[cfg(test)]
mod keeping_native_tests;
#[cfg(test)]
mod native_tests;
#[cfg(test)]
mod takeover_driver_native_tests;
#[cfg(test)]
mod takeover_loss_native_tests;
#[cfg(test)]
mod takeover_native_tests;
#[cfg(test)]
mod tests;

/// Persisted format of [`DfsMetadataShardWriteSet`]. Readers reject any other
/// value, so a record written by a later protocol fails closed.
pub const DFS_METADATA_SHARD_WRITE_SET_FORMAT: u16 = 1;

/// `/dfs/filesystems/<fs>/metadata-write-sets/<shard>`.
///
/// This lives outside `metadata-shards/` so shard-prefix scans, including
/// those of nodes that predate write sets, continue to decode only shard
/// configurations.
pub fn metadata_shard_write_set_key(file_system_id: &DfsFileSystemId, shard_id: &str) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-write-sets/{}",
        normalize_segment(shard_id)
    )
}

pub fn dfs_metadata_shard_write_set_key_is_exact(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some("metadata-write-sets"),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty()
    )
}

/// The members a commit must reach, bound to the shard configuration's epoch
/// and primary. Present only for a shard on the write-set protocol.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataShardWriteSet {
    pub format: u16,
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    /// Equal to the shard configuration's epoch.
    pub epoch: u64,
    /// Equal to the shard configuration's primary.
    pub primary_node_id: NodeId,
    /// Advances with every change to either key of the shard record.
    pub generation: u64,
    /// Sorted and unique; contains the primary and only configured members.
    pub write_set: Vec<NodeId>,
    /// Journal prefix at which the current primary's epoch began. `None` when
    /// the epoch began before the shard switched to the write-set protocol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch_start: Option<crate::DfsMetadataPrefixProof>,
    /// Epoch at which the shard switched. Acknowledgement floors written
    /// without an epoch belong to it.
    pub switched_at_epoch: u64,
    /// Members recruited to replace unavailable ones, most recent last.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub substitutes: Vec<DfsMetadataShardSubstitute>,
    /// The largest content W this shard has accepted for commit. The shard's
    /// effective N never falls below it (see [`metadata_shard_effective_n`]),
    /// so metadata keeps at least as many copies as the content it committed.
    /// It only grows: the switch starts it from the content the shard already
    /// holds, every change carries it forward, and only
    /// [`build_metadata_write_set_raise_n`] raises it. `None` is W = 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raised_n: Option<u64>,
    /// The change that produced this value.
    pub last_change: DfsMetadataShardWriteSetChange,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataShardSubstitute {
    pub node_id: NodeId,
    pub recruited_at_generation: u64,
}

/// Identity of the change that produced a write-set value. `change_id` is the
/// stable request id of the guarded transaction, or the handoff id of a
/// completed cooperative handoff.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataShardWriteSetChange {
    pub kind: DfsMetadataShardWriteSetChangeKind,
    pub change_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsMetadataShardWriteSetChangeKind {
    Switch,
    Shrink { node_id: NodeId },
    Add { node_id: NodeId },
    Recruit { node_id: NodeId },
    Remove { node_ids: Vec<NodeId> },
    RaiseN { n: u64 },
    Takeover { previous_primary_node_id: NodeId },
    Handoff { handoff_id: String },
}

impl DfsMetadataShardWriteSet {
    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.write_set.binary_search(node_id).is_ok()
    }

    /// The largest content W accepted for commit, at least one.
    pub fn accepted_w(&self) -> usize {
        self.raised_n
            .map_or(1, |n| usize::try_from(n).unwrap_or(usize::MAX))
            .max(1)
    }

    /// Check the record's own shape and its binding to `shard`. A record
    /// that fails this check is not authority for anything.
    pub fn validate_against(&self, shard: &DfsMetadataShardConfig) -> Result<(), String> {
        if self.format != DFS_METADATA_SHARD_WRITE_SET_FORMAT {
            return Err(format!("write-set format {} is not supported", self.format));
        }
        if self.file_system_id != shard.file_system_id || self.shard_id != shard.shard_id {
            return Err("write set names another shard".into());
        }
        if self.epoch != shard.epoch || self.primary_node_id != shard.primary_node_id {
            return Err(format!(
                "write set is bound to primary {} epoch {}, but the shard names primary {} epoch {}",
                self.primary_node_id, self.epoch, shard.primary_node_id, shard.epoch
            ));
        }
        if self.generation == 0 || self.switched_at_epoch > self.epoch {
            return Err("write-set generation or switch epoch is out of range".into());
        }
        if self.raised_n == Some(0) {
            return Err("write-set raised N must be at least one".into());
        }
        if !self.write_set.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err("write set is not sorted and unique".into());
        }
        if !self.contains(&shard.primary_node_id) {
            return Err("write set does not contain the primary".into());
        }
        let configured = metadata_shard_configured_node_ids(shard);
        if self.write_set.iter().any(|node| !configured.contains(node)) {
            return Err("write set names a node outside the configured members".into());
        }
        let mut substitutes = BTreeSet::new();
        if self.substitutes.iter().any(|substitute| {
            substitute.node_id == shard.primary_node_id
                || !configured.contains(&substitute.node_id)
                || !substitutes.insert(substitute.node_id.clone())
                || substitute.recruited_at_generation > self.generation
        }) {
            return Err("write-set substitutes are not distinct configured replicas".into());
        }
        if self
            .epoch_start
            .as_ref()
            .is_some_and(|proof| !proof.is_well_formed())
        {
            return Err("write-set epoch start is not a well-formed prefix proof".into());
        }
        Ok(())
    }

    fn next(
        &self,
        kind: DfsMetadataShardWriteSetChangeKind,
        change_id: String,
    ) -> Result<Self, KvError> {
        let generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| KvError::Serialization {
                detail: format!(
                    "metadata shard {} write-set generation is exhausted",
                    self.shard_id
                ),
            })?;
        Ok(Self {
            generation,
            last_change: DfsMetadataShardWriteSetChange { kind, change_id },
            ..self.clone()
        })
    }
}

/// How a shard's write-set key relates to its configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsMetadataShardWriteSetState {
    /// No write-set record: the all-member rule applies.
    Unswitched,
    /// A current, well-formed write set bound to the configuration.
    Switched(DfsMetadataShardWriteSet),
    /// A decodable record no longer bound to the configuration, for example
    /// after a writer that predates write sets changed the primary. The
    /// all-member rule applies, and a switch may overwrite the record.
    Stale { reason: String },
    /// A record this node cannot read, possibly from a later protocol. The
    /// all-member rule applies and no write-set change is attempted.
    Unreadable { reason: String },
}

impl DfsMetadataShardWriteSetState {
    pub fn switched(&self) -> Option<&DfsMetadataShardWriteSet> {
        match self {
            Self::Switched(write_set) => Some(write_set),
            _ => None,
        }
    }
}

/// Classify one shard's write-set entry. A decoding failure is scoped to this
/// shard; it never invalidates other shards' routing.
pub fn metadata_shard_write_set_state(
    shard: &DfsMetadataShardConfig,
    entry: Option<&[u8]>,
) -> DfsMetadataShardWriteSetState {
    let Some(value) = entry else {
        return DfsMetadataShardWriteSetState::Unswitched;
    };
    let write_set = match sinomo_core::kv::decode_kv_value::<DfsMetadataShardWriteSet>(value) {
        Ok(write_set) => write_set,
        Err(error) => {
            return DfsMetadataShardWriteSetState::Unreadable {
                reason: error.to_string(),
            };
        }
    };
    if write_set.format != DFS_METADATA_SHARD_WRITE_SET_FORMAT {
        return DfsMetadataShardWriteSetState::Unreadable {
            reason: format!("write-set format {} is not supported", write_set.format),
        };
    }
    match write_set.validate_against(shard) {
        Ok(()) => DfsMetadataShardWriteSetState::Switched(write_set),
        Err(reason) => DfsMetadataShardWriteSetState::Stale { reason },
    }
}

/// Whether the record has moved past a pending add's cut, so the add either
/// applied (its member is then in the write set) or can never apply.
///
/// The comparison is by order, not by equality: a retained view can lag the
/// authoritative read that produced the cut, and a view equal to or older than
/// the cut must keep the member required. Generation advances with every
/// change to the record, and the epoch only grows.
pub fn metadata_pending_write_set_add_superseded(
    cut: &DfsMetadataShardRecordCut,
    shard: &DfsMetadataShardConfig,
    write_set: Option<&DfsMetadataShardWriteSet>,
) -> bool {
    if shard.epoch != cut.shard.epoch {
        return shard.epoch > cut.shard.epoch;
    }
    let Some(cut_write_set) = cut.write_set_state().switched().cloned() else {
        // An add is only ever built from a switched record.
        return true;
    };
    write_set.is_some_and(|current| current.generation > cut_write_set.generation)
}

/// Exact bytes of one member's capability evidence.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataMemberCapabilityCut {
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_value: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_value: Option<Vec<u8>>,
}

/// Exact bytes, or absence, of every key a guarded write-set or takeover
/// transaction compares. Built from one exact-key-set authoritative read.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataShardRecordCut {
    pub shard: DfsMetadataShardConfig,
    pub shard_value: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_set_value: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff_value: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark_value: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_floor_value: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_system_config_value: Option<Vec<u8>>,
    /// The shard's storage record ([`crate::DfsMetadataShardStorage`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_value: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<DfsMetadataMemberCapabilityCut>,
}

impl DfsMetadataShardRecordCut {
    /// Keys of one cut, in the order [`Self::from_entries`] expects.
    pub fn keys(
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        members: &[NodeId],
    ) -> Vec<String> {
        let mut keys = vec![
            metadata_shard_key(file_system_id, shard_id),
            metadata_shard_write_set_key(file_system_id, shard_id),
            metadata_shard_membership_handoff_key(file_system_id, shard_id),
            metadata_shard_head_watermark_key(file_system_id, shard_id),
            metadata_shard_operation_floor_key(file_system_id, shard_id),
            file_system_config_key(file_system_id),
            crate::metadata_shard_storage_key(file_system_id, shard_id),
        ];
        for node in members {
            keys.push(cluster_member_key(node));
            keys.push(cluster_capability_policy_key(node));
        }
        keys
    }

    /// Build a cut from the entries returned for [`Self::keys`]. `None` when
    /// the shard has no configuration.
    pub fn from_entries(
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        members: &[NodeId],
        entries: Vec<Option<KvEntry>>,
    ) -> Result<Option<Self>, KvError> {
        let keys = Self::keys(file_system_id, shard_id, members);
        if entries.len() != keys.len()
            || entries
                .iter()
                .zip(&keys)
                .any(|(entry, key)| entry.as_ref().is_some_and(|entry| &entry.key != key))
        {
            return Err(KvError::Serialization {
                detail: "metadata shard record read returned a differently scoped key set".into(),
            });
        }
        let mut values = entries
            .into_iter()
            .map(|entry| entry.map(|entry| entry.value));
        let mut next = || values.next().flatten();
        let Some(shard_value) = next() else {
            return Ok(None);
        };
        let shard = sinomo_core::kv::decode_kv_value::<DfsMetadataShardConfig>(&shard_value)?;
        if shard.file_system_id != *file_system_id || shard.shard_id != shard_id {
            return Err(KvError::Serialization {
                detail: format!("metadata shard key {shard_id} holds another shard"),
            });
        }
        let write_set_value = next();
        let handoff_value = next();
        let watermark_value = next();
        let operation_floor_value = next();
        let file_system_config_value = next();
        let storage_value = next();
        let members = members
            .iter()
            .map(|node_id| DfsMetadataMemberCapabilityCut {
                node_id: node_id.clone(),
                member_value: next(),
                policy_value: next(),
            })
            .collect();
        Ok(Some(Self {
            shard,
            shard_value,
            write_set_value,
            handoff_value,
            watermark_value,
            operation_floor_value,
            file_system_config_value,
            storage_value,
            members,
        }))
    }

    pub fn write_set_state(&self) -> DfsMetadataShardWriteSetState {
        metadata_shard_write_set_state(&self.shard, self.write_set_value.as_deref())
    }

    /// Whether `other` describes the same shard record: both keys equal.
    pub fn same_record(&self, other: &Self) -> bool {
        self.shard_value == other.shard_value && self.write_set_value == other.write_set_value
    }

    pub fn file_system_config(&self) -> Result<Option<DfsFileSystemConfig>, KvError> {
        self.file_system_config_value
            .as_deref()
            .map(sinomo_core::kv::decode_kv_value::<DfsFileSystemConfig>)
            .transpose()
    }

    fn key(&self, f: fn(&DfsFileSystemId, &str) -> String) -> String {
        f(&self.shard.file_system_id, &self.shard.shard_id)
    }

    fn record_compares(&self) -> Vec<KvCompare> {
        vec![
            exact_compare(self.key(metadata_shard_key), Some(&self.shard_value)),
            exact_compare(
                self.key(metadata_shard_write_set_key),
                self.write_set_value.as_ref(),
            ),
        ]
    }

    fn file_system_config_compare(&self) -> KvCompare {
        exact_compare(
            file_system_config_key(&self.shard.file_system_id),
            self.file_system_config_value.as_ref(),
        )
    }

    fn member(&self, node_id: &NodeId) -> Result<&DfsMetadataMemberCapabilityCut, KvError> {
        self.members
            .iter()
            .find(|member| &member.node_id == node_id)
            .ok_or_else(|| KvError::Serialization {
                detail: format!("metadata shard record read did not include member {node_id}"),
            })
    }
}

fn exact_compare(key: String, value: Option<&Vec<u8>>) -> KvCompare {
    match value {
        Some(value) => KvCompare::ValueEquals {
            key,
            value: value.clone(),
        },
        None => KvCompare::Missing { key },
    }
}

/// Whether one member may serve on a write-set shard, and the compares that
/// bind that evidence into a transaction.
///
/// The capability states what code a node runs, so it must appear in the
/// member's own advertised list. A capability policy record may restrict that
/// list but cannot add to it.
pub fn metadata_write_set_member_capability_compares(
    member: &DfsMetadataMemberCapabilityCut,
    cluster_id: &mut Option<sinomo_core::Id>,
) -> Result<Vec<KvCompare>, KvError> {
    let refuse = |reason: &str| KvError::Serialization {
        detail: format!(
            "metadata write set cannot admit {}: {reason}",
            member.node_id
        ),
    };
    let member_value = member
        .member_value
        .as_ref()
        .ok_or_else(|| refuse("no cluster member record"))?;
    let record = sinomo_core::kv::decode_kv_value::<ClusterMemberRecord>(member_value)?;
    if record.node_id != member.node_id || !record.state.is_authorized() {
        return Err(refuse("not an authorized cluster member"));
    }
    let has = |capabilities: &[sinomo_core::CapabilityName], wanted: &str| {
        capabilities.iter().any(|capability| capability.as_str() == wanted)
    };
    let advertises =
        |capabilities: &[sinomo_core::CapabilityName]| has(capabilities, DFS_METADATA_WRITE_SET_V1_CAPABILITY);
    if !advertises(&record.capabilities) {
        return Err(refuse(
            "the member does not advertise the write-set capability",
        ));
    }
    // Every member of a shard holds its storage under the per-shard storage
    // record, so a member that cannot read that record is never admitted.
    let storage = sinomo_core::node::DFS_METADATA_STORAGE_GENERATION_V1_CAPABILITY;
    if !has(&record.capabilities, storage) {
        return Err(refuse("the member does not advertise the storage generation capability"));
    }
    if let Some(policy_value) = member.policy_value.as_ref() {
        let policy =
            sinomo_core::kv::decode_kv_value::<ClusterCapabilityPolicyRecord>(policy_value)?;
        if policy.node_id != member.node_id || policy.cluster_id != record.cluster_id {
            return Err(refuse("the capability policy is inconsistent"));
        }
        if !advertises(&policy.capabilities) {
            return Err(refuse(
                "the capability policy does not allow the write-set capability",
            ));
        }
        if !has(&policy.capabilities, storage) {
            return Err(refuse("the capability policy does not allow the storage generation capability"));
        }
    }
    if cluster_id
        .as_ref()
        .is_some_and(|cluster_id| *cluster_id != record.cluster_id)
    {
        return Err(refuse("member records name different clusters"));
    }
    *cluster_id = Some(record.cluster_id.clone());
    Ok(vec![
        exact_compare(cluster_member_key(&member.node_id), Some(member_value)),
        exact_compare(
            cluster_capability_policy_key(&member.node_id),
            member.policy_value.as_ref(),
        ),
    ])
}

/// One guarded transaction with its stable request identity and the record
/// it writes when it applies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataWriteSetChangeRequest {
    pub request: KvTxnRequest,
    pub write_set: DfsMetadataShardWriteSet,
    /// The new shard configuration when the change rewrites it.
    pub shard: Option<DfsMetadataShardConfig>,
}

fn refuse(cut: &DfsMetadataShardRecordCut, reason: impl std::fmt::Display) -> KvError {
    KvError::Serialization {
        detail: format!(
            "metadata shard {} epoch {} write-set change refused: {reason}",
            cut.shard.shard_id, cut.shard.epoch
        ),
    }
}

fn switched_write_set(
    cut: &DfsMetadataShardRecordCut,
) -> Result<DfsMetadataShardWriteSet, KvError> {
    match cut.write_set_state() {
        DfsMetadataShardWriteSetState::Switched(write_set) => Ok(write_set),
        DfsMetadataShardWriteSetState::Unswitched => {
            Err(refuse(cut, "the shard is not on the write-set protocol"))
        }
        DfsMetadataShardWriteSetState::Stale { reason }
        | DfsMetadataShardWriteSetState::Unreadable { reason } => Err(refuse(
            cut,
            format!("the write-set record is not current: {reason}"),
        )),
    }
}

/// Whether `cut` admits a write-set change at all: its filesystem
/// configuration is captured and belongs to the shard's filesystem. Every
/// builder applies it. The catalog generation is not read: it guards path
/// assignment, which no write-set change alters.
fn write_set_change_admitted(cut: &DfsMetadataShardRecordCut) -> Result<(), KvError> {
    let config = cut.file_system_config()?.ok_or_else(||
        refuse(cut, "write-set change requires the captured filesystem configuration"))?;
    if config.file_system_id != cut.shard.file_system_id {
        return Err(refuse(cut, "write-set change has a foreign filesystem configuration"));
    }
    Ok(())
}

fn change_request(
    cut: &DfsMetadataShardRecordCut,
    request_id: KvRequestId,
    mut compares: Vec<KvCompare>,
    write_set: DfsMetadataShardWriteSet,
    shard: Option<DfsMetadataShardConfig>,
    extra_success: Vec<KvOp>,
) -> Result<DfsMetadataWriteSetChangeRequest, KvError> {
    write_set_change_admitted(cut)?;
    if let Some(shard) = shard.as_ref() {
        write_set
            .validate_against(shard)
            .map_err(|reason| refuse(cut, reason))?;
    } else {
        write_set
            .validate_against(&cut.shard)
            .map_err(|reason| refuse(cut, reason))?;
    }
    let mut all = cut.record_compares();
    all.append(&mut compares);
    let mut success = vec![KvOp::put_serde(
        cut.key(metadata_shard_write_set_key),
        &write_set,
        None,
    )?];
    if let Some(shard) = shard.as_ref() {
        success.push(KvOp::put_serde(cut.key(metadata_shard_key), shard, None)?);
    }
    success.extend(extra_success);
    Ok(DfsMetadataWriteSetChangeRequest {
        request: KvTxnRequest::new(
            request_id,
            KvTxn {
                compares: all,
                success,
                failure: Vec::new(),
            },
        ),
        write_set,
        shard,
    })
}

fn handoff_missing(cut: &DfsMetadataShardRecordCut) -> Result<KvCompare, KvError> {
    if cut.handoff_value.is_some() {
        return Err(refuse(cut, "a membership handoff is pending"));
    }
    Ok(KvCompare::Missing {
        key: cut.key(metadata_shard_membership_handoff_key),
    })
}

fn change_id(request_id: &KvRequestId) -> String {
    request_id.as_id().to_string()
}

/// Switch an unswitched shard to the write-set protocol.
///
/// `initial_write_set` must hold only members with evidence that they hold
/// the primary's whole journal at the current epoch. Until the primary observes
/// the switch applied it keeps committing under the all-member rule, which is a
/// superset, so no pending requirement is needed.
pub fn build_metadata_write_set_switch(
    cut: &DfsMetadataShardRecordCut,
    initial_write_set: &BTreeSet<NodeId>,
) -> Result<DfsMetadataWriteSetChangeRequest, KvError> {
    build_metadata_write_set_switch_with_raised_n(cut, initial_write_set, None)
}

/// [`build_metadata_write_set_switch`] with the record's initial raised N:
/// the largest content W among the versions the shard already holds, since
/// that content was committed under the all-member rule and never compared
/// with the shard's N.
pub fn build_metadata_write_set_switch_with_raised_n(
    cut: &DfsMetadataShardRecordCut,
    initial_write_set: &BTreeSet<NodeId>,
    raised_n: Option<u64>,
) -> Result<DfsMetadataWriteSetChangeRequest, KvError> {
    if raised_n == Some(0) {
        return Err(refuse(cut, "the initial raised N must be at least one"));
    }
    let previous_generation = match cut.write_set_state() {
        DfsMetadataShardWriteSetState::Unswitched => 0,
        DfsMetadataShardWriteSetState::Stale { .. } => cut
            .write_set_value
            .as_deref()
            .and_then(|value| {
                sinomo_core::kv::decode_kv_value::<DfsMetadataShardWriteSet>(value).ok()
            })
            .map_or(0, |write_set| write_set.generation),
        DfsMetadataShardWriteSetState::Switched(_) => {
            return Err(refuse(
                cut,
                "the shard is already on the write-set protocol",
            ));
        }
        DfsMetadataShardWriteSetState::Unreadable { reason } => {
            return Err(refuse(
                cut,
                format!("the write-set record is unreadable: {reason}"),
            ));
        }
    };
    if cut.operation_floor_value.is_some() {
        return Err(refuse(
            cut,
            "a historical operation floor is still in force",
        ));
    }
    let configured = metadata_shard_configured_node_ids(&cut.shard);
    if !initial_write_set.contains(&cut.shard.primary_node_id)
        || initial_write_set
            .iter()
            .any(|node| !configured.contains(node))
    {
        return Err(refuse(
            cut,
            "the initial write set must be configured members including the primary",
        ));
    }
    // A shard switches only while its storage record is Active: a converting
    // shard is closed, and its conversion's install rule must not change
    // under it, and a shard with no record holds storage that migration has
    // yet to convert. The exact record is compared, so a conversion that
    // starts after the read cannot meet a switch that commits after it.
    let Some(storage_value) = cut.storage_value.as_ref() else {
        return Err(refuse(cut, "the shard has no storage record"));
    };
    let record = sinomo_core::kv::decode_kv_value::<crate::DfsMetadataShardStorage>(storage_value)?;
    if !record.is_active() {
        return Err(refuse(cut, "the shard's storage is converting"));
    }
    let mut compares = vec![
        handoff_missing(cut)?,
        KvCompare::Missing {
            key: cut.key(metadata_shard_operation_floor_key),
        },
        exact_compare(cut.key(crate::metadata_shard_storage_key), Some(storage_value)),
    ];
    let mut cluster_id = None;
    for node in &configured {
        compares.extend(metadata_write_set_member_capability_compares(
            cut.member(node)?,
            &mut cluster_id,
        )?);
    }
    let request_id = KvRequestId::generate();
    let generation = previous_generation
        .checked_add(1)
        .ok_or_else(|| refuse(cut, "generation exhausted"))?;
    let write_set = DfsMetadataShardWriteSet {
        format: DFS_METADATA_SHARD_WRITE_SET_FORMAT,
        file_system_id: cut.shard.file_system_id.clone(),
        shard_id: cut.shard.shard_id.clone(),
        epoch: cut.shard.epoch,
        primary_node_id: cut.shard.primary_node_id.clone(),
        generation,
        write_set: initial_write_set.iter().cloned().collect(),
        epoch_start: None,
        switched_at_epoch: cut.shard.epoch,
        substitutes: Vec::new(),
        raised_n,
        last_change: DfsMetadataShardWriteSetChange {
            kind: DfsMetadataShardWriteSetChangeKind::Switch,
            change_id: change_id(&request_id),
        },
    };
    change_request(cut, request_id, compares, write_set, None, Vec::new())
}

/// Remove `node_id` from the write set, never below the W floor.
pub fn build_metadata_write_set_shrink(
    cut: &DfsMetadataShardRecordCut,
    node_id: &NodeId,
    bounds: &DfsMetadataShardBounds,
) -> Result<DfsMetadataWriteSetChangeRequest, KvError> {
    let current = switched_write_set(cut)?;
    if node_id == &cut.shard.primary_node_id || !current.contains(node_id) {
        return Err(refuse(
            cut,
            format!("{node_id} is not a removable write-set member"),
        ));
    }
    if current.write_set.len().saturating_sub(1) < bounds.w_floor {
        return Err(refuse(
            cut,
            format!(
                "shrinking would leave the write set below the W floor {}",
                bounds.w_floor
            ),
        ));
    }
    let request_id = KvRequestId::generate();
    let mut next = current.next(
        DfsMetadataShardWriteSetChangeKind::Shrink {
            node_id: node_id.clone(),
        },
        change_id(&request_id),
    )?;
    next.write_set.retain(|member| member != node_id);
    // The W floor comes from the placement rules in the filesystem
    // configuration.
    let compares = vec![handoff_missing(cut)?, cut.file_system_config_compare()];
    change_request(cut, request_id, compares, next, None, Vec::new())
}

/// Add a caught-up configured member to the write set. The caller must already
/// hold the member's whole-journal evidence at this record's epoch and
/// generation, and must have durably recorded the pending add before it
/// submits this request.
pub fn build_metadata_write_set_add(
    cut: &DfsMetadataShardRecordCut,
    node_id: &NodeId,
) -> Result<DfsMetadataWriteSetChangeRequest, KvError> {
    let current = switched_write_set(cut)?;
    if current.contains(node_id)
        || !metadata_shard_configured_node_ids(&cut.shard).contains(node_id)
    {
        return Err(refuse(
            cut,
            format!("{node_id} is not a configured member outside the write set"),
        ));
    }
    let mut compares = vec![handoff_missing(cut)?];
    compares.extend(metadata_write_set_member_capability_compares(
        cut.member(node_id)?,
        &mut None,
    )?);
    let request_id = KvRequestId::generate();
    let mut next = current.next(
        DfsMetadataShardWriteSetChangeKind::Add {
            node_id: node_id.clone(),
        },
        change_id(&request_id),
    )?;
    next.write_set.push(node_id.clone());
    next.write_set.sort();
    change_request(cut, request_id, compares, next, None, Vec::new())
}

/// Why a recruit is admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DfsMetadataShardRecruit {
    /// The write set has fewer than N members.
    BelowN,
    /// Every configured member is in the write set and there are at most N of
    /// them. One member beyond N then replaces a dead member held at the W
    /// floor, which cannot leave the write set, or a mobile member; the
    /// surplus is retired once the replacement has joined. While the recruit
    /// is outside the write set the condition fails, so at most one member
    /// beyond N is ever added this way.
    Replacement,
}

/// Add an eligible member to the configured replicas: while the write set has
/// fewer than N members, or as a replacement (see [`DfsMetadataShardRecruit`]).
/// It joins the write set later through an add.
pub fn build_metadata_write_set_recruit(
    cut: &DfsMetadataShardRecordCut,
    node_id: &NodeId,
    bounds: &DfsMetadataShardBounds,
    admission: DfsMetadataShardRecruit,
) -> Result<DfsMetadataWriteSetChangeRequest, KvError> {
    let current = switched_write_set(cut)?;
    let configured = metadata_shard_configured_node_ids(&cut.shard);
    if configured.contains(node_id) {
        return Err(refuse(
            cut,
            format!("{node_id} is already a configured member"),
        ));
    }
    let n = metadata_shard_effective_n(bounds, Some(&current));
    match admission {
        DfsMetadataShardRecruit::BelowN if current.write_set.len() >= n => {
            return Err(refuse(
                cut,
                format!("the write set already has N = {n} members"),
            ));
        }
        DfsMetadataShardRecruit::Replacement
            if configured.len() > n
                || configured.iter().any(|member| !current.contains(member)) =>
        {
            return Err(refuse(
                cut,
                format!(
                    "a replacement recruit needs every configured member in the write set and at most N = {n} of them"
                ),
            ));
        }
        _ => {}
    }
    // N comes from the placement rules in the filesystem configuration.
    let mut compares = vec![handoff_missing(cut)?, cut.file_system_config_compare()];
    compares.extend(metadata_write_set_member_capability_compares(
        cut.member(node_id)?,
        &mut None,
    )?);
    let request_id = KvRequestId::generate();
    let mut next = current.next(
        DfsMetadataShardWriteSetChangeKind::Recruit {
            node_id: node_id.clone(),
        },
        change_id(&request_id),
    )?;
    next.substitutes.push(DfsMetadataShardSubstitute {
        node_id: node_id.clone(),
        recruited_at_generation: next.generation,
    });
    let mut shard = cut.shard.clone();
    shard.replica_node_ids.push(node_id.clone());
    change_membership(cut, request_id, compares, next, shard)
}

/// Raise the shard's accepted content W to `w`, so its effective N is at
/// least `w`: a commit whose content W exceeds the record's waits for this.
/// The value only grows; a request not above the current one is refused.
pub fn build_metadata_write_set_raise_n(
    cut: &DfsMetadataShardRecordCut,
    w: usize,
) -> Result<DfsMetadataWriteSetChangeRequest, KvError> {
    let current = switched_write_set(cut)?;
    if w <= current.accepted_w() {
        return Err(refuse(
            cut,
            format!(
                "the shard already accepts content W {}, not below {w}",
                current.accepted_w()
            ),
        ));
    }
    let n = u64::try_from(w).map_err(|_| refuse(cut, "content W is out of range"))?;
    let request_id = KvRequestId::generate();
    let mut next = current.next(
        DfsMetadataShardWriteSetChangeKind::RaiseN { n },
        change_id(&request_id),
    )?;
    next.raised_n = Some(n);
    let compares = vec![handoff_missing(cut)?];
    change_request(cut, request_id, compares, next, None, Vec::new())
}

/// Remove `node_ids` from the configured members, and from the write set when
/// they are in it. `retain_at_least` is the caller's lower bound on the
/// configured members that remain; the W floor always applies to the write
/// set. `retire_only` refuses members still in the write set, which is the
/// model's `retire`. A removed member deletes its copy through the ordinary
/// placement-removal path.
pub fn build_metadata_write_set_remove(
    cut: &DfsMetadataShardRecordCut,
    node_ids: &BTreeSet<NodeId>,
    bounds: &DfsMetadataShardBounds,
    retain_at_least: usize,
    retire_only: bool,
) -> Result<DfsMetadataWriteSetChangeRequest, KvError> {
    let current = switched_write_set(cut)?;
    let configured = metadata_shard_configured_node_ids(&cut.shard);
    if node_ids.is_empty()
        || node_ids.contains(&cut.shard.primary_node_id)
        || node_ids.iter().any(|node| !configured.contains(node))
    {
        return Err(refuse(
            cut,
            "only configured replicas other than the primary can be removed",
        ));
    }
    if retire_only && node_ids.iter().any(|node| current.contains(node)) {
        return Err(refuse(
            cut,
            "a retired member must already be outside the write set",
        ));
    }
    let remaining_write_set = current
        .write_set
        .iter()
        .filter(|node| !node_ids.contains(*node))
        .count();
    if remaining_write_set < bounds.w_floor {
        return Err(refuse(
            cut,
            format!(
                "removal would leave the write set below the W floor {}",
                bounds.w_floor
            ),
        ));
    }
    if configured.len() - node_ids.len() < retain_at_least.max(1) {
        return Err(refuse(
            cut,
            format!("removal would leave fewer than {retain_at_least} configured members"),
        ));
    }
    let request_id = KvRequestId::generate();
    let mut next = current.next(
        DfsMetadataShardWriteSetChangeKind::Remove {
            node_ids: node_ids.iter().cloned().collect(),
        },
        change_id(&request_id),
    )?;
    next.write_set.retain(|node| !node_ids.contains(node));
    next.substitutes
        .retain(|substitute| !node_ids.contains(&substitute.node_id));
    let mut shard = cut.shard.clone();
    shard
        .replica_node_ids
        .retain(|node| !node_ids.contains(node));
    // The W floor comes from the placement rules in the filesystem
    // configuration.
    let compares = vec![handoff_missing(cut)?, cut.file_system_config_compare()];
    change_membership(cut, request_id, compares, next, shard)
}

fn change_membership(
    cut: &DfsMetadataShardRecordCut,
    request_id: KvRequestId,
    mut compares: Vec<KvCompare>,
    next: DfsMetadataShardWriteSet,
    shard: DfsMetadataShardConfig,
) -> Result<DfsMetadataWriteSetChangeRequest, KvError> {
    // Membership changes within this epoch. An old head watermark can name a
    // removed member or have fewer ACKs than the expanded ring's quorum.
    // Retire it only in the CAS that installs the new configuration.
    compares.push(exact_compare(
        cut.key(metadata_shard_head_watermark_key),
        cut.watermark_value.as_ref(),
    ));
    let extra = cut
        .watermark_value
        .as_ref()
        .map(|_| KvOp::Delete {
            key: cut.key(metadata_shard_head_watermark_key),
        })
        .into_iter()
        .collect();
    change_request(cut, request_id, compares, next, Some(shard), extra)
}

/// The parts of a takeover swap that depend only on the record cut and the
/// candidate, with every refusal they can raise.
struct TakeoverSwapInputs {
    current: DfsMetadataShardWriteSet,
    shard: DfsMetadataShardConfig,
    compares: Vec<KvCompare>,
    superseded: Vec<KvOp>,
}

fn takeover_swap_inputs(
    cut: &DfsMetadataShardRecordCut,
    candidate: &NodeId,
) -> Result<TakeoverSwapInputs, KvError> {
    let current = switched_write_set(cut)?;
    if candidate == &cut.shard.primary_node_id || !current.contains(candidate) {
        return Err(refuse(
            cut,
            format!("{candidate} is not a write-set member other than the primary"),
        ));
    }
    let shard = metadata_shard_primary_handoff_target(&cut.shard, candidate)?;
    let (mut compares, superseded) = metadata_takeover_superseded_keys(
        &cut.shard,
        cut.handoff_value.as_ref(),
        cut.watermark_value.as_ref(),
    );
    compares.extend(metadata_write_set_member_capability_compares(
        cut.member(candidate)?,
        &mut None,
    )?);
    write_set_change_admitted(cut)?;
    Ok(TakeoverSwapInputs {
        current,
        shard,
        compares,
        superseded,
    })
}

/// The pending handoff and head watermark a takeover supersedes: compares of
/// both keys' exact bytes or absence, and a delete of each one present. A
/// takeover swap and a converting takeover's activation both use these.
pub(crate) fn metadata_takeover_superseded_keys(
    shard: &DfsMetadataShardConfig,
    handoff_value: Option<&Vec<u8>>,
    watermark_value: Option<&Vec<u8>>,
) -> (Vec<KvCompare>, Vec<KvOp>) {
    let keys = [
        (
            metadata_shard_membership_handoff_key(&shard.file_system_id, &shard.shard_id),
            handoff_value,
        ),
        (
            metadata_shard_head_watermark_key(&shard.file_system_id, &shard.shard_id),
            watermark_value,
        ),
    ];
    let compares = keys
        .iter()
        .map(|(key, value)| exact_compare(key.clone(), *value))
        .collect();
    let deletes = keys
        .into_iter()
        .filter(|(_, value)| value.is_some())
        .map(|(key, _)| KvOp::Delete { key })
        .collect();
    (compares, deletes)
}

/// Write-set value after a takeover: `next`'s primary at `next`'s epoch, the
/// old primary out of the write set, and the epoch starting at `epoch_start`.
/// The change is named by the takeover's request. A takeover swap and a
/// converting takeover's activation both use this.
pub(crate) fn metadata_write_set_after_takeover(
    current: &DfsMetadataShardWriteSet,
    previous_primary: &NodeId,
    next: &DfsMetadataShardConfig,
    epoch_start: &crate::DfsMetadataPrefixProof,
    request_id: &KvRequestId,
) -> Result<DfsMetadataShardWriteSet, KvError> {
    let mut write_set = current.next(
        DfsMetadataShardWriteSetChangeKind::Takeover {
            previous_primary_node_id: previous_primary.clone(),
        },
        change_id(request_id),
    )?;
    write_set.epoch = next.epoch;
    write_set.primary_node_id = next.primary_node_id.clone();
    write_set.substitutes.retain(|member| member.node_id != next.primary_node_id);
    write_set.write_set.retain(|node| node != previous_primary);
    write_set.epoch_start = Some(epoch_start.clone());
    Ok(write_set)
}

/// Whether a takeover swap can be built from `cut` for `candidate`: every
/// refusal [`build_metadata_takeover_swap`] can raise except a malformed
/// epoch start, which the promise itself produces. A candidate checks this
/// before it promises, because its swap is built from the same cut only after
/// the promise, and a promise whose swap can never be submitted could never
/// settle.
pub fn check_metadata_takeover_swap(
    cut: &DfsMetadataShardRecordCut,
    candidate: &NodeId,
) -> Result<(), KvError> {
    takeover_swap_inputs(cut, candidate).map(|_| ())
}

/// The takeover swap: `candidate` becomes primary at the next epoch and the
/// old primary leaves the write set, if and only if the whole record still
/// equals the candidate's cut. The request is a pure function of its inputs,
/// so the candidate can resubmit the identical request to settle an unknown
/// outcome. It compares no filesystem configuration: a sibling shard's swap
/// or keeper change never rejects it.
pub fn build_metadata_takeover_swap(
    cut: &DfsMetadataShardRecordCut,
    candidate: &NodeId,
    epoch_start: &crate::DfsMetadataPrefixProof,
    request_id: &KvRequestId,
) -> Result<DfsMetadataWriteSetChangeRequest, KvError> {
    let TakeoverSwapInputs {
        current,
        shard,
        compares,
        superseded,
    } = takeover_swap_inputs(cut, candidate)?;
    if !epoch_start.is_well_formed() {
        return Err(refuse(
            cut,
            "the epoch start is not a well-formed prefix proof",
        ));
    }
    let next = metadata_write_set_after_takeover(
        &current,
        &cut.shard.primary_node_id,
        &shard,
        epoch_start,
        request_id,
    )?;
    change_request(cut, request_id.clone(), compares, next, Some(shard), superseded)
}

/// Write-set value after a cooperative handoff completes on a switched shard:
/// the new primary joins the write set, which the old primary stays in, and
/// the epoch starts at the handoff's exact frozen prefix.
pub(crate) fn metadata_write_set_after_handoff(
    current: &DfsMetadataShardWriteSet,
    desired: &DfsMetadataShardConfig,
    proof: &DfsMetadataShardMembershipHandoffProof,
) -> Result<DfsMetadataShardWriteSet, KvError> {
    let mut next = current.next(
        DfsMetadataShardWriteSetChangeKind::Handoff {
            handoff_id: proof.handoff_id.clone(),
        },
        proof.handoff_id.clone(),
    )?;
    next.epoch = desired.epoch;
    next.primary_node_id = desired.primary_node_id.clone();
    // Recruitment history ranks substitute replicas. Promotion ends that
    // role even though this member remains in the write set.
    next.substitutes.retain(|member| member.node_id != desired.primary_node_id);
    if !next.contains(&desired.primary_node_id) {
        next.write_set.push(desired.primary_node_id.clone());
        next.write_set.sort();
    }
    next.epoch_start = crate::DfsMetadataPrefixProof::from_digest(
        proof.journal_sequence,
        proof.state_fingerprint.clone(),
    );
    next.validate_against(desired)
        .map_err(|reason| KvError::Serialization {
            detail: format!(
                "metadata shard {} handoff write set: {reason}",
                desired.shard_id
            ),
        })?;
    Ok(next)
}

/// A switched shard admits a cooperative handoff only as a primary rotation
/// over the same ring. Replica changes use recruit and remove.
fn metadata_write_set_admits_handoff(
    write_set_value: Option<&[u8]>,
    retiring: &DfsMetadataShardConfig,
    desired: &DfsMetadataShardConfig,
) -> Result<(), KvError> {
    match metadata_shard_write_set_state(retiring, write_set_value) {
        DfsMetadataShardWriteSetState::Unswitched | DfsMetadataShardWriteSetState::Stale { .. } => {
            Ok(())
        }
        DfsMetadataShardWriteSetState::Unreadable { reason } => Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} write-set record is unreadable: {reason}",
                retiring.shard_id
            ),
        }),
        DfsMetadataShardWriteSetState::Switched(_) => {
            let rotation = retiring.primary_node_id != desired.primary_node_id
                && metadata_shard_configured_node_ids(retiring)
                    == metadata_shard_configured_node_ids(desired);
            if rotation {
                Ok(())
            } else {
                Err(KvError::Serialization {
                    detail: format!(
                        "metadata shard {} is on the write-set protocol; it admits only a primary rotation handoff, and replica changes use recruit and remove",
                        retiring.shard_id
                    ),
                })
            }
        }
    }
}

/// Definite or unknown result of one submitted write-set transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsMetadataWriteSetTxnOutcome {
    Applied,
    /// Committed with failed compares: this request can never apply.
    Rejected,
    /// The reply was lost or the request could not be confirmed.
    Unknown {
        detail: String,
    },
}

enum WriteSetCommit {
    Applied,
    Rejected,
    /// The id can never commit again; whether it applied earlier is on the
    /// record.
    Expired,
    Unknown(String),
}

fn classify_write_set_commit(result: Result<KvTxnResponse, KvError>) -> WriteSetCommit {
    match result {
        Ok(response) if response.succeeded => WriteSetCommit::Applied,
        Ok(_) => WriteSetCommit::Rejected,
        Err(KvError::RequestExpired { .. }) => WriteSetCommit::Expired,
        Err(error) => WriteSetCommit::Unknown(error.to_string()),
    }
}

impl From<WriteSetCommit> for DfsMetadataWriteSetTxnOutcome {
    fn from(commit: WriteSetCommit) -> Self {
        match commit {
            WriteSetCommit::Applied => Self::Applied,
            WriteSetCommit::Rejected => Self::Rejected,
            WriteSetCommit::Expired => Self::Unknown {
                detail: "the request expired; the record decides".into(),
            },
            WriteSetCommit::Unknown(detail) => Self::Unknown { detail },
        }
    }
}

/// Settled result of a takeover swap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsMetadataTakeoverSettlement {
    /// The swap applied; the candidate is primary at the next epoch.
    Applied,
    /// The swap can no longer apply and the read epoch is still current: the
    /// candidate releases its promise.
    RejectedEpochUnchanged,
    /// The swap can no longer apply and the epoch advanced: the candidate
    /// drops its promise and follows the record.
    RejectedEpochAdvanced,
    /// Still unknown; the candidate keeps its promise.
    Unknown { detail: String },
}

fn takeover_settlement_from_record(
    request_id: &KvRequestId,
    cut: &DfsMetadataShardRecordCut,
    current_shard: Option<&DfsMetadataShardConfig>,
    current_write_set: Option<&[u8]>,
) -> DfsMetadataTakeoverSettlement {
    let applied_by_us = current_write_set
        .and_then(|value| sinomo_core::kv::decode_kv_value::<DfsMetadataShardWriteSet>(value).ok())
        .is_some_and(|write_set| {
            write_set.last_change.change_id == change_id(request_id)
                && matches!(
                    write_set.last_change.kind,
                    DfsMetadataShardWriteSetChangeKind::Takeover { .. }
                )
        });
    if applied_by_us {
        return DfsMetadataTakeoverSettlement::Applied;
    }
    match current_shard {
        Some(shard) if shard.epoch == cut.shard.epoch => {
            DfsMetadataTakeoverSettlement::RejectedEpochUnchanged
        }
        _ => DfsMetadataTakeoverSettlement::RejectedEpochAdvanced,
    }
}

impl DfsControlPlaneStore<'_> {
    /// Read the exact record cut one guarded write-set or takeover transaction
    /// compares, through one authoritative exact-key-set read.
    /// sinomo:kv-consistency: Read the exact shard record and member evidence that one globally ordered write-set or takeover transaction compares.
    pub fn read_metadata_shard_record_cut(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        members: &[NodeId],
    ) -> Result<Option<DfsMetadataShardRecordCut>, KvError> {
        let keys = DfsMetadataShardRecordCut::keys(file_system_id, shard_id, members);
        let entries = self.kv.get_authoritative_consistent(&keys)?;
        DfsMetadataShardRecordCut::from_entries(file_system_id, shard_id, members, entries)
    }

    /// Submit one guarded write-set transaction with its stable request id.
    /// sinomo:kv-consistency: Order one write-set, membership or primary change globally against the exact shard record it was built from.
    pub fn submit_metadata_write_set_change(
        &mut self,
        change: &DfsMetadataWriteSetChangeRequest,
    ) -> DfsMetadataWriteSetTxnOutcome {
        classify_write_set_commit(self.kv.txn_request(change.request.clone())).into()
    }

    /// Settle a takeover swap from its own outcome by resubmitting the
    /// identical request. An expired request can no longer apply, so the
    /// record then decides.
    /// sinomo:kv-consistency: Settle an unknown takeover outcome from the exact request's own committed outcome before the candidate acts.
    pub fn settle_metadata_takeover_swap(
        &mut self,
        cut: &DfsMetadataShardRecordCut,
        candidate: &NodeId,
        epoch_start: &crate::DfsMetadataPrefixProof,
        request_id: &KvRequestId,
    ) -> Result<DfsMetadataTakeoverSettlement, KvError> {
        let change = build_metadata_takeover_swap(cut, candidate, epoch_start, request_id)?;
        match classify_write_set_commit(self.kv.txn_request(change.request)) {
            WriteSetCommit::Applied => Ok(DfsMetadataTakeoverSettlement::Applied),
            WriteSetCommit::Unknown(detail) => {
                Ok(DfsMetadataTakeoverSettlement::Unknown { detail })
            }
            WriteSetCommit::Rejected | WriteSetCommit::Expired => {
                let keys = [
                    cut.key(metadata_shard_key),
                    cut.key(metadata_shard_write_set_key),
                ];
                let entries = self.kv.get_authoritative_consistent(&keys)?;
                let shard = entries[0]
                    .as_ref()
                    .map(|entry| entry.decode::<DfsMetadataShardConfig>())
                    .transpose()?;
                Ok(takeover_settlement_from_record(
                    request_id,
                    cut,
                    shard.as_ref(),
                    entries[1].as_ref().map(|entry| entry.value.as_slice()),
                ))
            }
        }
    }
}

impl DfsControlPlaneAsyncStore<'_> {
    /// Async counterpart of [`DfsControlPlaneStore::read_metadata_shard_record_cut`].
    /// sinomo:kv-consistency: Read the exact shard record and member evidence that one globally ordered write-set or takeover transaction compares.
    pub async fn read_metadata_shard_record_cut(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        members: &[NodeId],
    ) -> Result<Option<DfsMetadataShardRecordCut>, KvError> {
        let keys = DfsMetadataShardRecordCut::keys(file_system_id, shard_id, members);
        let entries = self.kv.get_authoritative_consistent_async(keys).await?;
        DfsMetadataShardRecordCut::from_entries(file_system_id, shard_id, members, entries)
    }

    /// Async counterpart of [`DfsControlPlaneStore::submit_metadata_write_set_change`].
    /// sinomo:kv-consistency: Order one write-set, membership or primary change globally against the exact shard record it was built from.
    pub async fn submit_metadata_write_set_change(
        &mut self,
        change: &DfsMetadataWriteSetChangeRequest,
    ) -> DfsMetadataWriteSetTxnOutcome {
        classify_write_set_commit(self.kv.txn_request_async(change.request.clone()).await).into()
    }
}

/// Typed events for write-set changes. Each carries the shard, epoch,
/// generation, member, request identity and cause; the change's caller emits
/// it once the outcome is known.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataWriteSetEvent {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    pub epoch: u64,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub node_ids: Vec<NodeId>,
    pub change_id: String,
    pub cause: String,
}

impl DfsMetadataWriteSetEvent {
    pub const SWITCHED: &'static str = "dfs.metadata.write_set.switched";
    pub const SHRUNK: &'static str = "dfs.metadata.write_set.shrunk";
    pub const ADD_SUBMITTED: &'static str = "dfs.metadata.write_set.add_submitted";
    pub const ADDED: &'static str = "dfs.metadata.write_set.added";
    pub const RECRUITED: &'static str = "dfs.metadata.write_set.recruited";
    pub const RETIRED: &'static str = "dfs.metadata.write_set.retired";
    pub const N_RAISED: &'static str = "dfs.metadata.write_set.n_raised";
    pub const PROMISED: &'static str = "dfs.metadata.takeover.promised";
    pub const SWAPPED: &'static str = "dfs.metadata.takeover.swapped";
    pub const PROMISE_RELEASED: &'static str = "dfs.metadata.takeover.promise_released";
    pub const HANDED_OFF: &'static str = "dfs.metadata.write_set.handed_off";

    /// The event describing an applied change, named by its kind.
    pub fn applied(
        write_set: &DfsMetadataShardWriteSet,
        cause: impl Into<String>,
    ) -> (&'static str, Self) {
        let (name, node_ids) = match &write_set.last_change.kind {
            DfsMetadataShardWriteSetChangeKind::Switch => {
                (Self::SWITCHED, write_set.write_set.clone())
            }
            DfsMetadataShardWriteSetChangeKind::Shrink { node_id } => {
                (Self::SHRUNK, vec![node_id.clone()])
            }
            DfsMetadataShardWriteSetChangeKind::Add { node_id } => {
                (Self::ADDED, vec![node_id.clone()])
            }
            DfsMetadataShardWriteSetChangeKind::Recruit { node_id } => {
                (Self::RECRUITED, vec![node_id.clone()])
            }
            DfsMetadataShardWriteSetChangeKind::Remove { node_ids } => {
                (Self::RETIRED, node_ids.clone())
            }
            DfsMetadataShardWriteSetChangeKind::RaiseN { .. } => (Self::N_RAISED, Vec::new()),
            DfsMetadataShardWriteSetChangeKind::Takeover {
                previous_primary_node_id,
            } => (
                Self::SWAPPED,
                vec![
                    write_set.primary_node_id.clone(),
                    previous_primary_node_id.clone(),
                ],
            ),
            DfsMetadataShardWriteSetChangeKind::Handoff { .. } => {
                (Self::HANDED_OFF, vec![write_set.primary_node_id.clone()])
            }
        };
        (
            name,
            Self {
                file_system_id: write_set.file_system_id.clone(),
                shard_id: write_set.shard_id.clone(),
                epoch: write_set.epoch,
                generation: write_set.generation,
                node_ids,
                change_id: write_set.last_change.change_id.clone(),
                cause: cause.into(),
            },
        )
    }
}

/// Typed events of one takeover attempt. The attempt id and the swap request
/// id link a candidate's events with the new primary's alignment of its
/// members.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataTakeoverEvent {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    /// The read epoch: the epoch whose primary was judged lost.
    pub epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_epoch: Option<u64>,
    pub generation: u64,
    pub previous_primary_node_id: NodeId,
    pub candidate_node_id: NodeId,
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swap_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch_start: Option<crate::DfsMetadataPrefixProof>,
    pub cause: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub answers: Vec<DfsTakeoverAnswerSummary>,
    /// The vetoing member, the hand-off target or the aligned members.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub node_ids: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dropped_records: Option<u64>,
}

/// One member's corroboration answer as a takeover event reports it.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsTakeoverAnswerSummary {
    pub member_node_id: NodeId,
    pub answer: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_sequence: Option<u64>,
}

impl DfsMetadataTakeoverEvent {
    /// An attempt started by asking the write-set members in `node_ids`.
    pub const CORROBORATION_STARTED: &'static str = "dfs.metadata.takeover.corroboration_started";
    /// An attempt started with no write-set member to ask: it goes straight
    /// to the confirm read on this node's own evidence.
    pub const UNCORROBORATED_STARTED: &'static str = "dfs.metadata.takeover.uncorroborated_started";
    pub const CORROBORATION_VETOED: &'static str = "dfs.metadata.takeover.corroboration_vetoed";
    pub const CORROBORATION_HANDED_OFF: &'static str =
        "dfs.metadata.takeover.corroboration_handed_off";
    pub const PROMISED: &'static str = DfsMetadataWriteSetEvent::PROMISED;
    pub const SWAPPED: &'static str = DfsMetadataWriteSetEvent::SWAPPED;
    pub const SWAP_SUPERSEDED: &'static str = "dfs.metadata.takeover.swap_superseded";
    pub const PROMISE_RELEASED: &'static str = DfsMetadataWriteSetEvent::PROMISE_RELEASED;
    pub const ACTIVATED: &'static str = "dfs.metadata.takeover.activated";
    pub const ALIGNED: &'static str = "dfs.metadata.takeover.aligned";
}

/// The write-set compare a cooperative handoff transaction includes, and the
/// write-set put its completion makes on a switched shard.
///
/// Beginning or completing a handoff compares the write-set key's exact bytes,
/// so it is ordered with every write-set change. Completion on a switched
/// shard also writes the next write set; on an unswitched or stale record it
/// writes nothing there.
pub(super) fn metadata_handoff_write_set_ops(
    retiring: &DfsMetadataShardConfig,
    desired: &DfsMetadataShardConfig,
    write_set_entry: Option<&KvEntry>,
    proof: Option<&DfsMetadataShardMembershipHandoffProof>,
) -> Result<(KvCompare, Option<KvOp>), KvError> {
    let value = write_set_entry.map(|entry| entry.value.as_slice());
    metadata_write_set_admits_handoff(value, retiring, desired)?;
    let key = metadata_shard_write_set_key(&retiring.file_system_id, &retiring.shard_id);
    let compare = exact_compare(key.clone(), write_set_entry.map(|entry| &entry.value));
    let put = match (proof, metadata_shard_write_set_state(retiring, value)) {
        (Some(proof), DfsMetadataShardWriteSetState::Switched(current)) => Some(KvOp::put_serde(
            key,
            &metadata_write_set_after_handoff(&current, desired, proof)?,
            None,
        )?),
        _ => None,
    };
    Ok((compare, put))
}
