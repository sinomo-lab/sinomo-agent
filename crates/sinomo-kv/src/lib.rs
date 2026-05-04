//! In-process cluster KV core.
//!
//! This crate provides the first etcd-like control-plane KV surface for
//! Sinomo: monotonic revisions, compare-and-swap transactions, leases,
//! watches, snapshots, and a deterministic Raft-style replication harness for
//! tests and local development. It is not a networked consensus service yet.

use serde::{Deserialize, Serialize};
use sinomo_core::{ClusterMemberRecord, NodeId};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct KvRevision(pub u64);

impl KvRevision {
    pub const ZERO: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct KvVersion(pub u64);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LeaseId(pub u64);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WatchId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub create_revision: KvRevision,
    pub modify_revision: KvRevision,
    pub version: KvVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<LeaseId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvLease {
    pub lease_id: LeaseId,
    pub ttl_ticks: u64,
    pub remaining_ticks: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub granted_at: OffsetDateTime,
}

impl KvLease {
    fn new(lease_id: LeaseId, ttl_ticks: u64) -> Self {
        Self {
            lease_id,
            ttl_ticks,
            remaining_ticks: ttl_ticks,
            granted_at: OffsetDateTime::now_utc(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvCompare {
    Exists {
        key: String,
    },
    Missing {
        key: String,
    },
    ValueEquals {
        key: String,
        value: Vec<u8>,
    },
    VersionEquals {
        key: String,
        version: KvVersion,
    },
    CreateRevisionEquals {
        key: String,
        revision: KvRevision,
    },
    ModifyRevisionEquals {
        key: String,
        revision: KvRevision,
    },
    LeaseEquals {
        key: String,
        lease_id: Option<LeaseId>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvOp {
    Put {
        key: String,
        value: Vec<u8>,
        lease_id: Option<LeaseId>,
    },
    Delete {
        key: String,
    },
    DeletePrefix {
        prefix: String,
    },
    LeaseGrant {
        lease_id: LeaseId,
        ttl_ticks: u64,
    },
    LeaseKeepAlive {
        lease_id: LeaseId,
    },
    LeaseRevoke {
        lease_id: LeaseId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvTxn {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compares: Vec<KvCompare>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub success: Vec<KvOp>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failure: Vec<KvOp>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvTxnResponse {
    pub succeeded: bool,
    pub revision: KvRevision,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses: Vec<KvOpResponse>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvOpResponse {
    Put {
        key: String,
        revision: KvRevision,
    },
    Delete {
        key: String,
        deleted: bool,
    },
    DeletePrefix {
        prefix: String,
        deleted_count: usize,
    },
    LeaseGrant {
        lease_id: LeaseId,
    },
    LeaseKeepAlive {
        lease_id: LeaseId,
    },
    LeaseRevoke {
        lease_id: LeaseId,
        deleted_count: usize,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvEventKind {
    Put { value: Vec<u8> },
    Delete,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvEvent {
    pub revision: KvRevision,
    pub key: String,
    pub kind: KvEventKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvWatch {
    pub watch_id: WatchId,
    pub prefix: String,
    pub start_revision: KvRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvSnapshot {
    pub revision: KvRevision,
    pub compacted_revision: KvRevision,
    pub entries: Vec<KvEntry>,
    pub leases: Vec<KvLease>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvLogEntry {
    pub index: u64,
    pub term: u64,
    pub command: KvCommand,
    pub revision: KvRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvCommand {
    Op(KvOp),
    Txn(KvTxn),
    PutClusterMember(ClusterMemberRecord),
}

#[derive(Clone, Debug, Default)]
pub struct InProcessKvStore {
    revision: KvRevision,
    compacted_revision: KvRevision,
    entries: BTreeMap<String, KvEntry>,
    events: Vec<KvEvent>,
    leases: BTreeMap<LeaseId, KvLease>,
    watches: BTreeMap<WatchId, KvWatchState>,
    next_watch_id: u64,
}

impl InProcessKvStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn revision(&self) -> KvRevision {
        self.revision
    }

    pub fn compacted_revision(&self) -> KvRevision {
        self.compacted_revision
    }

    pub fn get(&self, key: &str) -> Option<&KvEntry> {
        self.entries.get(key)
    }

    pub fn range_prefix(&self, prefix: &str) -> Vec<KvEntry> {
        self.entries
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(_, entry)| entry.clone())
            .collect()
    }

    pub fn put(
        &mut self,
        key: impl Into<String>,
        value: Vec<u8>,
        lease_id: Option<LeaseId>,
    ) -> Result<KvRevision, KvError> {
        self.apply_op(KvOp::Put {
            key: key.into(),
            value,
            lease_id,
        })
        .map(|response| response.revision())
    }

    pub fn delete(&mut self, key: impl Into<String>) -> Result<bool, KvError> {
        match self.apply_op(KvOp::Delete { key: key.into() })? {
            KvOpResponse::Delete { deleted, .. } => Ok(deleted),
            _ => unreachable!("delete response"),
        }
    }

    pub fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let succeeded = txn.compares.iter().all(|compare| self.compare(compare));
        let ops = if succeeded { txn.success } else { txn.failure };
        let mut responses = Vec::with_capacity(ops.len());
        let mut revision = self.revision;

        for op in ops {
            let response = self.apply_op(op)?;
            revision = self.revision;
            responses.push(response);
        }

        Ok(KvTxnResponse {
            succeeded,
            revision,
            responses,
        })
    }

    pub fn apply_command(&mut self, command: KvCommand) -> Result<KvRevision, KvError> {
        match command {
            KvCommand::Op(op) => {
                self.apply_op(op)?;
                Ok(self.revision)
            }
            KvCommand::Txn(txn) => self.txn(txn).map(|response| response.revision),
            KvCommand::PutClusterMember(member) => self.put_cluster_member(member),
        }
    }

    pub fn put_cluster_member(
        &mut self,
        member: ClusterMemberRecord,
    ) -> Result<KvRevision, KvError> {
        let key = cluster_member_key(&member.node_id);
        let value = serde_json::to_vec(&member).map_err(|source| KvError::Serialization {
            detail: source.to_string(),
        })?;
        self.put(key, value, None)
    }

    pub fn read_cluster_member(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<ClusterMemberRecord>, KvError> {
        let Some(entry) = self.get(&cluster_member_key(node_id)) else {
            return Ok(None);
        };
        serde_json::from_slice(&entry.value)
            .map(Some)
            .map_err(|source| KvError::Serialization {
                detail: source.to_string(),
            })
    }

    pub fn watch_prefix(
        &mut self,
        prefix: impl Into<String>,
        start_revision: KvRevision,
    ) -> Result<WatchId, KvError> {
        if start_revision <= self.compacted_revision && self.compacted_revision != KvRevision::ZERO
        {
            return Err(KvError::Compacted {
                requested: start_revision,
                compacted: self.compacted_revision,
            });
        }

        let watch_id = WatchId(self.next_watch_id.max(1));
        self.next_watch_id = watch_id.0.saturating_add(1);
        let prefix = prefix.into();
        let backlog = self
            .events
            .iter()
            .filter(|event| event.revision >= start_revision && event.key.starts_with(&prefix))
            .cloned()
            .collect();
        self.watches.insert(
            watch_id,
            KvWatchState {
                watch: KvWatch {
                    watch_id,
                    prefix,
                    start_revision,
                },
                pending: backlog,
            },
        );
        Ok(watch_id)
    }

    pub fn drain_watch(&mut self, watch_id: WatchId) -> Result<Vec<KvEvent>, KvError> {
        let watch = self
            .watches
            .get_mut(&watch_id)
            .ok_or(KvError::UnknownWatch { watch_id })?;
        Ok(watch.pending.drain(..).collect())
    }

    pub fn snapshot(&self) -> KvSnapshot {
        KvSnapshot {
            revision: self.revision,
            compacted_revision: self.compacted_revision,
            entries: self.entries.values().cloned().collect(),
            leases: self.leases.values().cloned().collect(),
        }
    }

    pub fn restore(snapshot: KvSnapshot) -> Self {
        Self {
            revision: snapshot.revision,
            compacted_revision: snapshot.compacted_revision,
            entries: snapshot
                .entries
                .into_iter()
                .map(|entry| (entry.key.clone(), entry))
                .collect(),
            events: Vec::new(),
            leases: snapshot
                .leases
                .into_iter()
                .map(|lease| (lease.lease_id, lease))
                .collect(),
            watches: BTreeMap::new(),
            next_watch_id: 1,
        }
    }

    pub fn compact(&mut self, revision: KvRevision) -> Result<(), KvError> {
        if revision > self.revision {
            return Err(KvError::RevisionAhead {
                requested: revision,
                current: self.revision,
            });
        }
        if revision > self.compacted_revision {
            self.compacted_revision = revision;
            self.events.retain(|event| event.revision > revision);
        }
        Ok(())
    }

    pub fn tick_leases(&mut self, ticks: u64) -> Result<Vec<LeaseId>, KvError> {
        let mut expired = Vec::new();
        for lease in self.leases.values_mut() {
            lease.remaining_ticks = lease.remaining_ticks.saturating_sub(ticks);
            if lease.remaining_ticks == 0 {
                expired.push(lease.lease_id);
            }
        }

        for lease_id in &expired {
            self.apply_op(KvOp::LeaseRevoke {
                lease_id: *lease_id,
            })?;
        }

        Ok(expired)
    }

    fn apply_op(&mut self, op: KvOp) -> Result<KvOpResponse, KvError> {
        match op {
            KvOp::Put {
                key,
                value,
                lease_id,
            } => self.apply_put(key, value, lease_id),
            KvOp::Delete { key } => self.apply_delete(key),
            KvOp::DeletePrefix { prefix } => self.apply_delete_prefix(prefix),
            KvOp::LeaseGrant {
                lease_id,
                ttl_ticks,
            } => {
                if ttl_ticks == 0 {
                    return Err(KvError::InvalidLeaseTtl { ttl_ticks });
                }
                self.leases
                    .insert(lease_id, KvLease::new(lease_id, ttl_ticks));
                Ok(KvOpResponse::LeaseGrant { lease_id })
            }
            KvOp::LeaseKeepAlive { lease_id } => {
                let lease = self
                    .leases
                    .get_mut(&lease_id)
                    .ok_or(KvError::UnknownLease { lease_id })?;
                lease.remaining_ticks = lease.ttl_ticks;
                Ok(KvOpResponse::LeaseKeepAlive { lease_id })
            }
            KvOp::LeaseRevoke { lease_id } => {
                self.leases.remove(&lease_id);
                let keys: Vec<String> = self
                    .entries
                    .iter()
                    .filter(|(_, entry)| entry.lease_id == Some(lease_id))
                    .map(|(key, _)| key.clone())
                    .collect();
                let deleted_count = keys.len();
                for key in keys {
                    self.apply_delete(key)?;
                }
                Ok(KvOpResponse::LeaseRevoke {
                    lease_id,
                    deleted_count,
                })
            }
        }
    }

    fn apply_put(
        &mut self,
        key: String,
        value: Vec<u8>,
        lease_id: Option<LeaseId>,
    ) -> Result<KvOpResponse, KvError> {
        if let Some(lease_id) = lease_id {
            if !self.leases.contains_key(&lease_id) {
                return Err(KvError::UnknownLease { lease_id });
            }
        }

        let revision = self.bump_revision();
        let entry = match self.entries.remove(&key) {
            Some(mut existing) => {
                existing.value = value.clone();
                existing.modify_revision = revision;
                existing.version = KvVersion(existing.version.0.saturating_add(1));
                existing.lease_id = lease_id;
                existing
            }
            None => KvEntry {
                key: key.clone(),
                value: value.clone(),
                create_revision: revision,
                modify_revision: revision,
                version: KvVersion(1),
                lease_id,
            },
        };
        self.entries.insert(key.clone(), entry);
        self.record_event(KvEvent {
            revision,
            key: key.clone(),
            kind: KvEventKind::Put { value },
        });
        Ok(KvOpResponse::Put { key, revision })
    }

    fn apply_delete(&mut self, key: String) -> Result<KvOpResponse, KvError> {
        if self.entries.remove(&key).is_none() {
            return Ok(KvOpResponse::Delete {
                key,
                deleted: false,
            });
        }
        let revision = self.bump_revision();
        self.record_event(KvEvent {
            revision,
            key: key.clone(),
            kind: KvEventKind::Delete,
        });
        Ok(KvOpResponse::Delete { key, deleted: true })
    }

    fn apply_delete_prefix(&mut self, prefix: String) -> Result<KvOpResponse, KvError> {
        let keys: Vec<String> = self
            .entries
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        let deleted_count = keys.len();
        for key in keys {
            self.apply_delete(key)?;
        }
        Ok(KvOpResponse::DeletePrefix {
            prefix,
            deleted_count,
        })
    }

    fn compare(&self, compare: &KvCompare) -> bool {
        match compare {
            KvCompare::Exists { key } => self.entries.contains_key(key),
            KvCompare::Missing { key } => !self.entries.contains_key(key),
            KvCompare::ValueEquals { key, value } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.value == value),
            KvCompare::VersionEquals { key, version } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.version == version),
            KvCompare::CreateRevisionEquals { key, revision } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.create_revision == revision),
            KvCompare::ModifyRevisionEquals { key, revision } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.modify_revision == revision),
            KvCompare::LeaseEquals { key, lease_id } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.lease_id == lease_id),
        }
    }

    fn bump_revision(&mut self) -> KvRevision {
        self.revision = self.revision.next();
        self.revision
    }

    fn record_event(&mut self, event: KvEvent) {
        for watch in self.watches.values_mut() {
            if event.revision >= watch.watch.start_revision
                && event.key.starts_with(&watch.watch.prefix)
            {
                watch.pending.push_back(event.clone());
            }
        }
        self.events.push(event);
    }
}

#[derive(Clone, Debug)]
struct KvWatchState {
    watch: KvWatch,
    pending: VecDeque<KvEvent>,
}

impl Default for KvRevision {
    fn default() -> Self {
        Self::ZERO
    }
}

impl KvOpResponse {
    fn revision(&self) -> KvRevision {
        match self {
            Self::Put { revision, .. } => *revision,
            Self::Delete { .. }
            | Self::DeletePrefix { .. }
            | Self::LeaseGrant { .. }
            | Self::LeaseKeepAlive { .. }
            | Self::LeaseRevoke { .. } => KvRevision::ZERO,
        }
    }
}

#[derive(Clone, Debug)]
pub struct InProcessRaftKvCluster {
    term: u64,
    leader_id: NodeId,
    voters: BTreeSet<NodeId>,
    online: BTreeSet<NodeId>,
    nodes: BTreeMap<NodeId, ReplicatedKvNode>,
    next_log_index: u64,
}

impl InProcessRaftKvCluster {
    pub fn new(voter_ids: Vec<NodeId>) -> Result<Self, KvError> {
        let Some(leader_id) = voter_ids.first().cloned() else {
            return Err(KvError::NoVoters);
        };
        let voters: BTreeSet<NodeId> = voter_ids.into_iter().collect();
        let online = voters.clone();
        let nodes = voters
            .iter()
            .cloned()
            .map(|node_id| {
                (
                    node_id.clone(),
                    ReplicatedKvNode {
                        store: InProcessKvStore::new(),
                        log: Vec::new(),
                    },
                )
            })
            .collect();
        Ok(Self {
            term: 1,
            leader_id,
            voters,
            online,
            nodes,
            next_log_index: 1,
        })
    }

    pub fn leader_id(&self) -> &NodeId {
        &self.leader_id
    }

    pub fn store(&self, node_id: &NodeId) -> Option<&InProcessKvStore> {
        self.nodes.get(node_id).map(|node| &node.store)
    }

    pub fn set_node_online(&mut self, node_id: &NodeId, online: bool) {
        if online {
            self.online.insert(node_id.clone());
        } else {
            self.online.remove(node_id);
        }
    }

    pub fn apply_as_leader(&mut self, command: KvCommand) -> Result<KvRevision, KvError> {
        if !self.online.contains(&self.leader_id) {
            return Err(KvError::LeaderOffline {
                leader_id: self.leader_id.clone(),
            });
        }
        let online_voters = self.voters.intersection(&self.online).count();
        if online_voters < self.quorum_size() {
            return Err(KvError::QuorumUnavailable {
                online_voters,
                quorum: self.quorum_size(),
            });
        }

        let index = self.next_log_index;
        self.next_log_index += 1;
        let mut committed_revision = KvRevision::ZERO;
        for node_id in self
            .voters
            .intersection(&self.online)
            .cloned()
            .collect::<Vec<_>>()
        {
            let node = self
                .nodes
                .get_mut(&node_id)
                .ok_or_else(|| KvError::UnknownNode {
                    node_id: node_id.clone(),
                })?;
            let revision = node.store.apply_command(command.clone())?;
            if node_id == self.leader_id {
                committed_revision = revision;
            }
            node.log.push(KvLogEntry {
                index,
                term: self.term,
                command: command.clone(),
                revision,
            });
        }
        Ok(committed_revision)
    }

    pub fn txn_response(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        if !self.online.contains(&self.leader_id) {
            return Err(KvError::LeaderOffline {
                leader_id: self.leader_id.clone(),
            });
        }
        let online_voters = self.voters.intersection(&self.online).count();
        if online_voters < self.quorum_size() {
            return Err(KvError::QuorumUnavailable {
                online_voters,
                quorum: self.quorum_size(),
            });
        }

        let index = self.next_log_index;
        self.next_log_index += 1;
        let mut leader_response = None;
        for node_id in self
            .voters
            .intersection(&self.online)
            .cloned()
            .collect::<Vec<_>>()
        {
            let node = self
                .nodes
                .get_mut(&node_id)
                .ok_or_else(|| KvError::UnknownNode {
                    node_id: node_id.clone(),
                })?;
            let response = node.store.txn(txn.clone())?;
            if node_id == self.leader_id {
                leader_response = Some(response.clone());
            }
            node.log.push(KvLogEntry {
                index,
                term: self.term,
                command: KvCommand::Txn(txn.clone()),
                revision: response.revision,
            });
        }
        leader_response.ok_or_else(|| KvError::UnknownNode {
            node_id: self.leader_id.clone(),
        })
    }

    pub fn put(&mut self, key: impl Into<String>, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.apply_as_leader(KvCommand::Op(KvOp::Put {
            key: key.into(),
            value,
            lease_id: None,
        }))
    }

    pub fn txn(&mut self, txn: KvTxn) -> Result<KvRevision, KvError> {
        self.txn_response(txn).map(|response| response.revision)
    }

    pub fn put_cluster_member(
        &mut self,
        member: ClusterMemberRecord,
    ) -> Result<KvRevision, KvError> {
        self.apply_as_leader(KvCommand::PutClusterMember(member))
    }

    pub fn install_snapshot(
        &mut self,
        target_node_id: NodeId,
        snapshot: KvSnapshot,
    ) -> Result<(), KvError> {
        let node = self
            .nodes
            .get_mut(&target_node_id)
            .ok_or(KvError::UnknownNode {
                node_id: target_node_id,
            })?;
        node.store = InProcessKvStore::restore(snapshot);
        Ok(())
    }

    pub fn add_learner_from_snapshot(
        &mut self,
        node_id: NodeId,
        snapshot: KvSnapshot,
    ) -> Result<(), KvError> {
        if self.nodes.contains_key(&node_id) {
            return Err(KvError::DuplicateNode { node_id });
        }
        self.nodes.insert(
            node_id.clone(),
            ReplicatedKvNode {
                store: InProcessKvStore::restore(snapshot),
                log: Vec::new(),
            },
        );
        self.online.insert(node_id);
        Ok(())
    }

    fn quorum_size(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

#[derive(Clone, Debug)]
struct ReplicatedKvNode {
    store: InProcessKvStore,
    log: Vec<KvLogEntry>,
}

pub fn cluster_member_key(node_id: &NodeId) -> String {
    format!("/cluster/members/{node_id}")
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum KvError {
    #[error("requested revision {requested:?} has been compacted at {compacted:?}")]
    Compacted {
        requested: KvRevision,
        compacted: KvRevision,
    },
    #[error("requested revision {requested:?} is ahead of current revision {current:?}")]
    RevisionAhead {
        requested: KvRevision,
        current: KvRevision,
    },
    #[error("unknown lease: {lease_id:?}")]
    UnknownLease { lease_id: LeaseId },
    #[error("invalid lease ttl: {ttl_ticks}")]
    InvalidLeaseTtl { ttl_ticks: u64 },
    #[error("unknown watch: {watch_id:?}")]
    UnknownWatch { watch_id: WatchId },
    #[error("serialization error: {detail}")]
    Serialization { detail: String },
    #[error("raft cluster requires at least one voter")]
    NoVoters,
    #[error("unknown replicated node: {node_id}")]
    UnknownNode { node_id: NodeId },
    #[error("duplicate replicated node: {node_id}")]
    DuplicateNode { node_id: NodeId },
    #[error("leader is offline: {leader_id}")]
    LeaderOffline { leader_id: NodeId },
    #[error("quorum unavailable: {online_voters} voters online, {quorum} required")]
    QuorumUnavailable { online_voters: usize, quorum: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{ClusterId, ClusterMemberRecord};

    #[test]
    fn put_and_compare_swap_txn_updates_revision_and_version() {
        let mut store = InProcessKvStore::new();
        let rev1 = store.put("/config/a", b"old".to_vec(), None).expect("put");
        let response = store
            .txn(KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key: "/config/a".to_string(),
                    value: b"old".to_vec(),
                }],
                success: vec![KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"new".to_vec(),
                    lease_id: None,
                }],
                failure: vec![KvOp::Put {
                    key: "/config/a".to_string(),
                    value: b"failed".to_vec(),
                    lease_id: None,
                }],
            })
            .expect("txn");

        let entry = store.get("/config/a").expect("entry");
        assert_eq!(rev1, KvRevision(1));
        assert!(response.succeeded);
        assert_eq!(entry.value, b"new");
        assert_eq!(entry.create_revision, KvRevision(1));
        assert_eq!(entry.modify_revision, KvRevision(2));
        assert_eq!(entry.version, KvVersion(2));
    }

    #[test]
    fn watch_prefix_receives_backlog_and_future_events() {
        let mut store = InProcessKvStore::new();
        store
            .put("/services/a", b"one".to_vec(), None)
            .expect("put");
        store.put("/other/a", b"skip".to_vec(), None).expect("put");
        let watch_id = store
            .watch_prefix("/services/", KvRevision(1))
            .expect("watch");

        let backlog = store.drain_watch(watch_id).expect("backlog");
        assert_eq!(backlog.len(), 1);
        assert_eq!(backlog[0].key, "/services/a");

        store
            .put("/services/b", b"two".to_vec(), None)
            .expect("put");
        let future = store.drain_watch(watch_id).expect("future");
        assert_eq!(future.len(), 1);
        assert_eq!(future[0].key, "/services/b");
    }

    #[test]
    fn lease_expiration_deletes_attached_keys() {
        let mut store = InProcessKvStore::new();
        store
            .apply_op(KvOp::LeaseGrant {
                lease_id: LeaseId(1),
                ttl_ticks: 2,
            })
            .expect("lease");
        store
            .put("/services/live", b"yes".to_vec(), Some(LeaseId(1)))
            .expect("put leased");

        assert!(store.get("/services/live").is_some());
        assert_eq!(store.tick_leases(1).expect("tick"), Vec::<LeaseId>::new());
        assert!(store.get("/services/live").is_some());
        assert_eq!(store.tick_leases(1).expect("expire"), vec![LeaseId(1)]);
        assert!(store.get("/services/live").is_none());
    }

    #[test]
    fn snapshot_restore_preserves_entries_and_revisions() {
        let mut store = InProcessKvStore::new();
        store
            .put("/dfs/workspaces/w1", b"meta".to_vec(), None)
            .expect("put");
        let snapshot = store.snapshot();

        let restored = InProcessKvStore::restore(snapshot);

        assert_eq!(restored.revision(), KvRevision(1));
        assert_eq!(
            restored.get("/dfs/workspaces/w1").expect("entry").value,
            b"meta"
        );
    }

    #[test]
    fn compacted_revision_rejects_old_watches() {
        let mut store = InProcessKvStore::new();
        store.put("/a", b"1".to_vec(), None).expect("put");
        store.compact(KvRevision(1)).expect("compact");

        let error = store
            .watch_prefix("/", KvRevision(1))
            .expect_err("compacted watch");
        assert!(matches!(error, KvError::Compacted { .. }));
    }

    #[test]
    fn raft_cluster_replicates_committed_writes_to_online_voters() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let node_c = NodeId::new("node_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");

        cluster.put("/cluster/meta", b"ok".to_vec()).expect("put");

        assert_eq!(
            cluster
                .store(&node_b)
                .expect("store")
                .get("/cluster/meta")
                .expect("entry")
                .value,
            b"ok"
        );
        assert_eq!(
            cluster.store(&node_c).expect("store").revision(),
            KvRevision(1)
        );
    }

    #[test]
    fn raft_cluster_rejects_writes_without_quorum() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let node_c = NodeId::new("node_c").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone(), node_c.clone()])
                .expect("cluster");
        cluster.set_node_online(&node_b, false);
        cluster.set_node_online(&node_c, false);

        let error = cluster
            .put("/cluster/meta", b"no".to_vec())
            .expect_err("no quorum");

        assert!(matches!(error, KvError::QuorumUnavailable { .. }));
    }

    #[test]
    fn cluster_member_records_are_stored_under_cluster_namespace() {
        let node_id = NodeId::new("node_desktop").expect("node");
        let member = ClusterMemberRecord::local_voter(
            ClusterId::new("cluster_home").expect("cluster"),
            node_id.clone(),
            "desktop",
            OffsetDateTime::now_utc(),
        );
        let mut store = InProcessKvStore::new();

        store
            .put_cluster_member(member.clone())
            .expect("put member");

        assert!(store.get("/cluster/members/node_desktop").is_some());
        assert_eq!(
            store
                .read_cluster_member(&node_id)
                .expect("member")
                .expect("exists"),
            member
        );
    }

    #[test]
    fn learner_can_catch_up_from_snapshot() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let learner = NodeId::new("node_learner").expect("node");
        let mut cluster =
            InProcessRaftKvCluster::new(vec![node_a.clone(), node_b.clone()]).expect("cluster");
        cluster.put("/policy/a", b"allow".to_vec()).expect("put");
        let snapshot = cluster.store(&node_a).expect("leader store").snapshot();

        cluster
            .add_learner_from_snapshot(learner.clone(), snapshot)
            .expect("learner");

        assert_eq!(
            cluster
                .store(&learner)
                .expect("learner store")
                .get("/policy/a")
                .expect("entry")
                .value,
            b"allow"
        );
    }
}
