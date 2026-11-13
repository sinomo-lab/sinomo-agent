//! Durable member-local state of a write-set shard.
//!
//! One file per shard holds the member's acceptance fence, a takeover
//! candidate's promise and a primary's pending adds. It lives under
//! `coordination/`, beside the acknowledgement floors, which snapshot install
//! never archives, so it survives both restart and snapshot install. Leaving
//! the shard's placement deletes it with the rest of the copy; by then any
//! transaction built from an earlier record can no longer apply.
//!
//! Every write and every acceptance check runs under the shard's metadata
//! writer lock, the same lock that serializes journal ingest, snapshot install
//! and acknowledgement-floor writes. A node that never promises or adds
//! writes nothing here.

use super::*;
use crate::DfsMetadataShardRecordCut;
use serde::{Deserialize, Serialize};
use sinomo_core::kv::KvRequestId;

/// Persisted format of [`DfsMetadataWriteSetMemberState`].
pub const DFS_METADATA_WRITE_SET_MEMBER_STATE_FORMAT: u16 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataWriteSetMemberState {
    pub format: u16,
    pub shard_id: DfsMetadataShardId,
    /// This member ingests and acknowledges nothing from a source epoch below
    /// this one.
    pub accept_from_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promise: Option<DfsMetadataTakeoverPromise>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_adds: Vec<DfsMetadataPendingWriteSetAdd>,
}

/// A takeover candidate's durable promise: it accepts nothing more at the
/// read epoch, and its swap is the exact request rebuilt from these values.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataTakeoverPromise {
    pub cut: DfsMetadataShardRecordCut,
    /// The candidate's durable journal head when it promised. No ingest at the
    /// read epoch can follow it, so it is exact.
    pub epoch_start: DfsMetadataPrefixProof,
    pub swap_request_id: KvRequestId,
}

/// A primary's submitted add: the member's acknowledgement stays required
/// while the shard record still equals `cut`, because the add may still apply.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataPendingWriteSetAdd {
    pub node_id: NodeId,
    pub cut: DfsMetadataShardRecordCut,
}

/// How a settled takeover promise ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DfsMetadataTakeoverPromiseEnd {
    /// The swap can no longer apply and the read epoch is still current: the
    /// fence returns to that epoch.
    Released,
    /// The swap applied, or the epoch advanced past the read epoch: the fence
    /// stays at the next epoch.
    Kept,
}

impl LocalSobjMetadataBackend {
    pub(super) fn write_set_member_state_path(&self) -> PathBuf {
        self.root
            .join("coordination")
            .join("write-set-member-state.sobj")
    }

    pub(super) fn read_write_set_member_state(
        &self,
    ) -> Result<Option<DfsMetadataWriteSetMemberState>> {
        let state: Option<DfsMetadataWriteSetMemberState> =
            crate::metadata_io::read_sobj_optional(&self.write_set_member_state_path())?;
        if let Some(state) = state.as_ref()
            && (state.format != DFS_METADATA_WRITE_SET_MEMBER_STATE_FORMAT
                || state.shard_id != self.shard_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} write-set member state has format {} for shard {}",
                self.shard_id.0, state.format, state.shard_id.0
            )));
        }
        Ok(state)
    }

    fn write_write_set_member_state(&self, state: &DfsMetadataWriteSetMemberState) -> Result<()> {
        let path = self.write_set_member_state_path();
        if state.accept_from_epoch == 0 && state.promise.is_none() && state.pending_adds.is_empty()
        {
            return crate::metadata_io::remove_file_if_exists(&path);
        }
        if let Some(parent) = path.parent() {
            create_dir_all(parent.to_path_buf())?;
        }
        crate::metadata_io::write_sobj_atomic(&path, state)
    }

    fn empty_write_set_member_state(&self) -> DfsMetadataWriteSetMemberState {
        DfsMetadataWriteSetMemberState {
            format: DFS_METADATA_WRITE_SET_MEMBER_STATE_FORMAT,
            shard_id: self.shard_id.clone(),
            accept_from_epoch: 0,
            promise: None,
            pending_adds: Vec::new(),
        }
    }

    /// Refuse an ingest or acknowledgement from `source_epoch` below this
    /// member's fence. The caller holds the shard writer lock. `None` names an
    /// acknowledgement without an epoch, which a fenced member also refuses.
    pub(super) fn require_source_epoch_accepted(&self, source_epoch: Option<u64>) -> Result<()> {
        let Some(state) = self.read_write_set_member_state()? else {
            return Ok(());
        };
        if state.accept_from_epoch == 0
            || source_epoch.is_some_and(|epoch| epoch >= state.accept_from_epoch)
        {
            return Ok(());
        }
        Err(MetadataBackendError::SourceEpochFenced {
            shard_id: self.shard_id.clone(),
            source_epoch: source_epoch.unwrap_or_default(),
            fenced_epoch: state.accept_from_epoch,
        })
    }

    /// Promise to accept nothing more at `cut`'s epoch and return the exact
    /// epoch start. Under the writer lock the journal is materialized to its
    /// durable head, the head is captured and the promise is written before
    /// the lock is released, so no ingest at the read epoch can follow the
    /// captured head. Repeating the same promise returns the same start.
    pub(crate) fn promise_metadata_takeover_owned(
        &mut self,
        cut: &DfsMetadataShardRecordCut,
        swap_request_id: &KvRequestId,
    ) -> Result<DfsMetadataPrefixProof> {
        if cut.shard.shard_id != self.shard_id.0 {
            return Err(MetadataBackendError::Serde(
                "takeover promise names another shard".into(),
            ));
        }
        if cut.write_set_state().switched().is_none() {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} is not on the write-set protocol",
                self.shard_id.0
            )));
        }
        let _lock = self.metadata_write_lock()?;
        let mut state = self
            .read_write_set_member_state()?
            .unwrap_or_else(|| self.empty_write_set_member_state());
        if let Some(promise) = state.promise.as_ref() {
            if &promise.cut == cut && &promise.swap_request_id == swap_request_id {
                return Ok(promise.epoch_start.clone());
            }
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} already holds an unsettled takeover promise",
                self.shard_id.0
            )));
        }
        let accept_from_epoch = cut.shard.epoch.checked_add(1).ok_or_else(|| {
            MetadataBackendError::Serde("metadata shard authority epoch is exhausted".into())
        })?;
        if state.accept_from_epoch > accept_from_epoch {
            return Err(MetadataBackendError::SourceEpochFenced {
                shard_id: self.shard_id.clone(),
                source_epoch: cut.shard.epoch,
                fenced_epoch: state.accept_from_epoch,
            });
        }
        self.refresh_after_metadata_write_lock()?;
        let epoch_start = self.metadata_prefix_proof()?;
        if epoch_start.journal_sequence != self.journal_last_sequence() {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} cannot promise before its journal is materialized",
                self.shard_id.0
            )));
        }
        state.accept_from_epoch = accept_from_epoch;
        state.promise = Some(DfsMetadataTakeoverPromise {
            cut: cut.clone(),
            epoch_start: epoch_start.clone(),
            swap_request_id: swap_request_id.clone(),
        });
        self.write_write_set_member_state(&state)?;
        Ok(epoch_start)
    }

    /// End a settled promise. Settling a promise this member does not hold is
    /// a no-op, so a repeated settlement is safe.
    pub(crate) fn settle_metadata_takeover_promise_owned(
        &mut self,
        swap_request_id: &KvRequestId,
        end: DfsMetadataTakeoverPromiseEnd,
    ) -> Result<()> {
        let _lock = self.metadata_write_lock()?;
        let Some(mut state) = self.read_write_set_member_state()? else {
            return Ok(());
        };
        let Some(promise) = state.promise.take() else {
            return Ok(());
        };
        if &promise.swap_request_id != swap_request_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} holds a different takeover promise",
                self.shard_id.0
            )));
        }
        if end == DfsMetadataTakeoverPromiseEnd::Released {
            state.accept_from_epoch = promise.cut.shard.epoch;
        }
        self.write_write_set_member_state(&state)
    }

    /// Drop the acceptance fence once the member's own record view has
    /// reached it: from then on the record's epoch rejects older sources.
    pub(crate) fn retire_write_set_member_fence_owned(
        &mut self,
        observed_epoch: u64,
    ) -> Result<()> {
        let _lock = self.metadata_write_lock()?;
        let Some(mut state) = self.read_write_set_member_state()? else {
            return Ok(());
        };
        if state.promise.is_some() || observed_epoch < state.accept_from_epoch {
            return Ok(());
        }
        state.accept_from_epoch = 0;
        self.write_write_set_member_state(&state)
    }

    /// Durably require `node_id`'s acknowledgement before the primary submits
    /// its add. In one writer-lock section the journal head is captured, the
    /// member's evidence is checked to cover exactly that head at the record's
    /// epoch and generation, and the pending add is written, so no prepare can
    /// land between the evidence and the requirement.
    pub(crate) fn record_pending_write_set_add_owned(
        &mut self,
        cut: &DfsMetadataShardRecordCut,
        node_id: &NodeId,
        evidence: &crate::DfsMetadataMemberAckView,
    ) -> Result<()> {
        let write_set = cut.write_set_state().switched().cloned().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} is not on the write-set protocol",
                self.shard_id.0
            ))
        })?;
        let _lock = self.metadata_write_lock()?;
        self.refresh_after_metadata_write_lock()?;
        let head = self.metadata_prefix_proof()?;
        if head.journal_sequence != self.journal_last_sequence()
            || evidence.epoch != cut.shard.epoch
            || evidence.write_set_generation != write_set.generation
            || evidence.durable_prefix != head
        {
            return Err(MetadataBackendError::Serde(format!(
                "member {node_id} has not acknowledged shard {}'s whole journal at epoch {} generation {}",
                self.shard_id.0, cut.shard.epoch, write_set.generation
            )));
        }
        let mut state = self
            .read_write_set_member_state()?
            .unwrap_or_else(|| self.empty_write_set_member_state());
        if state
            .pending_adds
            .iter()
            .any(|pending| &pending.node_id == node_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "member {node_id} already has a pending add on shard {}",
                self.shard_id.0
            )));
        }
        state.pending_adds.push(DfsMetadataPendingWriteSetAdd {
            node_id: node_id.clone(),
            cut: cut.clone(),
        });
        self.write_write_set_member_state(&state)
    }

    /// Drop every pending add the record `current` has moved past: it either
    /// applied, and the record now requires the member, or it can never apply.
    /// A record equal to or older than an add's cut keeps the add.
    pub(crate) fn release_pending_write_set_adds_owned(
        &mut self,
        current: &DfsMetadataShardRecordCut,
    ) -> Result<Vec<NodeId>> {
        let _lock = self.metadata_write_lock()?;
        let write_set = current.write_set_state().switched().cloned();
        self.release_superseded_pending_write_set_adds_locked(&current.shard, write_set.as_ref())
    }

    /// The same release for a caller that already holds the shard writer
    /// lock, such as the commit fence; `writer` is that proof.
    pub(crate) fn release_superseded_pending_write_set_adds_held_owned(
        &mut self,
        shard: &crate::DfsMetadataShardConfig,
        write_set: Option<&crate::DfsMetadataShardWriteSet>,
        writer: &crate::publication_guard::DfsPublicationWriterSession,
    ) -> Result<Vec<NodeId>> {
        if writer.owner_scope().shard_id() != &self.shard_id {
            return Err(MetadataBackendError::Serde(
                "pending-add release names another shard's writer".into(),
            ));
        }
        self.release_superseded_pending_write_set_adds_locked(shard, write_set)
    }

    fn release_superseded_pending_write_set_adds_locked(
        &mut self,
        shard: &crate::DfsMetadataShardConfig,
        write_set: Option<&crate::DfsMetadataShardWriteSet>,
    ) -> Result<Vec<NodeId>> {
        let Some(mut state) = self.read_write_set_member_state()? else {
            return Ok(Vec::new());
        };
        let mut released = Vec::new();
        state.pending_adds.retain(|pending| {
            let keep =
                !crate::metadata_pending_write_set_add_superseded(&pending.cut, shard, write_set);
            if !keep {
                released.push(pending.node_id.clone());
            }
            keep
        });
        if !released.is_empty() {
            self.write_write_set_member_state(&state)?;
        }
        Ok(released)
    }
}
