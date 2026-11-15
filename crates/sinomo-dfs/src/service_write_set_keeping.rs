//! The steps of keeping a write set that need the primary's resident shard
//! store: the switch's evidence and a member's join.
//!
//! The keeper runs these on the node's resident service while it holds the
//! same execution admission a maintenance turn holds, so no prepare or commit
//! record is appended while a join compares a member's evidence with the
//! journal head. Each step talks to members over the ordinary metadata
//! journal route with no DFS deadline: the network's connect and no-progress
//! limits end an attempt to a lost member.

use crate::{
    DfsError, DfsMessageTransport, DfsMetadataJournalBatchResult, DfsMetadataMemberAckView,
    DfsMetadataShardId, DfsMetadataShardRecordCut, NetworkedDfsService,
};
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;

/// What the switch takes from this primary's own store, read while no prepare
/// or commit record can be appended.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsWriteSetSwitchEvidence {
    /// The members that hold exactly this primary's journal head.
    pub(crate) members: BTreeSet<NodeId>,
    /// The largest content W among the versions the shard store holds that
    /// record their replication policy.
    pub(crate) content_w: Option<usize>,
    /// Versions with content (a hash or a size) that record no policy:
    /// content written before versions recorded it.
    pub(crate) unrecorded_content_versions: usize,
}

impl DfsWriteSetSwitchEvidence {
    /// Whether content of unknown W keeps every configured member.
    pub(crate) fn keeps_every_member(&self) -> bool {
        self.unrecorded_content_versions > 0
    }

    /// The record's initial raised N for a shard of `configured` members:
    /// the largest recorded content W, and, while some content's W is
    /// unknown, at least every configured member, so the switch's reduction
    /// and later removals keep them all.
    pub(crate) fn initial_raised_n(&self, configured: usize) -> Option<u64> {
        self.content_w
            .into_iter()
            .chain(self.keeps_every_member().then_some(configured))
            .max()
            .map(|n| u64::try_from(n).unwrap_or(u64::MAX))
    }
}

/// How aligning one member with the current epoch's start ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsWriteSetAlignment {
    /// The epoch began before the shard switched, or at its switch: there is
    /// no epoch start to align with.
    NotNeeded,
    /// The member holds a prefix of this epoch's history at `head`, checked by
    /// itself at the epoch start or by this primary below it; replication to
    /// it continues from `head`.
    Aligned { head: crate::DfsMetadataPrefixProof },
    /// The member's journal differs from this epoch's history or cannot be
    /// checked against it: it needs snapshot repair.
    NeedsSnapshot { reason: String },
    /// The member did not answer as a member of this record yet (its view is
    /// behind, or it refused), or could not be reached.
    NotReady { reason: String },
}

/// How one member's join ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsWriteSetJoin {
    /// The member holds the head under the record's generation, and its add
    /// is durably pending: submit the add built from the same cut.
    Pending { view: DfsMetadataMemberAckView },
    /// The member could not be brought to the head with evidence; nothing was
    /// recorded.
    NotReady { reason: String },
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// The switch's evidence: the members that hold exactly this primary's
    /// journal head now, and the content W of every version the shard store
    /// holds. Every version counts, current or retained history, visible or
    /// not: any of them can still be read or published. Under the all-member
    /// rule every later commit reaches every configured member, so the
    /// members keep holding everything until the switch applies; the caller
    /// submits the switch before releasing the admission, so no version is
    /// committed between this read and the switch.
    pub(crate) fn write_set_switch_evidence(
        &mut self,
        cut: &DfsMetadataShardRecordCut,
        sender: &crate::DfsMetadataJournalBatchSender,
    ) -> Result<DfsWriteSetSwitchEvidence, DfsError> {
        let shard = &cut.shard;
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let (content_w, unrecorded_content_versions) = self.write_set_content_w(&shard_id)?;
        let head = self.write_set_journal_head(&shard_id)?;
        let mut members = BTreeSet::from([self.node_id.clone()]);
        for member in &shard.replica_node_ids {
            if member == &self.node_id {
                continue;
            }
            let command = crate::metadata_replication::dfs_metadata_caught_up_probe_command_for(
                &self.node_id,
                member,
                &shard.file_system_id,
                &shard_id,
                shard.epoch,
                &head,
            );
            if let Ok(response) = sender(member, &command, None)
                && response.result == DfsMetadataJournalBatchResult::Applied
                && response.verified_prefix_only
            {
                members.insert(member.clone());
            }
        }
        Ok(DfsWriteSetSwitchEvidence {
            members,
            content_w,
            unrecorded_content_versions,
        })
    }

    /// One bounded pass over the shard store's version records: the largest
    /// recorded content W, and how many content versions record none.
    fn write_set_content_w(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<(Option<usize>, usize), DfsError> {
        let backend = self
            .metadata_backend
            .as_deref()
            .ok_or_else(|| DfsError::Metadata {
                message: "a write-set switch requires the shard's durable backend".into(),
            })?;
        let mut content_w = None::<usize>;
        let mut unrecorded = 0usize;
        let mut cursor = None;
        loop {
            let page = backend
                .version_records_on_shard_page(
                    shard_id,
                    cursor.as_ref(),
                    crate::metadata_limits::OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?
                .ok_or_else(|| DfsError::Metadata {
                    message: format!(
                        "this store does not hold metadata shard {} to read its versions",
                        shard_id.0
                    ),
                })?;
            for version in &page.versions {
                #[cfg(test)]
                if unrecorded_content_for_test(&version.object_id) {
                    unrecorded = unrecorded.saturating_add(1);
                    continue;
                }
                match &version.content_durability {
                    Some(durability) => {
                        content_w = content_w.max(Some(durability.policy.min_commit_replicas));
                    }
                    None if version.content_hash.is_some() || version.size_bytes.is_some() => {
                        unrecorded = unrecorded.saturating_add(1);
                    }
                    None => {}
                }
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => return Ok((content_w, unrecorded)),
            }
        }
    }

    /// Bring `member` to this primary's head and durably require its
    /// acknowledgement before its add is submitted.
    ///
    /// The ordinary durability barrier transfers what the member lacks from
    /// its matching prefix, or a snapshot when its copy differs or is empty.
    /// A probe naming the record's generation then confirms the exact head
    /// under that generation, and the pending add is recorded under the shard
    /// writer lock, which compares that evidence with the head again.
    pub(crate) fn join_write_set_member(
        &mut self,
        cut: &DfsMetadataShardRecordCut,
        member: &NodeId,
        sender: &crate::DfsMetadataJournalBatchSender,
    ) -> Result<DfsWriteSetJoin, DfsError> {
        let shard = &cut.shard;
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let Some(write_set) = cut.write_set_state().switched().cloned() else {
            return Ok(DfsWriteSetJoin::NotReady {
                reason: "the shard is not on the write-set protocol".into(),
            });
        };
        // A store reopened after a membership change keeps its completed
        // handoff marker until it is bound again; bind it as a lane does.
        self.rebind_completed_local_metadata_shard_instance_if_frozen(shard)?;
        let head = self.write_set_journal_head(&shard_id)?;
        let caught_up =
            self.catch_up_metadata_shard_member(shard, &shard_id, &head, member, None, true)?;
        if !caught_up {
            return Ok(DfsWriteSetJoin::NotReady {
                reason: format!("{member} did not reach the head {}", head.journal_sequence),
            });
        }
        let head = self.write_set_journal_head(&shard_id)?;
        let command = crate::metadata_replication::dfs_metadata_write_set_member_probe_command(
            &self.node_id,
            member,
            &shard.file_system_id,
            &shard_id,
            shard.epoch,
            &head,
            write_set.generation,
        );
        let response = match sender(member, &command, None) {
            Ok(response) => response,
            Err(error) => {
                return Ok(DfsWriteSetJoin::NotReady {
                    reason: error.to_string(),
                });
            }
        };
        crate::write_set_member_evidence::record_metadata_member_view(
            &self.node_id,
            &shard_id,
            member,
            response.member_view.as_ref(),
        );
        let view = match (response.result, response.member_view) {
            (DfsMetadataJournalBatchResult::Applied, Some(view)) if response.verified_prefix_only => {
                view
            }
            (result, _) => {
                return Ok(DfsWriteSetJoin::NotReady {
                    reason: format!("{member} did not confirm the head: {result:?}"),
                });
            }
        };
        let backend = self
            .metadata_backend
            .as_deref_mut()
            .ok_or_else(|| DfsError::Metadata {
                message: "a write-set join requires the shard's durable backend".into(),
            })?;
        match backend.record_pending_write_set_add(&shard_id, cut, member, &view) {
            Ok(()) => Ok(DfsWriteSetJoin::Pending { view }),
            Err(error) => Ok(DfsWriteSetJoin::NotReady {
                reason: error.to_string(),
            }),
        }
    }

    /// Align `member` with the epoch start `cut`'s record names, as a new
    /// primary does after a takeover or rotation handoff.
    ///
    /// The member checks the epoch start against its own record and journal:
    /// it drops records past a matching epoch start, or reports a head that
    /// ends before it. Such a head is checked against this primary's own
    /// journal and confirmed to the member, so a member that missed whole
    /// epochs and diverged below the epoch start is never extended from it.
    /// The member's sender cursor starts at the confirmed head.
    pub(crate) fn align_write_set_member(
        &mut self,
        cut: &DfsMetadataShardRecordCut,
        member: &NodeId,
        sender: &crate::DfsMetadataJournalBatchSender,
    ) -> Result<DfsWriteSetAlignment, DfsError> {
        let shard = &cut.shard;
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let Some(write_set) = cut.write_set_state().switched().cloned() else {
            return Ok(DfsWriteSetAlignment::NotNeeded);
        };
        let Some(epoch_start) = write_set.epoch_start.clone() else {
            return Ok(DfsWriteSetAlignment::NotNeeded);
        };
        let probe = |target: &crate::DfsMetadataPrefixProof| {
            crate::metadata_replication::dfs_metadata_epoch_alignment_probe_command(
                &self.node_id,
                member,
                &shard.file_system_id,
                &shard_id,
                shard.epoch,
                target,
                write_set.generation,
            )
        };
        let mut target = epoch_start.clone();
        loop {
            let response = match sender(member, &probe(&target), None) {
                Ok(response) => response,
                Err(error) => {
                    return Ok(DfsWriteSetAlignment::NotReady {
                        reason: error.to_string(),
                    });
                }
            };
            let head = response.member_view.as_ref().map(|view| view.durable_prefix.clone());
            match (&response.result, head) {
                (DfsMetadataJournalBatchResult::Applied, Some(head)) if response.verified_prefix_only => {
                    self.metadata_backend
                        .as_deref_mut()
                        .ok_or_else(|| DfsError::Metadata {
                            message: "a write-set alignment requires the shard's durable backend".into(),
                        })?
                        .put_metadata_replication_cursor(member, &shard_id, head.journal_sequence)?;
                    return Ok(DfsWriteSetAlignment::Aligned { head });
                }
                (DfsMetadataJournalBatchResult::Applied, Some(head))
                    if head.journal_sequence < epoch_start.journal_sequence && target == epoch_start =>
                {
                    let own = self
                        .metadata_backend
                        .as_deref()
                        .ok_or_else(|| DfsError::Metadata {
                            message: "a write-set alignment requires the shard's durable backend".into(),
                        })?
                        .historical_metadata_prefix_proof(&shard_id, head.journal_sequence)?;
                    if own.as_ref() != Some(&head) {
                        return Ok(DfsWriteSetAlignment::NeedsSnapshot {
                            reason: format!(
                                "{member}'s head {} is not a prefix of this primary's journal",
                                head.journal_sequence
                            ),
                        });
                    }
                    // Confirm the checked head; the member records it at this epoch.
                    target = head;
                }
                (DfsMetadataJournalBatchResult::Rejected { code, reason }, _)
                    if code.allows_snapshot_repair() =>
                {
                    return Ok(DfsWriteSetAlignment::NeedsSnapshot {
                        reason: reason.clone(),
                    });
                }
                (result, _) => {
                    return Ok(DfsWriteSetAlignment::NotReady {
                        reason: format!("{member} did not align: {result:?}"),
                    });
                }
            }
        }
    }

    fn write_set_journal_head(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<crate::DfsMetadataPrefixProof, DfsError> {
        self.metadata_backend
            .as_deref()
            .ok_or_else(|| DfsError::Metadata {
                message: "write-set keeping requires the shard's durable backend".into(),
            })?
            .metadata_prefix_proof(shard_id)?
            .filter(crate::DfsMetadataPrefixProof::is_well_formed)
            .ok_or_else(|| DfsError::Metadata {
                message: format!("metadata shard {} has no journal head", shard_id.0),
            })
    }
}

/// Test support: current code records every content version's policy, so
/// content of unrecorded W exists only in stores written before versions
/// recorded it. A test names an object whose versions the switch evidence
/// then reads as such content.
#[cfg(test)]
static UNRECORDED_CONTENT_FOR_TEST: std::sync::LazyLock<
    std::sync::Mutex<BTreeSet<crate::DfsObjectId>>,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
pub(crate) fn treat_content_as_unrecorded_for_test(object_id: crate::DfsObjectId) {
    UNRECORDED_CONTENT_FOR_TEST
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(object_id);
}

#[cfg(test)]
fn unrecorded_content_for_test(object_id: &crate::DfsObjectId) -> bool {
    UNRECORDED_CONTENT_FOR_TEST
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .contains(object_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_initial_raised_n_covers_recorded_content_and_keeps_members_for_the_rest() {
        let evidence = |content_w, unrecorded| DfsWriteSetSwitchEvidence {
            members: BTreeSet::new(),
            content_w,
            unrecorded_content_versions: unrecorded,
        };
        assert_eq!(evidence(None, 0).initial_raised_n(3), None);
        assert_eq!(evidence(Some(2), 0).initial_raised_n(3), Some(2));
        assert_eq!(evidence(Some(4), 0).initial_raised_n(3), Some(4));
        assert!(!evidence(Some(4), 0).keeps_every_member());
        assert_eq!(evidence(None, 1).initial_raised_n(3), Some(3));
        assert_eq!(evidence(Some(2), 1).initial_raised_n(3), Some(3));
        assert_eq!(evidence(Some(5), 2).initial_raised_n(3), Some(5));
        assert!(evidence(None, 1).keeps_every_member());
    }
}
