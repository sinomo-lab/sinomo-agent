//! Handoff for the local metadata backend.

use super::*;

impl LocalSobjMetadataBackend {
    pub(super) fn selected_local_recovery_fence_present(&self) -> Result<bool> {
        crate::metadata_api::selected_local_recovery_fence_present(
            &self.membership_handoff_write_freeze_path(),
            self.selected_original_data_layout.as_ref().map(|(_, layout)| layout.as_ref()),
        )
    }

    pub(super) fn require_no_selected_local_recovery_fence(&self) -> Result<()> {
        crate::metadata_api::require_no_selected_local_recovery_fence(
            &self.membership_handoff_write_freeze_path(),
            self.selected_original_data_layout.as_ref().map(|(_, layout)| layout.as_ref()),
        )
    }

    #[cfg(test)]
    pub(super) fn install_local_recovery_fence(
        &mut self,
        reservation: &crate::control_plane::DfsMetadataShardRecoveryBallotRequest,
        context: &crate::metadata_api::DfsSelectedLocalRecoveryContext,
    ) -> Result<crate::metadata_api::DfsSelectedLocalRecoveryFenceData> {
        use crate::metadata_api::{DfsLocalRecoveryInventoryKind, DfsSelectedLocalRecoveryFence, SELECTED_LOCAL_RECOVERY_PREPARE_LIMIT};
        let invalid = |message: &str| MetadataBackendError::Rejected { code: crate::DfsMetadataRejectionCode::Other, message: message.into() };
        let canonical = crate::control_plane::DfsMetadataShardRecoveryBallotRequest::prepare(
            &reservation.serving_shard, &reservation.ballot.candidate_node_id, reservation.expected_attempt_floor,
        ).map_err(|reason| invalid(&format!("invalid selected recovery reservation: {reason:?}")))?;
        if &canonical != reservation || reservation.serving_shard.shard_id != self.shard_id.0 {
            return Err(invalid("selected recovery reservation differs from this shard"));
        }
        let (filesystem, layout) = self.selected_original_data_layout.as_ref()
            .ok_or_else(|| invalid("default metadata handle cannot install a selected recovery fence"))?;
        if filesystem != &reservation.serving_shard.file_system_id || !Arc::ptr_eq(layout, context.layout()) {
            return Err(invalid("selected recovery context belongs to another physical layout"));
        }
        let layout = Arc::clone(layout);
        let physical = context.physical_identity(filesystem, &self.shard_id, &self.root)?;
        if context.local_node_id() != &reservation.serving_shard.primary_node_id
            && !reservation.serving_shard.replica_node_ids.contains(context.local_node_id())
        { return Err(invalid("selected recovery acceptor is not an assigned member")); }
        // Do not enter an ordinary handoff reader while holding this lock:
        // recovery markers deliberately make that reader reject.
        let _lock = self.metadata_write_lock()?;
        let path = self.membership_handoff_write_freeze_path();
        let previous = crate::metadata_api::read_selected_local_recovery_fence(&path, &layout)?;
        if previous.is_none() && path.exists() {
            return Err(invalid("selected recovery refuses every cooperative handoff marker, including activated"));
        }
        if let Some(previous) = &previous {
            let old = &previous.fence;
            if old.physical != physical || &old.acceptor_node_id != context.local_node_id() {
                return Err(invalid("selected recovery marker has a foreign physical acceptor"));
            }
            if old.reservation != *reservation
                && (reservation.ballot.attempt <= old.reservation.ballot.attempt
                    || reservation.ballot.epoch < old.reservation.ballot.epoch
                    || reservation.serving_shard.epoch < old.reservation.serving_shard.epoch)
            { return Err(invalid("selected recovery request conflicts with or regresses the retained ballot")); }
        }
        self.refresh_after_metadata_write_lock()?;
        let prefix = self.prefix_proof.clone().ok_or_else(|| invalid("selected recovery has no exact materialized prefix"))?;
        if !prefix.is_well_formed() || prefix.journal_sequence != self.journal_last_sequence()
            || prefix.journal_sequence != self.materialized_through_sequence
        { return Err(invalid("selected recovery cut is not the complete certified local prefix")); }
        let mut unresolved_prepares = Vec::new();
        for record in self.tables.prepared_operations_v7.values().filter(|record| record.status == DfsPreparedOperationStatus::Prepared) {
            if unresolved_prepares.len() == SELECTED_LOCAL_RECOVERY_PREPARE_LIMIT {
                return Err(invalid("selected recovery unresolved prepare inventory exceeds its bound"));
            }
            record.validate()?;
            if record.file_system_id != reservation.serving_shard.file_system_id || record.shard_id != self.shard_id {
                return Err(invalid("selected recovery inventory contains a foreign prepared operation"));
            }
            unresolved_prepares.push(record.digest.clone());
        }
        if let Some(previous) = &previous {
            // No accepted/head-changing API exists in this first slice. Even
            // a higher reservation must preserve the immutable fenced cut.
            if previous.fence.prefix != prefix || previous.fence.unresolved_prepares != unresolved_prepares {
                return Err(invalid("selected recovery canonical cut changed while fenced"));
            }
        }
        let fence = DfsSelectedLocalRecoveryFence {
            reservation: reservation.clone(), acceptor_node_id: context.local_node_id().clone(), physical,
            prefix, unresolved_prepares, inventory_kind: DfsLocalRecoveryInventoryKind::LegacyPrimaryAuthoredV7,
        };
        fence.validate()?;
        // A visible but uncertified publication stays fail-closed. Do not
        // restore an instance binding when a later namespace/post-check fails.
        self.instance_authority_epoch = None;
        let data = crate::metadata_api::write_selected_local_recovery_fence(&path, &fence, &layout)?;
        context.check()?;
        Ok(data)
    }

    pub(super) fn membership_handoff_write_freeze_path(&self) -> PathBuf {
        self.root
            .join("journal")
            .join("membership-handoff-write-freeze.sobj")
    }

    pub(super) fn membership_handoff_write_freeze(
        &self,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        crate::metadata_api::read_handoff_freeze_with_layout(
            &self.membership_handoff_write_freeze_path(),
            self.selected_original_data_layout.as_ref().map(|(_, layout)| layout.as_ref()),
        )
    }

    pub(super) fn freeze_for_membership_handoff(
        &mut self,
        handoff_id: &str,
        retiring_epoch: u64,
    ) -> Result<DfsMetadataShardWriteFreeze> {
        if handoff_id.is_empty() {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} membership write freeze has an empty handoff id",
                self.shard_id.0
            )));
        }
        let _lock = self.begin_metadata_write_allow_frozen()?;
        let existing = self.membership_handoff_write_freeze()?;
        if let Some(existing) = existing.as_ref() {
            let same_pending_handoff = existing.activated_epoch.is_none()
                && existing.handoff_id == handoff_id
                && existing.retiring_epoch == retiring_epoch;
            // A completed marker is a durable fence, not an authority grant.
            // A later control-plane-authorized handoff may supersede it even
            // when this backend instance deliberately stayed unbound after
            // recording the prior activation. Installing the new pending
            // marker below clears any instance binding again.
            let replacing_completed_generation = existing
                .activated_epoch
                .is_some_and(|activated_epoch| activated_epoch <= retiring_epoch);
            if !same_pending_handoff && !replacing_completed_generation {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata shard {} is already fenced by membership handoff {} retiring epoch {} active epoch {:?}",
                    self.shard_id.0,
                    existing.handoff_id,
                    existing.retiring_epoch,
                    existing.activated_epoch
                )));
            }
        }
        let journal_sequence = self.journal_last_sequence();
        let prefix_proof = self.prefix_proof.as_ref().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} membership freeze has no exact prefix proof",
                self.shard_id.0
            ))
        })?;
        if !prefix_proof.is_well_formed() || prefix_proof.journal_sequence != journal_sequence {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} membership freeze prefix sequence {} does not match head {}",
                self.shard_id.0, prefix_proof.journal_sequence, journal_sequence
            )));
        }
        let state_fingerprint = prefix_proof.digest.clone();
        if let Some(existing) = existing.as_ref()
            && (journal_sequence < existing.journal_sequence
                || (journal_sequence == existing.journal_sequence
                    && state_fingerprint != existing.state_fingerprint))
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} membership freeze state regressed or diverged at sequence {}",
                self.shard_id.0, journal_sequence
            )));
        }
        let freeze = DfsMetadataShardWriteFreeze {
            handoff_id: handoff_id.to_string(),
            retiring_epoch,
            activated_epoch: None,
            journal_sequence,
            state_fingerprint,
            rebind_prefix_proof: prefix_proof.clone(),
        };
        crate::metadata_api::write_handoff_freeze_with_layout(
                &self.membership_handoff_write_freeze_path(), &freeze,
                self.selected_original_data_layout.as_ref().map(|(_, layout)| layout.as_ref()),
            )?;
        self.instance_authority_epoch = None;
        Ok(freeze)
    }

    pub(super) fn mark_membership_handoff_write_freeze_activated_unlocked(
        &mut self,
        handoff_id: &str,
        activated_epoch: u64,
        rebound_prefix_proof: Option<&DfsMetadataPrefixProof>,
    ) -> Result<bool> {
        let Some(existing) = self.membership_handoff_write_freeze()? else {
            return Ok(false);
        };
        if existing.handoff_id != handoff_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} membership freeze {} does not match completed handoff {}",
                self.shard_id.0, existing.handoff_id, handoff_id
            )));
        }
        let expected_activated_epoch = existing.retiring_epoch.checked_add(1).ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} membership handoff {} retiring epoch is exhausted",
                self.shard_id.0, handoff_id
            ))
        })?;
        if activated_epoch != expected_activated_epoch {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} membership handoff {} retiring epoch {} can activate only exact successor epoch {}, not {}",
                self.shard_id.0,
                handoff_id,
                existing.retiring_epoch,
                expected_activated_epoch,
                activated_epoch
            )));
        }
        if existing
            .activated_epoch
            .is_some_and(|current| current != activated_epoch)
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} cannot advance membership handoff {} from retiring epoch {} active epoch {:?} to epoch {}",
                self.shard_id.0,
                handoff_id,
                existing.retiring_epoch,
                existing.activated_epoch,
                activated_epoch
            )));
        }
        let mut activated = existing.clone();
        activated.activated_epoch = Some(activated_epoch);
        // Primary rebind is the proof-validation boundary. Persist its exact
        // current prefix in the same atomic marker update as activation, so a
        // later current append restarts from the same certified checkpoint.
        if let Some(rebound_prefix_proof) = rebound_prefix_proof {
            if !rebound_prefix_proof.is_well_formed()
                || rebound_prefix_proof.journal_sequence < activated.journal_sequence
                || (rebound_prefix_proof.journal_sequence == activated.journal_sequence
                    && rebound_prefix_proof.digest != activated.state_fingerprint)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata shard {} membership handoff {} cannot checkpoint rebound prefix {} from marker prefix {}",
                    self.shard_id.0,
                    handoff_id,
                    rebound_prefix_proof.journal_sequence,
                    activated.journal_sequence
                )));
            }
            let previous = &activated.rebind_prefix_proof;
            if rebound_prefix_proof.journal_sequence < previous.journal_sequence
                || (rebound_prefix_proof.journal_sequence == previous.journal_sequence
                    && rebound_prefix_proof.digest != previous.digest)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata shard {} membership handoff {} rebound prefix regressed or diverged at sequence {}",
                    self.shard_id.0, handoff_id, rebound_prefix_proof.journal_sequence
                )));
            }
            activated.rebind_prefix_proof = rebound_prefix_proof.clone();
        }
        if activated != existing {
            crate::metadata_api::write_handoff_freeze_with_layout(
                &self.membership_handoff_write_freeze_path(), &activated,
                self.selected_original_data_layout.as_ref().map(|(_, layout)| layout.as_ref()),
            )?;
        }
        Ok(true)
    }

    pub(super) fn mark_membership_handoff_write_freeze_activated(
        &mut self,
        handoff_id: &str,
        activated_epoch: u64,
    ) -> Result<bool> {
        let _lock = self.begin_metadata_write_allow_frozen()?;
        self.mark_membership_handoff_write_freeze_activated_unlocked(
            handoff_id,
            activated_epoch,
            None,
        )
    }

    pub(super) fn clear_membership_handoff_write_freeze(
        &mut self,
        handoff_id: &str,
        active_shard: &DfsMetadataShardConfig,
        checked_prefix: &DfsMetadataPrefixProof,
    ) -> Result<bool> {
        let lock = self.begin_metadata_write_allow_frozen()?;
        let rebound_prefix_proof = self.prefix_proof.clone().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} membership handoff {} cannot bind without an exact current prefix proof",
                self.shard_id.0, handoff_id
            ))
        })?;
        if !rebound_prefix_proof.is_well_formed()
            || rebound_prefix_proof.journal_sequence != self.journal_last_sequence()
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} membership handoff {} current prefix sequence {} does not match durable head {}",
                self.shard_id.0,
                handoff_id,
                rebound_prefix_proof.journal_sequence,
                self.journal_last_sequence()
            )));
        }
        if !checked_prefix.is_well_formed() || rebound_prefix_proof != *checked_prefix {
            return Err(MetadataBackendError::prefix_mismatch(format!(
                "metadata shard {} membership handoff {} prefix changed before writer rebind: checked sequence {} current sequence {}",
                self.shard_id.0,
                handoff_id,
                checked_prefix.journal_sequence,
                rebound_prefix_proof.journal_sequence,
            )));
        }
        let previous_instance_authority_epoch =
            self.instance_authority_epoch.replace(active_shard.epoch);
        let marker_activated = self.mark_membership_handoff_write_freeze_activated_unlocked(
            handoff_id,
            active_shard.epoch,
            Some(&rebound_prefix_proof),
        );
        match marker_activated {
            Ok(true) => {}
            Ok(false) => {
                self.instance_authority_epoch = previous_instance_authority_epoch;
                return Ok(false);
            }
            Err(error) => {
                self.instance_authority_epoch = previous_instance_authority_epoch;
                return Err(error);
            }
        }
        // Rebind owns the actual writer while replaying durable current
        // decisions. It never authors a new terminal operation for a hidden
        // prepare, and independently opened retiring instances stay fenced.
        let recovery = self.recover_unmaterialized_local_journal_entries();
        if let Err(error) = recovery {
            self.instance_authority_epoch = previous_instance_authority_epoch;
            return Err(error);
        }
        drop(lock);
        Ok(true)
    }
}
