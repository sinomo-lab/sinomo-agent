//! Handoff for the local metadata backend.

use super::*;

impl LocalSobjMetadataBackend {

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
