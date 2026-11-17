//! The old copy a per-shard conversion keeps beside its activated image.
//!
//! Local activation, on the shard's primary and on every member that
//! activates the installed image, moves the shard's old copy to
//! `.metadata-retained/shard-conversion-<attempt>` and puts the image in its
//! place. That copy is the shard's retained original. It is recorded as a
//! `RetainedOriginal` artifact once the shard's storage record is Active for
//! that attempt and the local copy holds that attempt's activated generation;
//! the decision reads the shard's storage record, not a filesystem cohort.
//! Recording grants no serving or collection permit.
use super::*;
use sinomo_dfs::{DfsMetadataGenerationHeader, DfsMetadataShardStorage};

/// The attempt whose retained original sits beside the local copy described
/// by `header`: the copy holds an Active per-shard converted generation, and
/// `record`, the shard's storage record, activated that generation.
pub(super) fn retained_conversion_attempt(
    header: &DfsMetadataGenerationHeader,
    record: Option<&DfsMetadataShardStorage>,
) -> Option<String> {
    let identity = &header.identity;
    let sinomo_dfs::DfsMetadataGenerationOrigin::StorageTransition { transition_id } =
        &identity.origin
    else {
        return None;
    };
    if *identity
        != sinomo_dfs::metadata_shard_converted_generation(
            &identity.file_system_id,
            &identity.shard_id.0,
            transition_id,
        )
        || !matches!(
            header.state,
            sinomo_dfs::DfsMetadataGenerationState::Active { .. }
        )
    {
        return None;
    }
    let record = record?;
    let (attempt, _) = record.converted_activation()?;
    (record.generation == *identity && attempt.attempt_id == *transition_id)
        .then(|| transition_id.clone())
}

fn job_reference(
    owner: &MigrationOwner,
    destination: &Path,
    header: &DfsMetadataGenerationHeader,
) -> Result<inventory::JobReference> {
    Ok(inventory::JobReference {
        checkpoint: destination
            .join("metadata-generation.sobj")
            .strip_prefix(&owner.authority.data_root)
            .map_err(error)?
            .to_owned(),
        identity: digest(&(
            "dfs-shard-conversion-retained-original-v1",
            &header.identity,
        ))?,
    })
}

impl MigrationOwner {
    /// The attempt whose retained original sits beside `destination`, from
    /// its local header and the shard's storage record.
    fn retained_conversion_of(
        &self,
        destination: &Path,
    ) -> Result<Option<(DfsMetadataGenerationHeader, String)>> {
        let Ok(header) = sinomo_dfs::inspect_metadata_storage_generation(destination) else {
            return Ok(None);
        };
        if !matches!(
            header.identity.origin,
            sinomo_dfs::DfsMetadataGenerationOrigin::StorageTransition { .. }
        ) {
            return Ok(None);
        }
        let key = sinomo_dfs::metadata_shard_storage_key(
            &header.identity.file_system_id,
            &header.identity.shard_id.0,
        );
        let (entries, _) = self.authority.read_exact(std::slice::from_ref(&key))?;
        let record = entries[0]
            .entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardStorage>().map_err(error))
            .transpose()?;
        Ok(retained_conversion_attempt(&header, record.as_ref()).map(|attempt| (header, attempt)))
    }

    fn retained_conversion_root(&self, attempt_id: &str) -> PathBuf {
        self.metadata_root
            .join(".metadata-retained")
            .join(format!("shard-conversion-{attempt_id}"))
    }

    /// Record the retained original beside `destination` when its copy holds
    /// an activated per-shard conversion. Returns whether it does.
    pub(super) fn record_shard_conversion_original(&self, destination: &Path) -> Result<bool> {
        let Some((header, attempt_id)) = self.retained_conversion_of(destination)? else {
            return Ok(false);
        };
        let retained = self.retained_conversion_root(&attempt_id);
        if !retained.is_dir() {
            // This copy kept no old copy, as a member without one does.
            return Ok(true);
        }
        let job = job_reference(self, destination, &header)?;
        let unit = inventory::UnitKey {
            relative_root: retained
                .strip_prefix(&self.authority.data_root)
                .map_err(error)?
                .to_owned(),
            role: inventory::UnitRole::RetainedArtifact,
        };
        let mut reference = self
            .work
            .inventory
            .read(&unit)?
            .unwrap_or_else(|| inventory::UnitReference::discovered(unit.clone()));
        if reference
            .job
            .as_ref()
            .is_some_and(|original| original != &job)
        {
            return Err(error(
                "a retained conversion original changed its activated generation",
            ));
        }
        reference.job = Some(job);
        reference.state = inventory::ObservedState::RetainedOriginal;
        self.work.inventory.record(&reference)?;
        self.work
            .scheduler
            .lock()
            .map_err(error)?
            .finish(&unit.id()?, reference);
        Ok(true)
    }

    /// Whether `reference` is the retained original of a per-shard
    /// conversion whose shard is still Active for that conversion.
    pub(super) fn shard_conversion_original_observed(
        &self,
        reference: &inventory::UnitReference,
    ) -> Result<bool> {
        let Some(job) = &reference.job else {
            return Ok(false);
        };
        if job
            .checkpoint
            .file_name()
            .is_none_or(|name| name != "metadata-generation.sobj")
        {
            return Ok(false);
        }
        let marker = self.authority.data_root.join(&job.checkpoint);
        let destination = marker
            .parent()
            .ok_or_else(|| error("retained conversion original has no destination"))?;
        let Some((header, attempt_id)) = self.retained_conversion_of(destination)? else {
            return Ok(false);
        };
        Ok(*job == job_reference(self, destination, &header)?
            && self.authority.data_root.join(&reference.unit.relative_root)
                == self.retained_conversion_root(&attempt_id))
    }

    /// Whether the retained original beside `destination` is recorded.
    #[cfg(test)]
    pub(in crate::authority) fn shard_conversion_original_recorded(
        &self,
        destination: &Path,
    ) -> Result<bool> {
        let Some((_, attempt_id)) = self.retained_conversion_of(destination)? else {
            return Ok(false);
        };
        let unit = inventory::UnitKey {
            relative_root: self
                .retained_conversion_root(&attempt_id)
                .strip_prefix(&self.authority.data_root)
                .map_err(error)?
                .to_owned(),
            role: inventory::UnitRole::RetainedArtifact,
        };
        let Some(reference) = self.work.inventory.read(&unit)? else {
            return Ok(false);
        };
        Ok(
            reference.state == inventory::ObservedState::RetainedOriginal
                && self.shard_conversion_original_observed(&reference)?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard() -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            shard_id: "users".into(),
            primary_node_id: sinomo_core::node::NodeId::from_legacy_named("retained-primary")
                .unwrap(),
            replica_node_ids: Vec::new(),
            epoch: 2,
            assignment: sinomo_dfs::DfsMetadataShardAssignment::PathPrefix {
                prefix: "/users".into(),
            },
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn attempt(
        shard: &DfsMetadataShardConfig,
        sealed: bool,
    ) -> sinomo_dfs::DfsMetadataShardConversion {
        let proof = sinomo_dfs::DfsMetadataPrefixProof {
            version: 2,
            journal_sequence: 0,
            digest: format!("dfs-metadata-prefix-v2:blake3:{}", "1".repeat(64)),
        };
        let source = sinomo_dfs::DfsMetadataConversionSource {
            source_identity: "retained-source".into(),
            source_format: Some(10),
            source_journal_sequence: 1,
            source_state_fingerprint: "retained-state".into(),
            source_cut_digest: "a".repeat(64),
            pending_mapping_digest: "b".repeat(64),
        };
        sinomo_dfs::DfsMetadataShardConversion {
            attempt_id: sinomo_dfs::metadata_shard_conversion_attempt_id(
                &shard.file_system_id,
                &shard.shard_id,
                &shard.assignment,
                shard.epoch,
                &shard.primary_node_id,
                &source,
                None,
            )
            .unwrap(),
            epoch: shard.epoch,
            primary_node_id: shard.primary_node_id.clone(),
            takeover: None,
            source,
            seal: sealed.then(|| sinomo_dfs::DfsMetadataGenerationSeal {
                prefix_proof: proof,
                state_fingerprint: "c".repeat(64),
                file_fingerprint: "d".repeat(64),
            }),
            contributions_digest: sealed.then(|| "e".repeat(64)),
        }
    }

    /// The retained original is decided from the shard's storage record: only
    /// an Active record of the same attempt, over a local copy holding that
    /// attempt's Active generation, names it.
    #[test]
    fn the_retained_original_is_decided_from_the_storage_record() {
        let shard = shard();
        let sealed = attempt(&shard, true);
        let converting = DfsMetadataShardStorage::converting(&shard, sealed.clone());
        let mut active = converting.clone();
        active.state = sinomo_dfs::DfsMetadataShardStorageState::Active {
            activation: sinomo_dfs::DfsMetadataShardActivation::Converted {
                attempt: sealed.clone(),
                activation_digest: "f".repeat(64),
            },
        };
        let generation = sinomo_dfs::metadata_shard_converted_generation(
            &shard.file_system_id,
            &shard.shard_id,
            &sealed.attempt_id,
        );
        let header = |identity: sinomo_dfs::DfsMetadataGenerationIdentity, activated: bool| {
            DfsMetadataGenerationHeader {
                format_version: 1,
                identity,
                state: if activated {
                    sinomo_dfs::DfsMetadataGenerationState::Active {
                        activation_digest: "f".repeat(64),
                        standalone_seal: None,
                    }
                } else {
                    sinomo_dfs::DfsMetadataGenerationState::Staged {
                        seal: sealed.seal.clone(),
                    }
                },
            }
        };
        let activated = header(generation.clone(), true);
        assert_eq!(
            retained_conversion_attempt(&activated, Some(&active)),
            Some(sealed.attempt_id.clone())
        );
        assert_eq!(
            retained_conversion_attempt(&activated, Some(&converting)),
            None,
            "not yet Active"
        );
        assert_eq!(retained_conversion_attempt(&activated, None), None);
        let other = attempt(
            &DfsMetadataShardConfig {
                epoch: 3,
                ..shard.clone()
            },
            true,
        );
        let mut superseded = active.clone();
        superseded.state = sinomo_dfs::DfsMetadataShardStorageState::Active {
            activation: sinomo_dfs::DfsMetadataShardActivation::Converted {
                attempt: other,
                activation_digest: "f".repeat(64),
            },
        };
        assert_eq!(
            retained_conversion_attempt(&activated, Some(&superseded)),
            None,
            "another attempt activated"
        );
        let staged = header(generation, false);
        assert_eq!(
            retained_conversion_attempt(&staged, Some(&active)),
            None,
            "the local copy is not activated"
        );
    }
}
