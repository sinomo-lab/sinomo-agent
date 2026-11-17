//! A member's staged image of its shard's per-shard conversion, and its
//! local activation.
//!
//! The attempt's primary installs its sealed image on the members its
//! install rule names (`staged_service::conversion_install`). Each member
//! stages it under `<receiver root>/shard-conversions/<fs>/<shard>/<attempt>/`,
//! records the attempt and seal it staged in `staged.json` beside the image,
//! and registers a `MetadataGeneration` unit for the attempt's generation.
//!
//! The member registers that unit at its first effect for the attempt (the
//! floor challenge), so whatever it stages is always owned by the unit. The
//! unit reads the shard's serving, write-set and storage records in one
//! exact-set read and acts on the storage record:
//!
//! - **Active for this attempt and seal**, on a configured member that holds
//!   the staged image: the local copy moves aside to
//!   `.metadata-retained/shard-conversion-<attempt>`, where it is kept, the
//!   staged image takes its place, and `activate_metadata_shard_storage`
//!   turns its header Active under the record, as the primary's own
//!   activation does;
//! - **Converting for this attempt and seal**: the unit waits;
//! - **anything else** (another attempt, native genesis, no record, or an
//!   Active record of this attempt on a node that is no longer a configured
//!   member or never finished staging it): the stage holds nothing this node
//!   activates, and its image and ancestry evidence are deleted. A member
//!   that activates no image repairs by snapshot once the shard serves.
//!
//! The unit is woken only by changes to those three keys of its own shard;
//! nothing here owns a timer.
use super::*;
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsFileSystemId, DfsMetadataGenerationIdentity, DfsMetadataGenerationSeal,
    DfsMetadataShardConversion, DfsMetadataShardStorage, DfsStoragePreparation,
};

const STAGED_VERSION: u16 = 1;
const STAGED_RECORD: &str = "staged.json";
const MAX_STAGED_RECORD_BYTES: u64 = 64 * 1024;

/// What a member records once it holds a sealed image of an attempt.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct StagedConversion {
    pub version: u16,
    /// The attempt's primary, which sent the image.
    pub source_node_id: NodeId,
    pub generation: DfsMetadataGenerationIdentity,
    pub attempt_id: String,
    pub seal: DfsMetadataGenerationSeal,
}

impl StagedConversion {
    pub(in crate::authority) fn new(
        source_node_id: NodeId,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        attempt_id: &str,
        seal: DfsMetadataGenerationSeal,
    ) -> Self {
        Self {
            version: STAGED_VERSION,
            source_node_id,
            generation: sinomo_dfs::metadata_shard_converted_generation(
                file_system_id,
                shard_id,
                attempt_id,
            ),
            attempt_id: attempt_id.to_owned(),
            seal,
        }
    }

    fn read(stage: &Path) -> Result<Option<Self>> {
        let path = stage.join(STAGED_RECORD);
        if !path.try_exists().map_err(error)? {
            return Ok(None);
        }
        let bytes =
            sinomo_core::storage::durable_file::read_object_file_bytes(&path, MAX_STAGED_RECORD_BYTES)
                .map_err(error)?;
        let staged: Self = serde_json::from_slice(&bytes).map_err(error)?;
        if staged.version != STAGED_VERSION {
            return Err(error("staged conversion record has an unknown format"));
        }
        Ok(Some(staged))
    }

    pub(in crate::authority) fn record_name() -> &'static str {
        STAGED_RECORD
    }
}

/// The attempt a converted generation belongs to, or `None` for any other
/// generation (native genesis, or a complete-filesystem cohort's).
pub(in crate::authority) fn conversion_attempt_of(
    identity: &DfsMetadataGenerationIdentity,
) -> Option<&str> {
    let sinomo_dfs::DfsMetadataGenerationOrigin::StorageTransition { transition_id } =
        &identity.origin
    else {
        return None;
    };
    (sinomo_dfs::metadata_shard_converted_generation(
        &identity.file_system_id,
        &identity.shard_id.0,
        transition_id,
    ) == *identity)
        .then_some(transition_id.as_str())
}

/// Where a member stages one attempt of one shard.
pub(in crate::authority) fn conversion_stage_root(
    receiver_root: &Path,
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
    attempt_id: &str,
) -> Result<PathBuf> {
    let file_system = file_system_id.to_string();
    for segment in [file_system.as_str(), shard_id] {
        let path = Path::new(segment);
        if segment.is_empty()
            || segment.contains(['\\', ':', '\0'])
            || path.components().count() != 1
            || !matches!(path.components().next(), Some(std::path::Component::Normal(_)))
        {
            return Err(error("a staged conversion names an invalid path segment"));
        }
    }
    if attempt_id.len() != 64 || !attempt_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(error("a staged conversion attempt id is not a digest"));
    }
    Ok(receiver_root
        .join("shard-conversions")
        .join(file_system)
        .join(shard_id)
        .join(attempt_id))
}

/// What a member does with its staged image of one attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReceivedConversion {
    Activate,
    Wait,
    Superseded,
}

/// Decide from the shard's storage record. Only an Active record of exactly
/// this attempt and seal activates the image, and only on a configured
/// member that `holds_image` (staged, or already exchanged into place); a
/// Converting record of it waits; every other record supersedes the stage.
pub(super) fn received_conversion_action(
    attempt_id: &str,
    seal: &DfsMetadataGenerationSeal,
    shard: Option<&DfsMetadataShardConfig>,
    storage: Option<&DfsMetadataShardStorage>,
    local: &NodeId,
    holds_image: bool,
) -> ReceivedConversion {
    let names = |attempt: &DfsMetadataShardConversion| {
        attempt.attempt_id == attempt_id
            && attempt.sealed().is_some_and(|(sealed, _)| sealed == seal)
    };
    let Some(record) = storage else {
        return ReceivedConversion::Superseded;
    };
    if let Some((attempt, _)) = record.converted_activation()
        && names(attempt)
    {
        let member = shard.is_some_and(|shard| {
            shard.primary_node_id == *local || shard.replica_node_ids.contains(local)
        });
        return if member && holds_image {
            ReceivedConversion::Activate
        } else {
            ReceivedConversion::Superseded
        };
    }
    if record.converting_attempt().is_some_and(names) {
        return ReceivedConversion::Wait;
    }
    ReceivedConversion::Superseded
}

/// Apply `action` to the attempt staged at `stage`. Activation exchanges the
/// staged image with the local copy at `destination`, keeping the copy at
/// `retained`, then runs `activate`; a repeat after any interruption
/// finishes the same exchange. The stage is deleted once it holds nothing
/// the attempt still needs.
pub(super) fn settle_received_conversion(
    action: ReceivedConversion,
    stage: &Path,
    destination: &Path,
    retained: &Path,
    generation: &DfsMetadataGenerationIdentity,
    activate: impl FnOnce() -> Result<()>,
) -> Result<DfsStoragePreparation> {
    match action {
        ReceivedConversion::Wait => Ok(DfsStoragePreparation::Pending { processed_records: 0 }),
        ReceivedConversion::Superseded => {
            remove_stage(stage)?;
            Ok(DfsStoragePreparation::Ready)
        }
        ReceivedConversion::Activate => {
            super::shard_conversion::install_activated_image(
                destination,
                &stage.join("image"),
                retained,
                generation,
            )?;
            activate()?;
            remove_stage(stage)?;
            Ok(DfsStoragePreparation::Ready)
        }
    }
}

fn remove_stage(stage: &Path) -> Result<()> {
    match std::fs::remove_dir_all(stage) {
        Ok(()) => {}
        Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(failure) => return Err(error(failure)),
    }
    if let Some(parent) = stage.parent() {
        sinomo_core::storage::durable_file::sync_directory(parent).map_err(error)?;
    }
    Ok(())
}

/// Register the `MetadataGeneration` unit of an attempt this member staged.
/// Its job names the stage's record; the unit's own step does the rest.
pub(in crate::authority) fn enqueue_received_conversion(
    context: &sinomo_module::ComponentContext,
    staged: &StagedConversion,
    stage: &Path,
) -> Result<()> {
    let paths =
        sinomo_dfs::runtime_host::DfsRuntimePaths::for_host_role(context.data_dir(), context.host_role());
    let owner = OWNERS
        .get()
        .ok_or_else(|| error("receiver has no migration owner"))?
        .lock()
        .map_err(error)?
        .get(&paths.root)
        .and_then(Weak::upgrade)
        .ok_or_else(|| error("receiver migration owner is unavailable"))?;
    let scope = DfsStoragePreparationScope::MetadataGeneration {
        identity: staged.generation.clone(),
    };
    let destination = owner
        .authority
        .data_root
        .join("dfs/metadata")
        .join(&staged.generation.shard_id.0);
    let unit = inventory::UnitKey::from_scope(&owner.authority.data_root, &destination, &scope)?;
    let mut reference = owner
        .work
        .inventory
        .read(&unit)?
        .unwrap_or_else(|| inventory::UnitReference::discovered(unit.clone()));
    let job = inventory::JobReference {
        checkpoint: stage
            .join(STAGED_RECORD)
            .strip_prefix(&owner.authority.data_root)
            .map_err(error)?
            .to_owned(),
        identity: staged.attempt_id.clone(),
    };
    if reference.job.as_ref().is_some_and(|original| original != &job) {
        return Err(error("a staged conversion changed its generation unit's job"));
    }
    reference.job = Some(job);
    owner.work.inventory.record(&reference)?;
    let mut scheduler = owner.work.scheduler.lock().map_err(error)?;
    scheduler.recovered(reference)?;
    scheduler.enqueue(unit, false)?;
    drop(scheduler);
    owner.work.wake.notify_one();
    Ok(())
}

impl MigrationOwner {
    /// One step of the `MetadataGeneration` unit of a converted generation on
    /// a member the attempt installs on.
    pub(super) fn advance_received_conversion(
        &self,
        unit: &inventory::UnitKey,
        identity: &DfsMetadataGenerationIdentity,
        attempt_id: &str,
    ) -> Result<DfsStoragePreparation> {
        let fs = &identity.file_system_id;
        let shard_id = &identity.shard_id.0;
        let keys = vec![
            sinomo_dfs::metadata_shard_key(fs, shard_id),
            sinomo_dfs::metadata_shard_write_set_key(fs, shard_id),
            sinomo_dfs::metadata_shard_storage_key(fs, shard_id),
        ];
        // Watch first, so a change after this read wakes the parked unit.
        self.work
            .scheduler
            .lock()
            .map_err(error)?
            .watch_native_keys(unit, keys.iter().cloned().collect())?;
        let (entries, _) = self.authority.read_exact(&keys)?;
        let shard = entries[0]
            .entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardConfig>().map_err(error))
            .transpose()?;
        let storage = entries[2]
            .entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardStorage>().map_err(error))
            .transpose()?;
        if let (Some(shard), Some(record)) = (&shard, &storage) {
            record.validate_against(shard)?;
        }
        let stage = conversion_stage_root(&self.authority.receiver_root, fs, shard_id, attempt_id)?;
        let staged = StagedConversion::read(&stage)?;
        if let Some(staged) = &staged
            && (staged.generation != *identity || staged.attempt_id != attempt_id)
        {
            return Err(error("a staged conversion record names another generation"));
        }
        // The seal this member staged; once the stage is gone, the one the
        // record names for this attempt, so an interrupted exchange finishes.
        let holds_staged = staged.is_some();
        let seal = staged.map(|staged| staged.seal).or_else(|| {
            storage.as_ref().and_then(|record| {
                record
                    .converted_activation()
                    .map(|(attempt, _)| attempt)
                    .or_else(|| record.converting_attempt())
                    .filter(|attempt| attempt.attempt_id == attempt_id)
                    .and_then(|attempt| attempt.sealed().map(|(seal, _)| seal.clone()))
            })
        });
        let local = self.authority.local_node()?;
        let destination = self.metadata_root.join(shard_id);
        let holds_image = (holds_staged && stage.join("image/metadata-generation.sobj").is_file())
            || sinomo_dfs::inspect_metadata_storage_generation(&destination)
                .is_ok_and(|header| header.identity == *identity);
        let action = match &seal {
            Some(seal) => received_conversion_action(
                attempt_id,
                seal,
                shard.as_ref(),
                storage.as_ref(),
                &local,
                holds_image,
            ),
            None => ReceivedConversion::Superseded,
        };
        if action == ReceivedConversion::Activate {
            // A capture this node took of its own copy while it was the
            // shard's primary ends here, so the image is never read as a
            // source.
            self.metadata
                .finish_shard_conversion_capture(&destination, &identity.shard_id)?;
        }
        let retained = self
            .metadata_root
            .join(".metadata-retained")
            .join(format!("shard-conversion-{attempt_id}"));
        settle_received_conversion(action, &stage, &destination, &retained, identity, || {
            block_native(
                &self.authority.runtime,
                sinomo_dfs::activate_metadata_shard_storage(
                    &self.authority.component_context,
                    &destination,
                    identity,
                ),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).unwrap()
    }

    fn shard() -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            shard_id: "users".into(),
            primary_node_id: node("receiver-primary"),
            replica_node_ids: vec![node("receiver-member")],
            epoch: 3,
            assignment: sinomo_dfs::DfsMetadataShardAssignment::PathPrefix {
                prefix: "/users".into(),
            },
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn proof(digit: &str) -> sinomo_dfs::DfsMetadataPrefixProof {
        sinomo_dfs::DfsMetadataPrefixProof {
            version: 2,
            journal_sequence: 0,
            digest: format!("dfs-metadata-prefix-v2:blake3:{}", digit.repeat(64)),
        }
    }

    /// A sealed attempt of `shard`'s primary over a source at `sequence`.
    fn attempt(shard: &DfsMetadataShardConfig, sequence: u64, seal: DfsMetadataGenerationSeal) -> DfsMetadataShardConversion {
        let source = sinomo_dfs::DfsMetadataConversionSource {
            source_identity: "receiver-source".into(),
            source_format: Some(10),
            source_journal_sequence: sequence,
            source_state_fingerprint: "receiver-state".into(),
            source_cut_digest: "a".repeat(64),
            pending_mapping_digest: "b".repeat(64),
        };
        DfsMetadataShardConversion {
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
            seal: Some(seal),
            contributions_digest: Some("c".repeat(64)),
        }
    }

    fn active(shard: &DfsMetadataShardConfig, attempt: &DfsMetadataShardConversion) -> DfsMetadataShardStorage {
        let mut record = DfsMetadataShardStorage::converting(shard, attempt.clone());
        record.state = sinomo_dfs::DfsMetadataShardStorageState::Active {
            activation: sinomo_dfs::DfsMetadataShardActivation::Converted {
                activation_digest: sinomo_dfs::metadata_shard_storage_activation_digest(
                    &record.generation,
                    attempt,
                )
                .unwrap(),
                attempt: attempt.clone(),
            },
        };
        record.validate_against(shard).unwrap();
        record
    }

    /// Stage a sealed image of `attempt` as a member's install leaves it,
    /// beside a live copy whose bytes the test can recognise.
    fn stage(root: &Path, shard: &DfsMetadataShardConfig, attempt_id: &str) -> (PathBuf, PathBuf, DfsMetadataGenerationSeal) {
        let stage = root.join("receivers").join(attempt_id);
        let generation =
            sinomo_dfs::metadata_shard_converted_generation(&shard.file_system_id, &shard.shard_id, attempt_id);
        let writer =
            sinomo_dfs::DfsMetadataCandidateWriter::open_generation(stage.join("image"), generation).unwrap();
        let candidate = writer.write(Default::default(), proof("0")).unwrap();
        let seal = writer.seal_storage_generation(&candidate).unwrap();
        drop(writer);
        let record =
            StagedConversion::new(shard.primary_node_id.clone(), &shard.file_system_id, &shard.shard_id, attempt_id, seal.clone());
        std::fs::write(stage.join(STAGED_RECORD), serde_json::to_vec(&record).unwrap()).unwrap();
        assert_eq!(StagedConversion::read(&stage).unwrap(), Some(record));
        let destination = root.join("metadata/users");
        std::fs::create_dir_all(&destination).unwrap();
        std::fs::write(destination.join("old-copy"), b"the member's own copy").unwrap();
        (stage, destination, seal)
    }

    fn seal(digit: &str) -> DfsMetadataGenerationSeal {
        DfsMetadataGenerationSeal {
            prefix_proof: proof(digit),
            state_fingerprint: format!("state-{digit}"),
            file_fingerprint: format!("file-{digit}"),
        }
    }

    #[test]
    fn a_staged_image_waits_under_converting_and_activates_only_under_its_active_record() {
        let shard = shard();
        let member = node("receiver-member");
        let ours = attempt(&shard, 4, seal("1"));
        let converting = DfsMetadataShardStorage::converting(&shard, ours.clone());
        let action = |storage: Option<&DfsMetadataShardStorage>, seal: &DfsMetadataGenerationSeal| {
            received_conversion_action(&ours.attempt_id, seal, Some(&shard), storage, &member, true)
        };
        let staged = ours.seal.clone().unwrap();
        assert_eq!(action(Some(&converting), &staged), ReceivedConversion::Wait);
        assert_eq!(action(Some(&active(&shard, &ours)), &staged), ReceivedConversion::Activate);
        // An image staged with another seal is not the one the record activates.
        assert_eq!(action(Some(&active(&shard, &ours)), &seal("2")), ReceivedConversion::Superseded);
        assert_eq!(action(Some(&converting), &seal("2")), ReceivedConversion::Superseded);
        // A node the record's shard no longer counts as a member keeps nothing,
        // and neither does a member that never finished staging the image.
        assert_eq!(
            received_conversion_action(&ours.attempt_id, &staged, Some(&shard), Some(&active(&shard, &ours)), &node("outsider"), true),
            ReceivedConversion::Superseded
        );
        assert_eq!(
            received_conversion_action(&ours.attempt_id, &staged, Some(&shard), Some(&active(&shard, &ours)), &member, false),
            ReceivedConversion::Superseded
        );
        assert_eq!(
            received_conversion_action(&ours.attempt_id, &staged, Some(&shard), Some(&converting), &member, false),
            ReceivedConversion::Wait
        );

        // Only the Active record exchanges the copies and activates.
        let root = sinomo_testdir::TestDir::new("received-conversion-activation");
        let (stage, destination, image_seal) = stage(root.path(), &shard, &ours.attempt_id);
        let generation =
            sinomo_dfs::metadata_shard_converted_generation(&shard.file_system_id, &shard.shard_id, &ours.attempt_id);
        let retained = root.path().join("metadata/.metadata-retained/shard-conversion");
        let mut staged_ours = ours.clone();
        staged_ours.seal = Some(image_seal.clone());
        let converting = DfsMetadataShardStorage::converting(&shard, staged_ours.clone());
        let activated = std::cell::Cell::new(0);
        let settle = |storage: &DfsMetadataShardStorage| {
            let action =
                received_conversion_action(&ours.attempt_id, &image_seal, Some(&shard), Some(storage), &member, true);
            settle_received_conversion(action, &stage, &destination, &retained, &generation, || {
                activated.set(activated.get() + 1);
                Ok(())
            })
            .unwrap()
        };
        assert_eq!(settle(&converting), DfsStoragePreparation::Pending { processed_records: 0 });
        assert_eq!(activated.get(), 0);
        assert_eq!(std::fs::read(destination.join("old-copy")).unwrap(), b"the member's own copy");
        assert!(stage.join("image/metadata-generation.sobj").is_file(), "the staged image is kept while Converting");
        assert_eq!(settle(&active(&shard, &staged_ours)), DfsStoragePreparation::Ready);
        assert_eq!(activated.get(), 1);
        assert_eq!(sinomo_dfs::inspect_metadata_storage_generation(&destination).unwrap().identity, generation);
        assert_eq!(std::fs::read(retained.join("old-copy")).unwrap(), b"the member's own copy", "the local copy is kept");
        assert!(!stage.exists(), "nothing of the activated attempt stays staged");
    }

    #[test]
    fn a_superseded_staged_image_is_deleted() {
        let shard = shard();
        let member = node("receiver-member");
        let root = sinomo_testdir::TestDir::new("received-conversion-superseded");
        let stale_attempt = attempt(&shard, 4, seal("1"));
        let (stage, destination, image_seal) = stage(root.path(), &shard, &stale_attempt.attempt_id);
        std::fs::create_dir_all(stage.join("ancestry")).unwrap();
        std::fs::write(stage.join("ancestry/challenge.json"), b"{}").unwrap();
        let generation = sinomo_dfs::metadata_shard_converted_generation(
            &shard.file_system_id,
            &shard.shard_id,
            &stale_attempt.attempt_id,
        );
        // The epoch moved: the new primary's attempt is recorded Converting.
        let successor = DfsMetadataShardConfig {
            epoch: shard.epoch + 1,
            primary_node_id: member.clone(),
            replica_node_ids: vec![shard.primary_node_id.clone()],
            ..shard.clone()
        };
        let replacement = DfsMetadataShardStorage::converting(&successor, attempt(&successor, 4, seal("3")));
        let records = [
            Some(replacement),
            Some(DfsMetadataShardStorage::native_genesis(
                &successor,
                sinomo_dfs::DfsMetadataGenerationIdentity {
                    file_system_id: successor.file_system_id.clone(),
                    shard_id: sinomo_dfs::DfsMetadataShardId::new(&successor.shard_id),
                    generation: "native-genesis:users".into(),
                    origin: sinomo_dfs::DfsMetadataGenerationOrigin::NativeGenesis,
                },
            )),
            None,
        ];
        for record in &records {
            assert_eq!(
                received_conversion_action(&stale_attempt.attempt_id, &image_seal, Some(&successor), record.as_ref(), &member, true),
                ReceivedConversion::Superseded,
                "{record:?}"
            );
        }
        let action = received_conversion_action(
            &stale_attempt.attempt_id,
            &image_seal,
            Some(&successor),
            records[0].as_ref(),
            &member,
            true,
        );
        let retained = root.path().join("metadata/.metadata-retained/shard-conversion");
        let settled = settle_received_conversion(action, &stage, &destination, &retained, &generation, || {
            panic!("a superseded attempt never activates")
        })
        .unwrap();
        assert_eq!(settled, DfsStoragePreparation::Ready);
        assert!(!stage.exists(), "the superseded image and its ancestry evidence are deleted");
        assert!(stage.parent().unwrap().is_dir());
        assert_eq!(std::fs::read(destination.join("old-copy")).unwrap(), b"the member's own copy");
        assert!(!retained.exists());
    }

    #[test]
    fn only_converted_generations_name_an_attempt() {
        let shard = shard();
        let attempt_id = "d".repeat(64);
        let converted =
            sinomo_dfs::metadata_shard_converted_generation(&shard.file_system_id, &shard.shard_id, &attempt_id);
        assert_eq!(conversion_attempt_of(&converted), Some(attempt_id.as_str()));
        let mut cohort = converted.clone();
        cohort.generation = "filesystem-generation".into();
        assert_eq!(conversion_attempt_of(&cohort), None);
        let root = Path::new("/receivers");
        assert!(conversion_stage_root(root, &shard.file_system_id, "users", &attempt_id).is_ok());
        assert!(conversion_stage_root(root, &shard.file_system_id, "../users", &attempt_id).is_err());
        assert!(conversion_stage_root(root, &shard.file_system_id, "users", "not-a-digest").is_err());
    }
}
