//! Per-shard storage record: the current storage generation one metadata shard
//! serves and the evidence that activated it.
//!
//! The record is storage identity, separate from serving authority. Genesis
//! writes it, and only a per-shard conversion changes it afterwards. Takeover,
//! rotation, recruit and removal never write or compare it. An absent record
//! names storage that the migration owner still has to convert.
use super::*;
use crate::{DfsMetadataShardAssignment, DfsMetadataShardConfig};
use sinomo_core::node::NodeId;

/// Persisted format of [`DfsMetadataShardStorage`]. Readers reject any other
/// value, so a record written by a later protocol fails closed.
pub const DFS_METADATA_SHARD_STORAGE_FORMAT: u16 = 1;

/// The storage record of one metadata shard, stored at
/// [`crate::metadata_shard_storage_key`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataShardStorage {
    pub format: u16,
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    /// The shard's storage identity: the path assignment its store holds.
    pub assignment: DfsMetadataShardAssignment,
    /// The generation the shard serves (Active) or is converting to.
    pub generation: DfsMetadataGenerationIdentity,
    /// Table snapshot format of `generation`.
    pub table_format: u16,
    pub state: DfsMetadataShardStorageState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsMetadataShardStorageState {
    /// One conversion attempt has fixed its target image. The shard stays
    /// closed: no holder may serve `generation` until it is Active.
    Converting { attempt: DfsMetadataShardConversion },
    Active { activation: DfsMetadataShardActivation },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsMetadataShardActivation {
    /// Born current. The committed genesis terminal in the shard's own tables
    /// certifies the header's activation digest.
    NativeGenesis,
    /// Converted by the attempt; every holder's header carries this digest.
    Converted {
        attempt: DfsMetadataShardConversion,
        activation_digest: String,
    },
}

/// One per-shard conversion attempt. Its identity binds the serving record it
/// runs under, so a takeover or rotation supersedes it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataShardConversion {
    pub attempt_id: String,
    pub epoch: u64,
    pub primary_node_id: NodeId,
    pub source: DfsMetadataConversionSource,
    /// The exact current image every holder installs and activates.
    pub seal: DfsMetadataGenerationSeal,
    /// Digest of the count contributions the image was built from.
    pub contributions_digest: String,
}

/// The source copy one attempt converts. The shard is closed while it
/// converts, so these facts are fixed for the attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataConversionSource {
    pub source_identity: String,
    pub source_format: Option<u16>,
    pub source_journal_sequence: u64,
    pub source_state_fingerprint: String,
    pub source_cut_digest: String,
    pub pending_mapping_digest: String,
}

fn blake3_digest(value: &impl Serialize) -> Result<String> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn invalid(message: &str) -> MetadataBackendError {
    MetadataBackendError::Serde(format!("metadata shard storage record: {message}"))
}

/// Identity of one conversion attempt: the shard's storage identity, the
/// serving record it runs under and the source copy it converts. A restart on
/// the same primary at the same epoch derives the same attempt.
pub fn metadata_shard_conversion_attempt_id(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
    assignment: &DfsMetadataShardAssignment,
    epoch: u64,
    primary_node_id: &NodeId,
    source: &DfsMetadataConversionSource,
) -> Result<String> {
    blake3_digest(&(
        "dfs-shard-conversion-attempt-v1",
        file_system_id,
        shard_id,
        assignment,
        epoch,
        primary_node_id,
        &source.source_identity,
        source.source_journal_sequence,
        &source.source_state_fingerprint,
    ))
}

/// The generation identity a conversion attempt produces.
pub fn metadata_shard_converted_generation(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
    attempt_id: &str,
) -> DfsMetadataGenerationIdentity {
    DfsMetadataGenerationIdentity {
        file_system_id: file_system_id.clone(),
        shard_id: crate::DfsMetadataShardId::new(shard_id),
        generation: format!(
            "converted:{file_system_id}:{shard_id}:{}:{attempt_id}",
            crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
        ),
        origin: DfsMetadataGenerationOrigin::StorageTransition {
            transition_id: attempt_id.to_owned(),
        },
    }
}

/// Activation digest every holder of a converted generation records.
pub fn metadata_shard_storage_activation_digest(
    generation: &DfsMetadataGenerationIdentity,
    attempt: &DfsMetadataShardConversion,
) -> Result<String> {
    Ok(format!(
        "blake3:{}",
        blake3_digest(&("dfs-shard-storage-activation-v1", generation, attempt))?
    ))
}

/// The native-genesis identity of a shard born current. `activation` names a
/// filesystem-level generation the birth binds to; it exists only while the
/// filesystem-level storage state does.
pub(crate) fn native_genesis_generation_identity(
    shard: &DfsMetadataShardConfig,
    activation: Option<&crate::DfsMetadataStorageActivation>,
) -> DfsMetadataGenerationIdentity {
    DfsMetadataGenerationIdentity {
        file_system_id: shard.file_system_id.clone(),
        shard_id: crate::DfsMetadataShardId::new(shard.shard_id.clone()),
        generation: activation
            .map(|activation| activation.generation.clone())
            .unwrap_or_else(|| {
                format!(
                    "native-genesis:{}:{}:{}",
                    shard.file_system_id,
                    shard.shard_id,
                    crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
                )
            }),
        origin: activation
            .map(
                |activation| DfsMetadataGenerationOrigin::NativeGenesisInGeneration {
                    activation: activation.clone(),
                },
            )
            .unwrap_or(DfsMetadataGenerationOrigin::NativeGenesis),
    }
}

impl DfsMetadataShardStorage {
    /// The record genesis writes for a shard born current.
    pub fn native_genesis(
        shard: &DfsMetadataShardConfig,
        generation: DfsMetadataGenerationIdentity,
    ) -> Self {
        Self {
            format: DFS_METADATA_SHARD_STORAGE_FORMAT,
            file_system_id: shard.file_system_id.clone(),
            shard_id: shard.shard_id.clone(),
            assignment: shard.assignment.clone(),
            generation,
            table_format: crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            state: DfsMetadataShardStorageState::Active {
                activation: DfsMetadataShardActivation::NativeGenesis,
            },
        }
    }

    /// A Converting record for `attempt`, which must already carry its seal.
    pub fn converting(
        shard: &DfsMetadataShardConfig,
        attempt: DfsMetadataShardConversion,
    ) -> Self {
        Self {
            format: DFS_METADATA_SHARD_STORAGE_FORMAT,
            file_system_id: shard.file_system_id.clone(),
            shard_id: shard.shard_id.clone(),
            assignment: shard.assignment.clone(),
            generation: metadata_shard_converted_generation(
                &shard.file_system_id,
                &shard.shard_id,
                &attempt.attempt_id,
            ),
            table_format: crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            state: DfsMetadataShardStorageState::Converting { attempt },
        }
    }

    /// A Converting record of a well-formed attempt under `shard`'s current
    /// serving record, for tests.
    #[cfg(test)]
    pub(crate) fn converting_for_test(shard: &DfsMetadataShardConfig, sequence: u64) -> Self {
        let source = DfsMetadataConversionSource {
            source_identity: "retained-source".into(),
            source_format: Some(10),
            source_journal_sequence: sequence,
            source_state_fingerprint: "source-state".into(),
            source_cut_digest: "a".repeat(64),
            pending_mapping_digest: "b".repeat(64),
        };
        let attempt_id = metadata_shard_conversion_attempt_id(
            &shard.file_system_id, &shard.shard_id, &shard.assignment, shard.epoch,
            &shard.primary_node_id, &source,
        )
        .expect("serializable attempt");
        Self::converting(shard, DfsMetadataShardConversion {
            attempt_id,
            epoch: shard.epoch,
            primary_node_id: shard.primary_node_id.clone(),
            source,
            seal: DfsMetadataGenerationSeal {
                prefix_proof: crate::DfsMetadataPrefixProof {
                    version: 2,
                    journal_sequence: sequence,
                    digest: format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64)),
                },
                state_fingerprint: "image-state".into(),
                file_fingerprint: "image-file".into(),
            },
            contributions_digest: "c".repeat(64),
        })
    }

    /// The record's own shape. A record that fails this is not storage
    /// authority for anything.
    pub fn validate(&self) -> Result<()> {
        if self.format != DFS_METADATA_SHARD_STORAGE_FORMAT {
            return Err(invalid("unknown record format"));
        }
        self.generation.validate()?;
        if self.generation.file_system_id != self.file_system_id
            || self.generation.shard_id.0 != self.shard_id
        {
            return Err(invalid("generation names another shard"));
        }
        if self.table_format != crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION {
            return Err(invalid("generation is not the current table format"));
        }
        let attempt = match &self.state {
            DfsMetadataShardStorageState::Active {
                activation: DfsMetadataShardActivation::NativeGenesis,
            } => {
                if !self.generation.is_native_genesis() {
                    return Err(invalid("native-genesis activation names a converted generation"));
                }
                return Ok(());
            }
            DfsMetadataShardStorageState::Converting { attempt } => attempt,
            DfsMetadataShardStorageState::Active {
                activation:
                    DfsMetadataShardActivation::Converted {
                        attempt,
                        activation_digest,
                    },
            } => {
                if *activation_digest
                    != metadata_shard_storage_activation_digest(&self.generation, attempt)?
                {
                    return Err(invalid("activation digest differs from its attempt"));
                }
                attempt
            }
        };
        let expected_attempt = metadata_shard_conversion_attempt_id(
            &self.file_system_id,
            &self.shard_id,
            &self.assignment,
            attempt.epoch,
            &attempt.primary_node_id,
            &attempt.source,
        )?;
        if attempt.attempt_id != expected_attempt
            || self.generation
                != metadata_shard_converted_generation(
                    &self.file_system_id,
                    &self.shard_id,
                    &attempt.attempt_id,
                )
            || attempt.seal.prefix_proof.version != 2
            || !attempt.seal.prefix_proof.is_well_formed()
            || attempt.seal.state_fingerprint.is_empty()
            || attempt.seal.file_fingerprint.is_empty()
            || attempt.contributions_digest.is_empty()
        {
            return Err(invalid("conversion attempt differs from its derived identity"));
        }
        Ok(())
    }

    /// [`Self::validate`] plus the binding to the shard's storage identity.
    pub fn validate_against(&self, shard: &DfsMetadataShardConfig) -> Result<()> {
        self.validate()?;
        if self.file_system_id != shard.file_system_id
            || self.shard_id != shard.shard_id
            || self.assignment != shard.assignment
        {
            return Err(invalid("record names another storage identity"));
        }
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, DfsMetadataShardStorageState::Active { .. })
    }

    /// The Converting attempt, if any.
    pub fn converting_attempt(&self) -> Option<&DfsMetadataShardConversion> {
        match &self.state {
            DfsMetadataShardStorageState::Converting { attempt } => Some(attempt),
            DfsMetadataShardStorageState::Active { .. } => None,
        }
    }

    /// Whether a physical header may serve under this record: the record is
    /// Active, names the header's generation, and a converted generation's
    /// header carries the record's activation digest. A native-genesis
    /// header's digest is checked against its own committed genesis at open.
    pub fn admits_header(&self, header: &DfsMetadataGenerationHeader) -> bool {
        if header.identity != self.generation {
            return false;
        }
        match (&self.state, &header.state) {
            (
                DfsMetadataShardStorageState::Active {
                    activation: DfsMetadataShardActivation::NativeGenesis,
                },
                DfsMetadataGenerationState::Active { standalone_seal: None, .. },
            ) => true,
            (
                DfsMetadataShardStorageState::Active {
                    activation:
                        DfsMetadataShardActivation::Converted {
                            activation_digest, ..
                        },
                },
                DfsMetadataGenerationState::Active {
                    activation_digest: actual,
                    standalone_seal: None,
                },
            ) => actual == activation_digest,
            _ => false,
        }
    }

    /// Activation digest of an Active record's generation. A native-genesis
    /// digest comes from the shard's own committed genesis.
    pub(crate) fn activation_digest(
        &self,
        tables: &crate::metadata_tables::DfsMetadataTables,
    ) -> Result<String> {
        match &self.state {
            DfsMetadataShardStorageState::Active {
                activation: DfsMetadataShardActivation::NativeGenesis,
            } => native_genesis_activation_digest(&self.generation, tables),
            DfsMetadataShardStorageState::Active {
                activation:
                    DfsMetadataShardActivation::Converted {
                        activation_digest, ..
                    },
            } => Ok(activation_digest.clone()),
            DfsMetadataShardStorageState::Converting { .. } => {
                Err(invalid("a converting shard has no active generation"))
            }
        }
    }
}

#[cfg(feature = "storage-conversion")]
pub use transactions::*;

#[cfg(feature = "storage-conversion")]
mod transactions {
    //! The two per-shard storage transactions. Each compares only the shard's
    //! own keys: its serving record, write set, pending handoff and storage
    //! record, plus the member records the caller admits. Neither compares nor
    //! writes the filesystem configuration or its catalog generation.
    use super::*;
    use sinomo_core::kv::{KvCompare, KvEntry, KvError, KvOp, KvRequestId, KvTxn, KvTxnRequest};

    type KvResult<T> = std::result::Result<T, KvError>;

    fn refuse(detail: impl std::fmt::Display) -> KvError {
        KvError::Serialization {
            detail: format!("metadata shard storage transaction refused: {detail}"),
        }
    }

    fn exact(key: String, entry: Option<&KvEntry>) -> KvResult<KvCompare> {
        match entry {
            Some(entry) if entry.key != key => Err(refuse("an entry is bound to another key")),
            Some(entry) => Ok(KvCompare::ValueEquals { key, value: entry.value.clone() }),
            None => Ok(KvCompare::Missing { key }),
        }
    }

    /// The shard's own keys, read in one exact-key-set authoritative read.
    pub struct DfsMetadataShardStorageCut<'a> {
        pub shard: &'a KvEntry,
        pub write_set: Option<&'a KvEntry>,
        pub handoff: Option<&'a KvEntry>,
        pub storage: Option<&'a KvEntry>,
        /// Exact member and capability-policy compares for the nodes the
        /// caller admits (install set or acknowledgements).
        pub member_compares: Vec<KvCompare>,
    }

    impl DfsMetadataShardStorageCut<'_> {
        fn shard_config(&self) -> KvResult<DfsMetadataShardConfig> {
            let shard: DfsMetadataShardConfig = self.shard.decode()?;
            if self.shard.key != crate::metadata_shard_key(&shard.file_system_id, &shard.shard_id) {
                return Err(refuse("the shard entry is bound to another key"));
            }
            Ok(shard)
        }

        fn compares(&self, shard: &DfsMetadataShardConfig) -> KvResult<Vec<KvCompare>> {
            let fs = &shard.file_system_id;
            if self.handoff.is_some() {
                return Err(refuse("a membership handoff is pending on the shard"));
            }
            let mut compares = vec![
                exact(crate::metadata_shard_key(fs, &shard.shard_id), Some(self.shard))?,
                exact(crate::metadata_shard_write_set_key(fs, &shard.shard_id), self.write_set)?,
                KvCompare::Missing {
                    key: crate::metadata_shard_membership_handoff_key(fs, &shard.shard_id),
                },
                exact(crate::metadata_shard_storage_key(fs, &shard.shard_id), self.storage)?,
            ];
            compares.extend(self.member_compares.iter().cloned());
            Ok(compares)
        }

        fn current_storage(&self, shard: &DfsMetadataShardConfig) -> KvResult<Option<DfsMetadataShardStorage>> {
            self.storage
                .map(|entry| {
                    let record: DfsMetadataShardStorage = entry.decode()?;
                    record
                        .validate_against(shard)
                        .map_err(refuse)?;
                    Ok(record)
                })
                .transpose()
        }
    }

    /// Fix one attempt's target in the shard's storage record. The attempt
    /// must run under the current serving record. It replaces only an absent
    /// record or a Converting record of a superseded attempt (another epoch or
    /// primary); the same attempt is idempotent.
    pub fn build_metadata_shard_storage_converting(
        cut: &DfsMetadataShardStorageCut<'_>,
        record: &DfsMetadataShardStorage,
    ) -> KvResult<KvTxnRequest> {
        let shard = cut.shard_config()?;
        record.validate_against(&shard).map_err(refuse)?;
        let attempt = record
            .converting_attempt()
            .ok_or_else(|| refuse("the record is not a Converting attempt"))?;
        if attempt.epoch != shard.epoch || attempt.primary_node_id != shard.primary_node_id {
            return Err(refuse("the attempt does not run under the current serving record"));
        }
        match cut.current_storage(&shard)? {
            None => {}
            Some(current) if current.is_active() => {
                return Err(refuse("the shard's storage is already active"));
            }
            Some(current) => {
                let previous = current.converting_attempt().expect("not active");
                let superseded = previous.epoch != shard.epoch
                    || previous.primary_node_id != shard.primary_node_id;
                if previous.attempt_id != attempt.attempt_id && !superseded {
                    return Err(refuse("another attempt converts the shard at this epoch"));
                }
                if previous.attempt_id == attempt.attempt_id && current != *record {
                    return Err(refuse("the attempt changed its fixed target"));
                }
            }
        }
        let txn = KvTxn {
            compares: cut.compares(&shard)?,
            success: vec![KvOp::put_serde(
                crate::metadata_shard_storage_key(&shard.file_system_id, &shard.shard_id),
                record,
                None,
            )?],
            failure: Vec::new(),
        };
        let identity = serde_json::to_vec(&(&attempt.attempt_id, &attempt.seal))
            .map_err(refuse)?;
        Ok(KvTxnRequest::new(
            KvRequestId::derived("dfs-shard-storage-converting-v1", identity),
            txn,
        ))
    }

    /// Activate the shard's Converting attempt. The caller has established
    /// that its install rule holds for the current serving record; this
    /// transaction makes that observation exact.
    pub fn build_metadata_shard_storage_activate(
        cut: &DfsMetadataShardStorageCut<'_>,
    ) -> KvResult<(KvTxnRequest, DfsMetadataShardStorage)> {
        let shard = cut.shard_config()?;
        let current = cut
            .current_storage(&shard)?
            .ok_or_else(|| refuse("the shard has no Converting attempt"))?;
        let attempt = current
            .converting_attempt()
            .ok_or_else(|| refuse("the shard's storage is already active"))?
            .clone();
        if attempt.epoch != shard.epoch || attempt.primary_node_id != shard.primary_node_id {
            return Err(refuse("the attempt was superseded by a serving change"));
        }
        let activation_digest =
            metadata_shard_storage_activation_digest(&current.generation, &attempt).map_err(refuse)?;
        let mut active = current.clone();
        active.state = DfsMetadataShardStorageState::Active {
            activation: DfsMetadataShardActivation::Converted {
                attempt: attempt.clone(),
                activation_digest,
            },
        };
        active.validate_against(&shard).map_err(refuse)?;
        let txn = KvTxn {
            compares: cut.compares(&shard)?,
            success: vec![KvOp::put_serde(
                crate::metadata_shard_storage_key(&shard.file_system_id, &shard.shard_id),
                &active,
                None,
            )?],
            failure: Vec::new(),
        };
        let identity = serde_json::to_vec(&(
            &attempt.attempt_id,
            blake3::hash(&cut.shard.value).to_hex().to_string(),
            cut.write_set.map(|entry| blake3::hash(&entry.value).to_hex().to_string()),
        ))
        .map_err(refuse)?;
        Ok((
            KvTxnRequest::new(KvRequestId::derived("dfs-shard-storage-activate-v1", identity), txn),
            active,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(epoch: u64, primary: &str) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "users".into(),
            primary_node_id: NodeId::from_legacy_named(primary).unwrap(),
            replica_node_ids: vec![NodeId::from_legacy_named("replica-b").unwrap()],
            epoch,
            assignment: DfsMetadataShardAssignment::PathPrefix { prefix: "/users".into() },
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn source(sequence: u64) -> DfsMetadataConversionSource {
        DfsMetadataConversionSource {
            source_identity: "retained-source".into(),
            source_format: Some(10),
            source_journal_sequence: sequence,
            source_state_fingerprint: "source-state".into(),
            source_cut_digest: "a".repeat(64),
            pending_mapping_digest: "b".repeat(64),
        }
    }

    pub(super) fn attempt_for(shard: &DfsMetadataShardConfig, sequence: u64) -> DfsMetadataShardConversion {
        let source = source(sequence);
        DfsMetadataShardConversion {
            attempt_id: metadata_shard_conversion_attempt_id(
                &shard.file_system_id, &shard.shard_id, &shard.assignment, shard.epoch,
                &shard.primary_node_id, &source,
            ).unwrap(),
            epoch: shard.epoch,
            primary_node_id: shard.primary_node_id.clone(),
            source,
            seal: DfsMetadataGenerationSeal {
                prefix_proof: crate::DfsMetadataPrefixProof {
                    version: 2,
                    journal_sequence: sequence,
                    digest: format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64)),
                },
                state_fingerprint: "image-state".into(),
                file_fingerprint: "image-file".into(),
            },
            contributions_digest: "c".repeat(64),
        }
    }

    #[test]
    fn storage_record_validation_binds_its_attempt_generation_and_digest() {
        let shard = shard(4, "primary-a");
        let genesis = DfsMetadataShardStorage::native_genesis(
            &shard, native_genesis_generation_identity(&shard, None),
        );
        genesis.validate_against(&shard).unwrap();
        // A converted generation cannot claim native-genesis activation.
        let attempt = attempt_for(&shard, 7);
        let converting = DfsMetadataShardStorage::converting(&shard, attempt.clone());
        converting.validate_against(&shard).unwrap();
        let mut forged = genesis.clone();
        forged.generation = converting.generation.clone();
        assert!(forged.validate().is_err());
        // The attempt id is derived: another epoch or primary is another attempt.
        let mut renamed = converting.clone();
        if let DfsMetadataShardStorageState::Converting { attempt } = &mut renamed.state {
            attempt.epoch += 1;
        }
        assert!(renamed.validate().is_err());
        // The activation digest binds the attempt.
        let digest = metadata_shard_storage_activation_digest(&converting.generation, &attempt).unwrap();
        let mut active = converting.clone();
        active.state = DfsMetadataShardStorageState::Active {
            activation: DfsMetadataShardActivation::Converted { attempt: attempt.clone(), activation_digest: digest },
        };
        active.validate_against(&shard).unwrap();
        let mut wrong_digest = active.clone();
        if let DfsMetadataShardStorageState::Active {
            activation: DfsMetadataShardActivation::Converted { activation_digest, .. },
        } = &mut wrong_digest.state {
            activation_digest.push('0');
        }
        assert!(wrong_digest.validate().is_err());
        // Another storage identity never binds the record.
        let mut moved = shard.clone();
        moved.assignment = DfsMetadataShardAssignment::PathPrefix { prefix: "/elsewhere".into() };
        assert!(active.validate_against(&moved).is_err());
        // A serving change keeps the binding.
        let mut taken_over = shard.clone();
        taken_over.epoch += 1;
        taken_over.primary_node_id = NodeId::from_legacy_named("replica-b").unwrap();
        active.validate_against(&taken_over).unwrap();
        let mut unknown = active.clone();
        unknown.format += 1;
        assert!(unknown.validate().is_err());
    }

    #[test]
    fn only_an_active_record_admits_its_exact_header() {
        let shard = shard(2, "primary-a");
        let attempt = attempt_for(&shard, 3);
        let converting = DfsMetadataShardStorage::converting(&shard, attempt.clone());
        let digest = metadata_shard_storage_activation_digest(&converting.generation, &attempt).unwrap();
        let header = DfsMetadataGenerationHeader {
            format_version: crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            identity: converting.generation.clone(),
            state: DfsMetadataGenerationState::Active { activation_digest: digest.clone(), standalone_seal: None },
        };
        assert!(!converting.admits_header(&header), "a converting shard stays closed");
        let mut active = converting.clone();
        active.state = DfsMetadataShardStorageState::Active {
            activation: DfsMetadataShardActivation::Converted { attempt, activation_digest: digest },
        };
        assert!(active.admits_header(&header));
        let mut staged = header.clone();
        staged.state = DfsMetadataGenerationState::Staged { seal: None };
        assert!(!active.admits_header(&staged));
        let mut other_digest = header.clone();
        other_digest.state = DfsMetadataGenerationState::Active {
            activation_digest: format!("blake3:{}", "f".repeat(64)), standalone_seal: None,
        };
        assert!(!active.admits_header(&other_digest));
        let genesis_identity = native_genesis_generation_identity(&shard, None);
        let genesis = DfsMetadataShardStorage::native_genesis(&shard, genesis_identity.clone());
        assert!(!genesis.admits_header(&header), "another generation is not admitted");
        let genesis_header = DfsMetadataGenerationHeader {
            identity: genesis_identity,
            ..header
        };
        assert!(genesis.admits_header(&genesis_header));
    }
}

#[cfg(all(test, feature = "storage-conversion"))]
mod transaction_tests {
    use super::tests::attempt_for;
    use super::*;
    use sinomo_core::kv::{KvCompare, KvEntry};

    fn entry(key: String, value: &impl Serialize) -> KvEntry {
        KvEntry {
            key,
            value: sinomo_core::kv::encode_kv_value(value).unwrap(),
            create_revision: sinomo_core::kv::KvRevision(1),
            modify_revision: sinomo_core::kv::KvRevision(1),
            version: sinomo_core::kv::KvVersion(1),
            lease_id: None,
        }
    }

    fn shard(epoch: u64, primary: &str) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "users".into(),
            primary_node_id: NodeId::from_legacy_named(primary).unwrap(),
            replica_node_ids: vec![NodeId::from_legacy_named("replica-b").unwrap()],
            epoch,
            assignment: DfsMetadataShardAssignment::PathPrefix { prefix: "/users".into() },
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn shard_entry(shard: &DfsMetadataShardConfig) -> KvEntry {
        entry(crate::metadata_shard_key(&shard.file_system_id, &shard.shard_id), shard)
    }

    fn storage_entry(record: &DfsMetadataShardStorage) -> KvEntry {
        entry(crate::metadata_shard_storage_key(&record.file_system_id, &record.shard_id), record)
    }

    #[test]
    fn converting_fixes_one_attempt_under_the_current_serving_record() {
        let current = shard(3, "primary-a");
        let shard_kv = shard_entry(&current);
        let record = DfsMetadataShardStorage::converting(&current, attempt_for(&current, 9));
        let cut = DfsMetadataShardStorageCut {
            shard: &shard_kv, write_set: None, handoff: None, storage: None, member_compares: Vec::new(),
        };
        let request = build_metadata_shard_storage_converting(&cut, &record).unwrap();
        let storage_key = crate::metadata_shard_storage_key(&current.file_system_id, &current.shard_id);
        assert!(request.txn.compares.contains(&KvCompare::Missing { key: storage_key.clone() }));
        assert!(request.txn.compares.contains(&KvCompare::ValueEquals {
            key: shard_kv.key.clone(), value: shard_kv.value.clone(),
        }));
        assert!(request.txn.compares.iter().all(|compare| match compare {
            KvCompare::ValueEquals { key, .. } | KvCompare::Missing { key } => !key.ends_with("/config"),
            _ => true,
        }), "a per-shard storage transaction never compares the filesystem configuration");
        assert_eq!(request.txn.success.len(), 1, "it writes only the shard's storage record");
        // The same attempt is idempotent and keeps its request identity.
        let existing = storage_entry(&record);
        let again = build_metadata_shard_storage_converting(&DfsMetadataShardStorageCut {
            storage: Some(&existing), ..cut
        }, &record).unwrap();
        assert_eq!(again.request_id, request.request_id);
        // Another attempt at the same epoch and primary is refused.
        let rival = DfsMetadataShardStorage::converting(&current, attempt_for(&current, 10));
        assert!(build_metadata_shard_storage_converting(&DfsMetadataShardStorageCut {
            shard: &shard_kv, write_set: None, handoff: None, storage: Some(&existing), member_compares: Vec::new(),
        }, &rival).is_err());
        // An attempt built under an older serving record is refused.
        let successor = shard(4, "replica-b");
        let successor_kv = shard_entry(&successor);
        assert!(build_metadata_shard_storage_converting(&DfsMetadataShardStorageCut {
            shard: &successor_kv, write_set: None, handoff: None, storage: None, member_compares: Vec::new(),
        }, &record).is_err());
        // A successor's attempt replaces the superseded Converting record.
        let replacement = DfsMetadataShardStorage::converting(&successor, attempt_for(&successor, 9));
        let replaced = build_metadata_shard_storage_converting(&DfsMetadataShardStorageCut {
            shard: &successor_kv, write_set: None, handoff: None, storage: Some(&existing), member_compares: Vec::new(),
        }, &replacement).unwrap();
        assert!(replaced.txn.compares.contains(&KvCompare::ValueEquals {
            key: storage_key, value: existing.value.clone(),
        }));
        // A pending handoff refuses both transactions.
        let handoff = entry(crate::metadata_shard_membership_handoff_key(&current.file_system_id, &current.shard_id), &"pending");
        assert!(build_metadata_shard_storage_converting(&DfsMetadataShardStorageCut {
            shard: &shard_kv, write_set: None, handoff: Some(&handoff), storage: None, member_compares: Vec::new(),
        }, &record).is_err());
    }

    #[test]
    fn activation_requires_the_attempt_under_its_own_serving_record() {
        let current = shard(3, "primary-a");
        let shard_kv = shard_entry(&current);
        let attempt = attempt_for(&current, 9);
        let record = DfsMetadataShardStorage::converting(&current, attempt.clone());
        let existing = storage_entry(&record);
        let cut = DfsMetadataShardStorageCut {
            shard: &shard_kv, write_set: None, handoff: None, storage: Some(&existing), member_compares: Vec::new(),
        };
        let (request, active) = build_metadata_shard_storage_activate(&cut).unwrap();
        active.validate_against(&current).unwrap();
        assert!(active.is_active());
        assert!(matches!(&active.state, DfsMetadataShardStorageState::Active {
            activation: DfsMetadataShardActivation::Converted { attempt: activated, .. },
        } if *activated == attempt));
        assert!(request.txn.compares.contains(&KvCompare::ValueEquals {
            key: existing.key.clone(), value: existing.value.clone(),
        }));
        // A recruit at the same epoch keeps the attempt but needs a new request.
        let mut recruited = current.clone();
        recruited.replica_node_ids.push(NodeId::from_legacy_named("replica-c").unwrap());
        let recruited_kv = shard_entry(&recruited);
        let (rebuilt, _) = build_metadata_shard_storage_activate(&DfsMetadataShardStorageCut {
            shard: &recruited_kv, ..cut
        }).unwrap();
        assert_ne!(rebuilt.request_id, request.request_id);
        // A takeover supersedes it.
        let taken_over = shard(4, "replica-b");
        let taken_over_kv = shard_entry(&taken_over);
        assert!(build_metadata_shard_storage_activate(&DfsMetadataShardStorageCut {
            shard: &taken_over_kv, write_set: None, handoff: None, storage: Some(&existing), member_compares: Vec::new(),
        }).is_err());
        // Nothing to activate without a Converting attempt.
        assert!(build_metadata_shard_storage_activate(&DfsMetadataShardStorageCut {
            shard: &shard_kv, write_set: None, handoff: None, storage: None, member_compares: Vec::new(),
        }).is_err());
    }
}
