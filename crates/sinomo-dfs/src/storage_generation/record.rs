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
///
/// The attempt is recorded Converting before its image exists, so its target
/// is fixed before any count contribution is read. The image's seal and the
/// contributions digest are added together once the image is built
/// (`build_metadata_shard_storage_seal`); an Active attempt always has both.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataShardConversion {
    pub attempt_id: String,
    /// Epoch of the serving record the attempt runs under.
    pub epoch: u64,
    /// The node that converts from its own copy: the shard's primary, or for
    /// a takeover attempt the member that becomes primary when it activates.
    pub primary_node_id: NodeId,
    /// Present when a member converts a closed shard whose primary is lost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub takeover: Option<DfsMetadataConversionTakeover>,
    pub source: DfsMetadataConversionSource,
    /// The exact current image every holder installs and activates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seal: Option<DfsMetadataGenerationSeal>,
    /// Digest of the count contributions the image was built from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contributions_digest: Option<String>,
}

/// A converting takeover: the attempt's converter is a member other than the
/// primary, and the attempt's activation makes it primary at the next epoch.
///
/// Takeovers of one lost primary at one epoch run in rounds. The first round
/// replaces at most the lost primary's own attempt. Once a round is
/// recorded, only the next round replaces it, run by another member once the
/// recorded converter is judged lost; so rounds only increase, and each
/// recorded round has exactly one converter.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataConversionTakeover {
    /// The lost primary: the shard's primary at the attempt's epoch.
    pub previous_primary: NodeId,
    /// The round at the epoch, from 1.
    pub round: u64,
    /// The converters of earlier rounds at the epoch, other than this
    /// round's own, in ascending order. They were judged lost when their
    /// round was replaced, so this round does not count on them: on a
    /// switched shard its promises and its install do not wait for them.
    pub lost_converters: Vec<NodeId>,
}

impl DfsMetadataConversionTakeover {
    /// The first round of a takeover of `previous_primary`.
    pub fn first_round(previous_primary: NodeId) -> Self {
        Self { previous_primary, round: 1, lost_converters: Vec::new() }
    }

    /// The round after this one, run by `converter` in the place of this
    /// round's lost converter `replaced`: every converter of an earlier round
    /// except `converter` itself is lost to it.
    pub fn next_round(&self, replaced: &NodeId, converter: &NodeId) -> Self {
        let lost_converters = self
            .lost_converters
            .iter()
            .chain(std::iter::once(replaced))
            .filter(|node| *node != converter)
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        Self { previous_primary: self.previous_primary.clone(), round: self.round + 1, lost_converters }
    }

    /// The takeover `converter` runs under `shard` and its storage record:
    /// the round after the takeover recorded at the epoch, or the first round.
    pub fn for_converter(
        shard: &DfsMetadataShardConfig,
        record: Option<&DfsMetadataShardStorage>,
        converter: &NodeId,
    ) -> Self {
        match record.and_then(|record| record.epoch_takeover(shard)) {
            Some(recorded) => recorded
                .takeover
                .as_ref()
                .expect("an epoch takeover is a takeover")
                .next_round(&recorded.primary_node_id, converter),
            None => Self::first_round(shard.primary_node_id.clone()),
        }
    }

    /// The round's own shape: rounds start at 1, the first has no earlier
    /// converter, and a later one has at least the one it replaced and at
    /// most one per earlier round, in canonical order, never naming its own
    /// converter or the lost primary.
    fn validate(&self, converter: &NodeId) -> Result<()> {
        let lost = &self.lost_converters;
        let well_formed = match self.round {
            0 => false,
            1 => lost.is_empty(),
            round => !lost.is_empty() && (lost.len() as u64) < round,
        } && lost.windows(2).all(|pair| pair[0] < pair[1])
            && !lost.contains(converter)
            && !lost.contains(&self.previous_primary)
            && self.previous_primary != *converter;
        if !well_formed {
            return Err(invalid("malformed takeover round"));
        }
        Ok(())
    }
}

impl DfsMetadataShardConversion {
    /// The image seal and contributions digest, once the attempt is sealed.
    pub fn sealed(&self) -> Option<(&DfsMetadataGenerationSeal, &str)> {
        match (&self.seal, &self.contributions_digest) {
            (Some(seal), Some(digest)) => Some((seal, digest)),
            _ => None,
        }
    }
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
/// serving record it runs under, its converter and the source copy it
/// converts. A restart on the same primary at the same epoch derives the same
/// attempt. A takeover attempt also binds the lost primary, its round and the
/// earlier converters it does not count on under its own domain, so it never
/// equals an attempt of the primary itself or of another round.
pub fn metadata_shard_conversion_attempt_id(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
    assignment: &DfsMetadataShardAssignment,
    epoch: u64,
    primary_node_id: &NodeId,
    source: &DfsMetadataConversionSource,
    takeover: Option<&DfsMetadataConversionTakeover>,
) -> Result<String> {
    match takeover {
        None => blake3_digest(&(
            "dfs-shard-conversion-attempt-v1",
            file_system_id,
            shard_id,
            assignment,
            epoch,
            primary_node_id,
            &source.source_identity,
            source.source_journal_sequence,
            &source.source_state_fingerprint,
        )),
        Some(takeover) => blake3_digest(&(
            "dfs-shard-conversion-takeover-attempt-v1",
            file_system_id,
            shard_id,
            assignment,
            epoch,
            primary_node_id,
            &takeover.previous_primary,
            takeover.round,
            &takeover.lost_converters,
            &source.source_identity,
            source.source_journal_sequence,
            &source.source_state_fingerprint,
        )),
    }
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

/// Activation digest every holder of a converted generation records. It
/// covers the sealed attempt, so it binds the exact image and contributions;
/// an unsealed attempt has none.
pub fn metadata_shard_storage_activation_digest(
    generation: &DfsMetadataGenerationIdentity,
    attempt: &DfsMetadataShardConversion,
) -> Result<String> {
    if attempt.sealed().is_none() {
        return Err(invalid("an unsealed conversion attempt cannot activate"));
    }
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

    /// A Converting record for `attempt`, sealed or not.
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

    /// A sealed Converting record of a well-formed attempt under `shard`'s
    /// current serving record, for tests.
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
            &shard.primary_node_id, &source, None,
        )
        .expect("serializable attempt");
        Self::converting(shard, DfsMetadataShardConversion {
            attempt_id,
            epoch: shard.epoch,
            primary_node_id: shard.primary_node_id.clone(),
            takeover: None,
            source,
            seal: Some(DfsMetadataGenerationSeal {
                prefix_proof: crate::DfsMetadataPrefixProof {
                    version: 2,
                    journal_sequence: sequence,
                    digest: format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64)),
                },
                state_fingerprint: "image-state".into(),
                file_fingerprint: "image-file".into(),
            }),
            contributions_digest: Some("c".repeat(64)),
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
            attempt.takeover.as_ref(),
        )?;
        if attempt.attempt_id != expected_attempt
            || self.generation
                != metadata_shard_converted_generation(
                    &self.file_system_id,
                    &self.shard_id,
                    &attempt.attempt_id,
                )
        {
            return Err(invalid("conversion attempt differs from its derived identity"));
        }
        if let Some(takeover) = &attempt.takeover {
            takeover.validate(&attempt.primary_node_id)?;
        }
        // An unsealed attempt is valid only while Converting; the Active
        // branch above already refused one through its activation digest.
        match (&attempt.seal, &attempt.contributions_digest) {
            (None, None) => {}
            (Some(seal), Some(contributions_digest))
                if seal.prefix_proof.version == 2
                    && seal.prefix_proof.is_well_formed()
                    && !seal.state_fingerprint.is_empty()
                    && !seal.file_fingerprint.is_empty()
                    && !contributions_digest.is_empty() => {}
            _ => return Err(invalid("conversion attempt has a malformed or partial seal")),
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

    /// The takeover attempt recorded at `shard`'s epoch in the place of its
    /// current primary, whether or not the serving record still admits its
    /// converter. Once one is recorded, the epoch is the takeover's: only its
    /// next round replaces it there, and the primary does not convert.
    pub fn epoch_takeover(&self, shard: &DfsMetadataShardConfig) -> Option<&DfsMetadataShardConversion> {
        self.converting_attempt().filter(|attempt| {
            attempt.epoch == shard.epoch
                && attempt
                    .takeover
                    .as_ref()
                    .is_some_and(|takeover| takeover.previous_primary == shard.primary_node_id)
        })
    }

    /// The attempt that activated a converted generation, with its
    /// activation digest.
    pub fn converted_activation(&self) -> Option<(&DfsMetadataShardConversion, &str)> {
        match &self.state {
            DfsMetadataShardStorageState::Active {
                activation:
                    DfsMetadataShardActivation::Converted {
                        attempt,
                        activation_digest,
                    },
            } => Some((attempt, activation_digest)),
            _ => None,
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
    //! The per-shard storage transactions. Each compares only the shard's own
    //! keys: its serving record, write set, pending handoff and storage
    //! record, plus the member records the caller admits. None compares or
    //! writes the filesystem configuration or its catalog generation.
    //!
    //! An attempt is recorded Converting unsealed, then sealed with its
    //! image, then activated. A primary's own attempt activates with
    //! [`build_metadata_shard_storage_activate`]. A takeover attempt activates
    //! with [`build_metadata_shard_storage_takeover_activate`], which also
    //! makes its converter primary.
    use super::*;
    use crate::DfsMetadataShardWriteSetState;
    use sinomo_core::kv::{KvCompare, KvEntry, KvError, KvOp, KvRequestId, KvTxn, KvTxnRequest};
    use std::collections::BTreeSet;

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

    /// Request identity of one attempt's transaction against one exact cut.
    /// The same cut always gives the same identity, so a lost outcome is
    /// settled by resubmitting the same request; a definite rejection spends
    /// only that cut's identity, and a re-plan from the changed cut that
    /// caused it gets a new one.
    fn cut_request_id(
        domain: &'static str,
        identity: &impl Serialize,
        compares: &[KvCompare],
    ) -> KvResult<KvRequestId> {
        let bytes = serde_json::to_vec(&(identity, compares)).map_err(refuse)?;
        Ok(KvRequestId::derived(domain, bytes))
    }

    fn extend_unique(compares: &mut Vec<KvCompare>, more: Vec<KvCompare>) {
        for compare in more {
            if !compares.contains(&compare) {
                compares.push(compare);
            }
        }
    }

    /// The shard's own keys, read in one exact-key-set authoritative read.
    pub struct DfsMetadataShardStorageCut<'a> {
        pub shard: &'a KvEntry,
        pub write_set: Option<&'a KvEntry>,
        pub handoff: Option<&'a KvEntry>,
        /// The shard's head watermark. Only a takeover activation reads it:
        /// it compares and deletes the watermark as a takeover swap does.
        pub watermark: Option<&'a KvEntry>,
        pub storage: Option<&'a KvEntry>,
        /// Exact member and capability-policy compares for the nodes the
        /// caller admits (install set or acknowledgements).
        pub member_compares: Vec<KvCompare>,
        /// Records of every configured member. The transactions of a takeover
        /// attempt check each member's capabilities themselves
        /// ([`Self::takeover_member_compares`]).
        pub members: &'a [DfsMetadataShardStorageMember<'a>],
    }

    /// One member's records, read with the shard's keys.
    pub struct DfsMetadataShardStorageMember<'a> {
        pub node_id: NodeId,
        /// The entry at [`sinomo_core::node::cluster_member_key`].
        pub member: Option<&'a KvEntry>,
        /// The entry at [`sinomo_core::node::cluster_capability_policy_key`].
        pub policy: Option<&'a KvEntry>,
    }

    impl DfsMetadataShardStorageMember<'_> {
        /// Whether the node left the cluster: it has no member record, or
        /// its record names it and the cluster no longer authorizes it. This
        /// is the departed verdict of the write-set keeper and the takeover
        /// planner, which take a member without a record as departed and a
        /// record's member as departed once it is not authorized. A record
        /// that does not decode, names another node or sits at another key is
        /// not a departure; it fails every capability check instead.
        pub fn departed(&self) -> bool {
            match self.member {
                None => true,
                Some(entry) => {
                    entry.key == sinomo_core::node::cluster_member_key(&self.node_id)
                        && entry
                            .decode::<sinomo_core::ClusterMemberRecord>()
                            .is_ok_and(|record| record.node_id == self.node_id && !record.state.is_authorized())
                }
            }
        }
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

        /// The shard's current write set, `None` while the all-member rule
        /// applies (no record, or a stale one, as for a handoff).
        fn switched_write_set(
            &self,
            shard: &DfsMetadataShardConfig,
        ) -> KvResult<Option<crate::DfsMetadataShardWriteSet>> {
            let key = crate::metadata_shard_write_set_key(&shard.file_system_id, &shard.shard_id);
            if self.write_set.is_some_and(|entry| entry.key != key) {
                return Err(refuse("the write-set entry is bound to another key"));
            }
            match crate::metadata_shard_write_set_state(shard, self.write_set.map(|entry| entry.value.as_slice())) {
                DfsMetadataShardWriteSetState::Switched(write_set) => Ok(Some(write_set)),
                DfsMetadataShardWriteSetState::Unswitched | DfsMetadataShardWriteSetState::Stale { .. } => Ok(None),
                DfsMetadataShardWriteSetState::Unreadable { reason } => {
                    Err(refuse(format!("the write-set record is unreadable: {reason}")))
                }
            }
        }

        /// Refuse an attempt that no longer runs under the shard's current
        /// serving record. A primary's attempt runs under the epoch and
        /// primary it names. A takeover attempt runs under the epoch whose
        /// primary it names as lost, while its converter is a configured
        /// member other than that primary and, on a switched shard, a member
        /// of the write set.
        fn attempt_runs_under(
            &self,
            shard: &DfsMetadataShardConfig,
            attempt: &DfsMetadataShardConversion,
        ) -> KvResult<()> {
            if attempt.epoch != shard.epoch {
                return Err(refuse("the attempt was superseded by a serving change"));
            }
            let Some(takeover) = &attempt.takeover else {
                if attempt.primary_node_id != shard.primary_node_id {
                    return Err(refuse("the attempt was superseded by a serving change"));
                }
                return Ok(());
            };
            if takeover.previous_primary != shard.primary_node_id {
                return Err(refuse("the takeover's lost primary is not the shard's primary"));
            }
            if attempt.primary_node_id == shard.primary_node_id
                || !shard.replica_node_ids.contains(&attempt.primary_node_id)
            {
                return Err(refuse("the takeover's converter is not a configured member other than the primary"));
            }
            if self
                .switched_write_set(shard)?
                .is_some_and(|write_set| !write_set.contains(&attempt.primary_node_id))
            {
                return Err(refuse("the takeover's converter is not in the shard's write set"));
            }
            Ok(())
        }

        /// Exact compares of every configured member's records for a
        /// takeover attempt, which is admitted only while no member, the lost
        /// primary included, can still serve the shard's old format. A member
        /// that departed the cluster cannot serve, so its member record is
        /// compared exactly as it stands (absent, or not authorized): a node
        /// that rejoins before the commit rejects the transaction. Every
        /// other member must advertise and permit the storage generation
        /// capability. That is the prepared-operation admission check of every
        /// assigned node with the storage generation capability as its
        /// required capability; the write-set member check is not used
        /// because it also requires the write-set capability, which an
        /// unswitched shard's members need not advertise.
        fn storage_capable_member_compares(
            &self,
            shard: &DfsMetadataShardConfig,
        ) -> KvResult<Vec<KvCompare>> {
            let mut departed = Vec::new();
            let mut capable = BTreeSet::new();
            for node_id in crate::metadata_shard_configured_node_ids(shard) {
                let records = self
                    .members
                    .iter()
                    .find(|member| member.node_id == node_id)
                    .ok_or_else(|| refuse(format!("the cut has no records of member {node_id}")))?;
                if records.departed() {
                    departed.push(exact(sinomo_core::node::cluster_member_key(&node_id), records.member)?);
                } else {
                    capable.insert(node_id);
                }
            }
            let mut compares = self.storage_capable_compares(shard, &capable)?;
            compares.extend(departed);
            Ok(compares)
        }

        /// The compares [`Self::storage_capable_member_compares`] adds to a
        /// takeover attempt's transactions, so a converter can check the
        /// capability gate before it asks for promises.
        pub fn takeover_member_compares(&self) -> KvResult<Vec<KvCompare>> {
            let shard = self.shard_config()?;
            self.storage_capable_member_compares(&shard)
        }

        /// Exact member and capability-policy compares for `nodes`, each of
        /// which must be an authorized member that advertises and permits the
        /// storage generation capability. A conversion passes them as
        /// [`Self::member_compares`] for the members it installs on or counts
        /// as acknowledgements, so a member that loses the capability or its
        /// membership after the read cannot be admitted.
        pub fn storage_capable_compares_for(
            &self,
            nodes: &BTreeSet<NodeId>,
        ) -> KvResult<Vec<KvCompare>> {
            let shard = self.shard_config()?;
            self.storage_capable_compares(&shard, nodes)
        }

        fn storage_capable_compares(
            &self,
            shard: &DfsMetadataShardConfig,
            nodes: &BTreeSet<NodeId>,
        ) -> KvResult<Vec<KvCompare>> {
            let mut cluster_id = None;
            let mut compares = Vec::new();
            for node_id in nodes.iter().cloned() {
                let records = self
                    .members
                    .iter()
                    .find(|member| member.node_id == node_id)
                    .ok_or_else(|| refuse(format!("the cut has no records of member {node_id}")))?;
                let member_key = sinomo_core::node::cluster_member_key(&node_id);
                let policy_key = sinomo_core::node::cluster_capability_policy_key(&node_id);
                let member = records
                    .member
                    .filter(|entry| entry.key == member_key)
                    .ok_or_else(|| refuse(format!("member {node_id} has no cluster member record")))?;
                if records.policy.is_some_and(|entry| entry.key != policy_key) {
                    return Err(refuse("a capability policy is bound to another key"));
                }
                compares.extend(
                    crate::control_plane::metadata_prepared_operation_member_compares(
                        shard,
                        &node_id,
                        member_key,
                        member.clone(),
                        policy_key,
                        records.policy.cloned(),
                        &mut cluster_id,
                        sinomo_core::node::DFS_METADATA_STORAGE_GENERATION_V1_CAPABILITY,
                    )
                    .map_err(|error| refuse(format!("member {node_id}: {error}")))?,
                );
            }
            Ok(compares)
        }
    }

    /// Which members a primary's conversion attempt installs its image on,
    /// and which acknowledgements its activation needs. A switched shard
    /// installs on every member its commits require: the write set (a closed
    /// shard has no durable pending adds, since adds need joins under exact
    /// admission). An unswitched shard needs its primary and a majority of its
    /// configured members, the membership-handoff acknowledgement rule with
    /// the retiring and desired configurations equal; the remaining members
    /// repair by snapshot once the shard is active.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum DfsMetadataShardConversionInstallRule {
        WriteSet { required: BTreeSet<NodeId> },
        PrimaryAndMajority { shard: DfsMetadataShardConfig },
    }

    impl DfsMetadataShardConversionInstallRule {
        /// The rule under the shard's current serving record and write-set
        /// entry. A stale write-set record leaves the all-member rule in
        /// force, as it does for every other commit; an unreadable one is
        /// refused.
        pub fn for_shard(
            shard: &DfsMetadataShardConfig,
            write_set: Option<&KvEntry>,
        ) -> KvResult<Self> {
            let key = crate::metadata_shard_write_set_key(&shard.file_system_id, &shard.shard_id);
            if write_set.is_some_and(|entry| entry.key != key) {
                return Err(refuse("the write-set entry is bound to another key"));
            }
            match crate::metadata_shard_write_set_state(shard, write_set.map(|entry| entry.value.as_slice())) {
                DfsMetadataShardWriteSetState::Switched(write_set) => Ok(Self::WriteSet {
                    required: write_set.write_set.iter().cloned().collect(),
                }),
                DfsMetadataShardWriteSetState::Unswitched | DfsMetadataShardWriteSetState::Stale { .. } => {
                    Ok(Self::PrimaryAndMajority { shard: shard.clone() })
                }
                DfsMetadataShardWriteSetState::Unreadable { reason } => {
                    Err(refuse(format!("the write-set record is unreadable: {reason}")))
                }
            }
        }

        /// The rule a converting takeover by `converter` installs under: the
        /// rule of the configuration its activation makes current. A switched
        /// shard installs on its write set without the lost primary, which
        /// the takeover removes from it, and without the earlier converters
        /// the round does not count on: they are left to repair by snapshot
        /// once the shard is active. An unswitched shard needs the converter,
        /// as the next primary, and a majority of its configured members.
        pub fn for_takeover(
            shard: &DfsMetadataShardConfig,
            write_set: Option<&KvEntry>,
            converter: &NodeId,
            takeover: &DfsMetadataConversionTakeover,
        ) -> KvResult<Self> {
            match Self::for_shard(shard, write_set)? {
                Self::WriteSet { mut required } => {
                    required.remove(&shard.primary_node_id);
                    required.retain(|node| !takeover.lost_converters.contains(node));
                    if !required.contains(converter) {
                        return Err(refuse("the takeover's converter is not in the shard's write set"));
                    }
                    Ok(Self::WriteSet { required })
                }
                Self::PrimaryAndMajority { .. } => Ok(Self::PrimaryAndMajority {
                    shard: crate::control_plane::metadata_shard_primary_handoff_target(shard, converter)?,
                }),
            }
        }

        /// Every member the image is sent to.
        pub fn install_set(&self) -> BTreeSet<NodeId> {
            match self {
                Self::WriteSet { required } => required.clone(),
                Self::PrimaryAndMajority { shard } => crate::metadata_shard_configured_node_ids(shard),
            }
        }

        /// Whether `acknowledged` members holding the sealed image are enough
        /// to activate it.
        pub fn satisfied_by(&self, acknowledged: &BTreeSet<NodeId>) -> bool {
            match self {
                Self::WriteSet { required } => required.is_subset(acknowledged),
                Self::PrimaryAndMajority { shard } => {
                    let acknowledged = acknowledged
                        .intersection(&crate::metadata_shard_configured_node_ids(shard))
                        .cloned()
                        .collect();
                    crate::control_plane::metadata_shard_membership_handoff_acknowledgements_are_safe(
                        shard,
                        shard,
                        &acknowledged,
                    )
                }
            }
        }
    }

    /// Fix one attempt's target in the shard's storage record before its image
    /// exists: the record must be an unsealed Converting attempt that runs
    /// under the current serving record. It replaces an absent record, a
    /// Converting record of an attempt a serving change superseded, or, with
    /// a takeover attempt, the lost primary's own attempt at the same epoch.
    ///
    /// Once a takeover attempt is recorded at the epoch, the epoch is the
    /// takeover's ([`DfsMetadataShardStorage::epoch_takeover`]): the primary
    /// no longer replaces it, its converter only continues it, and only
    /// another member's attempt of the next round replaces it. Every takeover
    /// attempt must carry exactly the round
    /// [`DfsMetadataConversionTakeover::for_converter`] derives from the
    /// record it replaces, so rounds only increase, by one, and each round
    /// is recorded at most once. Whether the replaced converter is lost is
    /// established by the round's promise quorum before this is submitted.
    /// The same attempt is idempotent. The request identity binds the attempt
    /// and every compare: the same cut keeps the identity, and another cut
    /// gets another one.
    ///
    /// A takeover attempt also compares every configured member's records,
    /// each of which must advertise the storage generation capability unless
    /// it departed the cluster ([`DfsMetadataShardStorageCut::members`]).
    pub fn build_metadata_shard_storage_converting(
        cut: &DfsMetadataShardStorageCut<'_>,
        record: &DfsMetadataShardStorage,
    ) -> KvResult<KvTxnRequest> {
        let shard = cut.shard_config()?;
        record.validate_against(&shard).map_err(refuse)?;
        let attempt = record
            .converting_attempt()
            .ok_or_else(|| refuse("the record is not a Converting attempt"))?;
        if attempt.seal.is_some() || attempt.contributions_digest.is_some() {
            return Err(refuse("an attempt is recorded Converting before its image is sealed"));
        }
        cut.attempt_runs_under(&shard, attempt)?;
        let current = cut.current_storage(&shard)?;
        let mut same_attempt = false;
        match &current {
            None => {}
            Some(current) if current.is_active() => {
                return Err(refuse("the shard's storage is already active"));
            }
            Some(current) => {
                let previous = current.converting_attempt().expect("not active");
                if previous.attempt_id == attempt.attempt_id {
                    if previous.sealed().is_some() {
                        return Err(refuse("the attempt is already sealed"));
                    }
                    if current != record {
                        return Err(refuse("the attempt changed its fixed target"));
                    }
                    same_attempt = true;
                } else if let Some(recorded) = current.epoch_takeover(&shard) {
                    if attempt.takeover.is_none() {
                        return Err(refuse(
                            "a converting takeover runs at this epoch; only its converter continues it, or its next round replaces it",
                        ));
                    }
                    if attempt.primary_node_id == recorded.primary_node_id {
                        return Err(refuse("a takeover's converter continues its own attempt; only another member's next round replaces it"));
                    }
                } else if cut.attempt_runs_under(&shard, previous).is_ok() && attempt.takeover.is_none() {
                    return Err(refuse("another attempt of the same converter runs at this epoch"));
                }
            }
        }
        if let Some(takeover) = attempt.takeover.as_ref().filter(|_| !same_attempt) {
            let expected =
                DfsMetadataConversionTakeover::for_converter(&shard, current.as_ref(), &attempt.primary_node_id);
            if *takeover != expected {
                return Err(refuse(format!(
                    "a takeover attempt here runs round {} after converters {:?}, not round {} after {:?}",
                    expected.round, expected.lost_converters, takeover.round, takeover.lost_converters,
                )));
            }
        }
        let mut compares = cut.compares(&shard)?;
        if attempt.takeover.is_some() {
            extend_unique(&mut compares, cut.storage_capable_member_compares(&shard)?);
        }
        let request_id = cut_request_id("dfs-shard-storage-converting-v1", &attempt.attempt_id, &compares)?;
        let txn = KvTxn {
            compares,
            success: vec![KvOp::put_serde(
                crate::metadata_shard_storage_key(&shard.file_system_id, &shard.shard_id),
                record,
                None,
            )?],
            failure: Vec::new(),
        };
        Ok(KvTxnRequest::new(request_id, txn))
    }

    /// Seal the shard's Converting attempt with the image its holders install
    /// and the contributions it was built from. The attempt must still run
    /// under the current serving record, and the transaction compares the
    /// exact current record. Sealing the same image again is idempotent; the
    /// request identity binds the attempt, the image, the contributions and
    /// every compare, so it is kept while the cut is unchanged. Another image
    /// for a sealed attempt is refused.
    pub fn build_metadata_shard_storage_seal(
        cut: &DfsMetadataShardStorageCut<'_>,
        seal: &DfsMetadataGenerationSeal,
        contributions_digest: &str,
    ) -> KvResult<(KvTxnRequest, DfsMetadataShardStorage)> {
        let shard = cut.shard_config()?;
        let current = cut
            .current_storage(&shard)?
            .ok_or_else(|| refuse("the shard has no Converting attempt"))?;
        let attempt = current
            .converting_attempt()
            .ok_or_else(|| refuse("the shard's storage is already active"))?;
        cut.attempt_runs_under(&shard, attempt)?;
        match attempt.sealed() {
            None => {}
            Some((sealed, digest)) if sealed == seal && digest == contributions_digest => {}
            Some(_) => return Err(refuse("the attempt is already sealed with another image")),
        }
        let mut sealed = current.clone();
        if let DfsMetadataShardStorageState::Converting { attempt } = &mut sealed.state {
            attempt.seal = Some(seal.clone());
            attempt.contributions_digest = Some(contributions_digest.to_owned());
        }
        sealed.validate_against(&shard).map_err(refuse)?;
        let compares = cut.compares(&shard)?;
        let request_id = cut_request_id(
            "dfs-shard-storage-seal-v1",
            &(&attempt.attempt_id, seal, contributions_digest),
            &compares,
        )?;
        let txn = KvTxn {
            compares,
            success: vec![KvOp::put_serde(
                crate::metadata_shard_storage_key(&shard.file_system_id, &shard.shard_id),
                &sealed,
                None,
            )?],
            failure: Vec::new(),
        };
        Ok((KvTxnRequest::new(request_id, txn), sealed))
    }

    /// The Active record of a sealed attempt.
    fn activated(
        current: &DfsMetadataShardStorage,
        attempt: &DfsMetadataShardConversion,
    ) -> KvResult<DfsMetadataShardStorage> {
        if attempt.sealed().is_none() {
            return Err(refuse("the attempt's image is not sealed"));
        }
        let activation_digest =
            metadata_shard_storage_activation_digest(&current.generation, attempt).map_err(refuse)?;
        let mut active = current.clone();
        active.state = DfsMetadataShardStorageState::Active {
            activation: DfsMetadataShardActivation::Converted {
                attempt: attempt.clone(),
                activation_digest,
            },
        };
        Ok(active)
    }

    /// Activate the shard's sealed Converting attempt of its own primary. The
    /// caller has established that its install rule holds for the current
    /// serving record; this transaction makes that observation exact. Its
    /// request identity binds the attempt and every compare, the
    /// acknowledging members' records included, so a change to any of them
    /// is re-planned under a new identity.
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
        if attempt.takeover.is_some() {
            return Err(refuse("a takeover attempt activates with its takeover"));
        }
        cut.attempt_runs_under(&shard, &attempt)?;
        let active = activated(&current, &attempt)?;
        active.validate_against(&shard).map_err(refuse)?;
        let compares = cut.compares(&shard)?;
        let request_id = cut_request_id("dfs-shard-storage-activate-v1", &attempt.attempt_id, &compares)?;
        let txn = KvTxn {
            compares,
            success: vec![KvOp::put_serde(
                crate::metadata_shard_storage_key(&shard.file_system_id, &shard.shard_id),
                &active,
                None,
            )?],
            failure: Vec::new(),
        };
        Ok((KvTxnRequest::new(request_id, txn), active))
    }

    /// Activate a sealed takeover attempt and make its converter primary, in
    /// one transaction. The shard's configuration moves exactly as a
    /// cooperative handoff target or a takeover swap moves it: the converter
    /// is primary at the next epoch with the configured members preserved. On
    /// a switched shard the write set takes the takeover swap's change, with
    /// the epoch starting at the sealed image's prefix. A pending handoff and
    /// a head watermark are superseded as by the swap. Its compares are the
    /// exact serving record, write set, storage record, handoff and
    /// watermark, every configured member's capability records and the
    /// caller's member compares; it neither compares nor writes the
    /// filesystem configuration or its catalog generation. Its request
    /// identity binds the attempt and every compare.
    pub fn build_metadata_shard_storage_takeover_activate(
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
        if attempt.takeover.is_none() {
            return Err(refuse("the attempt is not a takeover; it activates without a serving change"));
        }
        cut.attempt_runs_under(&shard, &attempt)?;
        let active = activated(&current, &attempt)?;
        let (seal, _) = attempt.sealed().expect("activated a sealed attempt");
        let fs = &shard.file_system_id;
        let key = |key: fn(&DfsFileSystemId, &str) -> String| key(fs, &shard.shard_id);
        let bound = |entry: Option<&'_ KvEntry>, key: String| -> KvResult<Option<Vec<u8>>> {
            match entry {
                Some(entry) if entry.key != key => Err(refuse("an entry is bound to another key")),
                entry => Ok(entry.map(|entry| entry.value.clone())),
            }
        };
        let handoff = bound(cut.handoff, key(crate::metadata_shard_membership_handoff_key))?;
        let watermark = bound(cut.watermark, key(crate::metadata_shard_head_watermark_key))?;
        let (superseded_compares, superseded_deletes) =
            crate::control_plane::metadata_takeover_superseded_keys(
                &shard,
                handoff.as_ref(),
                watermark.as_ref(),
            );
        let mut compares = vec![
            exact(key(crate::metadata_shard_key), Some(cut.shard))?,
            exact(key(crate::metadata_shard_write_set_key), cut.write_set)?,
            exact(key(crate::metadata_shard_storage_key), cut.storage)?,
        ];
        compares.extend(superseded_compares);
        extend_unique(&mut compares, cut.storage_capable_member_compares(&shard)?);
        extend_unique(&mut compares, cut.member_compares.clone());
        // As for the other storage transactions, the identity binds every
        // compare, member records included: a definite rejection is
        // re-planned from the changed cut under a new identity.
        let request_id =
            cut_request_id("dfs-shard-storage-takeover-activate-v1", &attempt.attempt_id, &compares)?;
        let next_shard =
            crate::control_plane::metadata_shard_primary_handoff_target(&shard, &attempt.primary_node_id)?;
        let next_write_set = cut
            .switched_write_set(&shard)?
            .map(|write_set| {
                crate::control_plane::metadata_write_set_after_takeover(
                    &write_set,
                    &shard.primary_node_id,
                    &next_shard,
                    &seal.prefix_proof,
                    &request_id,
                )
            })
            .transpose()?;
        if let Some(write_set) = &next_write_set {
            write_set.validate_against(&next_shard).map_err(refuse)?;
        }
        active.validate_against(&next_shard).map_err(refuse)?;
        let mut success = vec![KvOp::put_serde(key(crate::metadata_shard_key), &next_shard, None)?];
        if let Some(write_set) = &next_write_set {
            success.push(KvOp::put_serde(key(crate::metadata_shard_write_set_key), write_set, None)?);
        }
        success.extend(superseded_deletes);
        success.push(KvOp::put_serde(key(crate::metadata_shard_storage_key), &active, None)?);
        Ok((
            KvTxnRequest::new(
                request_id,
                KvTxn {
                    compares,
                    success,
                    failure: Vec::new(),
                },
            ),
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

    pub(super) fn seal(sequence: u64) -> DfsMetadataGenerationSeal {
        DfsMetadataGenerationSeal {
            prefix_proof: crate::DfsMetadataPrefixProof {
                version: 2,
                journal_sequence: sequence,
                digest: format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64)),
            },
            state_fingerprint: "image-state".into(),
            file_fingerprint: "image-file".into(),
        }
    }

    /// An unsealed attempt under `shard`: of its primary, or with `converter`
    /// the first takeover round of that primary.
    pub(super) fn unsealed_attempt(
        shard: &DfsMetadataShardConfig, converter: Option<&str>, sequence: u64,
    ) -> DfsMetadataShardConversion {
        let takeover = converter.map(|_| DfsMetadataConversionTakeover::first_round(shard.primary_node_id.clone()));
        unsealed_round(shard, converter, takeover, sequence)
    }

    /// An unsealed attempt under `shard` by `converter` (the primary when
    /// `None`) with the given takeover round.
    pub(super) fn unsealed_round(
        shard: &DfsMetadataShardConfig, converter: Option<&str>,
        takeover: Option<DfsMetadataConversionTakeover>, sequence: u64,
    ) -> DfsMetadataShardConversion {
        let source = source(sequence);
        let primary_node_id = converter
            .map(|converter| NodeId::from_legacy_named(converter).unwrap())
            .unwrap_or_else(|| shard.primary_node_id.clone());
        DfsMetadataShardConversion {
            attempt_id: metadata_shard_conversion_attempt_id(
                &shard.file_system_id, &shard.shard_id, &shard.assignment, shard.epoch,
                &primary_node_id, &source, takeover.as_ref(),
            ).unwrap(),
            epoch: shard.epoch,
            primary_node_id,
            takeover,
            source,
            seal: None,
            contributions_digest: None,
        }
    }

    pub(super) fn sealed(mut attempt: DfsMetadataShardConversion, sequence: u64) -> DfsMetadataShardConversion {
        attempt.seal = Some(seal(sequence));
        attempt.contributions_digest = Some("c".repeat(64));
        attempt
    }

    /// A sealed attempt of `shard`'s primary.
    pub(super) fn attempt_for(shard: &DfsMetadataShardConfig, sequence: u64) -> DfsMetadataShardConversion {
        sealed(unsealed_attempt(shard, None, sequence), sequence)
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

    #[test]
    fn an_attempt_converts_before_its_image_is_sealed_and_activates_only_sealed() {
        let shard = shard(4, "primary-a");
        let attempt = unsealed_attempt(&shard, None, 7);
        let unsealed = DfsMetadataShardStorage::converting(&shard, attempt.clone());
        unsealed.validate_against(&shard).unwrap();
        assert!(attempt.sealed().is_none());
        // An unsealed attempt has no activation digest and never validates Active.
        assert!(metadata_shard_storage_activation_digest(&unsealed.generation, &attempt).is_err());
        let mut active = unsealed.clone();
        active.state = DfsMetadataShardStorageState::Active {
            activation: DfsMetadataShardActivation::Converted {
                attempt: attempt.clone(), activation_digest: format!("blake3:{}", "0".repeat(64)),
            },
        };
        assert!(active.validate().is_err());
        // A seal comes with its contributions digest; sealing keeps the attempt.
        let mut partial = attempt.clone();
        partial.seal = Some(seal(7));
        assert!(DfsMetadataShardStorage::converting(&shard, partial).validate().is_err());
        let full = sealed(attempt.clone(), 7);
        assert_eq!(full.attempt_id, attempt.attempt_id);
        let converting = DfsMetadataShardStorage::converting(&shard, full.clone());
        assert_eq!(converting.generation, unsealed.generation);
        converting.validate_against(&shard).unwrap();
        let mut malformed = full.clone();
        malformed.seal.as_mut().unwrap().prefix_proof.version = 1;
        assert!(DfsMetadataShardStorage::converting(&shard, malformed).validate().is_err());
        // The activation digest covers the seal.
        let mut other_image = full.clone();
        other_image.seal.as_mut().unwrap().state_fingerprint.push_str("-fork");
        assert_ne!(
            metadata_shard_storage_activation_digest(&converting.generation, &full).unwrap(),
            metadata_shard_storage_activation_digest(&converting.generation, &other_image).unwrap(),
        );
        // Unsealed bytes omit the seal fields; the record round-trips.
        let bytes = sinomo_core::kv::encode_kv_value(&unsealed).unwrap();
        assert_eq!(sinomo_core::kv::decode_kv_value::<DfsMetadataShardStorage>(&bytes).unwrap(), unsealed);
    }

    #[test]
    fn a_takeover_attempt_binds_its_lost_primary_into_its_identity() {
        let shard = shard(4, "primary-a");
        let own = unsealed_attempt(&shard, None, 7);
        let takeover = unsealed_attempt(&shard, Some("replica-b"), 7);
        assert_eq!(takeover.epoch, shard.epoch);
        assert_eq!(takeover.primary_node_id, NodeId::from_legacy_named("replica-b").unwrap());
        assert_eq!(takeover.takeover, Some(DfsMetadataConversionTakeover {
            previous_primary: shard.primary_node_id.clone(), round: 1, lost_converters: Vec::new(),
        }));
        // The takeover is another attempt than the primary's own, with another target.
        assert_ne!(takeover.attempt_id, own.attempt_id);
        let record = DfsMetadataShardStorage::converting(&shard, takeover.clone());
        record.validate_against(&shard).unwrap();
        assert_ne!(record.generation, DfsMetadataShardStorage::converting(&shard, own).generation);
        // The marker is part of the identity: dropping or renaming it is refused.
        let mut unmarked = takeover.clone();
        unmarked.takeover = None;
        assert!(DfsMetadataShardStorage::converting(&shard, unmarked).validate().is_err());
        let mut renamed = takeover.clone();
        renamed.takeover = Some(DfsMetadataConversionTakeover::first_round(NodeId::from_legacy_named("replica-c").unwrap()));
        assert!(DfsMetadataShardStorage::converting(&shard, renamed).validate().is_err());
        // A converter never takes over from itself.
        let from_itself = unsealed_attempt(&shard, Some("primary-a"), 7);
        assert!(DfsMetadataShardStorage::converting(&shard, from_itself).validate().is_err());
    }

    /// A takeover round is part of the attempt's identity, and a record
    /// holds only a well-formed round: the first without earlier converters,
    /// a later one with the converter it replaced and at most one per earlier
    /// round, in canonical order, never naming its own converter or the lost
    /// primary.
    #[test]
    fn a_takeover_round_binds_its_identity_and_has_a_strict_shape() {
        let shard = shard(4, "primary-a");
        let node = |name: &str| NodeId::from_legacy_named(name).unwrap();
        let first = DfsMetadataConversionTakeover::first_round(shard.primary_node_id.clone());
        // The next round loses the converter it replaces and every earlier
        // one, except its own converter.
        let second = first.next_round(&node("replica-b"), &node("replica-c"));
        assert_eq!((second.round, second.lost_converters.clone()), (2, vec![node("replica-b")]));
        let third = second.next_round(&node("replica-c"), &node("replica-d"));
        let mut both = vec![node("replica-b"), node("replica-c")];
        both.sort();
        assert_eq!((third.round, third.lost_converters.clone()), (3, both.clone()));
        let back = second.next_round(&node("replica-c"), &node("replica-b"));
        assert_eq!((back.round, back.lost_converters.clone()), (3, vec![node("replica-c")]),
            "a returned earlier converter runs a later round without counting itself lost");
        let attempt = |converter: &str, takeover: &DfsMetadataConversionTakeover| {
            DfsMetadataShardStorage::converting(&shard, unsealed_round(&shard, Some(converter), Some(takeover.clone()), 7))
        };
        attempt("replica-c", &second).validate_against(&shard).unwrap();
        attempt("replica-d", &third).validate_against(&shard).unwrap();
        // Each round is another attempt with another target.
        let ids = [attempt("replica-c", &first), attempt("replica-c", &second), attempt("replica-c", &back)]
            .map(|record| record.converting_attempt().unwrap().attempt_id.clone());
        assert!(ids[0] != ids[1] && ids[1] != ids[2] && ids[0] != ids[2], "{ids:?}");
        // A round or its earlier converters changed after derivation is refused.
        let mut forged = attempt("replica-c", &second);
        if let DfsMetadataShardStorageState::Converting { attempt } = &mut forged.state {
            attempt.takeover.as_mut().unwrap().round = 3;
        }
        assert!(forged.validate().is_err());
        let malformed = [
            DfsMetadataConversionTakeover { round: 0, ..first.clone() },
            DfsMetadataConversionTakeover { lost_converters: vec![node("replica-b")], ..first.clone() },
            DfsMetadataConversionTakeover { lost_converters: Vec::new(), ..second.clone() },
            DfsMetadataConversionTakeover { round: 2, ..third.clone() },
            DfsMetadataConversionTakeover { lost_converters: both.iter().rev().cloned().collect(), ..third.clone() },
            DfsMetadataConversionTakeover { lost_converters: vec![node("replica-b"), node("replica-b")], ..third.clone() },
            DfsMetadataConversionTakeover { lost_converters: vec![shard.primary_node_id.clone()], ..second.clone() },
        ];
        for (index, takeover) in malformed.iter().enumerate() {
            assert!(attempt("replica-d", takeover).validate().is_err(), "{index}: {takeover:?}");
        }
        assert!(attempt("replica-b", &second).validate().is_err(), "a round never counts its own converter lost");
        // The round's fields are required, and the record round-trips.
        let record = attempt("replica-c", &second);
        let bytes = sinomo_core::kv::encode_kv_value(&record).unwrap();
        assert_eq!(sinomo_core::kv::decode_kv_value::<DfsMetadataShardStorage>(&bytes).unwrap(), record);
        let value = serde_json::to_value(&record).unwrap();
        for field in ["round", "lost_converters"] {
            let mut without = value.clone();
            without["state"]["attempt"]["takeover"].as_object_mut().unwrap().remove(field).unwrap();
            assert!(serde_json::from_value::<DfsMetadataShardStorage>(without).is_err(), "{field}");
        }
    }
}

#[cfg(all(test, feature = "storage-conversion"))]
mod transaction_tests {
    use super::tests::{attempt_for, seal, sealed, unsealed_attempt, unsealed_round};
    use super::*;
    use sinomo_core::kv::{InProcessKvStore, KvCompare, KvEntry, KvOp, KvTxnRequest};

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).unwrap()
    }

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

    /// A three-member shard: `primary-a` with replicas `replica-b`, `replica-c`.
    fn ring(epoch: u64) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            replica_node_ids: vec![node("replica-b"), node("replica-c")],
            ..shard(epoch, "primary-a")
        }
    }

    fn shard_entry(shard: &DfsMetadataShardConfig) -> KvEntry {
        entry(crate::metadata_shard_key(&shard.file_system_id, &shard.shard_id), shard)
    }

    fn storage_entry(record: &DfsMetadataShardStorage) -> KvEntry {
        entry(crate::metadata_shard_storage_key(&record.file_system_id, &record.shard_id), record)
    }

    fn cut<'a>(shard: &'a KvEntry, storage: Option<&'a KvEntry>) -> DfsMetadataShardStorageCut<'a> {
        DfsMetadataShardStorageCut {
            shard, write_set: None, handoff: None, watermark: None, storage,
            member_compares: Vec::new(), members: &[],
        }
    }

    fn member_record(node_id: &NodeId, storage_capable: bool) -> sinomo_core::ClusterMemberRecord {
        let mut record = sinomo_core::ClusterMemberRecord::local_voter(
            sinomo_core::Id::from_legacy_named("cluster_shard_storage_tests").unwrap(),
            node_id.clone(),
            node_id.to_string(),
            format!("pk-{node_id}"),
            time::OffsetDateTime::UNIX_EPOCH,
        );
        record.capabilities = sinomo_core::node::main_daemon_capabilities(&record.role);
        if !storage_capable {
            record.capabilities.retain(|capability| {
                capability.as_str() != sinomo_core::node::DFS_METADATA_STORAGE_GENERATION_V1_CAPABILITY
            });
        }
        record
    }

    fn write_set_for(shard: &DfsMetadataShardConfig, members: &[NodeId]) -> crate::DfsMetadataShardWriteSet {
        let mut write_set = members.to_vec();
        write_set.sort();
        crate::DfsMetadataShardWriteSet {
            format: crate::DFS_METADATA_SHARD_WRITE_SET_FORMAT,
            file_system_id: shard.file_system_id.clone(),
            shard_id: shard.shard_id.clone(),
            epoch: shard.epoch,
            primary_node_id: shard.primary_node_id.clone(),
            generation: 1,
            write_set,
            epoch_start: None,
            switched_at_epoch: shard.epoch,
            substitutes: Vec::new(),
            raised_n: None,
            last_change: crate::DfsMetadataShardWriteSetChange {
                kind: crate::DfsMetadataShardWriteSetChangeKind::Switch,
                change_id: "switch".into(),
            },
        }
    }

    fn excludes_filesystem_config(request: &KvTxnRequest) -> bool {
        let config = crate::file_system_config_key(&crate::sifs_file_system_id());
        request.txn.compares.iter().all(|compare| match compare {
            KvCompare::ValueEquals { key, .. } | KvCompare::Missing { key } => *key != config,
            _ => true,
        }) && request.txn.success.iter().all(|op| match op {
            KvOp::Put { key, .. } | KvOp::Delete { key } => *key != config,
            _ => true,
        })
    }

    /// A definite rejection spends only its own cut's request identity: the
    /// Converting and seal transactions re-planned from the changed cut that
    /// rejected them commit.
    #[test]
    fn a_rejected_storage_transaction_is_replanned_from_the_changed_cut() {
        let mut kv = InProcessKvStore::new();
        let current = ring(3);
        let nodes = vec![node("primary-a"), node("replica-b"), node("replica-c")];
        put(&mut kv, crate::metadata_shard_key(&current.file_system_id, &current.shard_id), &current);
        for node_id in &nodes {
            put(&mut kv, sinomo_core::node::cluster_member_key(node_id), &member_record(node_id, true));
        }
        let write_set_key = crate::metadata_shard_write_set_key(&current.file_system_id, &current.shard_id);
        let record = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, None, 9));
        let converting = |kv: &InProcessKvStore| {
            let read = Read::from(kv, &current, &nodes);
            let members = read.members();
            build_metadata_shard_storage_converting(&read.cut(&members), &record).unwrap()
        };
        let first = converting(&kv);
        assert_eq!(converting(&kv), first, "the same cut gives the same request");
        // A same-epoch write-set change lands between the read and the commit.
        put(&mut kv, write_set_key.clone(), &write_set_for(&current, &nodes));
        assert!(!apply(&mut kv, &first));
        let replanned = converting(&kv);
        assert_ne!(replanned.request_id, first.request_id);
        assert!(apply(&mut kv, &replanned));
        let seal_request = |kv: &InProcessKvStore| {
            let read = Read::from(kv, &current, &nodes);
            let members = read.members();
            build_metadata_shard_storage_seal(&read.cut(&members), &seal(9), &"c".repeat(64)).unwrap().0
        };
        let first = seal_request(&kv);
        let mut changed = write_set_for(&current, &nodes);
        changed.generation = 2;
        put(&mut kv, write_set_key, &changed);
        assert!(!apply(&mut kv, &first));
        let replanned = seal_request(&kv);
        assert_ne!(replanned.request_id, first.request_id);
        assert!(apply(&mut kv, &replanned));
        let sealed: DfsMetadataShardStorage = kv
            .get(&crate::metadata_shard_storage_key(&current.file_system_id, &current.shard_id))
            .unwrap()
            .decode()
            .unwrap();
        assert!(sealed.converting_attempt().unwrap().sealed().is_some());
        // A member-record change between the activation's read and its commit.
        let activation = |kv: &InProcessKvStore| {
            let read = Read::from(kv, &current, &nodes);
            let members = read.members();
            let mut cut = read.cut(&members);
            cut.member_compares = cut.storage_capable_compares_for(&nodes.iter().cloned().collect()).unwrap();
            build_metadata_shard_storage_activate(&cut).unwrap().0
        };
        let first = activation(&kv);
        assert_eq!(activation(&kv), first, "the same cut gives the same request");
        let mut moved = member_record(&nodes[2], true);
        moved.display_name.push_str("-renamed");
        put(&mut kv, sinomo_core::node::cluster_member_key(&nodes[2]), &moved);
        assert!(!apply(&mut kv, &first));
        let replanned = activation(&kv);
        assert_ne!(replanned.request_id, first.request_id);
        assert!(apply(&mut kv, &replanned));
    }

    #[test]
    fn conversion_install_rule_follows_the_shards_commit_rule() {
        use std::collections::BTreeSet;
        let current = ring(3);
        let (primary, b, c) = (node("primary-a"), node("replica-b"), node("replica-c"));
        let set = |nodes: &[&NodeId]| nodes.iter().map(|node| (*node).clone()).collect::<BTreeSet<_>>();
        // Unswitched: the image goes to every member; the primary and a
        // majority of the configured members are enough.
        let rule = DfsMetadataShardConversionInstallRule::for_shard(&current, None).unwrap();
        assert_eq!(rule.install_set(), set(&[&primary, &b, &c]));
        assert!(rule.satisfied_by(&set(&[&primary, &b])));
        assert!(!rule.satisfied_by(&set(&[&b, &c])), "the primary must hold the image");
        assert!(
            !rule.satisfied_by(&set(&[&primary, &node("outsider")])),
            "a node outside the configured members is not counted"
        );
        // Switched: every member of the write set, and only those.
        let switched = entry(
            crate::metadata_shard_write_set_key(&current.file_system_id, &current.shard_id),
            &write_set_for(&current, &[primary.clone(), b.clone()]),
        );
        let rule = DfsMetadataShardConversionInstallRule::for_shard(&current, Some(&switched)).unwrap();
        assert_eq!(rule.install_set(), set(&[&primary, &b]));
        assert!(!rule.satisfied_by(&set(&[&primary, &c])), "a majority outside the write set is not enough");
        assert!(rule.satisfied_by(&set(&[&primary, &b])));
        // A write set bound to an older serving record leaves the all-member rule.
        let successor = DfsMetadataShardConfig { epoch: 4, ..current.clone() };
        assert!(matches!(
            DfsMetadataShardConversionInstallRule::for_shard(&successor, Some(&switched)).unwrap(),
            DfsMetadataShardConversionInstallRule::PrimaryAndMajority { .. }
        ));
    }

    /// A converting takeover installs under the rule of the configuration its
    /// activation makes current, never needing the lost primary.
    #[test]
    fn a_takeover_installs_under_the_configuration_its_activation_makes_current() {
        use std::collections::BTreeSet;
        let current = ring(3);
        let (primary, b, c) = (node("primary-a"), node("replica-b"), node("replica-c"));
        let set = |nodes: &[&NodeId]| nodes.iter().map(|node| (*node).clone()).collect::<BTreeSet<_>>();
        let rule = |write_set: Option<&KvEntry>, converter: &NodeId| {
            DfsMetadataShardConversionInstallRule::for_takeover(
                &current, write_set, converter, &DfsMetadataConversionTakeover::first_round(current.primary_node_id.clone()))
        };
        // Unswitched: the converter, as the next primary, and a majority of
        // the configured members.
        let unswitched = rule(None, &b).unwrap();
        assert_eq!(unswitched.install_set(), set(&[&primary, &b, &c]));
        assert!(unswitched.satisfied_by(&set(&[&b, &c])), "the lost primary is not needed");
        assert!(!unswitched.satisfied_by(&set(&[&primary, &c])), "the converter must hold the image");
        assert!(!unswitched.satisfied_by(&set(&[&b])));
        // Switched: the whole write set without the lost primary.
        let write_set_key = crate::metadata_shard_write_set_key(&current.file_system_id, &current.shard_id);
        let switched = entry(write_set_key.clone(), &write_set_for(&current, &[primary.clone(), b.clone(), c.clone()]));
        let rule_switched = rule(Some(&switched), &b).unwrap();
        assert_eq!(rule_switched.install_set(), set(&[&b, &c]));
        assert!(rule_switched.satisfied_by(&set(&[&b, &c])));
        assert!(!rule_switched.satisfied_by(&set(&[&primary, &b])));
        // A converter outside the write set, or the primary itself, has none.
        let partial = entry(write_set_key, &write_set_for(&current, &[primary.clone(), b.clone()]));
        assert!(rule(Some(&partial), &c).is_err());
        assert!(rule(None, &primary).is_err());
        // A later round does not wait for the earlier converters it counts
        // lost on a switched shard; an unswitched shard's majority rule is
        // unchanged.
        let second = DfsMetadataConversionTakeover::first_round(primary.clone()).next_round(&c, &b);
        let later = |write_set: Option<&KvEntry>| {
            DfsMetadataShardConversionInstallRule::for_takeover(&current, write_set, &b, &second).unwrap()
        };
        assert_eq!(later(Some(&switched)).install_set(), set(&[&b]));
        assert!(later(Some(&switched)).satisfied_by(&set(&[&b])), "the lost converter is not needed");
        assert_eq!(later(None), unswitched);
        assert!(DfsMetadataShardConversionInstallRule::for_takeover(&current, Some(&switched), &c, &second).is_err(),
            "a converter the round counts lost has no rule");
    }

    #[test]
    fn converting_fixes_one_attempt_under_the_current_serving_record() {
        let current = shard(3, "primary-a");
        let shard_kv = shard_entry(&current);
        let record = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, None, 9));
        let request = build_metadata_shard_storage_converting(&cut(&shard_kv, None), &record).unwrap();
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
        // The target is fixed before the image exists: a sealed record is refused.
        let presealed = DfsMetadataShardStorage::converting(&current, attempt_for(&current, 9));
        assert!(build_metadata_shard_storage_converting(&cut(&shard_kv, None), &presealed).is_err());
        // The same cut keeps its request identity; the same attempt over its
        // own recorded target is idempotent under another identity.
        let same = build_metadata_shard_storage_converting(&cut(&shard_kv, None), &record).unwrap();
        assert_eq!(same, request);
        let existing = storage_entry(&record);
        let again = build_metadata_shard_storage_converting(&cut(&shard_kv, Some(&existing)), &record).unwrap();
        assert_ne!(again.request_id, request.request_id);
        assert_eq!(again.txn.success, request.txn.success);
        // Another attempt at the same epoch and primary is refused.
        let rival = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, None, 10));
        assert!(build_metadata_shard_storage_converting(&cut(&shard_kv, Some(&existing)), &rival).is_err());
        // An attempt built under an older serving record is refused.
        let successor = shard(4, "replica-b");
        let successor_kv = shard_entry(&successor);
        assert!(build_metadata_shard_storage_converting(&cut(&successor_kv, None), &record).is_err());
        // A successor's attempt replaces the superseded Converting record.
        let replacement = DfsMetadataShardStorage::converting(&successor, unsealed_attempt(&successor, None, 9));
        let replaced = build_metadata_shard_storage_converting(&cut(&successor_kv, Some(&existing)), &replacement).unwrap();
        assert!(replaced.txn.compares.contains(&KvCompare::ValueEquals {
            key: storage_key, value: existing.value.clone(),
        }));
        // A pending handoff refuses it.
        let handoff = entry(crate::metadata_shard_membership_handoff_key(&current.file_system_id, &current.shard_id), &"pending");
        assert!(build_metadata_shard_storage_converting(&DfsMetadataShardStorageCut {
            handoff: Some(&handoff), ..cut(&shard_kv, None)
        }, &record).is_err());
    }

    #[test]
    fn activation_requires_the_attempt_under_its_own_serving_record() {
        let current = shard(3, "primary-a");
        let shard_kv = shard_entry(&current);
        let attempt = attempt_for(&current, 9);
        let record = DfsMetadataShardStorage::converting(&current, attempt.clone());
        let existing = storage_entry(&record);
        let (request, active) = build_metadata_shard_storage_activate(&cut(&shard_kv, Some(&existing))).unwrap();
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
        let (rebuilt, _) = build_metadata_shard_storage_activate(&cut(&recruited_kv, Some(&existing))).unwrap();
        assert_ne!(rebuilt.request_id, request.request_id);
        // A takeover supersedes it.
        let taken_over_kv = shard_entry(&shard(4, "replica-b"));
        assert!(build_metadata_shard_storage_activate(&cut(&taken_over_kv, Some(&existing))).is_err());
        // Nothing to activate without a Converting attempt.
        assert!(build_metadata_shard_storage_activate(&cut(&shard_kv, None)).is_err());
        // A takeover attempt activates only with its takeover.
        let takeover = DfsMetadataShardStorage::converting(
            &current, sealed(unsealed_attempt(&current, Some("replica-b"), 9), 9));
        let takeover_kv = storage_entry(&takeover);
        assert!(build_metadata_shard_storage_activate(&cut(&shard_kv, Some(&takeover_kv))).is_err());
    }

    #[test]
    fn sealing_moves_one_attempt_from_unsealed_to_sealed() {
        let current = shard(3, "primary-a");
        let shard_kv = shard_entry(&current);
        let unsealed = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, None, 9));
        let unsealed_kv = storage_entry(&unsealed);
        let image = seal(9);
        let contributions = "c".repeat(64);
        // An unsealed attempt cannot activate.
        assert!(build_metadata_shard_storage_activate(&cut(&shard_kv, Some(&unsealed_kv))).is_err());
        let (request, sealed_record) =
            build_metadata_shard_storage_seal(&cut(&shard_kv, Some(&unsealed_kv)), &image, &contributions).unwrap();
        let attempt = sealed_record.converting_attempt().expect("still converting");
        assert_eq!(attempt.attempt_id, unsealed.converting_attempt().unwrap().attempt_id);
        assert_eq!(attempt.sealed(), Some((&image, contributions.as_str())));
        assert_eq!(sealed_record.generation, unsealed.generation, "sealing keeps the target generation");
        assert!(request.txn.compares.contains(&KvCompare::ValueEquals {
            key: unsealed_kv.key.clone(), value: unsealed_kv.value.clone(),
        }), "sealing compares the exact unsealed record");
        assert!(request.txn.compares.contains(&KvCompare::ValueEquals {
            key: shard_kv.key.clone(), value: shard_kv.value.clone(),
        }));
        assert_eq!(request.txn.success, vec![KvOp::put_serde(unsealed_kv.key.clone(), &sealed_record, None).unwrap()]);
        assert!(excludes_filesystem_config(&request));
        // The sealed attempt activates, and its activation carries the seal.
        let sealed_kv = storage_entry(&sealed_record);
        let (_, active) = build_metadata_shard_storage_activate(&cut(&shard_kv, Some(&sealed_kv))).unwrap();
        assert!(matches!(active.converted_activation(),
            Some((activated, _)) if activated.sealed() == Some((&image, contributions.as_str()))));
        // The same cut keeps its identity; sealing the same image again over
        // the sealed record is idempotent under another identity.
        let (same, _) =
            build_metadata_shard_storage_seal(&cut(&shard_kv, Some(&unsealed_kv)), &image, &contributions).unwrap();
        assert_eq!(same, request);
        let (again, again_record) =
            build_metadata_shard_storage_seal(&cut(&shard_kv, Some(&sealed_kv)), &image, &contributions).unwrap();
        assert_ne!(again.request_id, request.request_id);
        assert_eq!(again_record, sealed_record);
        // Another image, other contributions or a malformed seal are refused.
        let mut other = image.clone();
        other.state_fingerprint.push_str("-fork");
        assert!(build_metadata_shard_storage_seal(&cut(&shard_kv, Some(&sealed_kv)), &other, &contributions).is_err());
        assert!(build_metadata_shard_storage_seal(&cut(&shard_kv, Some(&sealed_kv)), &image, &"d".repeat(64)).is_err());
        let mut malformed = image.clone();
        malformed.file_fingerprint.clear();
        assert!(build_metadata_shard_storage_seal(&cut(&shard_kv, Some(&unsealed_kv)), &malformed, &contributions).is_err());
        assert!(build_metadata_shard_storage_seal(&cut(&shard_kv, Some(&unsealed_kv)), &image, "").is_err());
        // The Converting transaction never rewrites a sealed attempt.
        assert!(build_metadata_shard_storage_converting(&cut(&shard_kv, Some(&sealed_kv)), &unsealed).is_err());
        // A serving change supersedes the attempt before it is sealed.
        let successor_kv = shard_entry(&shard(4, "replica-b"));
        assert!(build_metadata_shard_storage_seal(&cut(&successor_kv, Some(&unsealed_kv)), &image, &contributions).is_err());
        // Nothing to seal without a Converting attempt, or once Active.
        assert!(build_metadata_shard_storage_seal(&cut(&shard_kv, None), &image, &contributions).is_err());
        let active_kv = storage_entry(&active);
        assert!(build_metadata_shard_storage_seal(&cut(&shard_kv, Some(&active_kv)), &image, &contributions).is_err());
        // A pending handoff refuses it.
        let handoff = entry(crate::metadata_shard_membership_handoff_key(&current.file_system_id, &current.shard_id), &"pending");
        assert!(build_metadata_shard_storage_seal(&DfsMetadataShardStorageCut {
            handoff: Some(&handoff), ..cut(&shard_kv, Some(&unsealed_kv))
        }, &image, &contributions).is_err());
    }

    #[test]
    fn a_takeover_attempt_converts_only_from_an_eligible_member_of_a_capable_shard() {
        let current = ring(3);
        let shard_kv = shard_entry(&current);
        let configured = crate::metadata_shard_configured_node_ids(&current);
        let records = configured.iter()
            .map(|node_id| (node_id.clone(), entry(sinomo_core::node::cluster_member_key(node_id), &member_record(node_id, true))))
            .collect::<Vec<_>>();
        let members = records.iter()
            .map(|(node_id, record)| DfsMetadataShardStorageMember { node_id: node_id.clone(), member: Some(record), policy: None })
            .collect::<Vec<_>>();
        let takeover_cut = |storage, write_set| DfsMetadataShardStorageCut {
            write_set, members: &members, ..cut(&shard_kv, storage)
        };
        let by_b = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, Some("replica-b"), 9));
        let request = build_metadata_shard_storage_converting(&takeover_cut(None, None), &by_b).unwrap();
        for (node_id, record) in &records {
            assert!(request.txn.compares.contains(&KvCompare::ValueEquals {
                key: sinomo_core::node::cluster_member_key(node_id), value: record.value.clone(),
            }), "the takeover compares member {node_id}");
            assert!(request.txn.compares.contains(&KvCompare::Missing {
                key: sinomo_core::node::cluster_capability_policy_key(node_id),
            }));
        }
        assert!(excludes_filesystem_config(&request));
        // Every configured member's records must be in the cut.
        assert!(build_metadata_shard_storage_converting(&DfsMetadataShardStorageCut {
            members: &members[..2], ..takeover_cut(None, None)
        }, &by_b).is_err());
        // Every member, the lost primary included, must advertise the storage
        // generation capability.
        let primary = current.primary_node_id.clone();
        let incapable = entry(sinomo_core::node::cluster_member_key(&primary), &member_record(&primary, false));
        let without_capability = members.iter()
            .map(|member| DfsMetadataShardStorageMember {
                node_id: member.node_id.clone(),
                member: if member.node_id == primary { Some(&incapable) } else { member.member },
                policy: None,
            })
            .collect::<Vec<_>>();
        let refused = build_metadata_shard_storage_converting(&DfsMetadataShardStorageCut {
            members: &without_capability, ..takeover_cut(None, None)
        }, &by_b).unwrap_err();
        assert!(refused.to_string().contains(sinomo_core::node::DFS_METADATA_STORAGE_GENERATION_V1_CAPABILITY), "{refused}");
        // The converter must be a configured member other than the primary.
        let outsider = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, Some("outsider-d"), 9));
        assert!(build_metadata_shard_storage_converting(&takeover_cut(None, None), &outsider).is_err());
        // The lost primary must be the shard's current primary.
        let rotated = DfsMetadataShardConfig {
            primary_node_id: node("replica-c"),
            replica_node_ids: vec![node("primary-a"), node("replica-b")],
            ..current.clone()
        };
        let stale = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&rotated, Some("replica-b"), 9));
        assert!(build_metadata_shard_storage_converting(&takeover_cut(None, None), &stale).is_err());
        // On a switched shard the converter must be in the write set.
        let write_set = entry(
            crate::metadata_shard_write_set_key(&current.file_system_id, &current.shard_id),
            &write_set_for(&current, &[primary.clone(), node("replica-b")]),
        );
        let by_c = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, Some("replica-c"), 9));
        assert!(build_metadata_shard_storage_converting(&takeover_cut(None, Some(&write_set)), &by_c).is_err());
        build_metadata_shard_storage_converting(&takeover_cut(None, Some(&write_set)), &by_b).unwrap();
        build_metadata_shard_storage_converting(&takeover_cut(None, None), &by_c).unwrap();
        // A takeover replaces the lost primary's own attempt at the same epoch.
        let own = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, None, 9));
        let own_kv = storage_entry(&own);
        let replaced = build_metadata_shard_storage_converting(&takeover_cut(Some(&own_kv), None), &by_b).unwrap();
        assert!(replaced.txn.compares.contains(&KvCompare::ValueEquals {
            key: own_kv.key.clone(), value: own_kv.value.clone(),
        }));
        // The same converter never replaces its own attempt with another one;
        // the same attempt is idempotent.
        let by_b_kv = storage_entry(&by_b);
        let by_b_again = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, Some("replica-b"), 10));
        assert!(build_metadata_shard_storage_converting(&takeover_cut(Some(&by_b_kv), None), &by_b_again).is_err());
        build_metadata_shard_storage_converting(&takeover_cut(Some(&by_b_kv), None), &by_b).unwrap();
    }

    /// A takeover round recorded under the current serving record is the
    /// only attempt of its round: neither another member's attempt of that
    /// round nor the primary's own attempt replaces it, sealed or not, and
    /// its converter only continues it. Only another member's attempt of the
    /// next round replaces it, so rounds only increase, by one. A serving
    /// change that supersedes it lets the next epoch's attempt replace it.
    #[test]
    fn a_recorded_takeover_round_is_replaced_only_by_the_next_round() {
        let current = ring(3);
        let shard_kv = shard_entry(&current);
        let configured = crate::metadata_shard_configured_node_ids(&current);
        let records = configured.iter()
            .map(|node_id| (node_id.clone(), entry(sinomo_core::node::cluster_member_key(node_id), &member_record(node_id, true))))
            .collect::<Vec<_>>();
        let members = records.iter()
            .map(|(node_id, record)| DfsMetadataShardStorageMember { node_id: node_id.clone(), member: Some(record), policy: None })
            .collect::<Vec<_>>();
        let converting = |shard_kv: &KvEntry, recorded: &DfsMetadataShardStorage, next: &DfsMetadataShardStorage| {
            let recorded_kv = storage_entry(recorded);
            build_metadata_shard_storage_converting(
                &DfsMetadataShardStorageCut { members: &members, ..cut(shard_kv, Some(&recorded_kv)) }, next)
        };
        let round = |converter: &str, takeover: &DfsMetadataConversionTakeover| {
            DfsMetadataShardStorage::converting(&current, unsealed_round(&current, Some(converter), Some(takeover.clone()), 9))
        };
        let first = DfsMetadataConversionTakeover::first_round(current.primary_node_id.clone());
        let by_b = round("replica-b", &first);
        let by_c = round("replica-c", &first);
        let own = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, None, 9));
        let by_b_sealed = DfsMetadataShardStorage::converting(
            &current, sealed(unsealed_attempt(&current, Some("replica-b"), 9), 9));
        let second = first.next_round(&node("replica-b"), &node("replica-c"));
        let by_c_second = round("replica-c", &second);
        for recorded in [&by_b, &by_b_sealed] {
            let refused = converting(&shard_kv, recorded, &by_c).unwrap_err();
            assert!(refused.to_string().contains("runs round 2"), "{refused}");
            let refused = converting(&shard_kv, recorded, &own).unwrap_err();
            assert!(refused.to_string().contains("only its converter continues it"), "{refused}");
            assert!(converting(&shard_kv, recorded, &round("replica-b", &first.next_round(&node("replica-b"), &node("replica-b"))))
                .is_err());
            // Another member's next round replaces it, comparing it exactly.
            let replaced = converting(&shard_kv, recorded, &by_c_second).unwrap();
            assert!(replaced.txn.compares.contains(&KvCompare::ValueEquals {
                key: storage_entry(recorded).key, value: storage_entry(recorded).value,
            }));
        }
        // Rounds are never skipped, repeated or reversed.
        let third = second.next_round(&node("replica-c"), &node("replica-b"));
        let skipped = round("replica-c", &DfsMetadataConversionTakeover { round: 3, ..second.clone() });
        skipped.validate_against(&current).unwrap();
        let refused = converting(&shard_kv, &by_b, &skipped).unwrap_err();
        assert!(refused.to_string().contains("runs round 2"), "a skipped round: {refused}");
        let repeated = round("replica-b", &first.next_round(&node("replica-c"), &node("replica-b")));
        repeated.validate_against(&current).unwrap();
        let refused = converting(&shard_kv, &by_c_second, &repeated).unwrap_err();
        assert!(refused.to_string().contains("runs round 3"), "a repeated round: {refused}");
        let refused = converting(&shard_kv, &by_c_second, &by_b).unwrap_err();
        assert!(refused.to_string().contains("runs round 3"), "an earlier round: {refused}");
        assert!(converting(&shard_kv, &by_c_second, &own).is_err());
        // The converter never replaces its own round with a later one.
        let refused = converting(&shard_kv, &by_c_second, &round("replica-c", &second.next_round(&node("replica-c"), &node("replica-c"))))
            .unwrap_err();
        assert!(refused.to_string().contains("continues its own attempt"), "{refused}");
        // A converter of an earlier round runs a later one without counting
        // itself lost.
        assert_eq!(third.lost_converters, vec![node("replica-c")]);
        converting(&shard_kv, &by_c_second, &round("replica-b", &third)).unwrap();
        // A round whose converter the serving record no longer admits is
        // still the epoch's: the primary does not replace it, the next round does.
        let shrunk = DfsMetadataShardConfig { replica_node_ids: vec![node("replica-c")], ..current.clone() };
        let shrunk_kv = shard_entry(&shrunk);
        assert!(converting(&shrunk_kv, &by_b, &own).is_err());
        converting(&shrunk_kv, &by_b, &by_c_second).unwrap();
        // The lost primary's own attempt is replaced by a first round.
        converting(&shard_kv, &own, &by_c).unwrap();
        assert!(converting(&shard_kv, &own, &by_c_second).is_err(), "nothing to replace in a second round");
        // A serving change supersedes the takeover attempt: the next epoch's
        // primary replaces it.
        let successor = DfsMetadataShardConfig {
            primary_node_id: node("replica-c"),
            replica_node_ids: vec![node("primary-a"), node("replica-b")],
            epoch: current.epoch + 1,
            ..current.clone()
        };
        let successor_kv = shard_entry(&successor);
        let next = DfsMetadataShardStorage::converting(&successor, unsealed_attempt(&successor, None, 9));
        converting(&successor_kv, &by_c_second, &next).unwrap();
    }

    /// A configured member that departed the cluster no longer counts
    /// against a takeover's capability gate: it cannot serve the old format.
    /// Its member record is compared exactly as it stands, absent or not
    /// authorized, so a node that rejoins before the commit rejects the
    /// transaction. A member that is still one must advertise the
    /// capability, and a record that does not decode or names another node
    /// is no departure.
    #[test]
    fn a_departed_member_no_longer_counts_against_the_takeover_capability_gate() {
        let mut kv = InProcessKvStore::new();
        let current = ring(3);
        let nodes = vec![node("primary-a"), node("replica-b"), node("replica-c")];
        let (fs, id) = (current.file_system_id.clone(), current.shard_id.clone());
        put(&mut kv, crate::metadata_shard_key(&fs, &id), &current);
        for node_id in &nodes[1..] {
            put(&mut kv, sinomo_core::node::cluster_member_key(node_id), &member_record(node_id, true));
        }
        // The lost primary left the cluster: its record is no longer
        // authorized, and it never advertised the capability.
        let primary_key = sinomo_core::node::cluster_member_key(&nodes[0]);
        let mut removed = member_record(&nodes[0], false);
        removed.state = sinomo_core::ClusterMemberState::Removed;
        put(&mut kv, primary_key.clone(), &removed);
        let record = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, Some("replica-b"), 9));
        let converting = |kv: &InProcessKvStore| {
            let read = Read::from(kv, &current, &nodes);
            let members = read.members();
            build_metadata_shard_storage_converting(&read.cut(&members), &record)
        };
        let request = converting(&kv).unwrap();
        assert!(request.txn.compares.contains(&KvCompare::ValueEquals {
            key: primary_key.clone(), value: kv.get(&primary_key).unwrap().value.clone(),
        }), "the departed member's record is compared exactly");
        assert!(!request.txn.compares.iter().any(|compare| matches!(compare,
            KvCompare::ValueEquals { key, .. } | KvCompare::Missing { key }
                if *key == sinomo_core::node::cluster_capability_policy_key(&nodes[0]))));
        // It rejoins before the commit: the transaction is rejected, and
        // its capability counts again.
        put(&mut kv, primary_key.clone(), &member_record(&nodes[0], false));
        assert!(!apply(&mut kv, &request));
        let refused = converting(&kv).unwrap_err();
        assert!(refused.to_string().contains(sinomo_core::node::DFS_METADATA_STORAGE_GENERATION_V1_CAPABILITY), "{refused}");
        // A member without any record departed too; its absence is compared.
        kv.delete(primary_key.clone()).unwrap();
        let request = converting(&kv).unwrap();
        assert!(request.txn.compares.contains(&KvCompare::Missing { key: primary_key.clone() }));
        assert!(apply(&mut kv, &request));
        // The takeover activation applies the same gate.
        let read = Read::from(&kv, &current, &nodes);
        let members = read.members();
        let (sealing, _) = build_metadata_shard_storage_seal(&read.cut(&members), &seal(9), &"c".repeat(64)).unwrap();
        assert!(apply(&mut kv, &sealing));
        let activation = |kv: &InProcessKvStore| {
            let read = Read::from(kv, &current, &nodes);
            let members = read.members();
            build_metadata_shard_storage_takeover_activate(&read.cut(&members)).map(|(request, _)| request)
        };
        let request = activation(&kv).unwrap();
        assert!(request.txn.compares.contains(&KvCompare::Missing { key: primary_key.clone() }));
        put(&mut kv, primary_key.clone(), &removed);
        assert!(!apply(&mut kv, &request), "a record written after the read rejects the activation");
        // A record that does not decode is no departure: the gate refuses it.
        let garbage = KvEntry { value: b"not a member record".to_vec(), ..entry(primary_key.clone(), &removed) };
        {
            let read = Read::from(&kv, &current, &nodes);
            let mut members = read.members();
            members[0].member = Some(&garbage);
            assert!(build_metadata_shard_storage_takeover_activate(&read.cut(&members)).is_err());
        }
        let request = activation(&kv).unwrap();
        assert!(apply(&mut kv, &request));
        // The departed verdict: no record, or a record of the node that is
        // not authorized; never a record that does not decode or names
        // another node.
        let verdict = |entry: Option<&KvEntry>| DfsMetadataShardStorageMember { node_id: nodes[0].clone(), member: entry, policy: None }.departed();
        let mut pending = member_record(&nodes[0], true);
        pending.state = sinomo_core::ClusterMemberState::Pending;
        let mut offline = member_record(&nodes[0], true);
        offline.state = sinomo_core::ClusterMemberState::Offline;
        assert!(verdict(None));
        assert!(verdict(Some(&entry(primary_key.clone(), &removed))));
        assert!(verdict(Some(&entry(primary_key.clone(), &pending))));
        assert!(!verdict(Some(&entry(primary_key.clone(), &offline))), "an offline member is still a member");
        assert!(!verdict(Some(&garbage)));
        assert!(!verdict(Some(&entry(primary_key.clone(), &{ let mut other = removed.clone(); other.node_id = nodes[1].clone(); other }))));
        assert!(!verdict(Some(&entry(sinomo_core::node::cluster_member_key(&nodes[1]), &removed))));
    }

    /// The shard's own keys and its members' records, read from `kv`.
    struct Read {
        shard: KvEntry,
        write_set: Option<KvEntry>,
        handoff: Option<KvEntry>,
        watermark: Option<KvEntry>,
        storage: Option<KvEntry>,
        members: Vec<(NodeId, Option<KvEntry>)>,
    }

    impl Read {
        fn from(kv: &InProcessKvStore, shard: &DfsMetadataShardConfig, nodes: &[NodeId]) -> Self {
            let (fs, id) = (&shard.file_system_id, shard.shard_id.as_str());
            Self {
                shard: kv.get(&crate::metadata_shard_key(fs, id)).cloned().expect("shard"),
                write_set: kv.get(&crate::metadata_shard_write_set_key(fs, id)).cloned(),
                handoff: kv.get(&crate::metadata_shard_membership_handoff_key(fs, id)).cloned(),
                watermark: kv.get(&crate::metadata_shard_head_watermark_key(fs, id)).cloned(),
                storage: kv.get(&crate::metadata_shard_storage_key(fs, id)).cloned(),
                members: nodes.iter()
                    .map(|node_id| (node_id.clone(), kv.get(&sinomo_core::node::cluster_member_key(node_id)).cloned()))
                    .collect(),
            }
        }

        fn members(&self) -> Vec<DfsMetadataShardStorageMember<'_>> {
            self.members.iter()
                .map(|(node_id, member)| DfsMetadataShardStorageMember {
                    node_id: node_id.clone(), member: member.as_ref(), policy: None,
                })
                .collect()
        }

        fn cut<'a>(&'a self, members: &'a [DfsMetadataShardStorageMember<'a>]) -> DfsMetadataShardStorageCut<'a> {
            DfsMetadataShardStorageCut {
                shard: &self.shard,
                write_set: self.write_set.as_ref(),
                handoff: self.handoff.as_ref(),
                watermark: self.watermark.as_ref(),
                storage: self.storage.as_ref(),
                member_compares: Vec::new(),
                members,
            }
        }
    }

    fn put(kv: &mut InProcessKvStore, key: String, value: &impl Serialize) {
        kv.put(key, sinomo_core::kv::encode_kv_value(value).unwrap(), None).unwrap();
    }

    fn apply(kv: &mut InProcessKvStore, request: &KvTxnRequest) -> bool {
        kv.txn(request.txn.clone()).unwrap().succeeded
    }

    /// A ring shard with a sealed Converting takeover attempt by `replica-b`,
    /// written through the Converting and seal transactions.
    fn sealed_takeover(switched: bool) -> (InProcessKvStore, DfsMetadataShardConfig, Vec<NodeId>) {
        let mut kv = InProcessKvStore::new();
        let current = ring(3);
        let nodes = vec![node("primary-a"), node("replica-b"), node("replica-c"), node("replica-d")];
        put(&mut kv, crate::metadata_shard_key(&current.file_system_id, &current.shard_id), &current);
        if switched {
            put(&mut kv, crate::metadata_shard_write_set_key(&current.file_system_id, &current.shard_id),
                &write_set_for(&current, &nodes[..3]));
        }
        put(&mut kv, crate::file_system_config_key(&current.file_system_id), &"filesystem configuration");
        for node_id in &nodes {
            put(&mut kv, sinomo_core::node::cluster_member_key(node_id), &member_record(node_id, true));
        }
        let record = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, Some("replica-b"), 9));
        let read = Read::from(&kv, &current, &nodes);
        let members = read.members();
        let converting = build_metadata_shard_storage_converting(&read.cut(&members), &record).unwrap();
        assert!(apply(&mut kv, &converting));
        let read = Read::from(&kv, &current, &nodes);
        let members = read.members();
        let (sealing, _) = build_metadata_shard_storage_seal(&read.cut(&members), &seal(9), &"c".repeat(64)).unwrap();
        assert!(apply(&mut kv, &sealing));
        (kv, current, nodes)
    }

    #[test]
    fn a_takeover_activation_makes_the_converter_primary_in_one_transaction() {
        let (mut kv, current, nodes) = sealed_takeover(true);
        let (fs, id) = (current.file_system_id.clone(), current.shard_id.clone());
        let config_key = crate::file_system_config_key(&fs);
        let config_before = kv.get(&config_key).cloned().unwrap();
        // A takeover supersedes a pending handoff and the head watermark.
        put(&mut kv, crate::metadata_shard_membership_handoff_key(&fs, &id), &"pending");
        put(&mut kv, crate::metadata_shard_head_watermark_key(&fs, &id), &"watermark");
        let read = Read::from(&kv, &current, &nodes);
        let members = read.members();
        // Only the takeover activation activates a takeover attempt.
        assert!(build_metadata_shard_storage_activate(&read.cut(&members)).is_err());
        let (request, active) = build_metadata_shard_storage_takeover_activate(&read.cut(&members)).unwrap();
        for entry in [&read.shard, read.write_set.as_ref().unwrap(), read.storage.as_ref().unwrap(),
            read.handoff.as_ref().unwrap(), read.watermark.as_ref().unwrap()]
        {
            assert!(request.txn.compares.contains(&KvCompare::ValueEquals {
                key: entry.key.clone(), value: entry.value.clone(),
            }), "the activation compares {}", entry.key);
        }
        for node_id in &nodes[..3] {
            assert!(request.txn.compares.iter().any(|compare| matches!(compare,
                KvCompare::ValueEquals { key, .. } if *key == sinomo_core::node::cluster_member_key(node_id))));
        }
        assert!(excludes_filesystem_config(&request), "it neither compares nor writes the filesystem configuration");
        // A request built before a same-epoch serving change cannot commit.
        let mut recruited = current.clone();
        recruited.replica_node_ids.push(nodes[3].clone());
        put(&mut kv, crate::metadata_shard_key(&fs, &id), &recruited);
        assert!(!apply(&mut kv, &request), "a stale serving record refuses the activation");
        let read = Read::from(&kv, &recruited, &nodes);
        let members = read.members();
        let (request, rebuilt) = build_metadata_shard_storage_takeover_activate(&read.cut(&members)).unwrap();
        assert_eq!(rebuilt, active);
        assert!(apply(&mut kv, &request));
        let attempt = active.converted_activation().unwrap().0.clone();
        let seal = attempt.seal.clone().unwrap();
        let shard: DfsMetadataShardConfig = kv.get(&crate::metadata_shard_key(&fs, &id)).unwrap().decode().unwrap();
        assert_eq!(shard.primary_node_id, attempt.primary_node_id);
        assert_eq!(shard.primary_node_id, nodes[1]);
        assert_eq!(shard.epoch, current.epoch + 1);
        assert_eq!(crate::metadata_shard_configured_node_ids(&shard),
            crate::metadata_shard_configured_node_ids(&recruited), "the takeover keeps every configured member");
        let write_set: crate::DfsMetadataShardWriteSet =
            kv.get(&crate::metadata_shard_write_set_key(&fs, &id)).unwrap().decode().unwrap();
        write_set.validate_against(&shard).unwrap();
        assert_eq!(write_set.last_change.kind, crate::DfsMetadataShardWriteSetChangeKind::Takeover {
            previous_primary_node_id: current.primary_node_id.clone(),
        });
        assert_eq!(write_set.last_change.change_id, request.request_id.as_id().to_string());
        assert_eq!(write_set.epoch_start, Some(seal.prefix_proof.clone()), "the epoch starts at the sealed image");
        assert!(!write_set.contains(&current.primary_node_id), "the lost primary leaves the write set");
        assert_eq!(write_set.generation, 2);
        let stored: DfsMetadataShardStorage =
            kv.get(&crate::metadata_shard_storage_key(&fs, &id)).unwrap().decode().unwrap();
        assert_eq!(stored, active);
        stored.validate_against(&shard).unwrap();
        assert!(kv.get(&crate::metadata_shard_membership_handoff_key(&fs, &id)).is_none());
        assert!(kv.get(&crate::metadata_shard_head_watermark_key(&fs, &id)).is_none());
        let config_after = kv.get(&config_key).unwrap();
        assert_eq!((&config_after.value, config_after.modify_revision),
            (&config_before.value, config_before.modify_revision));
        // Once Active, nothing activates again; the old epoch's attempt is
        // superseded for any builder.
        let read = Read::from(&kv, &shard, &nodes);
        let members = read.members();
        assert!(build_metadata_shard_storage_takeover_activate(&read.cut(&members)).is_err());
    }

    /// A member-record change between the takeover activation's read and its
    /// commit rejects it, and the activation re-planned from the changed cut
    /// commits under a new identity.
    #[test]
    fn a_takeover_activation_rejected_by_a_member_record_change_is_replanned() {
        let (mut kv, current, nodes) = sealed_takeover(false);
        let activation = |kv: &InProcessKvStore| {
            let read = Read::from(kv, &current, &nodes);
            let members = read.members();
            build_metadata_shard_storage_takeover_activate(&read.cut(&members)).unwrap().0
        };
        let first = activation(&kv);
        assert_eq!(activation(&kv), first, "the same cut gives the same request");
        let mut moved = member_record(&nodes[2], true);
        moved.display_name.push_str("-renamed");
        put(&mut kv, sinomo_core::node::cluster_member_key(&nodes[2]), &moved);
        assert!(!apply(&mut kv, &first));
        let replanned = activation(&kv);
        assert_ne!(replanned.request_id, first.request_id);
        assert!(apply(&mut kv, &replanned));
    }

    #[test]
    fn a_takeover_activation_refuses_a_superseded_unsealed_or_ordinary_attempt() {
        let (mut kv, current, nodes) = sealed_takeover(false);
        let (fs, id) = (current.file_system_id.clone(), current.shard_id.clone());
        // An unswitched shard's takeover writes the serving and storage
        // records only, and compares the write-set key's absence.
        let read = Read::from(&kv, &current, &nodes);
        let members = read.members();
        let (request, active) = build_metadata_shard_storage_takeover_activate(&read.cut(&members)).unwrap();
        let write_set_key = crate::metadata_shard_write_set_key(&fs, &id);
        assert!(request.txn.compares.contains(&KvCompare::Missing { key: write_set_key.clone() }));
        assert_eq!(request.txn.success.len(), 2);
        // A serving change at a new epoch supersedes the takeover.
        let rotated = DfsMetadataShardConfig {
            primary_node_id: node("replica-c"),
            replica_node_ids: vec![node("primary-a"), node("replica-b")],
            epoch: current.epoch + 1,
            ..current.clone()
        };
        let rotated_kv = shard_entry(&rotated);
        assert!(build_metadata_shard_storage_takeover_activate(&DfsMetadataShardStorageCut {
            shard: &rotated_kv, ..read.cut(&members)
        }).is_err());
        // The converter must still be eligible: a member that left is refused.
        let shrunk = DfsMetadataShardConfig {
            replica_node_ids: vec![node("replica-c")],
            ..current.clone()
        };
        let shrunk_kv = shard_entry(&shrunk);
        assert!(build_metadata_shard_storage_takeover_activate(&DfsMetadataShardStorageCut {
            shard: &shrunk_kv, ..read.cut(&members)
        }).is_err());
        // A member that stops advertising the storage capability refuses it.
        let incapable = entry(sinomo_core::node::cluster_member_key(&nodes[2]), &member_record(&nodes[2], false));
        let without_capability = members.iter()
            .map(|member| DfsMetadataShardStorageMember {
                node_id: member.node_id.clone(),
                member: if member.node_id == nodes[2] { Some(&incapable) } else { member.member },
                policy: None,
            })
            .collect::<Vec<_>>();
        assert!(build_metadata_shard_storage_takeover_activate(&read.cut(&without_capability)).is_err());
        // An unsealed takeover attempt cannot activate.
        let unsealed = DfsMetadataShardStorage::converting(&current, unsealed_attempt(&current, Some("replica-b"), 9));
        let unsealed_kv = storage_entry(&unsealed);
        assert!(build_metadata_shard_storage_takeover_activate(&DfsMetadataShardStorageCut {
            storage: Some(&unsealed_kv), ..read.cut(&members)
        }).is_err());
        // The primary's own attempt is not a takeover.
        let own = DfsMetadataShardStorage::converting(&current, attempt_for(&current, 9));
        let own_kv = storage_entry(&own);
        assert!(build_metadata_shard_storage_takeover_activate(&DfsMetadataShardStorageCut {
            storage: Some(&own_kv), ..read.cut(&members)
        }).is_err());
        build_metadata_shard_storage_activate(&DfsMetadataShardStorageCut {
            storage: Some(&own_kv), ..read.cut(&members)
        }).unwrap();
        assert!(apply(&mut kv, &request));
        let shard: DfsMetadataShardConfig = kv.get(&crate::metadata_shard_key(&fs, &id)).unwrap().decode().unwrap();
        assert_eq!((shard.primary_node_id.clone(), shard.epoch), (nodes[1].clone(), current.epoch + 1));
        assert!(kv.get(&write_set_key).is_none(), "an unswitched shard stays unswitched");
        assert_eq!(kv.get(&crate::metadata_shard_storage_key(&fs, &id)).unwrap()
            .decode::<DfsMetadataShardStorage>().unwrap(), active);
    }
}
