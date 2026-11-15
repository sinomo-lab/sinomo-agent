//! Current storage generation identity. This module does not decode earlier
//! formats or interpret conversion operation names and checkpoint artifacts.
use crate::metadata_error::{MetadataBackendError, Result};
use crate::{DfsFileSystemId, DfsMetadataShardId};
use serde::{Deserialize, Serialize};
use std::path::Path;
#[cfg(feature = "storage-conversion")]
mod activation;
mod standalone;
#[cfg(feature = "storage-conversion")]
pub use standalone::{
    DfsStandaloneStorageActivationWitness, activate_standalone_metadata_storage_generation,
};
#[cfg(test)]
mod native_tests;
#[cfg(feature = "storage-conversion")]
pub use activation::activate_metadata_storage_generation;

/// Immutable identity embedded in every current snapshot of a physical unit.
/// It is DATA, not authorization to install or serve that unit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataGenerationIdentity {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub generation: String,
    pub origin: DfsMetadataGenerationOrigin,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DfsMetadataGenerationOrigin {
    NativeGenesis,
    NativeGenesisInGeneration {
        activation: crate::DfsMetadataStorageActivation,
    },
    StorageTransition {
        transition_id: String,
    },
    StandaloneOwned {
        owner_node_id: sinomo_core::node::NodeId,
        source_binding: String,
    },
}

impl DfsMetadataGenerationIdentity {
    pub(crate) fn is_native_genesis(&self) -> bool {
        matches!(
            self.origin,
            DfsMetadataGenerationOrigin::NativeGenesis
                | DfsMetadataGenerationOrigin::NativeGenesisInGeneration { .. }
        )
    }
    pub(crate) fn matches_native_file_system_storage(
        &self,
        storage: Option<&crate::DfsMetadataStorageState>,
    ) -> bool {
        match &self.origin {
            DfsMetadataGenerationOrigin::NativeGenesis => storage.is_none(),
            DfsMetadataGenerationOrigin::NativeGenesisInGeneration { activation } => {
                storage
                    .and_then(|storage| storage.active_descriptor())
                    .as_ref()
                    == Some(activation)
            }
            _ => false,
        }
    }
    pub fn validate(&self) -> Result<()> {
        fn valid_id(value: &str) -> bool {
            !value.is_empty() && value.len() <= 1024 && !value.contains('\0')
        }
        if !valid_id(&self.generation)
            || !valid_id(&self.shard_id.0)
            || matches!(&self.origin, DfsMetadataGenerationOrigin::StorageTransition { transition_id } if !valid_id(transition_id))
            || matches!(&self.origin, DfsMetadataGenerationOrigin::StandaloneOwned { source_binding, .. } if !valid_id(source_binding))
        {
            return Err(MetadataBackendError::Serde(
                "invalid current metadata generation identity".into(),
            ));
        }
        if let DfsMetadataGenerationOrigin::NativeGenesisInGeneration { activation } = &self.origin
        {
            activation.validate().map_err(MetadataBackendError::Serde)?;
            if activation.generation != self.generation
                || activation.target_format
                    != u32::from(crate::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)
            {
                return Err(MetadataBackendError::Serde(
                    "native birth names a different current storage activation".into(),
                ));
            }
        }
        Ok(())
    }
}

/// The current root's admission envelope. Staging survives restart without
/// conferring serving authority. Active receipts are installed only by the
/// native genesis or storage-import owner after its actual authorization.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataGenerationHeader {
    pub format_version: u16,
    pub identity: DfsMetadataGenerationIdentity,
    pub state: DfsMetadataGenerationState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DfsMetadataGenerationState {
    Staged {
        seal: Option<DfsMetadataGenerationSeal>,
    },
    Active {
        activation_digest: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        standalone_seal: Option<DfsMetadataGenerationSeal>,
    },
}
pub(crate) type GenerationEnvelope = DfsMetadataGenerationHeader;
pub(crate) type GenerationState = DfsMetadataGenerationState;

/// Bounded current-format DATA inspection for the preparation owner. Neither a
/// returned Active value nor this function constructs an authorization permit.
pub fn inspect_metadata_storage_generation(root: &Path) -> Result<DfsMetadataGenerationHeader> {
    read_generation(root)
}

/// Exact current image cut inspected by the native activation owner. The seal
/// grants no authority; a caller cannot activate DATA merely by constructing it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataGenerationSeal {
    pub prefix_proof: crate::DfsMetadataPrefixProof,
    pub state_fingerprint: String,
    pub file_fingerprint: String,
}

pub(crate) fn read_generation(root: &Path) -> Result<GenerationEnvelope> {
    let path = root.join("metadata-generation.sobj");
    use std::io::Read;
    let file = std::fs::File::open(&path).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            MetadataBackendError::Rejected {
                code: crate::DfsMetadataRejectionCode::ReplicaUnreadable,
                message: format!("current metadata generation identity is absent at {}; owned preparation or authenticated snapshot required", path.display()),
            }
        } else {
            MetadataBackendError::Io { path: path.clone(), source }
        }
    })?;
    let mut bytes = Vec::new();
    file.take(16 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| MetadataBackendError::Io { path, source })?;
    if bytes.len() > 16 * 1024 {
        return Err(MetadataBackendError::Serde(
            "metadata generation envelope exceeds its bound".into(),
        ));
    }
    let envelope: GenerationEnvelope = sinomo_core::object::serde_ext::from_bytes(&bytes)
        .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
    envelope.identity.validate()?;
    if envelope.format_version
        != crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
    {
        return Err(MetadataBackendError::Serde(
            "unsupported metadata generation envelope".into(),
        ));
    }
    if matches!(&envelope.state, GenerationState::Active { activation_digest, .. }
        if !activation_digest.strip_prefix("blake3:").is_some_and(|hex|
            hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))))
    {
        return Err(MetadataBackendError::Serde(
            "invalid metadata generation activation digest".into(),
        ));
    }
    if let GenerationState::Active {
        activation_digest,
        standalone_seal,
    } = &envelope.state
    {
        match &envelope.identity.origin {
            DfsMetadataGenerationOrigin::StandaloneOwned { .. } => {
                let seal = standalone_seal.as_ref().ok_or_else(|| {
                    MetadataBackendError::Serde(
                        "standalone generation has no retained activation seal".into(),
                    )
                })?;
                if standalone::standalone_receipt(&envelope.identity, seal)? != *activation_digest {
                    return Err(MetadataBackendError::Serde("standalone generation receipt differs from its retained source/image binding".into()));
                }
            }
            _ if standalone_seal.is_some() => {
                return Err(MetadataBackendError::Serde(
                    "native generation cannot borrow standalone activation evidence".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(envelope)
}

pub(crate) fn write_generation(root: &Path, envelope: &GenerationEnvelope) -> Result<()> {
    envelope.identity.validate()?;
    let bytes = sinomo_core::object::serde_ext::to_bytes(
        envelope,
        crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
        sinomo_core::object::LengthMode::U32,
    )
    .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
    crate::metadata_io::write_file_atomic(&root.join("metadata-generation.sobj"), &bytes)
}

/// Called only by the native bootstrap owner after selecting an absent initial
/// singleton assignment. It refuses every retained root lacking this identity.
pub(crate) fn stage_native_genesis_storage_generation(
    root: &Path,
    shard: &crate::DfsMetadataShardConfig,
    activation: Option<&crate::DfsMetadataStorageActivation>,
    selected_layout: Option<&crate::DfsSelectedOriginalDataLayout>,
) -> Result<()> {
    if shard.epoch != 1 || !shard.replica_node_ids.is_empty() {
        return Err(MetadataBackendError::Serde(
            "fresh storage generation requires the initial singleton assignment".into(),
        ));
    }
    let identity = DfsMetadataGenerationIdentity {
        file_system_id: shard.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
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
    };
    let selected_stage = if let Some(layout) = selected_layout {
        #[cfg(any(test, feature = "test-support"))]
        layout.admit_stage(&shard.file_system_id, &identity.shard_id, root)?;
        Some(layout.stage(&shard.file_system_id, &identity.shard_id, root)?)
    } else { None };
    if root.join("metadata-generation.sobj").exists() {
        let existing = read_generation(root)?;
        if existing.identity != identity {
            return Err(MetadataBackendError::Serde(
                "native genesis storage identity differs from retained root".into(),
            ));
        }
        return Ok(());
    }
    if root.exists()
        && std::fs::read_dir(root)
            .map_err(|source| MetadataBackendError::Io {
                path: root.to_owned(),
                source,
            })?
            .next()
            .is_some()
        && !selected_stage.as_ref().map(|stage| stage.contains_only_marker().map_err(|error| MetadataBackendError::Serde(error.to_string()))).transpose()?.unwrap_or(false)
    {
        return Err(MetadataBackendError::Serde(
            "retained metadata without current generation requires owned preparation".into(),
        ));
    }
    std::fs::create_dir_all(root).map_err(|source| MetadataBackendError::Io {
        path: root.to_owned(),
        source,
    })?;
    write_generation(
        root,
        &GenerationEnvelope {
            format_version: crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            identity,
            state: GenerationState::Staged { seal: None },
        },
    )
}

fn native_genesis_terminal(
    identity: &DfsMetadataGenerationIdentity,
    tables: &crate::metadata_tables::DfsMetadataTables,
) -> Result<crate::DfsPreparedOperationEviction> {
    identity.validate()?;
    let expected_binding = match &identity.origin {
        DfsMetadataGenerationOrigin::NativeGenesis => {
            if identity.generation
                != format!(
                    "native-genesis:{}:{}:{}",
                    identity.file_system_id,
                    identity.shard_id.0,
                    crate::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
                )
            {
                return Err(MetadataBackendError::Serde(
                    "native generation differs from its original fresh genesis".into(),
                ));
            }
            None
        }
        DfsMetadataGenerationOrigin::NativeGenesisInGeneration { activation } => Some(activation),
        _ => {
            return Err(MetadataBackendError::Serde(
                "generation is not a native birth".into(),
            ));
        }
    };
    let operation = format!(
        "genesis:{}:{}:1",
        identity.file_system_id, identity.shard_id.0
    );
    let terminal = if let Some(record) = tables.prepared_operations_v7.get(&operation) {
        crate::DfsPreparedOperationEviction::from_terminal_record(record)?
    } else if let Some(record) = tables.prepared_operation_evictions.get(&operation) {
        record.validate()?;
        record.clone()
    } else {
        return Err(MetadataBackendError::Serde(
            "native generation image has no original committed genesis".into(),
        ));
    };
    if terminal.digest.file_system_id != identity.file_system_id
        || terminal.digest.shard_id != identity.shard_id
        || terminal.digest.owner_epoch != 1
        || terminal.status != crate::DfsPreparedOperationStatus::Committed
        || terminal.committed_entry.is_none()
        || terminal.genesis_storage_activation.as_ref() != expected_binding
    {
        return Err(MetadataBackendError::Serde(
            "native generation differs from its original committed storage binding".into(),
        ));
    }
    Ok(terminal)
}

pub(crate) fn validate_native_genesis_snapshot(
    identity: &DfsMetadataGenerationIdentity,
    tables: &crate::metadata_tables::DfsMetadataTables,
) -> Result<()> {
    if identity.is_native_genesis() {
        native_genesis_terminal(identity, tables)?;
    }
    Ok(())
}

pub(crate) fn native_genesis_activation_digest(
    identity: &DfsMetadataGenerationIdentity,
    tables: &crate::metadata_tables::DfsMetadataTables,
) -> Result<String> {
    let terminal = native_genesis_terminal(identity, tables)?;
    // Preserve the original unbound native receipt encoding. A generation-bound
    // birth additionally binds its exact retained terminal activation witness.
    let bytes = if let Some(binding) = &terminal.genesis_storage_activation {
        serde_json::to_vec(&(&terminal.digest, terminal.committed_entry.as_ref(), binding))
    } else {
        serde_json::to_vec(&(&terminal.digest, terminal.committed_entry.as_ref()))
    }
    .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
    Ok(format!("blake3:{}", blake3::hash(&bytes).to_hex()))
}

#[cfg(all(test, feature = "storage-conversion"))]
mod tests {
    use super::*;
    fn identity() -> DfsMetadataGenerationIdentity {
        DfsMetadataGenerationIdentity {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: DfsMetadataShardId::new("root"),
            generation: "generation-data-control".into(),
            origin: DfsMetadataGenerationOrigin::StorageTransition {
                transition_id: "transition-data-control".into(),
            },
        }
    }
    #[test]
    fn current_candidate_generation_is_embedded_sealed_and_never_serving_authority() {
        let root = sinomo_testdir::TestDir::new("current-generation-data");
        let id = identity();
        let writer =
            crate::DfsMetadataCandidateWriter::open_generation(root.path(), id.clone()).unwrap();
        let prefix = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&id.shard_id);
        let candidate = writer.write(Default::default(), prefix).unwrap();
        let seal = writer.seal_storage_generation(&candidate).unwrap();
        assert_eq!(seal.prefix_proof, candidate.prefix_proof);
        assert!(
            writer
                .write(Default::default(), candidate.prefix_proof.clone())
                .is_err()
        );
        drop(writer);
        assert!(matches!(
            crate::LocalSobjMetadataBackend::new(id.shard_id.clone(), root.path()),
            Err(MetadataBackendError::StoragePreparationPending { .. })
        ));
        let mut changed = read_generation(root.path()).unwrap();
        changed.identity.generation.push_str("-foreign");
        write_generation(root.path(), &changed).unwrap();
        assert!(crate::inspect_metadata_candidate(root.path()).is_err());
    }
    #[test]
    fn current_generation_missing_or_corrupt_envelope_never_becomes_fresh_storage() {
        let root = sinomo_testdir::TestDir::new("missing-current-generation");
        let id = identity();
        assert!(crate::LocalSobjMetadataBackend::new(id.shard_id.clone(), root.path()).is_err());
        assert!(!root.join("records").exists());
        std::fs::write(
            root.join("metadata-generation.sobj"),
            b"invalid current generation",
        )
        .unwrap();
        assert!(crate::LocalSobjMetadataBackend::new(id.shard_id, root.path()).is_err());
        assert!(!root.join("records").exists());
    }

    #[test]
    fn transition_reservations_require_retiring_scope_and_do_not_authorize_open() {
        let root = sinomo_testdir::TestDir::new("unsettled-generation-data");
        let id = identity();
        let writer =
            crate::DfsMetadataCandidateWriter::open_generation(root.path(), id.clone()).unwrap();
        let prefix = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&id.shard_id);
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                file_system_id: id.file_system_id.clone(),
                shard_id: id.shard_id.clone(),
                owner_epoch: 1,
                operation_id: "retained-body-data-negative".into(),
            },
            Vec::new(),
            Vec::new(),
            Vec::new(),
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
        let (prepared, prefix) = writer.prepare(payload, &prefix).unwrap();
        let mut rows = crate::DfsMetadataTableSnapshotTables::default();
        rows.prepared_operations_v7.push(prepared);
        let cut = writer.write(rows, prefix).unwrap();
        writer.seal_storage_generation(&cut).unwrap();
        let inspection = crate::DfsMetadataCandidateInspection::capture(root.path()).unwrap();
        let retiring = crate::DfsMetadataShardConfig {
            file_system_id: id.file_system_id.clone(), shard_id: id.shard_id.0.clone(), epoch: 1,
            primary_node_id: sinomo_core::node::NodeId::from_legacy_named("reservation-owner").unwrap(),
            replica_node_ids: Vec::new(), assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        inspection.validate_retiring_reservations(&retiring).unwrap();
        let mut foreign = retiring.clone();
        foreign.shard_id = "other".into();
        assert!(inspection.validate_retiring_reservations(&foreign).is_err());
        foreign = retiring;
        foreign.epoch = 0;
        assert!(inspection.validate_retiring_reservations(&foreign).is_err());
        assert!(crate::LocalSobjMetadataBackend::new(id.shard_id, root.path()).is_err());
        assert_eq!(crate::inspect_metadata_candidate(root.path()).unwrap(), cut);
    }

    #[test]
    fn transition_seal_rejects_full_terminal_bodies() {
        let root = sinomo_testdir::TestDir::new("terminal-generation-data");
        let id = identity();
        let writer = crate::DfsMetadataCandidateWriter::open_generation(root.path(), id.clone()).unwrap();
        let initial = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&id.shard_id);
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope { file_system_id: id.file_system_id, shard_id: id.shard_id,
                owner_epoch: 1, operation_id: "terminal-body-data-negative".into() },
            Vec::new(), Vec::new(), Vec::new(), time::OffsetDateTime::UNIX_EPOCH,
        ).unwrap();
        let (prepared, prefix) = writer.prepare(payload, &initial).unwrap();
        let (terminal, prefix) = writer.commit(prepared, &prefix, time::OffsetDateTime::UNIX_EPOCH).unwrap();
        let mut rows = crate::DfsMetadataTableSnapshotTables::default();
        rows.prepared_operations_v7.push(terminal);
        let cut = writer.write(rows, prefix).unwrap();
        assert!(writer.seal_storage_generation(&cut).is_err());
        assert_eq!(crate::inspect_metadata_candidate(root.path()).unwrap(), cut);
    }

    #[test]
    fn staged_genesis_without_real_committed_outcome_cannot_activate() {
        let root = sinomo_testdir::TestDir::new("uncommitted-genesis-generation");
        let shard = crate::DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: sinomo_core::node::NodeId::from_legacy_named(
                "generation-data-negative",
            )
            .unwrap(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        stage_native_genesis_storage_generation(root.path(), &shard, None, None).unwrap();
        let mut backend = crate::LocalSobjMetadataBackend::open_staged_native_genesis(
            &shard,
            root.path().to_owned(),
            None,
        )
        .unwrap();
        let before = backend.metadata_prefix_proof().unwrap();
        assert!(
            backend
                .activate_native_genesis_storage_generation(&shard)
                .is_err()
        );
        assert_eq!(backend.metadata_prefix_proof().unwrap(), before);
        assert!(matches!(
            read_generation(root.path()).unwrap().state,
            GenerationState::Staged { .. }
        ));
    }
    fn generation_bound_birth_data(
        root: &Path,
    ) -> (
        DfsMetadataGenerationIdentity,
        crate::metadata_tables::DfsMetadataTables,
        crate::DfsPreparedOperationDigest,
    ) {
        let activation = crate::DfsMetadataStorageActivation {
            generation: "bound-generation-a".into(),
            target_format: u32::from(crate::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION),
            catalog_generation: 7,
            activation_digest: format!("blake3:{}", "a".repeat(64)),
        };
        let identity = DfsMetadataGenerationIdentity {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: DfsMetadataShardId::new("new-scope"),
            generation: activation.generation.clone(),
            origin: DfsMetadataGenerationOrigin::NativeGenesisInGeneration {
                activation: activation.clone(),
            },
        };
        let mut fs = crate::DfsFileSystem::sifs(
            sinomo_core::node::NodeId::from_legacy_named("birth-data-only").unwrap(),
        );
        fs.catalog_generation = 8;
        fs.metadata_storage = Some(crate::DfsMetadataStorageState::Active {
            generation: activation.generation,
            target_format: activation.target_format,
            catalog_generation: activation.catalog_generation,
            activation_digest: activation.activation_digest,
        });
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                file_system_id: identity.file_system_id.clone(),
                shard_id: identity.shard_id.clone(),
                owner_epoch: 1,
                operation_id: format!(
                    "genesis:{}:{}:1",
                    identity.file_system_id, identity.shard_id.0
                ),
            },
            Vec::new(),
            Vec::new(),
            vec![crate::DfsPreparedMetadataEffect::PutFileSystem {
                file_system: fs.clone(),
            }],
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
        let writer =
            crate::DfsMetadataCandidateWriter::open_generation(root, identity.clone()).unwrap();
        let prefix = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&identity.shard_id);
        let (record, prefix) = writer.prepare(payload, &prefix).unwrap();
        let (record, _) = writer
            .commit(record, &prefix, time::OffsetDateTime::UNIX_EPOCH)
            .unwrap();
        let mut state = crate::DfsMetadataCandidateState::new(Default::default()).unwrap();
        state
            .apply(crate::DfsPreparedMetadataEffect::PutFileSystem { file_system: fs })
            .unwrap();
        let digest = record.digest.clone();
        state.restore_prepared(record).unwrap();
        let tables =
            crate::metadata_table_store::metadata_tables_from_snapshot(state.snapshot()).unwrap();
        (identity, tables, digest)
    }

    #[test]
    fn birth_binding_data_survives_checked_eviction_and_rejects_restamped_identity() {
        // These are DATA codec/validator controls, not a claimed native birth.
        let root = sinomo_testdir::TestDir::new("birth-binding-compaction-data");
        let (identity, mut tables, digest) = generation_bound_birth_data(root.path());
        let original = native_genesis_activation_digest(&identity, &tables).unwrap();
        let mut restamped = identity.clone();
        let DfsMetadataGenerationOrigin::NativeGenesisInGeneration { activation } =
            &mut restamped.origin
        else {
            unreachable!()
        };
        activation.generation = "bound-generation-b".into();
        activation.activation_digest = format!("blake3:{}", "b".repeat(64));
        restamped.generation = activation.generation.clone();
        assert!(native_genesis_activation_digest(&restamped, &tables).is_err());
        tables
            .apply_checked(crate::metadata_records::DfsMetadataTableOp::RemovePreparedOperation {
                digest: digest.clone(),
            })
            .unwrap();
        assert!(
            !tables
                .prepared_operations_v7
                .contains_key(&digest.operation_id)
        );
        assert!(
            tables.prepared_operation_evictions[&digest.operation_id]
                .genesis_storage_activation
                .is_some()
        );
        assert_eq!(
            native_genesis_activation_digest(&identity, &tables).unwrap(),
            original
        );
        assert!(
            native_genesis_activation_digest(&restamped, &tables).is_err(),
            "restamping cannot replace the original retained witness"
        );
        let unbound = DfsMetadataGenerationIdentity {
            generation: format!(
                "native-genesis:{}:{}:{}",
                identity.file_system_id,
                identity.shard_id.0,
                crate::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
            ),
            origin: DfsMetadataGenerationOrigin::NativeGenesis,
            ..identity
        };
        assert!(native_genesis_activation_digest(&unbound, &tables).is_err());
    }
}
