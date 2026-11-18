//! Local activation of a converted generation under its shard's storage
//! record. Any configured member, primary or replica, activates its staged
//! copy once the record is Active for that generation; the record carries the
//! activation evidence, so no handoff proof or filesystem state is read.
use super::*;

/// Activate this holder's staged copy of `identity`, the converted generation
/// its shard's storage record activated. The record must be Active for exactly
/// this generation, the local node a configured member of the shard, and the
/// staged header sealed with the record's seal; the installed image is checked
/// against that seal under the writer lock. A header already Active with the
/// record's digest is left as it is. This grants no publication authority.
/// sinomo:kv-consistency: Bind local activation of a converted image to its shard's committed Active storage record and current membership.
pub async fn activate_metadata_shard_storage(
    context: &sinomo_module::ComponentContext,
    root: &Path,
    identity: &DfsMetadataGenerationIdentity,
) -> Result<()> {
    let _owner = require_owned_unit(context, root, identity)?;
    let cluster = context
        .cluster_access()
        .ok_or_else(|| invalid("storage activation requires native cluster authority"))?;
    let storage_key =
        crate::metadata_shard_storage_key(&identity.file_system_id, &identity.shard_id.0);
    let shard_key = crate::metadata_shard_key(&identity.file_system_id, &identity.shard_id.0);
    // One native exact-set cut binds the storage record and the membership.
    let read = cluster
        .kv_get_authoritative_exact_set_async(vec![storage_key.clone(), shard_key.clone()])
        .await
        .map_err(invalid)?;
    require_current_authority(&read.authority)?;
    if read.value.len() != 2 {
        return Err(invalid("storage activation exact-set cardinality changed"));
    }
    let record: DfsMetadataShardStorage = entry(&read.value[0], &storage_key)?
        .decode()
        .map_err(invalid)?;
    let shard: crate::DfsMetadataShardConfig = entry(&read.value[1], &shard_key)?
        .decode()
        .map_err(invalid)?;
    let local = sinomo_core::cluster::read_cluster_state_without_kv(
        &sinomo_core::network::ClusterPaths::new(context.data_dir()),
    )
    .map_err(invalid)?
    .local_node_id;
    activate_shard_storage_header(root, identity, &local, &shard, &record)
}

/// The local part of [`activate_metadata_shard_storage`], given the record and
/// shard configuration it read.
pub(super) fn activate_shard_storage_header(
    root: &Path,
    identity: &DfsMetadataGenerationIdentity,
    local: &sinomo_core::node::NodeId,
    shard: &crate::DfsMetadataShardConfig,
    record: &DfsMetadataShardStorage,
) -> Result<()> {
    record.validate_against(shard)?;
    let Some((attempt, activation_digest)) = record.converted_activation() else {
        return Err(invalid(
            "the shard's storage record has not activated a converted generation",
        ));
    };
    let Some((seal, _)) = attempt.sealed() else {
        return Err(invalid("the activated conversion has no sealed image"));
    };
    if record.generation != *identity
        || shard.file_system_id != identity.file_system_id
        || shard.shard_id != identity.shard_id.0
    {
        return Err(invalid(
            "the shard's storage record activated another generation",
        ));
    }
    if !crate::metadata_shard_configured_node_ids(shard).contains(local) {
        return Err(invalid(
            "the local node is not a configured member of the shard",
        ));
    }
    let _writer = crate::metadata_lock::metadata_write_lock(
        root.to_owned(),
        crate::metadata_cursor::journal_write_lock_path(root),
    )?;
    let mut envelope = read_generation(root)?;
    if envelope.identity != *identity {
        return Err(invalid("installed storage generation identity changed"));
    }
    let active = GenerationState::Active {
        activation_digest: activation_digest.to_owned(),
    };
    match &envelope.state {
        state if state == &active => return Ok(()),
        GenerationState::Staged { seal: Some(staged) } if staged == seal => {}
        _ => {
            return Err(invalid(
                "installed generation is not staged with the activated seal",
            ));
        }
    }
    let inspection = crate::DfsMetadataCandidateInspection::capture(root)?;
    // Inherited preparations belong to the source's authority, at most the
    // epoch the attempt converted under.
    inspection.validate_retiring_reservations(&crate::DfsMetadataShardConfig {
        epoch: attempt.epoch,
        ..shard.clone()
    })?;
    let actual = inspection.identity();
    if actual.prefix_proof != seal.prefix_proof
        || actual.state_fingerprint != seal.state_fingerprint
        || actual.file_fingerprint != seal.file_fingerprint
    {
        return Err(invalid(
            "installed candidate differs from the activated seal",
        ));
    }
    envelope.state = active;
    write_generation(root, &envelope)
}

/// The checks [`activate_metadata_shard_storage`] makes before it reads authority: an
/// owned (not shadow) context, a valid identity whose exact unit root inside
/// the data directory is `root`, and the live data-owner lock, which the
/// caller holds for the whole activation.
fn require_owned_unit(
    context: &sinomo_module::ComponentContext,
    root: &Path,
    identity: &DfsMetadataGenerationIdentity,
) -> Result<impl Sized + use<>> {
    if context.host_role() == sinomo_module::ComponentHostRole::Shadow {
        return Err(invalid(
            "shadow context cannot activate owned metadata storage",
        ));
    }
    identity.validate()?;
    let expected_root =
        crate::local_dfs_metadata_shard_path(context.data_dir(), &identity.shard_id);
    let actual_root = root.canonicalize().map_err(invalid)?;
    let data_root = context.data_dir().root().canonicalize().map_err(invalid)?;
    if actual_root != expected_root.canonicalize().map_err(invalid)?
        || !actual_root.starts_with(&data_root)
    {
        return Err(invalid(
            "storage activation root differs from the actual component unit",
        ));
    }
    crate::runtime_host::require_daemon_dfs_data_owner_lock(
        &crate::runtime_host::DfsRuntimePaths::new(context.data_dir()),
    )
    .map_err(invalid)
}

/// Require an authoritative read confirmed by a current-term voter quorum.
fn require_current_authority(authority: &sinomo_core::kv::KvAuthorityProof) -> Result<()> {
    match authority.basis {
        sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
            voter_acks,
            required_quorum,
        } if authority.term != 0 && required_quorum > 0 && voter_acks >= required_quorum => Ok(()),
        _ => Err(invalid("storage activation lacks current native authority")),
    }
}

fn invalid(error: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(format!("current storage generation activation: {error}"))
}
fn entry<'a>(
    entry: &'a Option<sinomo_core::kv::KvEntry>,
    expected: &str,
) -> Result<&'a sinomo_core::kv::KvEntry> {
    entry
        .as_ref()
        .filter(|entry| entry.key == expected)
        .ok_or_else(|| invalid("native activation entry is missing or misbound"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::node::NodeId;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).unwrap()
    }

    fn shard(
        file_system_id: crate::DfsFileSystemId,
        shard_id: &str,
        primary: &str,
    ) -> crate::DfsMetadataShardConfig {
        crate::DfsMetadataShardConfig {
            file_system_id,
            shard_id: shard_id.into(),
            primary_node_id: node(primary),
            replica_node_ids: vec![node("activation-replica")],
            epoch: 3,
            assignment: crate::DfsMetadataShardAssignment::PathPrefix {
                prefix: format!("/{shard_id}"),
            },
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        }
    }

    /// Stage a sealed converted candidate of one attempt under `shard` at
    /// `root`, as a receiver's install leaves it, and return the attempt's
    /// sealed Converting record.
    fn stage(root: &Path, shard: &crate::DfsMetadataShardConfig) -> DfsMetadataShardStorage {
        stage_image(root, shard, "0")
    }

    /// [`stage`] with an image whose prefix digest repeats `digit`.
    fn stage_image(
        root: &Path,
        shard: &crate::DfsMetadataShardConfig,
        digit: &str,
    ) -> DfsMetadataShardStorage {
        let source = DfsMetadataConversionSource {
            source_identity: "retained-source".into(),
            source_format: Some(10),
            source_journal_sequence: 4,
            source_state_fingerprint: "source-state".into(),
            source_cut_digest: "a".repeat(64),
            pending_mapping_digest: "b".repeat(64),
        };
        let attempt_id = metadata_shard_conversion_attempt_id(
            &shard.file_system_id,
            &shard.shard_id,
            &shard.assignment,
            shard.epoch,
            &shard.primary_node_id,
            &source,
            None,
        )
        .unwrap();
        let generation = metadata_shard_converted_generation(
            &shard.file_system_id,
            &shard.shard_id,
            &attempt_id,
        );
        let writer = crate::DfsMetadataCandidateWriter::open_generation(root, generation).unwrap();
        let proof = crate::DfsMetadataPrefixProof {
            version: 2,
            journal_sequence: 0,
            digest: format!("dfs-metadata-prefix-v2:blake3:{}", digit.repeat(64)),
        };
        let candidate = writer.write(Default::default(), proof).unwrap();
        let seal = writer.seal_storage_generation(&candidate).unwrap();
        drop(writer);
        DfsMetadataShardStorage::converting(
            shard,
            DfsMetadataShardConversion {
                attempt_id,
                epoch: shard.epoch,
                primary_node_id: shard.primary_node_id.clone(),
                takeover: None,
                source,
                seal: Some(seal),
                contributions_digest: Some("c".repeat(64)),
            },
        )
    }

    fn activated(converting: &DfsMetadataShardStorage) -> DfsMetadataShardStorage {
        let attempt = converting.converting_attempt().unwrap().clone();
        let mut active = converting.clone();
        active.state = DfsMetadataShardStorageState::Active {
            activation: DfsMetadataShardActivation::Converted {
                activation_digest: metadata_shard_storage_activation_digest(
                    &active.generation,
                    &attempt,
                )
                .unwrap(),
                attempt,
            },
        };
        active
    }

    #[test]
    fn a_member_activates_its_staged_copy_only_under_the_active_record() {
        let root = sinomo_testdir::TestDir::new("shard-storage-local-activation");
        let shard = shard(crate::sifs_file_system_id(), "users", "activation-primary");
        let converting = stage(root.path(), &shard);
        let active = activated(&converting);
        let identity = active.generation.clone();
        let replica = node("activation-replica");
        let staged = std::fs::read(root.path().join("metadata-generation.sobj")).unwrap();
        let unchanged = || {
            assert_eq!(
                std::fs::read(root.path().join("metadata-generation.sobj")).unwrap(),
                staged,
                "a refused activation leaves the staged header"
            );
        };
        // A Converting record activates nothing: the shard stays closed.
        assert!(
            activate_shard_storage_header(root.path(), &identity, &replica, &shard, &converting)
                .is_err()
        );
        unchanged();
        // Only a configured member activates its copy.
        assert!(
            activate_shard_storage_header(
                root.path(),
                &identity,
                &node("activation-outsider"),
                &shard,
                &active
            )
            .is_err()
        );
        unchanged();
        // The record must activate exactly the staged generation and seal.
        let other = activated(&DfsMetadataShardStorage::converting(&shard, {
            let mut attempt = converting.converting_attempt().unwrap().clone();
            attempt
                .seal
                .as_mut()
                .unwrap()
                .state_fingerprint
                .push_str("-fork");
            attempt
        }));
        assert_eq!(
            other.generation, identity,
            "the seal is not part of the generation"
        );
        assert!(
            activate_shard_storage_header(root.path(), &identity, &replica, &shard, &other)
                .is_err()
        );
        unchanged();
        let mut foreign = identity.clone();
        foreign.generation.push_str("-foreign");
        assert!(
            activate_shard_storage_header(root.path(), &foreign, &replica, &shard, &active)
                .is_err()
        );
        unchanged();
        // The installed image is checked against the seal: another complete
        // image of the same generation is refused.
        let other_root = sinomo_testdir::TestDir::new("shard-storage-local-activation-other");
        let other_image = stage_image(other_root.path(), &shard, "1");
        assert_eq!(other_image.generation, identity);
        assert_ne!(
            other_image.converting_attempt().unwrap().seal,
            converting.converting_attempt().unwrap().seal
        );
        let image = crate::metadata_table_store::metadata_table_snapshot_path(root.path());
        let original = std::fs::read(&image).unwrap();
        std::fs::copy(
            crate::metadata_table_store::metadata_table_snapshot_path(other_root.path()),
            &image,
        )
        .unwrap();
        assert!(
            activate_shard_storage_header(root.path(), &identity, &replica, &shard, &active)
                .is_err()
        );
        unchanged();
        std::fs::write(&image, &original).unwrap();
        // A replica activates under the record, whatever node is primary now.
        let mut taken_over = shard.clone();
        taken_over.epoch += 1;
        taken_over.primary_node_id = replica.clone();
        taken_over.replica_node_ids = vec![shard.primary_node_id.clone()];
        activate_shard_storage_header(root.path(), &identity, &replica, &taken_over, &active)
            .unwrap();
        let header = read_generation(root.path()).unwrap();
        assert!(matches!(
            &header.state,
            GenerationState::Active {
                ..
            }
        ));
        assert!(
            active.admits_header(&header),
            "the header carries the record's activation digest"
        );
        let bytes = std::fs::read(root.path().join("metadata-generation.sobj")).unwrap();
        // Activation is idempotent.
        activate_shard_storage_header(root.path(), &identity, &replica, &taken_over, &active)
            .unwrap();
        assert_eq!(
            std::fs::read(root.path().join("metadata-generation.sobj")).unwrap(),
            bytes
        );
        // A record with another digest does not admit the active header.
        assert!(
            activate_shard_storage_header(root.path(), &identity, &replica, &taken_over, &other)
                .is_err()
        );
    }

    #[test]
    fn native_activation_reads_the_shards_committed_storage_record() {
        use sinomo_core::kv::{KvCompare, KvOp, KvTxn};
        let (native, nodes) = crate::test_runtime::DfsTestRuntime::start_cluster(
            "native-shard-storage-activation",
            1,
        )
        .unwrap();
        let local = nodes[0].clone();
        // A shard of a filesystem the running node does not serve, so no
        // owner opens the staged root while the test drives it. The local
        // node is a replica; the primary is not a cluster member.
        let file_system_id =
            crate::DfsFileSystemId::from_legacy_named("shard-activation-fs").unwrap();
        let mut shard = shard(
            file_system_id,
            "shard-activation-converted",
            "activation-absent-primary",
        );
        shard.replica_node_ids = vec![local.clone()];
        let shard_id = crate::DfsMetadataShardId::new(&shard.shard_id);
        let root = crate::local_dfs_metadata_shard_path(native.data_dir(&local), &shard_id);
        let converting = stage(&root, &shard);
        let active = activated(&converting);
        let identity = active.generation.clone();
        let context = native.component_context(&local).unwrap();
        let shard_key = crate::metadata_shard_key(&shard.file_system_id, &shard.shard_id);
        let storage_key = crate::metadata_shard_storage_key(&shard.file_system_id, &shard.shard_id);
        let shared = native.shared_control_plane().unwrap();
        let txn = |compares: Vec<KvCompare>, success: Vec<KvOp>| {
            shared
                .with_client(|kv| {
                    kv.as_mut().txn(KvTxn {
                        compares,
                        success,
                        failure: Vec::new(),
                    })
                })
                .unwrap()
                .succeeded
        };
        let activate = || {
            native
                .runtime_handle()
                .block_on(activate_metadata_shard_storage(&context, &root, &identity))
        };
        // Without a record the shard has nothing to activate.
        assert!(activate().is_err());
        assert!(txn(
            vec![
                KvCompare::Missing {
                    key: shard_key.clone()
                },
                KvCompare::Missing {
                    key: storage_key.clone()
                }
            ],
            vec![
                KvOp::put_serde(shard_key.clone(), &shard, None).unwrap(),
                KvOp::put_serde(storage_key.clone(), &converting, None).unwrap(),
            ],
        ));
        // A Converting record keeps the shard closed.
        let refused = activate().unwrap_err();
        assert!(
            refused.to_string().contains("has not activated"),
            "{refused}"
        );
        assert!(matches!(
            read_generation(&root).unwrap().state,
            GenerationState::Staged { seal: Some(_) }
        ));
        assert!(txn(
            vec![KvCompare::ValueEquals {
                key: storage_key.clone(),
                value: sinomo_core::kv::encode_kv_value(&converting).unwrap(),
            }],
            vec![KvOp::put_serde(storage_key.clone(), &active, None).unwrap()],
        ));
        activate().unwrap();
        let header = read_generation(&root).unwrap();
        assert!(active.admits_header(&header));
        activate().unwrap();
        assert_eq!(
            read_generation(&root).unwrap(),
            header,
            "activation is idempotent"
        );
        assert!(txn(
            Vec::new(),
            vec![
                KvOp::Delete { key: storage_key },
                KvOp::Delete { key: shard_key }
            ],
        ));
        drop(shared);
        drop(context);
        native.close().unwrap();
    }
}
