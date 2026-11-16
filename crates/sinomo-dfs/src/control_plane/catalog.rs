//! A filesystem's complete shard set is fenced by its existing config record.
use super::*;

/// Preserve every configuration field while advancing the complete catalog.
/// The result is DATA: its exact predecessor comparison and admitted catalog
/// mutation must be in the same authoritative transaction.
pub fn metadata_catalog_generation_successor(
    expected: &DfsFileSystemConfig,
) -> Result<DfsFileSystemConfig, KvError> {
    validate_catalog_transition(expected, None)?;
    catalog_generation_successor_unchecked(expected)
}

fn catalog_generation_successor_unchecked(
    expected: &DfsFileSystemConfig,
) -> Result<DfsFileSystemConfig, KvError> {
    let mut successor = expected.clone();
    successor.catalog_generation =
        expected
            .catalog_generation
            .checked_add(1)
            .ok_or_else(|| KvError::Serialization {
                detail: "metadata catalog generation exhausted".into(),
            })?;
    Ok(successor)
}

pub(super) fn validate_catalog_transition(
    config: &DfsFileSystemConfig,
    transition: Option<&str>,
) -> Result<(), KvError> {
    match (&config.metadata_storage, transition) {
        (
            Some(crate::DfsMetadataStorageState::PendingTransition { transition_id, .. }),
            Some(actual),
        ) if transition_id == actual => Ok(()),
        (Some(crate::DfsMetadataStorageState::PendingTransition { .. }), _) => {
            Err(KvError::Serialization {
                detail: "metadata catalog is reserved by its pending storage transition".into(),
            })
        }
        (_, None) => Ok(()),
        (_, Some(_)) => Err(KvError::Serialization {
            detail: "storage handoff has no matching pending filesystem transition".into(),
        }),
    }
}

/// Validate generic handoff admission using current filesystem and previous
/// completed proof DATA. The caller includes every returned comparison in its
/// existing native handoff transaction; validation alone grants no authority.
pub(super) fn catalog_handoff_guard(
    config_entry: Option<KvEntry>,
    previous_proof: Option<KvEntry>,
    retiring: &DfsMetadataShardConfig,
    desired: &DfsMetadataShardConfig,
    cause: &DfsMetadataShardHandoffCause,
) -> Result<(Vec<KvCompare>, DfsFileSystemConfig), KvError> {
    let entry = config_entry.ok_or_else(|| KvError::Serialization {
        detail: "metadata handoff requires filesystem catalog configuration".into(),
    })?;
    let config = entry.decode::<DfsFileSystemConfig>()?;
    if entry.key != file_system_config_key(&retiring.file_system_id)
        || config.file_system_id != retiring.file_system_id
    {
        return Err(KvError::Serialization {
            detail: "metadata handoff has foreign filesystem configuration".into(),
        });
    }
    let transition = match cause {
        DfsMetadataShardHandoffCause::Membership => None,
        DfsMetadataShardHandoffCause::StorageImport { cut } => cut.storage_transition_id.as_deref(),
    };
    validate_catalog_transition(&config, transition)?;
    validate_metadata_shard_handoff_cause(retiring, desired, cause)?;
    let proof_key =
        metadata_shard_membership_handoff_proof_key(&retiring.file_system_id, &retiring.shard_id);
    let proof_compare = match previous_proof {
        Some(previous) => {
            if previous.key != proof_key {
                return Err(KvError::Serialization {
                    detail: "metadata handoff returned foreign completed proof".into(),
                });
            }
            let proof: DfsMetadataShardMembershipHandoffProof = previous.decode()?;
            if transition.is_some()
                && matches!(&proof.cause,
                DfsMetadataShardHandoffCause::StorageImport { cut } if cut.storage_transition_id.as_deref() == transition)
            {
                return Err(KvError::Serialization {
                    detail: "storage transition already activated this scope; observe its original native outcome".into(),
                });
            }
            KvCompare::ValueEquals {
                key: proof_key,
                value: previous.value,
            }
        }
        None => KvCompare::Missing { key: proof_key },
    };
    Ok((
        vec![
            KvCompare::ValueEquals {
                key: entry.key,
                value: entry.value,
            },
            proof_compare,
        ],
        config,
    ))
}

pub(super) fn catalog_handoff_successor(config: &DfsFileSystemConfig) -> Result<KvOp, KvError> {
    let successor = catalog_generation_successor_unchecked(config)?;
    KvOp::put_serde(
        file_system_config_key(&config.file_system_id),
        &successor,
        None,
    )
}

/// Build Pending from exact predecessor bytes, retaining its entire Active
/// descriptor. The migration owner must also fence its complete catalog in the
/// same native transaction; this builder does not certify that catalog.
pub fn metadata_storage_pending_transition_update(
    expected: &KvEntry,
    transition_id: String,
    target_generation: String,
    target_format: u32,
    expected_catalog_digest: String,
) -> Result<(KvCompare, KvOp), KvError> {
    let mut successor = expected.decode::<DfsFileSystemConfig>()?;
    if expected.key != file_system_config_key(&successor.file_system_id) {
        return Err(KvError::Serialization {
            detail: "storage transition has foreign filesystem predecessor".into(),
        });
    }
    if matches!(
        successor.metadata_storage,
        Some(crate::DfsMetadataStorageState::PendingTransition { .. })
    ) {
        return Err(KvError::Serialization {
            detail: "storage transition must resume its original pending identity".into(),
        });
    }
    let source_activation = successor
        .metadata_storage
        .as_ref()
        .and_then(|state| state.active_descriptor());
    let pending = crate::DfsMetadataStorageState::PendingTransition {
        source_activation,
        transition_id,
        source_catalog_generation: successor.catalog_generation,
        target_generation,
        target_format,
        expected_catalog_digest,
    };
    pending
        .validate_at_catalog_generation(successor.catalog_generation)
        .map_err(|detail| KvError::Serialization { detail })?;
    successor.metadata_storage = Some(pending);
    Ok((
        KvCompare::ValueEquals {
            key: expected.key.clone(),
            value: expected.value.clone(),
        },
        KvOp::put_serde(expected.key.clone(), &successor, expected.lease_id)?,
    ))
}

/// Plan one real catalog change from exact captured filesystem bytes. The
/// caller must publish this pair atomically with its admitted shard mutation.
/// This does not authorize that mutation or certify a catalog read.
pub fn metadata_catalog_generation_update(
    expected: &KvEntry,
) -> Result<(KvCompare, KvOp), KvError> {
    let successor =
        metadata_catalog_generation_successor(&expected.decode::<DfsFileSystemConfig>()?)?;
    if expected.key != file_system_config_key(&successor.file_system_id) {
        return Err(KvError::Serialization {
            detail: "catalog generation requires its exact filesystem configuration key".into(),
        });
    }
    Ok((
        KvCompare::ValueEquals {
            key: expected.key.clone(),
            value: expected.value.clone(),
        },
        KvOp::put_serde(expected.key.clone(), &successor, expected.lease_id)?,
    ))
}

pub(super) fn append_catalog_generation_update(
    entry: Option<KvEntry>,
    fs: &DfsFileSystemId,
    compares: &mut Vec<KvCompare>,
    success: &mut Vec<KvOp>,
) -> Result<(), KvError> {
    let entry = entry.ok_or_else(|| KvError::Serialization {
        detail: format!("metadata catalog mutation requires filesystem configuration {fs}"),
    })?;
    if entry.key != file_system_config_key(fs) {
        return Err(KvError::Serialization {
            detail: "catalog update returned a foreign filesystem".into(),
        });
    }
    let (compare, put) = metadata_catalog_generation_update(&entry)?;
    compares.push(compare);
    success.push(put);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DfsMetadataBackend;

    fn current_config(kv: &mut dyn DfsControlPlaneKvClient) -> KvEntry {
        kv.get_authoritative_consistent(&[file_system_config_key(&sifs_file_system_id())])
            .unwrap()
            .pop()
            .unwrap()
            .unwrap()
    }

    fn change_from(entry: &KvEntry, extra: Vec<KvOp>) -> KvTxn {
        let (compare, put) = metadata_catalog_generation_update(entry).unwrap();
        let mut success = extra;
        success.push(put);
        KvTxn {
            compares: vec![compare],
            success,
            failure: Vec::new(),
        }
    }

    #[test]
    fn native_genesis_birth_refuses_changed_captured_storage_before_assignment() {
        let (runtime, nodes) = crate::test_runtime::DfsTestRuntime::start_cluster(
            "native-genesis-storage-binding-cas", 1).unwrap();
        let shared = runtime.shared_control_plane().unwrap();
        shared.with_client(|boxed| {
            let kv = boxed.as_mut();
            let before = current_config(kv);
            let captured = before.decode::<DfsFileSystemConfig>().unwrap();
            assert!(captured.metadata_storage.is_none());
            let desired = DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(), shard_id: "guarded-birth".into(),
                primary_node_id: nodes[0].clone(), replica_node_ids: Vec::new(), epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix { prefix: "/guarded-birth".into() },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            };
            let mut changed = captured.clone();
            // Negative admission setup only: actual authoritative configuration
            // changes after capture. No generation image is activated by this.
            changed.metadata_storage = Some(crate::DfsMetadataStorageState::Active {
                generation: "later-storage-binding".into(), target_format: 12,
                catalog_generation: captured.catalog_generation,
                activation_digest: format!("blake3:{}", "a".repeat(64)),
            });
            assert!(kv.txn(KvTxn {
                compares: vec![KvCompare::ValueEquals { key: before.key.clone(), value: before.value.clone() }],
                success: vec![KvOp::put_serde(before.key.clone(), &changed, None).unwrap()],
                failure: Vec::new(),
            }).unwrap().succeeded);
            let changed_entry = current_config(kv);
            assert!(!DfsControlPlaneStore::new(kv)
                .compare_and_put_metadata_shard_for_genesis(&desired, captured.metadata_storage.as_ref()).unwrap());
            assert!(kv.get(&metadata_shard_key(&desired.file_system_id, &desired.shard_id)).unwrap().is_none());
            let after = current_config(kv);
            assert_eq!(after.value, changed_entry.value);
            assert_eq!(after.modify_revision, changed_entry.modify_revision);
            Ok::<(), KvError>(())
        }).unwrap();
        drop(shared);
        runtime.close().unwrap();
    }

    #[test]
    fn native_catalog_generation_fences_phantom_insert_remove_rescope_and_races() {
        let (runtime, nodes) =
            crate::test_runtime::DfsTestRuntime::start_cluster("native-catalog-generation", 1)
                .unwrap();
        let shared = runtime.shared_control_plane().unwrap();
        shared
            .with_client(|kv| {
                let kv = kv.as_mut();
                let first = current_config(kv);
                let mut extra = DfsMetadataShardConfig {
                    file_system_id: sifs_file_system_id(),
                    shard_id: "catalog-phantom".into(),
                    primary_node_id: nodes[0].clone(),
                    replica_node_ids: Vec::new(),
                    epoch: 1,
                    assignment: DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/catalog-phantom".into(),
                    },
                    default_policy: DfsObjectPolicy::ReplicatedEventual,
                };
                DfsControlPlaneStore::new(kv)
                    .put_metadata_shard(&extra)
                    .unwrap();
                let inserted = current_config(kv);
                assert_eq!(
                    inserted
                        .decode::<DfsFileSystemConfig>()
                        .unwrap()
                        .catalog_generation,
                    first
                        .decode::<DfsFileSystemConfig>()
                        .unwrap()
                        .catalog_generation
                        + 1
                );
                assert!(
                    !kv.txn(change_from(&first, Vec::new())).unwrap().succeeded,
                    "an unlisted inserted shard invalidates the captured complete catalog"
                );
                DfsControlPlaneStore::new(kv)
                    .put_metadata_shard(&extra)
                    .unwrap();
                assert_eq!(
                    current_config(kv),
                    inserted,
                    "exact idempotent publication is not a new catalog"
                );

                // Exercise the generic transaction guard through native Raft for
                // empty test-only catalog rows. This does not authorize retirement
                // of a live shard or substitute for its owner handoff protocol.
                let key = metadata_shard_key(&extra.file_system_id, &extra.shard_id);
                assert!(
                    kv.txn(change_from(
                        &inserted,
                        vec![KvOp::Delete { key: key.clone() }]
                    ))
                    .unwrap()
                    .succeeded
                );
                assert!(
                    !kv.txn(change_from(&inserted, Vec::new()))
                        .unwrap()
                        .succeeded,
                    "removal changes the catalog even though old enumerated rows can still match"
                );
                let removed = current_config(kv);
                assert!(
                    kv.txn(change_from(
                        &removed,
                        vec![KvOp::put_serde(key.clone(), &extra, None).unwrap()]
                    ))
                    .unwrap()
                    .succeeded
                );
                assert!(
                    !kv.txn(change_from(&inserted, Vec::new()))
                        .unwrap()
                        .succeeded,
                    "remove and recreate cannot restore an earlier generation"
                );
                let recreated = current_config(kv);
                extra.assignment = DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/catalog-rescoped".into(),
                };
                assert!(
                    kv.txn(change_from(
                        &recreated,
                        vec![KvOp::put_serde(key.clone(), &extra, None).unwrap()]
                    ))
                    .unwrap()
                    .succeeded
                );
                assert!(
                    !kv.txn(change_from(&recreated, Vec::new()))
                        .unwrap()
                        .succeeded
                );

                let before_config = current_config(kv);
                let mut ordinary = before_config.decode::<DfsFileSystemConfig>().unwrap();
                ordinary.display_name = "concurrent ordinary config update".into();
                DfsControlPlaneStore::new(kv)
                    .put_file_system_config(&ordinary)
                    .unwrap();
                assert!(
                    !kv.txn(change_from(&before_config, Vec::new()))
                        .unwrap()
                        .succeeded,
                    "catalog publication cannot erase concurrent ordinary config changes"
                );
                let before_race = current_config(kv);
                let left = change_from(&before_race, vec![KvOp::Delete { key: key.clone() }]);
                let right = change_from(
                    &before_race,
                    vec![KvOp::put_serde(key, &extra, None).unwrap()],
                );
                assert!(kv.txn(left).unwrap().succeeded);
                assert!(
                    !kv.txn(right).unwrap().succeeded,
                    "two catalog successors cannot share one generation"
                );
                let final_config = current_config(kv).decode::<DfsFileSystemConfig>().unwrap();
                assert_eq!(final_config.display_name, ordinary.display_name);
                assert_eq!(
                    final_config.catalog_generation,
                    ordinary.catalog_generation + 1
                );
                Ok::<(), KvError>(())
            })
            .unwrap();
        drop(shared);
        runtime.close().unwrap();
    }

    #[test]
    fn native_pending_catalog_transition_blocks_unrelated_mutations_and_binds_handoff() {
        let (runtime, nodes) = crate::test_runtime::DfsTestRuntime::start_cluster(
            "native-catalog-transition-guard",
            1,
        )
        .unwrap();
        let shard = runtime.root_shard().unwrap();
        let prefix = runtime
            .node_metadata(&nodes[0])
            .unwrap()
            .metadata_prefix_proof(&crate::DfsMetadataShardId::new("root"))
            .unwrap()
            .unwrap();
        let shared = runtime.shared_control_plane().unwrap();
        shared
            .with_client(|boxed| {
                let kv = boxed.as_mut();
                let before = current_config(kv);
                let stale_change = change_from(&before, Vec::new());
                let mut pending = before.decode::<DfsFileSystemConfig>().unwrap();
                pending.metadata_storage =
                    Some(crate::DfsMetadataStorageState::PendingTransition {
                        source_activation: None,
                        transition_id: "exact-native-transition".into(),
                        source_catalog_generation: pending.catalog_generation,
                        target_generation: "target-generation".into(),
                        target_format: 12,
                        expected_catalog_digest: format!("blake3:{}", "a".repeat(64)),
                    });
                // Authoritative native test setup for guard admission only. This
                // writes no converted generation and supplies no activation proof.
                assert!(
                    kv.txn(KvTxn {
                        compares: vec![KvCompare::ValueEquals {
                            key: before.key.clone(),
                            value: before.value.clone()
                        }],
                        success: vec![KvOp::put_serde(before.key.clone(), &pending, None).unwrap()],
                        failure: Vec::new(),
                    })
                    .unwrap()
                    .succeeded
                );
                assert!(!kv.txn(stale_change).unwrap().succeeded);
                let mut extra = shard.clone();
                extra.shard_id = "blocked-extra".into();
                extra.assignment = DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/blocked-extra".into(),
                };
                assert!(
                    matches!(DfsControlPlaneStore::new(kv).put_metadata_shard(&extra),
                Err(KvError::Serialization { detail }) if detail.contains("reserved"))
                );
                let active_entry = current_config(kv);
                assert!(metadata_catalog_generation_update(&active_entry).is_err());
                let mut changed = pending.clone();
                changed.metadata_storage = None;
                assert!(
                    DfsControlPlaneStore::new(kv)
                        .compare_and_put_file_system_config(Some(&pending), &changed)
                        .is_err()
                );
                changed = pending.clone();
                changed.display_name = "ordinary edit during transition".into();
                assert!(
                    DfsControlPlaneStore::new(kv)
                        .compare_and_put_file_system_config(Some(&pending), &changed)
                        .unwrap()
                );
                assert_eq!(
                    current_config(kv)
                        .decode::<DfsFileSystemConfig>()
                        .unwrap()
                        .metadata_storage,
                    pending.metadata_storage
                );

                let mut membership = shard.clone();
                membership.epoch += 1;
                membership.replica_node_ids.push(NodeId::generate());
                assert!(matches!(DfsControlPlaneStore::new(kv).begin_metadata_shard_membership_handoff(&shard, &membership),
                    Err(KvError::Serialization { detail }) if detail.contains("reserved")));
                let mut desired = shard.clone();
                desired.epoch += 1;
                let mut cut = DfsMetadataStorageImportCut {
                    storage_transition_id: Some("wrong-transition".into()),
                    source_cut_digest: "a".repeat(64),
                    source_journal_sequence: prefix.journal_sequence,
                    source_state_fingerprint: "native-source".into(),
                    target_prefix: prefix.clone(),
                    converted_image_fingerprint: "test-only-unpublished-target".into(),
                    pending_mapping_digest: "b".repeat(64),
                };
                assert!(
                    DfsControlPlaneStore::new(kv)
                        .begin_metadata_shard_handoff_with_cause(
                            &shard,
                            &desired,
                            DfsMetadataShardHandoffCause::StorageImport { cut: cut.clone() }
                        )
                        .is_err()
                );
                assert!(
                    DfsControlPlaneStore::new(kv)
                        .get_metadata_shard_membership_handoff(
                            &shard.file_system_id,
                            &shard.shard_id
                        )
                        .unwrap()
                        .is_none()
                );
                cut.storage_transition_id = Some("exact-native-transition".into());
                let cause = DfsMetadataShardHandoffCause::StorageImport { cut };
                let current = current_config(kv);
                assert!(
                    catalog_handoff_guard(Some(current.clone()), None, &shard, &desired, &cause)
                        .is_ok(),
                    "structural admission is not a native activation certificate"
                );
                let previous = DfsMetadataShardMembershipHandoffProof {
                    cause: cause.clone(),
                    handoff_id: "prior-step-data".into(),
                    retiring_shard: shard.clone(),
                    desired_shard: desired.clone(),
                    journal_sequence: prefix.journal_sequence,
                    state_fingerprint: "prior-image".into(),
                    acknowledged_node_ids: nodes.clone(),
                };
                let mut proof_entry = current.clone();
                proof_entry.key = metadata_shard_membership_handoff_proof_key(
                    &shard.file_system_id,
                    &shard.shard_id,
                );
                proof_entry.value = sinomo_core::kv::encode_kv_value(&previous).unwrap();
                let mut second = desired.clone();
                second.epoch += 1;
                assert!(
                    catalog_handoff_guard(
                        Some(current),
                        Some(proof_entry),
                        &desired,
                        &second,
                        &cause
                    )
                    .is_err(),
                    "isolated proof DATA cannot authorize a second same-scope step"
                );
                assert_eq!(
                    DfsControlPlaneStore::new(kv)
                        .get_metadata_shard(&shard.file_system_id, &shard.shard_id)
                        .unwrap(),
                    Some(shard.clone())
                );
                Ok::<(), KvError>(())
            })
            .unwrap();
        drop(shared);
        runtime.close().unwrap();
    }

    #[test]
    fn catalog_generation_ordinary_writers_preserve_and_cannot_author_or_overflow() {
        let mut current =
            DfsFileSystemConfig::from(&crate::DfsFileSystem::sifs(NodeId::generate()));
        current.catalog_generation = 7;
        let mut proposed = current.clone();
        proposed.catalog_generation = 0;
        proposed.display_name = "changed".into();
        let merged = merge_file_system_config_publication(Some(&current), &proposed);
        assert_eq!(merged.catalog_generation, 7);
        assert_eq!(merged.display_name, "changed");
        assert_eq!(
            merge_file_system_config_defaults(Some(&current), &proposed).catalog_generation,
            7
        );
        assert!(validate_ordinary_metadata_storage(Some(&current), &merged).is_ok());
        assert!(validate_ordinary_metadata_storage(Some(&current), &proposed).is_err());
        assert!(validate_ordinary_metadata_storage(None, &current).is_err());
        current.metadata_storage = Some(crate::DfsMetadataStorageState::Active {
            generation: "prior-generation".into(),
            target_format: 12,
            catalog_generation: 6,
            activation_digest: format!("blake3:{}", "b".repeat(64)),
        });
        let mut data = InProcessKvStore::new();
        data
            .put(
                file_system_config_key(&current.file_system_id),
                sinomo_core::kv::encode_kv_value(&current).unwrap(),
                None,
            )
            .unwrap();
        let entry = data.get(&file_system_config_key(&current.file_system_id)).unwrap();
        let (_, pending_put) = metadata_storage_pending_transition_update(
            &entry,
            "next-transition".into(),
            "next-generation".into(),
            12,
            format!("blake3:{}", "c".repeat(64)),
        )
        .unwrap();
        let KvOp::Put { value, .. } = pending_put else {
            unreachable!()
        };
        let pending: DfsFileSystemConfig = sinomo_core::kv::decode_kv_value(&value).unwrap();
        let Some(crate::DfsMetadataStorageState::PendingTransition {
            source_activation,
            source_catalog_generation,
            ..
        }) = &pending.metadata_storage
        else {
            unreachable!()
        };
        assert_eq!(
            *source_activation,
            current
                .metadata_storage
                .as_ref()
                .unwrap()
                .active_descriptor()
        );
        assert_eq!(*source_catalog_generation, current.catalog_generation);
        assert_eq!(pending.display_name, current.display_name);
        assert!(
            metadata_storage_pending_transition_update(
                &entry,
                "next-transition".into(),
                "prior-generation".into(),
                12,
                format!("blake3:{}", "c".repeat(64))
            )
            .is_err(),
            "a new target cannot reuse the source identity"
        );
        current.catalog_generation = u64::MAX;
        assert!(metadata_catalog_generation_successor(&current).is_err());
    }
}
