//! A filesystem's complete shard set is fenced by its existing config record.
use super::*;

/// Preserve every configuration field while advancing the complete catalog.
/// The result is DATA: its exact predecessor comparison and admitted catalog
/// mutation must be in the same authoritative transaction.
pub(super) fn metadata_catalog_generation_successor(
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

/// Plan one real catalog change from exact captured filesystem bytes. The
/// caller must publish this pair atomically with its admitted shard mutation.
/// This does not authorize that mutation or certify a catalog read.
pub(super) fn metadata_catalog_generation_update(
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

    /// Every way a shard is born records its storage with its assignment:
    /// default setup's split, a later prefix birth through the default
    /// setup host, and the store's raw and compare-and-put creation, sync and
    /// async. No born shard is left without a record, which would close it.
    #[test]
    fn every_shard_birth_writes_an_active_native_genesis_storage_record() {
        let (runtime, nodes) = crate::test_runtime::DfsTestRuntime::start_cluster(
            "native-birth-storage-records", 1).unwrap();
        runtime.bootstrap_empty_metadata_prefix("born-prefix", "/born-prefix").unwrap();
        let shared = runtime.shared_control_plane().unwrap();
        shared.with_client(|boxed| {
            let kv = boxed.as_mut();
            let assert_born = |kv: &mut dyn DfsControlPlaneKvClient, shard: &DfsMetadataShardConfig| {
                let key = metadata_shard_storage_key(&shard.file_system_id, &shard.shard_id);
                let record = kv.get_authoritative_consistent(&[key]).unwrap().pop().unwrap()
                    .unwrap_or_else(|| panic!("shard {} was born without a storage record", shard.shard_id))
                    .decode::<crate::DfsMetadataShardStorage>().unwrap();
                record.validate_against(shard).unwrap();
                assert_eq!(record.state, crate::DfsMetadataShardStorageState::Active {
                    activation: crate::DfsMetadataShardActivation::NativeGenesis,
                }, "shard {}", shard.shard_id);
                assert_eq!(record.generation,
                    crate::storage_generation::native_genesis_generation_identity(shard));
            };
            let setup = runtime.metadata_shards().unwrap();
            let mut ids = setup.iter().map(|shard| shard.shard_id.as_str()).collect::<Vec<_>>();
            ids.sort_unstable();
            assert_eq!(ids, ["born-prefix", "root", "services", "users"]);
            for shard in &setup {
                assert_born(kv, shard);
            }
            let birth = |shard_id: &str| DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(), shard_id: shard_id.into(),
                primary_node_id: nodes[0].clone(), replica_node_ids: Vec::new(), epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix { prefix: format!("/{shard_id}") },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            };
            let (raw, cas, raw_async, cas_async) =
                (birth("raw-birth"), birth("cas-birth"), birth("raw-async-birth"), birth("cas-async-birth"));
            DfsControlPlaneStore::new(kv).put_metadata_shard(&raw).unwrap();
            assert!(DfsControlPlaneStore::new(kv).compare_and_put_metadata_shard(None, &cas).unwrap());
            runtime.runtime_handle().block_on(async {
                let client = kv.async_client().expect("native async CP capability");
                DfsControlPlaneAsyncStore::new(&mut *client).put_metadata_shard(&raw_async).await.unwrap();
                assert!(DfsControlPlaneAsyncStore::new(client)
                    .compare_and_put_metadata_shard(None, &cas_async).await.unwrap());
            });
            for shard in [&raw, &cas, &raw_async, &cas_async] {
                assert_born(kv, shard);
            }
            // Retire the unmaterialized test assignments through the catalog guard.
            let born = current_config(kv);
            let retire = [&raw, &cas, &raw_async, &cas_async].into_iter().flat_map(|shard| [
                KvOp::Delete { key: metadata_shard_key(&shard.file_system_id, &shard.shard_id) },
                KvOp::Delete { key: metadata_shard_storage_key(&shard.file_system_id, &shard.shard_id) },
            ]).collect();
            assert!(kv.txn(change_from(&born, retire)).unwrap().succeeded);
            Ok::<(), KvError>(())
        }).unwrap();
        drop(shared);
        runtime.close().unwrap();
    }

    #[test]
    fn native_birth_waits_while_its_parent_shard_is_closed() {
        let (runtime, nodes) = crate::test_runtime::DfsTestRuntime::start_cluster(
            "native-birth-parent-storage", 1).unwrap();
        let users = runtime.metadata_shards().unwrap().into_iter()
            .find(|shard| shard.shard_id == "users").unwrap();
        let shared = runtime.shared_control_plane().unwrap();
        shared.with_client(|boxed| {
            let kv = boxed.as_mut();
            let read = |kv: &mut dyn DfsControlPlaneKvClient, key: &str| {
                kv.get_authoritative_consistent(&[key.to_owned()]).unwrap().pop().unwrap()
            };
            let desired = DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(), shard_id: "users-born-child".into(),
                primary_node_id: nodes[0].clone(), replica_node_ids: Vec::new(), epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix { prefix: "/users/born-child".into() },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            };
            let desired_key = metadata_shard_key(&desired.file_system_id, &desired.shard_id);
            let desired_storage_key = metadata_shard_storage_key(&desired.file_system_id, &desired.shard_id);
            let parent_key = metadata_shard_storage_key(&users.file_system_id, &users.shard_id);
            let active = read(kv, &parent_key).expect("born shards carry a storage record");
            assert!(active.decode::<crate::DfsMetadataShardStorage>().unwrap().is_active());
            // Test setup only: the parent's record names a conversion that no
            // driver runs, or is absent, as for old storage that migration has
            // yet to convert, so the parent is closed until the record is restored.
            let replace_parent = |kv: &mut dyn DfsControlPlaneKvClient, expected: Option<&KvEntry>, value: Option<Vec<u8>>| {
                let compare = match expected {
                    Some(expected) => KvCompare::ValueEquals { key: parent_key.clone(), value: expected.value.clone() },
                    None => KvCompare::Missing { key: parent_key.clone() },
                };
                let op = match value {
                    Some(value) => KvOp::Put { key: parent_key.clone(), value, lease_id: None },
                    None => KvOp::Delete { key: parent_key.clone() },
                };
                assert!(kv.txn(KvTxn { compares: vec![compare], success: vec![op], failure: Vec::new() })
                    .unwrap().succeeded);
                runtime.wait_published_configuration_current(&nodes[0]).unwrap();
                read(kv, &parent_key)
            };
            let refused = |kv: &mut dyn DfsControlPlaneKvClient, closed: &str| {
                let before = current_config(kv);
                assert!(!DfsControlPlaneStore::new(kv)
                    .compare_and_put_metadata_shard(None, &desired).unwrap(),
                    "a birth under the {closed} parent is refused");
                let async_refused = runtime.runtime_handle().block_on(async {
                    DfsControlPlaneAsyncStore::new(kv.async_client().expect("native async CP capability"))
                        .compare_and_put_metadata_shard(None, &desired).await
                }).unwrap();
                assert!(!async_refused, "the async birth compares the same parent record");
                assert!(matches!(DfsControlPlaneStore::new(kv).put_metadata_shard(&desired),
                    Err(KvError::Busy { .. })), "a raw birth under the {closed} parent is busy");
                assert!(read(kv, &desired_key).is_none());
                assert!(read(kv, &desired_storage_key).is_none());
                let unchanged = current_config(kv);
                assert_eq!((unchanged.value, unchanged.modify_revision), (before.value, before.modify_revision));
            };
            let converting = replace_parent(kv, Some(&active), Some(sinomo_core::kv::encode_kv_value(
                &crate::DfsMetadataShardStorage::converting_for_test(&users, 1)).unwrap())).unwrap();
            refused(kv, "converting");
            assert!(replace_parent(kv, Some(&converting), None).is_none());
            refused(kv, "unconverted");

            replace_parent(kv, None, Some(active.value.clone()));
            assert!(DfsControlPlaneStore::new(kv)
                .compare_and_put_metadata_shard(None, &desired).unwrap(),
                "the same birth under the Active parent is published");
            assert_eq!(read(kv, &desired_key).unwrap().decode::<DfsMetadataShardConfig>().unwrap(), desired);
            assert!(read(kv, &desired_storage_key).unwrap()
                .decode::<crate::DfsMetadataShardStorage>().unwrap().is_active());
            // Retire the unmaterialized test assignment through the catalog guard.
            let born = current_config(kv);
            assert!(kv.txn(change_from(&born, vec![
                KvOp::Delete { key: desired_key.clone() },
                KvOp::Delete { key: desired_storage_key.clone() },
            ])).unwrap().succeeded);
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
        assert!(validate_ordinary_catalog_generation(Some(&current), &merged).is_ok());
        assert!(validate_ordinary_catalog_generation(Some(&current), &proposed).is_err());
        assert!(validate_ordinary_catalog_generation(None, &current).is_err());
        assert_eq!(metadata_catalog_generation_successor(&current).unwrap().catalog_generation, 8);
        current.catalog_generation = u64::MAX;
        assert!(metadata_catalog_generation_successor(&current).is_err());
    }
}
