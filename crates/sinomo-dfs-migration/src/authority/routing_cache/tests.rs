use super::actual_docker_fixture::ORIGINAL;
use super::*;
use crate::authority::native_fixtures::{native_migration_cluster, canonical_migration_runtime};
use sinomo_dfs::DfsStoragePreparationScope;

fn archive(authority: &NativeImportAuthority, bytes: &[u8]) -> PathBuf {
    authority
        .receiver_root
        .join("routing-topology-cache-v1")
        .join(blake3::hash(bytes).to_hex().to_string())
}
fn install(root: &Path, bytes: &[u8]) {
    std::fs::create_dir_all(path(root).parent().unwrap()).unwrap();
    std::fs::write(path(root), bytes).unwrap();
}
fn original() -> serde_json::Value {
    decode_original_document(ORIGINAL).unwrap()
}
fn source(value: &serde_json::Value) -> Vec<u8> {
    serde_ext::to_bytes(value, 1, LengthMode::U32).unwrap()
}

#[test]
fn actual_1664_byte_docker_cache_preserves_every_active_and_fallback_descriptor() {
    assert_eq!(ORIGINAL.len(), 1664);
    sinomo_core::object::DocumentView::parse(ORIGINAL).unwrap();
    assert!(
        strict_current(ORIGINAL).is_err(),
        "ordinary DTO must reject retired codec"
    );
    let history = convert(ORIGINAL).unwrap();
    let mut expected = original();
    for value in expected["active"].as_array_mut().unwrap() {
        value
            .as_object_mut()
            .unwrap()
            .remove("namespace_prepare_codec");
        crate::legacy_wire::source_identity_fields(value).unwrap();
    }
    for topology in expected["fallbacks"].as_array_mut().unwrap() {
        for value in topology.as_array_mut().unwrap() {
            value
                .as_object_mut()
                .unwrap()
                .remove("namespace_prepare_codec");
            crate::legacy_wire::source_identity_fields(value).unwrap();
        }
    }
    assert_eq!(serde_json::to_value(&history).unwrap(), expected);
    assert_eq!(
        history
            .active
            .iter()
            .map(|s| s.shard_id.as_str())
            .collect::<Vec<_>>(),
        vec!["root", "services", "users"]
    );
    assert!(
        history
            .active
            .iter()
            .all(|s| s.epoch == 3 && s.replica_node_ids.len() == 5)
    );
    assert_eq!(history.fallbacks.len(), 1);
    assert_eq!(history.fallbacks[0].len(), 1);
    let fallback = &history.fallbacks[0][0];
    assert_eq!(fallback.shard_id, "root");
    assert_eq!(fallback.epoch, 1);
    assert!(fallback.replica_node_ids.is_empty());
    assert_eq!(fallback.primary_node_id, history.active[0].primary_node_id);
    let current = serde_ext::to_bytes(&history, 1, LengthMode::U32).unwrap();
    assert_eq!(strict_current(&current).unwrap(), history);
}

#[test]
fn source_conversion_rejects_unknown_codec_fields_duplicate_scopes_and_source_changes() {
    let mut changed = original();
    changed["fallbacks"][0][0]["namespace_prepare_codec"] = serde_json::json!("unknown-v99");
    assert!(convert(&source(&changed)).is_err());
    let mut changed = original();
    changed["active"][1]["foreign_authority"] = serde_json::json!(true);
    assert!(convert(&source(&changed)).is_err());
    let mut changed = original();
    changed["future_generation"] = serde_json::json!(42);
    assert!(convert(&source(&changed)).is_err());
    let mut changed = original();
    let duplicate = changed["active"][0].clone();
    changed["active"].as_array_mut().unwrap().push(duplicate);
    assert!(convert(&source(&changed)).is_err());
    let root = sinomo_testdir::TestDir::new("routing-cache-changed-original");
    let changed = source(&changed);
    install(root.path(), &changed);
    let replacement = serde_ext::to_bytes(&convert(ORIGINAL).unwrap(), 1, LengthMode::U32).unwrap();
    assert!(publish_unchanged(root.path(), ORIGINAL, &replacement).is_err());
    assert_eq!(read(root.path()).unwrap().unwrap(), changed);
}

#[test]
fn actual_docker_cache_conversion_requires_real_native_configuration_and_archives_source() {
    let name = "routing-cache-actual-docker-native-authority";
    let (mut native, nodes) = native_migration_cluster(name, 2);
    let context = native.component_context(&nodes[0]).unwrap();
    let runtime = canonical_migration_runtime(&context);
    let root = runtime.metadata_root.clone();
    let expected = convert(ORIGINAL).unwrap();
    assert!(
        expected
            .records()
            .all(|config| config.file_system_id == sinomo_dfs::sifs_file_system_id())
    );
    let binding = runtime.binding();
    let provider = &binding.provider;
    install(&root, ORIGINAL);
    assert_eq!(
        await_routing(&runtime, &binding, &root).unwrap(),
        DfsStoragePreparation::Ready
    );
    let bytes = read(&root).unwrap().unwrap();
    assert_eq!(strict_current(&bytes).unwrap(), expected);
    assert_eq!(
        std::fs::read(archive(&runtime.authority, ORIGINAL).join("original.sobj"))
            .unwrap()
            .as_slice(),
        ORIGINAL.as_slice()
    );
    let saved = std::fs::read_dir(archive(&runtime.authority, ORIGINAL))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(saved.iter().any(|name| name.starts_with("native-input-")));
    assert!(
        saved
            .iter()
            .any(|name| name.starts_with("native-publication-"))
    );
    assert_eq!(
        await_routing(&runtime, &binding, &root).unwrap(),
        DfsStoragePreparation::Ready
    );
    assert_eq!(
        read(&root).unwrap().unwrap(),
        bytes,
        "already-current preparation is byte-idempotent"
    );
    // Current control-plane rows and their real proof come from the SAME mounted
    // runtime. Historical six-member cached assignments never become native rows.
    let keys = expected.configuration_keys();
    let (actual, proof) = runtime.authority.read_exact(&keys).unwrap();
    super::super::current_authority(&proof).unwrap();
    NativeImportAuthority::validate_routing_configuration(&expected, &actual).unwrap();
    for entry in actual
        .iter()
        .filter(|e| e.key.contains("/metadata-shards/"))
    {
        let current: DfsMetadataShardConfig = entry.entry.as_ref().unwrap().decode().unwrap();
        assert!(nodes.contains(&current.primary_node_id));
        assert!(
            current
                .replica_node_ids
                .iter()
                .all(|node| nodes.contains(node))
        );
    }
    drop(binding);
    drop(runtime);
    drop(context);
    native.close().unwrap();
}

#[test]
fn absent_native_filesystem_configuration_preserves_original_and_never_becomes_ready() {
    let name = "routing-cache-missing-native-configuration";
    let (mut native, nodes) = native_migration_cluster(name, 2);
    let context = native.component_context(&nodes[0]).unwrap();
    let runtime = canonical_migration_runtime(&context);
    let root = runtime.metadata_root.clone();
    let mut foreign = original();
    let fs = sinomo_core::Id::generate().to_string();
    for config in foreign["active"].as_array_mut().unwrap() {
        config["file_system_id"] = serde_json::json!(fs);
    }
    for topology in foreign["fallbacks"].as_array_mut().unwrap() {
        for config in topology.as_array_mut().unwrap() {
            config["file_system_id"] = serde_json::json!(fs);
        }
    }
    let original = source(&foreign);
    install(&root, &original);
    let binding = runtime.binding();
    let provider = &binding.provider;
    assert!(await_routing(&runtime, &binding, &root).is_err());
    assert_eq!(read(&root).unwrap().unwrap(), original);
    assert_eq!(
        std::fs::read(archive(&runtime.authority, &original).join("original.sobj")).unwrap(),
        original
    );
    assert!(!current(&root).unwrap());
    native.stop_node(&nodes[0]).unwrap();
    assert!(provider(&root, &DfsStoragePreparationScope::MetadataRouting, 256).is_err());
    assert_eq!(read(&root).unwrap().unwrap(), original);
    drop(binding);
    drop(runtime);
    drop(context);
    native.close().unwrap();
}

// Demand only schedules the actual scoped owner. Await its existing progress
// watch; neither this helper nor the request thread advances a converter.
fn await_routing(
    owner: &crate::authority::MigrationOwner,
    binding: &sinomo_dfs::DfsStoragePreparationProviderBinding,
    root: &Path,
) -> Result<sinomo_dfs::DfsStoragePreparation> {
    let mut changed = binding.readiness.clone();
    let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        changed.borrow_and_update();
        let state = (binding.provider)(root, &DfsStoragePreparationScope::MetadataRouting, 256)?;
        if state == sinomo_dfs::DfsStoragePreparation::Ready { return Ok(state); }
        owner.authority.runtime.block_on(async {
            tokio::time::timeout_at(deadline, changed.changed()).await
                .map_err(|_| error("actual routing migration exceeded existing network deadline"))?
                .map_err(error)
        })?;
    }
}
