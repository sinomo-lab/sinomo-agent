use super::*;

#[test]
fn empty_recovery_discovery_does_not_rebind_a_current_primary() {
    let name = "sandbox-native-empty-discovery";
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 3);
    let (native, initial, original) =
        sinomo_dfs::test_support::current_native_metadata_service(name, 3, "/services")
            .expect("real native component owners");
    drop(initial);
    let primary = nodes
        .iter()
        .find(|node| **node != original.primary_node_id)
        .unwrap()
        .clone();
    let replicas = nodes
        .iter()
        .filter(|node| **node != primary)
        .cloned()
        .collect();
    let current = native
        .reassign_metadata_shard(
            &original.file_system_id,
            &original.shard_id,
            primary.clone(),
            replicas,
        )
        .expect("actual freeze, acknowledgement and joint activation");
    assert!(current.epoch > original.epoch);
    let mut writer = native.metadata_primary_service("/services").unwrap();
    writer
        .posix_write_file(
            "/services/sandbox-discovery-anchor",
            b"completed native activity",
            sinomo_dfs::DfsWriteOptions {
                policy: sinomo_dfs::DfsReplicationPolicy::new(3, 3).unwrap(),
                replica_candidates: nodes,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let head = writer
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    drop(writer);
    let data_dir = native.data_dir(&primary).clone();
    let marker = data_dir
        .dfs_dir()
        .join("metadata")
        .join(&current.shard_id)
        .join("journal/membership-handoff-write-freeze.sobj");
    let before = std::fs::read(&marker).unwrap();
    let before_mark = std::fs::metadata(&marker).unwrap().modified().unwrap();
    let value: serde_json::Value = sinomo_core::object::serde_ext::from_bytes(&before).unwrap();
    let value = value.get("repair_custody_v1").unwrap_or(&value);
    let bound = value["rebind_prefix_proof"]["journal_sequence"]
        .as_u64()
        .unwrap();
    assert!(
        bound < head,
        "completed writes have advanced beyond the previous rebind checkpoint"
    );
    let context = native.component_context(&primary).unwrap();
    let backend = ClusteredSandboxBackend::new(
        Arc::new(data_dir),
        context.cluster_access().unwrap(),
        Some(native.runtime_handle()),
    );
    drop(context);
    #[cfg(unix)]
    let content_shard = backend
        .data_dir
        .dfs_dir()
        .join("content/content-reservations/00");
    #[cfg(unix)]
    let content_mark = {
        std::fs::File::open(&content_shard)
            .unwrap()
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1))
            .unwrap();
        std::fs::metadata(&content_shard)
            .unwrap()
            .modified()
            .unwrap()
    };
    let result = backend.run_lifecycle_recovery_scan(BTreeSet::new());
    #[cfg(unix)]
    let content_after = std::fs::metadata(&content_shard)
        .unwrap()
        .modified()
        .unwrap();
    let after = std::fs::read(&marker).unwrap();
    let after_mark = std::fs::metadata(&marker).unwrap().modified().unwrap();
    eprintln!(
        "empty_discovery current_epoch={} journal_head={head} prior_rebind={bound} marker_changed={} result={result:?}",
        current.epoch,
        before != after
    );
    drop(backend);
    // A real subsequent effect still uses the original writable opener and
    // performs its necessary current-primary binding before publishing.
    let mut writer = native.metadata_primary_service("/services").unwrap();
    writer
        .posix_write_file(
            "/services/sandbox-after-discovery",
            b"new genuine activity",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![primary]),
        )
        .unwrap();
    drop(writer);
    let after_effect = std::fs::read(&marker).unwrap();
    native.close().unwrap();
    assert_ne!(
        after_effect, before,
        "actual effects retain their existing rebind boundary"
    );
    assert_eq!(result.unwrap(), 0);
    assert_eq!(
        after, before,
        "empty discovery must not advance an already activated primary's durable rebind checkpoint"
    );
    assert_eq!(after_mark, before_mark);
    #[cfg(unix)]
    assert_eq!(
        content_after, content_mark,
        "empty recovery discovery must not stage a redundant content sentinel"
    );
}

#[test]
fn discovery_reads_current_pending_manifests_and_rejects_malformed_state() {
    let name = "sandbox-native-pending-discovery";
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 3);
    let (native, initial, config) =
        sinomo_dfs::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    drop(initial);
    let writer = native.metadata_primary_service("/services").unwrap();
    let mut store = SifsSandboxInstanceStore::new(writer).with_replica_candidates(nodes.clone());
    let id = SandboxId::generate();
    let candidate = SandboxInstanceManifest::new(
        id.clone(),
        SandboxToolchainId::generate(),
        time::OffsetDateTime::now_utc(),
    )
    .unwrap();
    let pending = store.create_manifest(&candidate).unwrap();
    assert!(pending.pending_reconciliation.is_some());
    let context = native.component_context(&config.primary_node_id).unwrap();
    let primary_reader = ClusteredSandboxBackend::new(
        Arc::new(native.data_dir(&config.primary_node_id).clone()),
        context.cluster_access().unwrap(),
        Some(native.runtime_handle()),
    );
    drop(context);
    let local_observed = primary_reader
        .discover_lifecycle_recovery(primary_reader.open_content_store().unwrap())
        .unwrap()
        .1;
    assert_eq!(local_observed, vec![pending.clone()]);
    drop(primary_reader);
    let remote = nodes
        .iter()
        .find(|node| **node != config.primary_node_id)
        .unwrap();
    let context = native.component_context(remote).unwrap();
    let backend = ClusteredSandboxBackend::new(
        Arc::new(native.data_dir(remote).clone()),
        context.cluster_access().unwrap(),
        Some(native.runtime_handle()),
    );
    drop(context);
    let content = backend.open_content_store().unwrap();
    let observed = backend
        .discover_lifecycle_recovery(content.clone())
        .unwrap()
        .1;
    let reference = store.list_pending_authoritative_manifests().unwrap();
    assert_eq!(observed, reference);
    assert_eq!(observed, vec![pending]);

    // A current malformed manifest is uncertainty, never an empty discovery.
    // The fixture authors the corrupt body through the ordinary native writer.
    let mut writer = store.into_inner();
    writer
        .posix_write_file(
            &sandbox_manifest_path(&id),
            b"not a sandbox manifest",
            sinomo_dfs::DfsWriteOptions {
                policy: sinomo_dfs::DfsReplicationPolicy::new(3, 3).unwrap(),
                replica_candidates: nodes,
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(writer);
    backend
        .recovery_state()
        .mark_authoritative_recovery_pending(&id)
        .unwrap();
    let pending_before = backend.recovery_state().pending_sandbox_ids().unwrap();
    let unknown = backend.discover_lifecycle_recovery(content);
    let pending_after = backend.recovery_state().pending_sandbox_ids().unwrap();
    drop(backend);
    native.close().unwrap();
    assert!(
        unknown.is_err(),
        "invalid authoritative data cannot become an empty result: {unknown:?}"
    );
    assert_eq!(pending_after, pending_before);
    assert_eq!(pending_after, BTreeSet::from([id]));
}
