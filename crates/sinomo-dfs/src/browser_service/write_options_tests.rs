//! Native browser policy, projection and durable retry contracts.
use super::*;

fn node(name: &str) -> sinomo_core::node::NodeId {
    sinomo_core::node::NodeId::from_legacy_named(name).expect("node id")
}

fn native_browser_service(
    name: &str,
    local: sinomo_core::node::NodeId,
) -> (
    crate::test_support::DfsTestRuntime,
    NetworkedDfsService<crate::DfsNodeTransport>,
) {
    let mut runtime = crate::test_support::DfsTestRuntime::new(name, local.clone()).unwrap();
    runtime.start_nodes().unwrap();
    let service = runtime.client(&local).unwrap();
    (runtime, service)
}

#[test]
fn browser_parent_exact_hint_does_not_recreate_a_confirmed_deleted_parent() {
    browser_parent_hint_deletion_control(false);
}

#[test]
fn browser_parent_hint_survives_actual_backend_catchup_without_recreating_parent() {
    browser_parent_hint_deletion_control(true);
}

fn browser_parent_hint_deletion_control(hint_before_deletion: bool) {
    let local = node(if hint_before_deletion {
        "browser_parent_hint_before_deletion"
    } else {
        "browser_parent_confirmed_tombstone"
    });
    let (runtime, mut browser) = native_browser_service(
        if hint_before_deletion { "browser-parent-hint-before-deletion" }
        else { "browser-parent-confirmed-tombstone" }, local.clone(),
    );
    let parent = "/services/browser-parent-confirmed-tombstone/parent";
    let child = format!("{parent}/child");
    browser.posix_mkdir_all(parent).unwrap();
    let old_parent = {
        let owner = runtime.metadata_primary_service(parent).unwrap();
        owner.metadata_backend().unwrap()
            .get_object_by_path(&sifs_file_system_id(), parent).unwrap().unwrap()
    };
    assert_eq!(old_parent.kind, DfsObjectKind::Directory);
    if hint_before_deletion {
        browser.add_transient_object_metadata(old_parent.clone()).unwrap();
        assert!(!browser.transient_metadata_object_ids.contains(&old_parent.object_id),
            "the genuinely current backend row initially needs no pagination overlay");
    }
    let mut remover = runtime.client(&local).unwrap();
    remover.posix_rmdir(parent).unwrap();
    drop(remover);
    let deleted_parent = {
        let owner = runtime.metadata_primary_service(parent).unwrap();
        assert!(matches!(owner.posix_stat(parent), Err(DfsPosixError::NotFound { path }) if path == parent));
        let backend = owner.metadata_backend().unwrap();
        backend.get_object(&old_parent.object_id).unwrap().unwrap()
    };
    assert_eq!(deleted_parent.kind, DfsObjectKind::Tombstone);
    assert_ne!(deleted_parent.version, old_parent.version);
    assert_eq!(browser.metadata_backend().unwrap().get_object(&old_parent.object_id).unwrap(),
        Some(deleted_parent.clone()), "actual backend catchup, not a supplied tombstone");
    // The sole private hint is the genuinely observed old row. It grants no
    // authority and does not recreate its ancestors or overwrite the tombstone.
    if !hint_before_deletion {
        browser.add_transient_object_metadata(old_parent.clone()).unwrap();
    }
    assert_eq!(browser.metadata().object(&old_parent.object_id), Some(&old_parent));
    let capture = runtime.capture_served_work(&local, 32).unwrap();
    prepare_sifs_browser_write_parent(&mut browser, &child).unwrap();
    let reports = capture.records();
    let implicit_creates = reports.iter().filter(|work| matches!(
        &work.command.routed_message.message,
        DfsNetworkMessage::NamespaceRequest(request)
            if matches!(&request.operation, DfsNamespaceOperation::MkdirAll { path } if path == parent)
    )).count();
    let after = {
        let owner = runtime.metadata_primary_service(parent).unwrap();
        let backend = owner.metadata_backend().unwrap();
        assert!(backend.get_object_by_path(&sifs_file_system_id(), &child).unwrap().is_none());
        backend.get_object(&old_parent.object_id).unwrap().unwrap()
    };
    std::fs::write(
        runtime.data_dir(&local).root().join("boundary-diagnostic-cached-parent-preparation.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "old_hint": old_parent, "deleted_parent": deleted_parent, "after_preparation": after,
            "hint_before_deletion": hint_before_deletion,
            "implicit_mkdir_all_calls": implicit_creates,
            "actual_invocations": reports.iter().map(|work| serde_json::json!({
                "command": work.command.routed_message,
                "publish_metadata": work.committed_work.publish_metadata,
                "semantic_changes": work.committed_work.semantic_changes,
                "succeeded": work.succeeded,
            })).collect::<Vec<_>>(),
            "qualification": "Actual registered Main created and removed the parent. The copied old row is advisory DATA only; no fabricated receipt or supplied successful response."
        })).unwrap(),
    ).unwrap();
    drop(capture);
    drop(browser);
    runtime.close().unwrap();
    assert_eq!(implicit_creates, 0, "an old positive hint cannot authorize implicit recreation");
    assert_eq!(after, deleted_parent, "preparation must preserve the actual tombstone exactly");
}

#[test]
fn browser_policy_projection_preserves_links_dot_paths_and_namespace_errors() {
    let local = node("browser_policy_paths");
    let (mut runtime, mut dfs) = native_browser_service(
        "browser_policy_projection_preserves_links_dot_paths_and_namespace_errors",
        local.clone(),
    );
    dfs.posix_mkdir_all("/documents/dir").expect("directory");
    let options = DfsWriteOptions::replicated_eventual(vec![local]);
    let file = dfs
        .posix_write_file("/documents/dir/file", b"body", options.clone())
        .expect("file");
    dfs.posix_hardlink("/documents/dir/file", "/documents/hard")
        .expect("hardlink");
    dfs.posix_symlink("dir/file", "/documents/link", options.clone())
        .expect("symlink");
    dfs.posix_symlink("link", "/documents/chain", options.clone())
        .expect("symlink chain");
    dfs.posix_symlink("dir", "/documents/dir-link", options.clone())
        .expect("directory symlink");
    dfs.posix_symlink("missing", "/documents/dangling", options)
        .expect("dangling symlink");
    let mut object = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&file.object_id)
        .unwrap()
        .unwrap();
    object.policy = sinomo_core::DfsObjectPolicy::ReplicatedConfirmed;
    dfs.add_object_metadata(object)
        .expect("stronger durable policy");
    for path in [
        "/documents/dir/file",
        "/documents/hard",
        "/documents/link",
        "/documents/chain",
        "/documents/dir-link/file",
        "/documents/dir/../link",
        "/documents/dir/./file",
        "/documents/dir/",
        "/documents/missing",
        "/documents/dangling",
        "/documents/dir/file/child",
        "/documents/dir/file/",
    ] {
        assert_eq!(
            dfs.posix_object_metadata_hint(path)
                .map(|object| (object.object_id, object.kind, object.policy))
                .map_err(|e| e.to_string()),
            dfs.posix_stat(path)
                .map(|stat| (stat.object_id, stat.kind, stat.policy))
                .map_err(|e| e.to_string()),
            "metadata hint differs for {path}",
        );
    }
    assert_eq!(
        dfs.posix_object_policy("/documents/hard").unwrap(),
        sinomo_core::DfsObjectPolicy::ReplicatedConfirmed
    );
    let parent = dfs.posix_stat("/documents/dir").unwrap();
    let mut tombstone = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&parent.object_id)
        .unwrap()
        .unwrap();
    tombstone.kind = DfsObjectKind::Tombstone;
    dfs.add_object_metadata(tombstone)
        .expect("tombstoned ancestor");
    for path in [
        "/documents/dir/file",
        "/documents/link",
        "/documents/dir-link/file",
    ] {
        assert!(dfs.posix_stat(path).is_err(), "fixture must hide {path}");
        assert_eq!(
            dfs.posix_object_metadata_hint(path)
                .map(|object| (object.object_id, object.kind, object.policy))
                .map_err(|e| e.to_string()),
            dfs.posix_stat(path)
                .map(|stat| (stat.object_id, stat.kind, stat.policy))
                .map_err(|e| e.to_string()),
        );
    }
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn browser_parent_hints_skip_directory_counts_and_preserve_namespace_checks() {
    let local = node("browser_parent_hints");
    let (mut runtime, mut dfs) = native_browser_service(
        "browser_parent_hints_skip_directory_counts_and_preserve_namespace_checks",
        local.clone(),
    );
    dfs.posix_mkdir_all("/documents/dir/child").unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![local]);
    dfs.posix_write_file("/documents/dir/file", b"body", options.clone())
        .unwrap();
    dfs.posix_symlink("dir", "/documents/alias", options.clone())
        .unwrap();
    dfs.posix_symlink("missing", "/documents/dangling", options)
        .unwrap();

    crate::posix::reset_posix_directory_link_count_calls();
    assert_eq!(dfs.posix_stat("/documents/dir").unwrap().link_count, 3);
    assert_eq!(crate::posix::posix_directory_link_count_calls(), 1);
    for parent in ["/documents/dir", "/documents/alias"] {
        assert_eq!(
            dfs.posix_object_metadata_hint(parent).unwrap().kind,
            DfsObjectKind::Directory,
        );
        prepare_sifs_browser_write_parent(&mut dfs, &format!("{parent}/new-file")).unwrap();
    }
    assert_eq!(
        dfs.posix_object_metadata_hint("/documents/dir/file")
            .unwrap()
            .kind,
        DfsObjectKind::File,
    );
    assert!(matches!(
        prepare_sifs_browser_write_parent(&mut dfs, "/documents/dir/file/new-file"),
        Err(DfsPosixError::NotDirectory { .. }),
    ));
    assert!(matches!(
        dfs.posix_object_metadata_hint("/documents/dangling"),
        Err(DfsPosixError::NotFound { .. }),
    ));
    let registry = dfs.cross_shard_namespace_lock_registry();
    for locked_path in ["/documents/alias", "/documents/dir"] {
        registry.upsert_job(
            None,
            "parent-hint-flight".into(),
            sifs_file_system_id(),
            locked_path.to_owned(),
        );
        assert!(dfs.posix_object_metadata_hint("/documents/alias").is_err());
        registry.remove_job("parent-hint-flight");
    }
    assert_eq!(crate::posix::posix_directory_link_count_calls(), 1);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn browser_policy_projection_preserves_requested_and_resolved_flight_fences() {
    let local = node("browser_policy_fences");
    let (mut runtime, mut dfs) = native_browser_service(
        "browser_policy_projection_preserves_requested_and_resolved_flight_fences",
        local.clone(),
    );
    dfs.posix_mkdir_all("/documents/target").unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![local]);
    dfs.posix_write_file("/documents/target/file", b"body", options.clone())
        .unwrap();
    dfs.posix_symlink("target/file", "/documents/link", options)
        .unwrap();
    let registry = dfs.cross_shard_namespace_lock_registry();
    for locked_path in ["/documents/link", "/documents/target"] {
        registry.upsert_job(
            None,
            "policy-flight".into(),
            sifs_file_system_id(),
            locked_path.to_owned(),
        );
        assert!(dfs.posix_stat("/documents/link").is_err());
        assert_eq!(
            dfs.posix_object_metadata_hint("/documents/link")
                .map(|object| (object.kind, object.policy))
                .map_err(|e| e.to_string()),
            dfs.posix_stat("/documents/link")
                .map(|stat| (stat.kind, stat.policy))
                .map_err(|e| e.to_string()),
        );
        registry.remove_job("policy-flight");
    }
    assert!(dfs.posix_object_policy("/documents/link").is_ok());
    drop(dfs);
    runtime.close().unwrap();
}

fn native_browser_runtime(
    name: &str,
    count: usize,
) -> (
    crate::test_support::DfsTestRuntime,
    Vec<sinomo_core::node::NodeId>,
) {
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, count);
    let mut runtime = crate::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    (runtime, nodes)
}

fn native_browser_policy(
    control: &crate::SharedDfsControlPlaneKvClient<Box<dyn crate::DfsControlPlaneKvClient>>,
    path: &str,
    policy: crate::DfsPlacementPolicy,
) {
    control.with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv.as_mut());
        let expected = store
            .get_file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap();
        let mut desired = expected.clone();
        crate::upsert_placement_rule(
            &mut desired.subtree_policies,
            crate::normalize_placement_rule(path, DfsPlacementScope::Subtree, policy).unwrap(),
        );
        assert!(
            store
                .compare_and_put_file_system_config(Some(&expected), &desired)
                .unwrap()
        );
    });
}

fn native_browser_observed_zones(
    service: &NetworkedDfsService<crate::DfsNodeTransport>,
) -> std::collections::BTreeMap<sinomo_core::node::NodeId, String> {
    // Read the normal publisher's retained observation DATA. No test writes
    // health, capacity, zone, or eligibility into the native control plane.
    service
        .control_plane
        .as_deref()
        .unwrap()
        .write_configuration_projection()
        .unwrap()
        .unwrap()
        .content_advertisements()
        .unwrap()
        .into_iter()
        .filter_map(|advertisement| advertisement.zone.map(|zone| (advertisement.node_id, zone)))
        .collect()
}

#[test]
fn browser_options_preserve_policy_and_candidates_without_metadata_advertisement_reads() {
    let (runtime, nodes) = native_browser_runtime("native-browser-policy-and-candidates", 3);
    let candidates = vec![nodes[2].clone(), nodes[0].clone()];
    for (consistency, minimum, zones) in [
        (sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 0),
        (sinomo_core::DfsObjectPolicy::ReplicatedConfirmed, 2, 2),
    ] {
        let mut policy = DfsPlacementPolicy::new(consistency, minimum, 2).unwrap();
        policy.required_node_ids = vec![nodes[1].clone()];
        policy.forbidden_node_ids = vec![nodes[2].clone()];
        policy.min_distinct_zones = zones;
        native_browser_policy(
            &runtime.shared_control_plane().unwrap(),
            "/documents",
            policy,
        );
        let (mut service, probe) = runtime.client_with_control_plane_probe(&nodes[0]).unwrap();
        let observed_zones = native_browser_observed_zones(&service);
        let actual =
            sifs_browser_write_options(&mut service, "/documents/new", &candidates).unwrap();
        assert_eq!(
            (
                actual.policy.min_commit_replicas,
                actual.policy.desired_replicas
            ),
            (minimum, 2)
        );
        assert_eq!(actual.policy.required_node_ids, vec![nodes[1].clone()]);
        assert_eq!(actual.policy.forbidden_node_ids, vec![nodes[2].clone()]);
        assert_eq!(actual.policy.min_distinct_zones, zones);
        assert_eq!(actual.policy.node_zones, observed_zones);
        assert_eq!(
            &actual.replica_candidates[..candidates.len()],
            candidates.as_slice()
        );
        assert!(actual.replica_candidates.contains(&nodes[1]));
        assert_eq!(probe.point_read_calls(), 0);
        assert_eq!(probe.prefix_read_calls(), 0);
        assert_eq!(probe.txn_calls(), 0);
        let selected = [nodes[0].clone(), nodes[1].clone()];
        if zones == 0 {
            crate::validate_replication_plan(&selected, &actual.policy).unwrap();
        } else {
            assert!(
                crate::validate_replication_plan(&selected, &actual.policy).is_err(),
                "same-process native observations cannot supply independently configured zones"
            );
        }
        assert!(
            crate::validate_replication_plan(&[nodes[0].clone(), nodes[2].clone()], &actual.policy)
                .is_err()
        );
    }
    runtime.close().unwrap();
}

#[test]
fn browser_policy_zone_data_keeps_complete_map_and_rejects_unknown_or_shared_zones() {
    // Pure policy DATA exercises multiple configured zones independently of
    // same-process native ownership. It publishes no advertisement or ACK.
    let nodes = [
        node("zone-data-a"),
        node("zone-data-b"),
        node("zone-data-c"),
    ];
    let zones = std::collections::BTreeMap::from([
        (nodes[0].clone(), "east".to_owned()),
        (nodes[1].clone(), "west".to_owned()),
        (nodes[2].clone(), "central".to_owned()),
    ]);
    for minimum_zones in [0, 2] {
        let mut policy =
            DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedConfirmed, 2, 2)
                .unwrap();
        policy.required_node_ids = vec![nodes[1].clone()];
        policy.forbidden_node_ids = vec![nodes[2].clone()];
        policy.min_distinct_zones = minimum_zones;
        let replication = policy.replication_policy(zones.clone()).unwrap();
        assert_eq!(replication.node_zones, zones);
        crate::validate_replication_plan(&[nodes[0].clone(), nodes[1].clone()], &replication)
            .unwrap();
        assert!(
            crate::validate_replication_plan(&[nodes[0].clone(), nodes[2].clone()], &replication)
                .is_err()
        );
    }
    let mut policy =
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    policy.min_distinct_zones = 2;
    for observed in [
        std::collections::BTreeMap::new(),
        std::collections::BTreeMap::from([
            (nodes[0].clone(), "shared".to_owned()),
            (nodes[1].clone(), "shared".to_owned()),
        ]),
    ] {
        let replication = policy.replication_policy(observed).unwrap();
        assert!(
            crate::validate_replication_plan(&[nodes[0].clone(), nodes[1].clone()], &replication)
                .is_err()
        );
    }
}

#[test]
fn browser_options_keep_confirmed_and_missing_path_defaults() {
    let (runtime, nodes) = native_browser_runtime("native-browser-confirmed-defaults", 2);
    native_browser_policy(
        &runtime.shared_control_plane().unwrap(),
        "/documents",
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap(),
    );
    let mut service = runtime.client(&nodes[0]).unwrap();
    service.posix_mkdir_all("/documents").unwrap();
    let options = sifs_browser_write_options(&mut service, "/documents/file", &nodes).unwrap();
    service
        .posix_write_file("/documents/file", b"actual confirmed bytes", options)
        .unwrap();
    assert_eq!(
        service.posix_stat("/documents/file").unwrap().policy,
        sinomo_core::DfsObjectPolicy::ReplicatedConfirmed
    );
    for path in ["/documents/file", "/documents/missing"] {
        let actual = sifs_browser_write_options(&mut service, path, &nodes).unwrap();
        assert_eq!(
            (
                actual.policy.min_commit_replicas,
                actual.policy.desired_replicas
            ),
            (2, 2)
        );
        assert!(actual.wait_for_commit);
    }
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn browser_options_preserve_local_one_replica_and_reject_local_constraints() {
    let (runtime, nodes) = native_browser_runtime("native-browser-local-policy", 2);
    let mut service = runtime.client(&nodes[0]).unwrap();
    let path = crate::sifs_node_files_path(&nodes[0]);
    assert_eq!(
        service.posix_object_policy(&path).unwrap(),
        sinomo_core::DfsObjectPolicy::LocalOwned
    );
    let observed_zones = native_browser_observed_zones(&service);
    let actual = sifs_browser_write_options(&mut service, &path, &nodes).unwrap();
    assert_eq!(
        (
            actual.policy.min_commit_replicas,
            actual.policy.desired_replicas
        ),
        (1, 1)
    );
    assert_eq!(actual.policy.min_distinct_zones, 0);
    assert_eq!(actual.policy.node_zones, observed_zones);
    for constraint in ["required", "forbidden", "zones"] {
        let mut policy =
            DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::LocalOwned, 1, 1).unwrap();
        match constraint {
            "required" => policy.required_node_ids = vec![nodes[0].clone()],
            "forbidden" => policy.forbidden_node_ids = vec![nodes[1].clone()],
            _ => policy.min_distinct_zones = 1,
        }
        assert!(
            crate::normalize_placement_rule(&path, DfsPlacementScope::Subtree, policy).is_err(),
            "normal current policy admission rejects {constraint} before publishing invalid local rules"
        );
    }
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn browser_options_replay_committed_operation_after_actual_reopen_without_changing_zone_policy() {
    let (mut runtime, nodes) = native_browser_runtime("native-browser-stable-policy-reopen", 1);
    let path = "/documents/stable-operation";
    let operation = "native-browser-stable-operation";
    let mut service = runtime.client(&nodes[0]).unwrap();
    service.posix_mkdir_all("/documents").unwrap();
    let options = sifs_browser_write_options(&mut service, path, &nodes).unwrap();
    let created = service
        .posix_create_file_with_operation_id(path, b"exact bytes", options, operation)
        .unwrap();
    let intent_id = crate::intent::stable_write_intent_id(&nodes[0], &created.object_id, operation);
    let original = service
        .metadata_backend()
        .unwrap()
        .get_intent(&created.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(original.status, DfsWriteIntentStatus::Committed);
    drop(service);
    runtime.stop_node(&nodes[0]).unwrap();
    runtime.reopen_node(nodes[0].clone()).unwrap();
    let mut service = runtime.client(&nodes[0]).unwrap();
    let options = sifs_browser_write_options(&mut service, path, &nodes).unwrap();
    let replay = service
        .posix_create_file_with_operation_id(path, b"exact bytes", options.clone(), operation)
        .unwrap();
    assert_eq!(
        (replay.object_id, replay.version),
        (created.object_id.clone(), created.version)
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_intent(&created.object_id, &intent_id)
            .unwrap()
            .unwrap(),
        original
    );
    assert!(
        service
            .posix_create_file_with_operation_id(path, b"changed", options, operation)
            .is_err()
    );
    assert_eq!(service.posix_read_file(path).unwrap(), b"exact bytes");
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn write_configuration_browser_warm_options_and_committed_writes_keep_full_zero_minimum_zones() {
    // Separate normal fixtures preserve the shared runtime's fixed roster.
    // The Docker formation gate exercises sequential membership growth.
    for (name, node_count) in [
        ("native-browser-warm-solo-configuration", 1),
        ("native-browser-warm-write-configuration", 3),
    ] {
        let (runtime, nodes) = native_browser_runtime(name, node_count);
        let path = "/services/native-write";
        let owner = runtime
            .metadata_primary_service(path)
            .unwrap()
            .node_id()
            .clone();
        // Warm-policy assertions require the actual published content roster,
        // not merely mounted metadata routes. Observe normal startup before
        // creating the zero-KV-read probe; do not inject candidate defaults.
        let readiness = runtime.client(&owner).unwrap();
        let wanted = nodes
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let ready_started = std::time::Instant::now();
        let ready_deadline = ready_started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            let projection = readiness
                .control_plane
                .as_deref()
                .unwrap()
                .write_configuration_projection()
                .unwrap();
            if projection.as_ref().is_some_and(|projection| {
                projection
                    .content_replica_candidates()
                    .unwrap()
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    == wanted
            }) {
                break;
            }
            assert!(
                std::time::Instant::now() < ready_deadline,
                "normal content roster did not publish before the existing readiness budget"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        eprintln!(
            "NATIVE_BROWSER_ROSTER_READY nodes={} elapsed_ms={}",
            nodes.len(),
            ready_started.elapsed().as_millis()
        );
        drop(readiness);
        let backend = DfsBrowserBackend::for_component_data_dir(
            Arc::new(runtime.data_dir(&owner).clone()),
            &runtime.runtime_handle(),
        )
        .unwrap();
        let (mut service, probe) = runtime.client_with_control_plane_probe(&owner).unwrap();
        let implicit = service
            .control_plane
            .as_deref()
            .unwrap()
            .write_configuration_projection()
            .unwrap()
            .unwrap();
        let config = implicit
            .file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap();
        assert!(config.subtree_policies.iter().all(|rule| {
            rule.path_prefix != crate::SIFS_USERS_PATH
                && rule.path_prefix != crate::SIFS_SERVICES_PATH
        }));
        let candidates = implicit.content_replica_candidates().unwrap();
        assert_eq!(
            candidates.iter().collect::<std::collections::BTreeSet<_>>(),
            nodes.iter().collect::<std::collections::BTreeSet<_>>()
        );
        let implicit_options = sifs_browser_write_options_with_configuration(
            &mut service,
            path,
            &candidates,
            Some(&implicit),
        )
        .unwrap();
        assert_eq!(
            (
                implicit_options.policy.min_commit_replicas,
                implicit_options.policy.desired_replicas
            ),
            (1, node_count.min(2))
        );
        let initial_bytes = vec![0x44; 4096];
        backend
            .write_file(DaemonDfsBrowserWriteFileRequest {
                path: path.into(),
                bytes: initial_bytes.clone(),
            })
            .unwrap();
        {
            let mut stored = backend.lock_service().unwrap();
            let stat = stored.posix_stat_distributed(path).unwrap();
            assert_eq!(
                stored.posix_read_file_distributed(path).unwrap(),
                initial_bytes
            );
            let intents = stored
                .metadata_backend()
                .unwrap()
                .intents_for_object_version_page(&stat.object_id, &stat.version, None, 16)
                .unwrap();
            let actual = intents
                .intents
                .iter()
                .find(|intent| {
                    intent.status == DfsWriteIntentStatus::Committed
                        && intent.publication.is_current()
                })
                .unwrap();
            assert_eq!(actual.file_system_id, stat.file_system_id);
            assert_eq!(actual.object_id, stat.object_id);
            assert_eq!(actual.target_version, stat.version);
            assert_eq!(actual.content_hash, stat.content_hash);
            assert_eq!(actual.policy, implicit_options.policy);
            assert_eq!(actual.desired_replica_node_ids.len(), node_count.min(2));
        }
        assert_eq!(probe.point_read_calls(), 0);
        assert_eq!(probe.prefix_read_calls(), 0);
        assert_eq!(probe.txn_calls(), 0);
        drop(service);
        let mut policy =
            DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 1)
                .unwrap();
        policy.required_node_ids = vec![owner.clone()];
        let expected_rule = crate::normalize_placement_rule(
            "/services",
            DfsPlacementScope::Subtree,
            policy.clone(),
        )
        .unwrap();
        runtime
            .apply_placement_policy(&owner, "/services", DfsPlacementScope::Subtree, policy)
            .unwrap();
        let applied = runtime
            .shared_control_plane()
            .unwrap()
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .get_file_system_config(&sifs_file_system_id())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            applied
                .subtree_policies
                .iter()
                .filter(|rule| {
                    rule.path_prefix == expected_rule.path_prefix
                        && rule.scope == expected_rule.scope
                })
                .collect::<Vec<_>>(),
            vec![&expected_rule]
        );
        // Open the next caller after publication, as the normal Apply API specifies.
        // The earlier held implicit view remains separate, unchanged DATA.
        let (mut service, probe) = runtime.client_with_control_plane_probe(&owner).unwrap();
        let observed_zones = native_browser_observed_zones(&service);
        let expected = sifs_browser_write_options(&mut service, path, &candidates).unwrap();
        assert_eq!(
            (
                expected.policy.min_commit_replicas,
                expected.policy.desired_replicas
            ),
            (1, 1)
        );
        assert_eq!(expected.policy.required_node_ids, vec![owner]);
        assert_eq!(
            sifs_browser_write_options_with_configuration(
                &mut service,
                path,
                &candidates,
                Some(&implicit),
            )
            .unwrap(),
            implicit_options
        );
        for _ in 0..3 {
            assert_eq!(
                sifs_browser_write_options(&mut service, path, &candidates).unwrap(),
                expected
            );
        }
        assert_eq!(probe.point_read_calls(), 0);
        assert_eq!(probe.prefix_read_calls(), 0);
        assert_eq!(probe.txn_calls(), 0);
        assert_eq!(expected.policy.min_distinct_zones, 0);
        assert_eq!(expected.policy.node_zones, observed_zones);
        let mut versions = Vec::new();
        for bytes in [vec![0x11; 4096], vec![0x99; 4096]] {
            backend
                .write_file(DaemonDfsBrowserWriteFileRequest {
                    path: path.into(),
                    bytes: bytes.clone(),
                })
                .unwrap();
            let mut stored = backend.lock_service().unwrap();
            let stat = stored.posix_stat_distributed(path).unwrap();
            assert_eq!(stored.posix_read_file_distributed(path).unwrap(), bytes);
            let intents = stored
                .metadata_backend()
                .unwrap()
                .intents_for_object_version_page(&stat.object_id, &stat.version, None, 16)
                .unwrap();
            let actual = intents
                .intents
                .iter()
                .find(|intent| intent.status == DfsWriteIntentStatus::Committed)
                .unwrap();
            assert_eq!(actual.policy, expected.policy);
            versions.push(stat.version);
        }
        assert_ne!(versions[0], versions[1]);
        drop(service);
        drop(backend);
        runtime.close().unwrap();
    }
}

#[test]
fn write_configuration_browser_coherent_updates_keep_retained_view_and_current_policy_separate() {
    let (runtime, nodes) = native_browser_runtime("native-browser-coherent-current-policy", 3);
    native_browser_policy(
        &runtime.shared_control_plane().unwrap(),
        "/documents",
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap(),
    );
    let mut service = runtime.client(&nodes[0]).unwrap();
    let old = service
        .control_plane
        .as_deref()
        .unwrap()
        .write_configuration_projection()
        .unwrap()
        .unwrap();
    let old_candidates = old.content_replica_candidates().unwrap();
    let old_options = sifs_browser_write_options_with_configuration(
        &mut service,
        "/documents/coherent",
        &old_candidates,
        Some(&old),
    )
    .unwrap();
    let mut policy =
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    policy.required_node_ids = vec![nodes[0].clone()];
    policy.forbidden_node_ids = vec![nodes[2].clone()];
    policy.min_distinct_zones = 2;
    native_browser_policy(
        &runtime.shared_control_plane().unwrap(),
        "/documents",
        policy,
    );
    assert_eq!(
        sifs_browser_write_options_with_configuration(
            &mut service,
            "/documents/coherent",
            &old_candidates,
            Some(&old)
        )
        .unwrap(),
        old_options
    );
    let (mut current, probe) = runtime.client_with_control_plane_probe(&nodes[0]).unwrap();
    let view = current
        .control_plane
        .as_deref()
        .unwrap()
        .write_configuration_projection()
        .unwrap()
        .unwrap();
    assert!(view.revision() > old.revision());
    let candidates = view.content_replica_candidates().unwrap();
    let observed_zones = native_browser_observed_zones(&current);
    let actual =
        sifs_browser_write_options(&mut current, "/documents/coherent", &candidates).unwrap();
    assert_eq!(
        (
            actual.policy.min_commit_replicas,
            actual.policy.desired_replicas,
            actual.policy.min_distinct_zones
        ),
        (1, 2, 2)
    );
    assert_eq!(actual.policy.forbidden_node_ids, vec![nodes[2].clone()]);
    assert_eq!(actual.policy.node_zones, observed_zones);
    assert!(
        crate::validate_replication_plan(&[nodes[0].clone(), nodes[1].clone()], &actual.policy)
            .is_err(),
        "retained unknown or shared native zones cannot satisfy a two-zone rule"
    );
    assert_ne!(actual.policy, old_options.policy);
    assert_eq!(probe.point_read_calls(), 0);
    assert_eq!(probe.prefix_read_calls(), 0);
    drop(current);
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn write_configuration_browser_pending_preserves_payload_without_parent_intent_or_journal_effects()
{
    let (runtime, nodes) = native_browser_runtime("native-browser-configuration-gap", 1);
    let backend = DfsBrowserBackend::for_component_data_dir(
        Arc::new(runtime.data_dir(&nodes[0]).clone()),
        &runtime.runtime_handle(),
    )
    .unwrap();
    let key = crate::file_system_config_key(&sifs_file_system_id());
    let context = runtime.component_context(&nodes[0]).unwrap();
    let owner = crate::runtime_host::retain_component_data_owner(&context).unwrap();
    assert!(
        crate::runtime_host::registered_component_cluster(&context)
            .unwrap()
            .is_some()
    );
    // Configuration publication uses the normal native KV client independently
    // of the DFS metadata projection whose real repair read is paused below.
    let control = crate::SharedDfsControlPlaneKvClient::new(Box::new(
        crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &ClusterPaths::new(context.data_dir()),
            context.runtime().unwrap(),
        ),
    )
        as Box<dyn crate::DfsControlPlaneKvClient>);
    let gap = runtime
        .lose_configuration_delta_and_pause_rebuild(&nodes[0], key.clone())
        .unwrap();
    let first =
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    native_browser_policy(&control, "/documents", first.clone());
    let mut second = first;
    second.required_node_ids = vec![nodes[0].clone()];
    native_browser_policy(&control, "/documents", second);
    let rebuilt = gap.wait_rebuild(std::time::Duration::from_secs(5));
    if rebuilt.is_err() {
        eprintln!(
            "native browser gap boundary: root={} key={key} result={rebuilt:?} lost_delta={:?}",
            runtime.data_dir(&nodes[0]).root().display(),
            gap.dropped_delta().map(|delta| (
                delta.revision,
                delta.compacted_revision,
                delta
                    .events
                    .iter()
                    .map(|event| event.key.clone())
                    .collect::<Vec<_>>(),
                delta.final_key_states.len(),
            )),
        );
    }
    rebuilt.unwrap();
    let actual_lost = gap
        .dropped_delta()
        .expect("one real committed delta was withheld");
    assert!(actual_lost.events.iter().any(|event| event.key == key));
    let parent = "/documents/configuration-handoff/deeper";
    let path = format!("{parent}/file");
    let bytes = vec![0x5a; 4096];
    let mut request = DaemonDfsBrowserWriteFileRequest {
        path: path.clone(),
        bytes: bytes.clone(),
    };
    let allocation = request.bytes.as_ptr();
    let before = backend
        .lock_service()
        .unwrap()
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    for _ in 0..2 {
        request = match backend.try_write_file(request).unwrap() {
            DfsBrowserWriteAttempt::ConfigurationPending(original) => original,
            DfsBrowserWriteAttempt::Complete(_) => {
                panic!("native configuration gap admitted a write")
            }
        };
        assert_eq!(request.bytes.as_ptr(), allocation);
        assert_eq!(request.path, path);
        assert_eq!(request.bytes, bytes);
        let service = backend.lock_service().unwrap();
        let metadata = service.metadata_backend().unwrap();
        assert_eq!(metadata.journal_last_sequence().unwrap(), before);
        for absent in ["/documents/configuration-handoff", parent, path.as_str()] {
            assert!(
                metadata
                    .get_object_by_path(&sifs_file_system_id(), absent)
                    .unwrap()
                    .is_none()
            );
        }
    }
    gap.release();
    let authority = crate::runtime_host::daemon_dfs_metadata_authority_generation_for_identity(
        &ClusterPaths::new(runtime.data_dir(&nodes[0])),
        &sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(
            runtime.data_dir(&nodes[0]),
        ))
        .unwrap()
        .cluster_id,
        &nodes[0],
        Some(&runtime.runtime_handle()),
    )
    .unwrap();
    runtime.runtime_handle().block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            authority.ensure_write_configuration_ready_async(),
        )
        .await
        .unwrap()
        .unwrap();
    });
    assert!(matches!(
        backend.try_write_file(request).unwrap(),
        DfsBrowserWriteAttempt::Complete(_)
    ));
    let service = backend.lock_service().unwrap();
    assert_eq!(service.posix_read_file(&path).unwrap(), bytes);
    let stat = service.posix_stat(&path).unwrap();
    let intents = service
        .metadata_backend()
        .unwrap()
        .intents_for_object_version_page(&stat.object_id, &stat.version, None, 16)
        .unwrap();
    assert_eq!(
        intents
            .intents
            .iter()
            .filter(|intent| intent.status == DfsWriteIntentStatus::Committed)
            .count(),
        1
    );
    drop(service);
    drop(authority);
    drop(gap);
    drop(backend);
    drop(control);
    drop(owner);
    drop(context);
    runtime.close().unwrap();
}

#[test]
fn write_configuration_browser_unavailable_native_floor_fails_before_parent_or_file_mutation() {
    let (runtime, nodes) = native_browser_runtime("native-browser-floor-return-unavailable", 1);
    let backend = DfsBrowserBackend::for_component_data_dir(
        Arc::new(runtime.data_dir(&nodes[0]).clone()),
        &runtime.runtime_handle(),
    )
    .unwrap();
    let before = backend
        .lock_service()
        .unwrap()
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let fault = runtime.deny_configuration_floor(&nodes[0]).unwrap();
    let parent = "/documents/native-floor-absent/deeper";
    let path = format!("{parent}/file");
    let error = backend
        .write_file(DaemonDfsBrowserWriteFileRequest {
            path: path.clone(),
            bytes: b"must not publish".to_vec(),
        })
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("native configuration floor is unavailable")
    );
    let service = backend.lock_service().unwrap();
    let metadata = service.metadata_backend().unwrap();
    assert_eq!(metadata.journal_last_sequence().unwrap(), before);
    for absent in ["/documents/native-floor-absent", parent, path.as_str()] {
        assert!(
            metadata
                .get_object_by_path(&sifs_file_system_id(), absent)
                .unwrap()
                .is_none()
        );
    }
    drop(service);
    drop(fault);
    backend
        .write_file(DaemonDfsBrowserWriteFileRequest {
            path: path.clone(),
            bytes: b"real normal recovery".to_vec(),
        })
        .unwrap();
    assert_eq!(
        backend
            .lock_service()
            .unwrap()
            .posix_read_file(&path)
            .unwrap(),
        b"real normal recovery"
    );
    drop(backend);
    runtime.close().unwrap();
}
