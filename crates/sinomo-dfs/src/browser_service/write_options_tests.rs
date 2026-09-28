//! Browser write options must retain POSIX policy and durable retry identity.

use super::*;
use sinomo_core::kv::{InProcessKvStore, KvRevision};
use sinomo_core::network::{InProcessNetwork, InProcessTransport};
use std::collections::BTreeMap;
use std::path::Path;

type TestService = NetworkedDfsService<InProcessTransport>;

fn node(name: &str) -> sinomo_core::node::NodeId {
    sinomo_core::node::NodeId::from_legacy_named(name).expect("node id")
}

fn service(root: &Path, local: sinomo_core::node::NodeId) -> TestService {
    let network = InProcessNetwork::new();
    let mut service = NetworkedDfsService::new(
        network.add_node(local),
        LocalContentStore::new(root.join("content")).expect("content"),
    )
    .with_metadata_backend(
        open_root_local_metadata_backend(root.join("metadata")).expect("metadata"),
    )
    .expect("backend");
    service.bootstrap_distributed_sifs().expect("bootstrap");
    service
}

fn legacy_options(
    service: &mut TestService,
    path: &str,
    candidates: &[sinomo_core::node::NodeId],
) -> DfsWriteOptions {
    let policy = service
        .posix_stat(path)
        .map(|stat| stat.policy)
        .unwrap_or(sinomo_core::DfsObjectPolicy::ReplicatedEventual);
    let (_, _, policy) = service
        .managed_replication_policy_for_path(path, policy, candidates.len())
        .expect("legacy options policy");
    DfsWriteOptions {
        replica_candidates: sifs_browser_write_candidates(candidates, &policy),
        policy,
        wait_for_commit: true,
        foreground_replica_repair: false,
    }
}

#[test]
fn browser_policy_projection_preserves_links_dot_paths_and_namespace_errors() {
    let root = sinomo_testdir::TestDir::new("browser-policy-paths");
    let local = node("browser_policy_paths");
    let mut dfs = service(root.path(), local.clone());
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
            dfs.posix_object_policy(path).map_err(|e| e.to_string()),
            dfs.posix_stat(path)
                .map(|stat| stat.policy)
                .map_err(|e| e.to_string()),
            "policy projection differs for {path}",
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
            dfs.posix_object_policy(path).map_err(|e| e.to_string()),
            dfs.posix_stat(path)
                .map(|stat| stat.policy)
                .map_err(|e| e.to_string()),
        );
    }
}

#[test]
fn browser_policy_projection_preserves_requested_and_resolved_flight_fences() {
    let root = sinomo_testdir::TestDir::new("browser-policy-fences");
    let local = node("browser_policy_fences");
    let mut dfs = service(root.path(), local.clone());
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
            dfs.posix_object_policy("/documents/link")
                .map_err(|e| e.to_string()),
            dfs.posix_stat("/documents/link")
                .map(|stat| stat.policy)
                .map_err(|e| e.to_string()),
        );
        registry.remove_job("policy-flight");
    }
    assert!(dfs.posix_object_policy("/documents/link").is_ok());
}

#[derive(Clone, Debug)]
struct PrefixRecorder {
    inner: SharedDfsControlPlaneKvClient<InProcessKvStore>,
    prefixes: Arc<Mutex<Vec<String>>>,
}

impl DfsControlPlaneKvClient for PrefixRecorder {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::get(kv, key))
    }
    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.prefixes.lock().unwrap().push(prefix.to_owned());
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::range_prefix(kv, prefix))
    }
    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::put(kv, key, value))
    }
    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::txn(kv, txn))
    }
}

fn configure_control(
    dfs: &TestService,
    nodes: &[(sinomo_core::node::NodeId, &str)],
) -> SharedDfsControlPlaneKvClient<InProcessKvStore> {
    let shared = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
    shared.with_client(|kv| {
        let mut control = DfsControlPlaneStore::new(kv);
        control
            .put_file_system_config(&DfsFileSystemConfig::from(
                dfs.metadata()
                    .filesystems
                    .get(&sifs_file_system_id())
                    .unwrap(),
            ))
            .unwrap();
        control
            .put_metadata_shard(&DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".into(),
                primary_node_id: dfs.node_id().clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            })
            .unwrap();
        for (node, zone) in nodes {
            let mut advertisement = DfsNodeAdvertisement::content(node.clone());
            advertisement.zone = Some((*zone).into());
            control.advertise_content_service(&advertisement).unwrap();
            control
                .advertise_metadata_service(&DfsNodeAdvertisement::metadata(node.clone()))
                .unwrap();
        }
    });
    shared
}

#[test]
fn browser_options_preserve_policy_and_candidates_without_metadata_advertisement_reads() {
    let root = sinomo_testdir::TestDir::new("browser-policy-advertisements");
    let local = node("browser_policy_ads_local");
    let required = node("browser_policy_ads_required");
    let forbidden = node("browser_policy_ads_forbidden");
    let mut dfs = service(root.path(), local.clone());
    dfs.posix_mkdir_all("/documents").unwrap();
    let control = configure_control(
        &dfs,
        &[
            (local.clone(), "zone-a"),
            (required.clone(), "zone-b"),
            (forbidden.clone(), "zone-c"),
        ],
    );
    let prefixes = Arc::new(Mutex::new(Vec::new()));
    dfs = dfs.with_control_plane(PrefixRecorder {
        inner: control.clone(),
        prefixes: prefixes.clone(),
    });
    let candidates = vec![forbidden.clone(), local.clone()];
    for (consistency, zones) in [
        (sinomo_core::DfsObjectPolicy::ReplicatedEventual, 0),
        (sinomo_core::DfsObjectPolicy::ReplicatedConfirmed, 2),
    ] {
        control.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            let mut config = store
                .get_file_system_config(&sifs_file_system_id())
                .unwrap()
                .unwrap();
            let expected_config = config.clone();
            let mut policy =
                DfsPlacementPolicy::new(consistency.clone(), if zones == 0 { 1 } else { 2 }, 2)
                    .unwrap();
            policy.required_node_ids = vec![required.clone()];
            policy.forbidden_node_ids = vec![forbidden.clone()];
            policy.min_distinct_zones = zones;
            crate::upsert_placement_rule(
                &mut config.subtree_policies,
                crate::normalize_placement_rule("/documents", DfsPlacementScope::Subtree, policy)
                    .unwrap(),
            );
            // Generic filesystem publication deliberately preserves current
            // placement rules. Apply this test's explicit rule edit with exact CAS.
            assert!(
                store
                    .compare_and_put_file_system_config(Some(&expected_config), &config)
                    .unwrap()
            );
            assert_eq!(
                store
                    .get_file_system_config(&sifs_file_system_id())
                    .unwrap()
                    .unwrap(),
                config
            );
        });
        let resolved = dfs
            .resolved_placement_policy_for_path(
                "/documents/new",
                sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                candidates.len(),
            )
            .unwrap();
        assert_eq!(resolved.source_path.as_deref(), Some("/documents"));
        assert_eq!(resolved.policy.consistency, consistency);
        assert_eq!(resolved.policy.required_node_ids, vec![required.clone()]);
        assert_eq!(resolved.policy.forbidden_node_ids, vec![forbidden.clone()]);
        assert_eq!(resolved.policy.min_distinct_zones, zones);
        assert_eq!(
            (
                resolved.policy.min_commit_replicas,
                resolved.policy.desired_replicas
            ),
            (if zones == 0 { 1 } else { 2 }, 2)
        );
        let expected = legacy_options(&mut dfs, "/documents/new", &candidates);
        prefixes.lock().unwrap().clear();
        let actual = sifs_browser_write_options(&mut dfs, "/documents/new", &candidates).unwrap();
        assert_eq!(actual, expected);
        assert!(
            !actual.policy.node_zones.is_empty(),
            "zero minimum also retains durable zone-map identity"
        );
        assert_eq!(
            *prefixes.lock().unwrap(),
            vec![crate::content_services_prefix().to_string()]
        );
        assert!(actual.replica_candidates.contains(&required));
        assert_eq!(
            &actual.replica_candidates[..candidates.len()],
            candidates.as_slice(),
            "provider-derived candidate order is unchanged"
        );
        assert_eq!(actual.policy.required_node_ids, vec![required.clone()]);
        assert_eq!(actual.policy.forbidden_node_ids, vec![forbidden.clone()]);
        let selected = vec![local.clone(), required.clone()];
        crate::validate_replication_plan(&selected, &actual.policy).expect("allowed exact plan");
        assert!(
            crate::validate_replication_plan(&[local.clone(), forbidden.clone()], &actual.policy)
                .is_err()
        );
        if zones > 0 {
            assert_eq!(
                (
                    actual.policy.min_commit_replicas,
                    actual.policy.desired_replicas
                ),
                (2, 2)
            );
            for advertised_zone in [Some("zone-a"), None] {
                control.with_client(|kv| {
                    let mut advertisement = DfsNodeAdvertisement::content(required.clone());
                    advertisement.zone = advertised_zone.map(str::to_owned);
                    DfsControlPlaneStore::new(kv)
                        .advertise_content_service(&advertisement)
                        .unwrap();
                });
                let expected_failure_policy =
                    legacy_options(&mut dfs, "/documents/new", &candidates);
                let unavailable =
                    sifs_browser_write_options(&mut dfs, "/documents/new", &candidates).unwrap();
                assert_eq!(unavailable, expected_failure_policy);
                assert!(
                    crate::validate_replication_plan(&selected, &unavailable.policy).is_err(),
                    "a configured positive zone minimum must still fail with same-zone or missing advertisements"
                );
            }
        }
    }
}

#[test]
fn browser_options_keep_confirmed_and_missing_path_defaults() {
    let root = sinomo_testdir::TestDir::new("browser-policy-consistency");
    let local = node("browser_policy_consistency");
    let mut dfs = service(root.path(), local.clone());
    dfs.posix_mkdir_all("/documents").unwrap();
    let file = dfs
        .posix_write_file(
            "/documents/file",
            b"body",
            DfsWriteOptions::replicated_eventual(vec![local.clone()]),
        )
        .unwrap();
    let mut object = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&file.object_id)
        .unwrap()
        .unwrap();
    object.policy = sinomo_core::DfsObjectPolicy::ReplicatedConfirmed;
    dfs.add_object_metadata(object).unwrap();
    let candidates = vec![local.clone(), node("browser_policy_consistency_other")];
    for path in ["/documents/file", "/documents/missing"] {
        let expected = legacy_options(&mut dfs, path, &candidates);
        assert_eq!(
            sifs_browser_write_options(&mut dfs, path, &candidates).unwrap(),
            expected
        );
    }
    let confirmed = sifs_browser_write_options(&mut dfs, "/documents/file", &candidates).unwrap();
    assert_eq!(
        (
            confirmed.policy.min_commit_replicas,
            confirmed.policy.desired_replicas
        ),
        (2, 2)
    );
}

#[test]
fn browser_options_preserve_local_one_replica_and_reject_local_constraints() {
    let root = sinomo_testdir::TestDir::new("browser-policy-local-contract");
    let local = node("browser_policy_local_contract");
    let other = node("browser_policy_local_contract_other");
    let mut dfs = service(root.path(), local.clone());
    let path = crate::sifs_node_files_path(&local);
    let control = configure_control(&dfs, &[(local.clone(), "local-zone")]);
    dfs = dfs.with_control_plane(control.clone());
    assert_eq!(
        dfs.posix_object_policy(&path).unwrap(),
        sinomo_core::DfsObjectPolicy::LocalOwned
    );

    let singleton = vec![local.clone()];
    let legacy_singleton = legacy_options(&mut dfs, &path, &singleton);
    let local_options = sifs_browser_write_options(&mut dfs, &path, &singleton).unwrap();
    assert_eq!(local_options, legacy_singleton);
    assert_eq!(
        (
            local_options.policy.min_commit_replicas,
            local_options.policy.desired_replicas
        ),
        (1, 1)
    );
    assert_eq!(local_options.policy.min_distinct_zones, 0);
    assert_eq!(
        local_options
            .policy
            .node_zones
            .get(&local)
            .map(String::as_str),
        Some("local-zone"),
        "zone observations alone are not a local placement constraint"
    );

    let multiple = vec![local.clone(), other.clone()];
    let expected_reason = "local placement requires W=1, N=1, and no replica constraints";
    // Legacy local rules omit counts: their valid implicit default remains
    // W=1/N=1 even when the cluster exposes multiple candidate nodes. Zone
    // observations remain available without becoming a local constraint.
    let (_, _, legacy_policy) = dfs
        .managed_replication_policy_for_path(
            &path,
            sinomo_core::DfsObjectPolicy::LocalOwned,
            multiple.len(),
        )
        .expect("implicit local counts are independent of candidate count");
    let multiple_options = sifs_browser_write_options(&mut dfs, &path, &multiple)
        .expect("browser preserves the valid local default");
    assert_eq!(multiple_options.policy, legacy_policy);
    assert_eq!(
        (
            multiple_options.policy.min_commit_replicas,
            multiple_options.policy.desired_replicas
        ),
        (1, 1)
    );
    assert_eq!(
        multiple_options.policy.node_zones,
        local_options.policy.node_zones
    );
    assert_eq!(
        multiple_options
            .policy
            .node_zones
            .get(&local)
            .map(String::as_str),
        Some("local-zone")
    );

    let mut explicit_local =
        crate::DfsSubtreePolicy::legacy(&path, sinomo_core::DfsObjectPolicy::LocalOwned);
    explicit_local.min_commit_replicas = Some(1);
    explicit_local.desired_replicas = Some(1);
    // Include a valid explicit rule followed by each independently invalid
    // local constraint. Persist exact fixture records, then exercise the same
    // policy validation both public browser option paths rely on.
    for constraint in ["none", "required", "forbidden", "zones"] {
        let mut rule = explicit_local.clone();
        match constraint {
            "required" => rule.required_node_ids = vec![local.clone()],
            "forbidden" => rule.forbidden_node_ids = vec![other.clone()],
            "zones" => rule.min_distinct_zones = 1,
            _ => {}
        }
        control.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            let expected = store
                .get_file_system_config(&sifs_file_system_id())
                .unwrap()
                .unwrap();
            let mut desired = expected.clone();
            crate::upsert_placement_rule(&mut desired.subtree_policies, rule);
            assert!(
                store
                    .compare_and_put_file_system_config(Some(&expected), &desired)
                    .unwrap()
            );
            assert_eq!(
                store
                    .get_file_system_config(&sifs_file_system_id())
                    .unwrap()
                    .unwrap(),
                desired
            );
        });
        if constraint == "none" {
            let legacy = legacy_options(&mut dfs, &path, &multiple);
            let actual = sifs_browser_write_options(&mut dfs, &path, &multiple).unwrap();
            assert_eq!(actual, legacy);
            assert_eq!(
                (
                    actual.policy.min_commit_replicas,
                    actual.policy.desired_replicas
                ),
                (1, 1)
            );
        } else {
            let legacy_error = dfs
                .managed_replication_policy_for_path(
                    &path,
                    sinomo_core::DfsObjectPolicy::LocalOwned,
                    singleton.len(),
                )
                .unwrap_err();
            let actual_error = dfs
                .managed_write_replication_policy_for_path(
                    &path,
                    sinomo_core::DfsObjectPolicy::LocalOwned,
                    singleton.len(),
                )
                .unwrap_err();
            assert_eq!(actual_error.to_string(), legacy_error.to_string());
            assert!(
                legacy_error.to_string().contains(expected_reason),
                "{constraint}"
            );
            assert!(
                sifs_browser_write_options(&mut dfs, &path, &singleton)
                    .unwrap_err()
                    .to_string()
                    .contains(expected_reason),
                "{constraint}"
            );
        }
    }
}

#[test]
fn browser_options_replay_old_committed_operation_after_reopen_without_changing_zone_policy() {
    let root = sinomo_testdir::TestDir::new("browser-policy-old-replay");
    let local = node("browser_policy_old_replay");
    let path = "/documents/old-operation";
    let operation = "browser-policy-stable-operation";
    let bytes = vec![0x5c; 4096];
    let mut dfs = service(root.path(), local.clone());
    dfs.posix_mkdir_all("/documents").unwrap();
    let control = configure_control(&dfs, &[(local.clone(), "historical-zone")]);
    dfs = dfs.with_control_plane(control.clone());
    let old_options = legacy_options(&mut dfs, path, std::slice::from_ref(&local));
    assert_eq!(old_options.policy.min_distinct_zones, 0);
    assert_eq!(
        old_options.policy.node_zones,
        BTreeMap::from([(local.clone(), "historical-zone".to_string())])
    );
    let created = dfs
        .posix_create_file_with_operation_id(path, &bytes, old_options.clone(), operation)
        .expect("original operation with pre-optimization options");
    let intent_id = crate::intent::stable_write_intent_id(&local, &created.object_id, operation);
    let original = dfs
        .metadata_backend()
        .unwrap()
        .get_intent(&created.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(original.status, DfsWriteIntentStatus::Committed);
    assert_eq!(original.policy, old_options.policy);
    drop(dfs);

    let mut reopened = service(root.path(), local.clone()).with_control_plane(control);
    let new_options =
        sifs_browser_write_options(&mut reopened, path, std::slice::from_ref(&local)).unwrap();
    assert_eq!(new_options.policy, original.policy);
    let replay = reopened
        .posix_create_file_with_operation_id(path, &bytes, new_options.clone(), operation)
        .expect("lost-response retry through new options after restart");
    assert_eq!(
        (replay.object_id, replay.version),
        (created.object_id.clone(), created.version)
    );
    let after = reopened
        .metadata_backend()
        .unwrap()
        .get_intent(&created.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        after, original,
        "retry must reuse the exact durable identity, not rewrite policy or timestamps"
    );
    assert!(
        reopened
            .posix_create_file_with_operation_id(path, b"different", new_options, operation)
            .is_err(),
        "stable content identity remains exact"
    );
    assert_eq!(reopened.posix_read_file(path).unwrap(), bytes);
}
