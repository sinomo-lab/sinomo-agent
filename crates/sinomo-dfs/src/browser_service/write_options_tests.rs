//! Browser write options must retain POSIX policy and durable retry identity.

use super::*;
use sinomo_core::kv::{
    ClusterKvRaftConfigRecord, InProcessKvStore, KvAuthoritativeRead, KvAuthorityBasis,
    KvAuthorityProof, KvLocalRead, KvRevision, cluster_kv_raft_config_key, encode_kv_value,
};
use sinomo_core::network::{InProcessNetwork, InProcessTransport};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

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

fn legacy_options<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
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

fn configure_control<T: DfsMessageTransport>(
    dfs: &NetworkedDfsService<T>,
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

#[derive(Clone, Debug)]
struct ProjectionRecorder {
    inner: SharedDfsControlPlaneKvClient<InProcessKvStore>,
    view: Arc<Mutex<Arc<DfsWriteConfigurationProjection>>>,
    floor_available: Arc<AtomicBool>,
    configuration_pending: Arc<AtomicBool>,
    prefixes: Arc<Mutex<Vec<String>>>,
}

impl DfsControlPlaneKvClient for ProjectionRecorder {
    fn write_configuration_readiness(&self) -> Result<DfsWriteConfigurationReadiness, KvError> {
        let view = self.write_configuration_projection()?;
        if self.configuration_pending.load(Ordering::Acquire) {
            Ok(DfsWriteConfigurationReadiness::Pending)
        } else {
            Ok(DfsWriteConfigurationReadiness::Ready(view))
        }
    }

    fn write_configuration_projection(
        &self,
    ) -> Result<Option<Arc<DfsWriteConfigurationProjection>>, KvError> {
        if !self.floor_available.load(Ordering::Acquire) {
            return Err(KvError::AuthorityUnavailable {
                detail: "test native write configuration floor is unavailable".into(),
            });
        }
        Ok(Some(Arc::clone(&self.view.lock().unwrap())))
    }

    fn placement_authority_process_identity(&self) -> Option<DfsPlacementAuthorityProcessIdentity> {
        self.inner
            .with_client(|kv| kv.placement_authority_process_identity())
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::get(kv, key))
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.prefixes.lock().unwrap().push(prefix.to_string());
        self.inner
            .with_client(|kv| DfsControlPlaneKvClient::range_prefix(kv, prefix))
    }

    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<DfsAuthoritativePrefixProjection, KvError> {
        self.prefixes.lock().unwrap().push(prefix.to_string());
        self.inner
            .with_client(|kv| kv.range_prefix_authoritative(prefix))
    }

    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.inner
            .with_client(|kv| kv.get_authoritative_consistent(keys))
    }

    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        deadline: Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.inner
            .with_client(|kv| kv.get_authoritative_consistent_before(keys, deadline))
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

fn projected_configuration(
    control: &SharedDfsControlPlaneKvClient<InProcessKvStore>,
    local: &sinomo_core::node::NodeId,
) -> Arc<DfsWriteConfigurationProjection> {
    use crate::write_configuration_projection::{
        DFS_WRITE_CONFIGURATION_PREFIXES, DfsWriteConfigurationState,
    };
    let (native, floor) = control.with_client(|kv| {
        let config_key = cluster_kv_raft_config_key();
        if DfsControlPlaneKvClient::get(kv, &config_key)
            .unwrap()
            .is_none()
        {
            let config = ClusterKvRaftConfigRecord {
                cluster_id: sinomo_core::Id::derived("browser-write-projection", b"cluster"),
                term: 1,
                kv_revision: 0,
                voters: vec![local.clone()],
                learners: Vec::new(),
                non_voting: Vec::new(),
                updated_at: time::OffsetDateTime::UNIX_EPOCH,
            };
            DfsControlPlaneKvClient::put(kv, config_key.clone(), encode_kv_value(&config).unwrap())
                .unwrap();
        }
        // Fixture bootstrap captures real configured data once. Warm calls
        // below return this already-validated immutable publication.
        let snapshot = kv.try_snapshot().unwrap();
        let authority = KvAuthorityProof {
            term: 1,
            commit_index: snapshot.revision.0 + 1,
            revision: snapshot.revision,
            basis: KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 1,
                required_quorum: 1,
            },
        };
        let native = std::array::from_fn(|scope| KvAuthoritativeRead {
            value: snapshot
                .prefix_entries(DFS_WRITE_CONFIGURATION_PREFIXES[scope])
                .cloned()
                .collect(),
            authority,
        });
        let floor = KvLocalRead {
            value: snapshot.get(&config_key).cloned(),
            revision: snapshot.revision,
            observed_term: 1,
            applied_index: snapshot.revision.0 + 1,
            known_leader: Some(local.clone()),
        };
        (native, floor)
    });
    let mut state = DfsWriteConfigurationState::new();
    let token = state.begin_refresh();
    assert!(state.install_bootstrap(token, native, floor).unwrap());
    state.snapshot().unwrap()
}

fn put_provider(kv: &mut InProcessKvStore, node_id: &sinomo_core::node::NodeId, eligible: bool) {
    let mut provider =
        sinomo_core::service::ServiceInstanceRecord::new("browser-dfs", node_id.clone());
    if eligible {
        provider
            .provider_capabilities
            .push("dfs.object.transfer".into());
    }
    DfsControlPlaneKvClient::put(
        kv,
        sinomo_core::service::cluster_service_instance_key(&provider.service_id, node_id),
        encode_kv_value(&provider).unwrap(),
    )
    .unwrap();
}

fn configure_projection_policy(
    control: &SharedDfsControlPlaneKvClient<InProcessKvStore>,
    local: &sinomo_core::node::NodeId,
    desired: usize,
) {
    control.with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv);
        let mut config = store
            .get_file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap();
        let expected = config.clone();
        let mut policy =
            DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, desired)
                .unwrap();
        policy.required_node_ids = vec![local.clone()];
        crate::upsert_placement_rule(
            &mut config.subtree_policies,
            crate::normalize_placement_rule("/documents", DfsPlacementScope::Subtree, policy)
                .unwrap(),
        );
        assert!(
            store
                .compare_and_put_file_system_config(Some(&expected), &config)
                .unwrap()
        );
    });
}

struct ProjectionBrowserHost;

impl DfsBrowserServiceHost for ProjectionBrowserHost {
    fn control_plane_client(
        &self,
        _: &ClusterPaths,
        _: &tokio::runtime::Handle,
    ) -> Option<Box<dyn DfsControlPlaneKvClient>> {
        unreachable!()
    }
    fn peer_transports(
        &self,
        _: &SinomoDataDir,
        _: &tokio::runtime::Handle,
    ) -> (Arc<dyn DfsPeerTransport>, Arc<dyn AsyncDfsPeerTransport>) {
        unreachable!()
    }
    fn schedule_metadata_publish(
        &self,
        _: &SinomoDataDir,
        _: &tokio::runtime::Handle,
        _: Option<&str>,
    ) {
        unreachable!()
    }
    fn notify_maintenance(&self) {}
    fn publish_namespace_change(&self, _: sinomo_core::Id, _: &str, _: Vec<String>) {
        unreachable!()
    }
    fn publish_placement_change(&self, _: sinomo_core::Id, _: &str, _: Vec<String>) {
        unreachable!()
    }
    fn inspect_placement_authority(
        &self,
        _: &tokio::runtime::Handle,
        _: &DfsFileSystemId,
    ) -> Result<Option<DfsPlacementAuthorityPermitStatus>, ServiceError> {
        unreachable!()
    }
    fn recover_removed_placement_authority(
        &self,
        _: &tokio::runtime::Handle,
        _: &DfsFileSystemId,
        _: &str,
        _: bool,
    ) -> Result<DfsPlacementAuthorityRecoveryOutcome, ServiceError> {
        unreachable!()
    }
    fn break_glass_recover_placement_authority(
        &self,
        _: &tokio::runtime::Handle,
        _: &DfsFileSystemId,
        _: &str,
        _: &str,
    ) -> Result<DfsPlacementAuthorityRecoveryOutcome, ServiceError> {
        unreachable!()
    }
}

fn projection_browser(root: &Path) -> (DfsBrowserBackend, ProjectionRecorder) {
    let opened = crate::open_mounted_sifs_service(Some(root.to_path_buf())).unwrap();
    let local = opened.service.node_id().clone();
    let others = [
        node("browser_projection_other_a"),
        node("browser_projection_other_b"),
    ];
    let control = configure_control(
        &opened.service,
        &[
            (local.clone(), "zone-a"),
            (others[0].clone(), "zone-b"),
            (others[1].clone(), "zone-c"),
        ],
    );
    configure_projection_policy(&control, &local, 1);
    control.with_client(|kv| {
        put_provider(kv, &local, true);
        for other in &others {
            put_provider(kv, other, true);
        }
    });
    let recorder = ProjectionRecorder {
        view: Arc::new(Mutex::new(projected_configuration(&control, &local))),
        inner: control,
        floor_available: Arc::new(AtomicBool::new(true)),
        configuration_pending: Arc::new(AtomicBool::new(false)),
        prefixes: Arc::new(Mutex::new(Vec::new())),
    };
    let service = opened.service.with_control_plane(recorder.clone());
    let backend = DfsBrowserBackend {
        data_dir: SinomoDataDir::new(root),
        host: Arc::new(ProjectionBrowserHost),
        node_id: local,
        service: Mutex::new(service),
        replica_control_plane: Mutex::new(Some(Box::new(recorder.clone()))),
        peer_targets_refreshed_at: Mutex::new(None),
        stat_cache: Mutex::new(HashMap::new()),
    };
    (backend, recorder)
}

fn assert_no_write_provider_prefix_reads(recorder: &ProjectionRecorder) {
    let prefixes = recorder.prefixes.lock().unwrap();
    assert!(
        prefixes.iter().all(|prefix| {
            !prefix.starts_with(crate::content_services_prefix())
                && !prefix.starts_with(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)
        }),
        "warm writes fetched content/provider configuration: {prefixes:?}"
    );
}

#[test]
fn write_configuration_browser_warm_options_and_committed_writes_keep_full_zero_minimum_zones() {
    let root = sinomo_testdir::TestDir::new("browser-native-write-configuration");
    let (backend, recorder) = projection_browser(root.path());
    let path = "/documents/native-write";
    let expected = {
        let candidates =
            crate::discover_mounted_sifs_content_replica_candidates_with_control_plane(
                &recorder.inner,
            )
            .unwrap();
        let mut service = backend.lock_service().unwrap();
        let expected = legacy_options(&mut service, path, &candidates);
        assert_eq!(expected.policy.min_distinct_zones, 0);
        assert_eq!(expected.policy.node_zones.len(), 3);
        recorder.prefixes.lock().unwrap().clear();
        for _ in 0..3 {
            let candidates =
                crate::discover_mounted_sifs_content_replica_candidates_with_control_plane(
                    &recorder,
                )
                .unwrap();
            assert_eq!(
                sifs_browser_write_options(&mut service, path, &candidates).unwrap(),
                expected
            );
        }
        expected
    };
    let mut versions = Vec::new();
    for bytes in [vec![0x11; 4096], vec![0x99; 4096]] {
        backend
            .write_file(DaemonDfsBrowserWriteFileRequest {
                path: path.into(),
                bytes: bytes.clone(),
            })
            .unwrap();
        let service = backend.lock_service().unwrap();
        let stat = service.posix_stat(path).unwrap();
        assert_eq!(service.posix_read_file(path).unwrap(), bytes);
        let metadata = service.metadata_backend().unwrap();
        let durable = metadata.get_object(&stat.object_id).unwrap().unwrap();
        assert_eq!(durable.version, stat.version);
        let intents = metadata
            .intents_for_object_version_page(&stat.object_id, &stat.version, None, 16)
            .unwrap();
        let committed = intents
            .intents
            .iter()
            .find(|intent| intent.status == DfsWriteIntentStatus::Committed)
            .expect("the visible version has its durable committed intent");
        assert_eq!(committed.policy, expected.policy);
        assert_eq!(committed.policy.node_zones.len(), 3);
        versions.push(stat.version);
    }
    assert_ne!(
        versions[0], versions[1],
        "overwrite publishes a new durable version"
    );
    assert_no_write_provider_prefix_reads(&recorder);
}

#[test]
fn write_configuration_browser_pending_preserves_payload_without_parent_intent_or_journal_effects()
{
    let root = sinomo_testdir::TestDir::new("browser-pending-configuration-handoff");
    let (backend, recorder) = projection_browser(root.path());
    let parent = "/documents/configuration-handoff/deeper";
    let path = format!("{parent}/file");
    let bytes = vec![0x5a; 4096];
    let mut request = DaemonDfsBrowserWriteFileRequest {
        path: path.clone(),
        bytes: bytes.clone(),
    };
    let allocation = request.bytes.as_ptr();
    let journal_before = backend
        .lock_service()
        .unwrap()
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    recorder
        .configuration_pending
        .store(true, Ordering::Release);
    for _ in 0..2 {
        request = match backend.try_write_file(request).unwrap() {
            DfsBrowserWriteAttempt::ConfigurationPending(original) => original,
            DfsBrowserWriteAttempt::Complete(_) => panic!("pending configuration admitted a write"),
        };
        assert_eq!(request.path, path);
        assert_eq!(request.bytes, bytes);
        assert_eq!(request.bytes.as_ptr(), allocation);
        let service = backend.lock_service().unwrap();
        let metadata = service.metadata_backend().unwrap();
        assert_eq!(metadata.journal_last_sequence().unwrap(), journal_before);
        for absent in ["/documents/configuration-handoff", parent, path.as_str()] {
            assert!(
                metadata
                    .get_object_by_path(&sifs_file_system_id(), absent)
                    .unwrap()
                    .is_none(),
                "pending configuration created {absent}"
            );
        }
    }
    recorder
        .configuration_pending
        .store(false, Ordering::Release);
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
        1,
        "the unchanged original request commits once after readiness returns"
    );
    assert_no_write_provider_prefix_reads(&recorder);
}

#[test]
fn write_configuration_browser_unavailable_native_floor_fails_before_parent_or_file_mutation() {
    let root = sinomo_testdir::TestDir::new("browser-unavailable-native-configuration");
    let (backend, recorder) = projection_browser(root.path());
    let parent = "/services/native-floor-absent/deeper";
    let path = format!("{parent}/file");
    recorder.floor_available.store(false, Ordering::Release);
    let error = backend
        .write_file(DaemonDfsBrowserWriteFileRequest {
            path: path.clone(),
            bytes: b"must not publish".to_vec(),
        })
        .expect_err("unknown native floor rejects before mutation");
    assert!(
        error
            .to_string()
            .contains("native write configuration floor is unavailable")
    );
    let service = backend.lock_service().unwrap();
    let metadata = service.metadata_backend().unwrap();
    for absent in ["/services/native-floor-absent", parent, path.as_str()] {
        assert!(
            metadata
                .get_object_by_path(&sifs_file_system_id(), absent)
                .unwrap()
                .is_none(),
            "rejected preflight created {absent}"
        );
    }
    assert_no_write_provider_prefix_reads(&recorder);
}

#[test]
fn write_configuration_browser_coherent_config_provider_and_zone_updates_match_legacy_options() {
    let root = sinomo_testdir::TestDir::new("browser-native-configuration-change");
    let (backend, recorder) = projection_browser(root.path());
    let path = "/documents/coherent-options";
    let local = backend.node_id.clone();
    let old = Arc::clone(&recorder.view.lock().unwrap());
    let old_candidates = old.content_replica_candidates().unwrap();
    let old_options = {
        let mut service = backend.lock_service().unwrap();
        sifs_browser_write_options_with_configuration(
            &mut service,
            path,
            &old_candidates,
            Some(&old),
        )
        .unwrap()
    };
    let omitted = node("browser_projection_other_b");
    recorder.inner.with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv);
        let mut config = store
            .get_file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap();
        let expected = config.clone();
        let mut policy =
            DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 2)
                .unwrap();
        policy.required_node_ids = vec![local.clone()];
        policy.min_distinct_zones = 2;
        crate::upsert_placement_rule(
            &mut config.subtree_policies,
            crate::normalize_placement_rule("/documents", DfsPlacementScope::Subtree, policy)
                .unwrap(),
        );
        assert!(
            store
                .compare_and_put_file_system_config(Some(&expected), &config)
                .unwrap()
        );
        let mut advertisement = DfsNodeAdvertisement::content(node("browser_projection_other_a"));
        advertisement.zone = Some("updated-zone".into());
        store.advertise_content_service(&advertisement).unwrap();
        put_provider(kv, &omitted, false);
    });
    let refreshed = projected_configuration(&recorder.inner, &local);
    assert!(refreshed.revision() > old.revision());
    let expected_candidates =
        crate::discover_mounted_sifs_content_replica_candidates_with_control_plane(&recorder.inner)
            .unwrap();
    let expected = {
        let mut service = backend.lock_service().unwrap();
        legacy_options(&mut service, path, &expected_candidates)
    };
    // Publication replaces all three scopes together; a retained old view stays
    // coherent while current KV configuration is being changed beneath it.
    {
        let mut service = backend.lock_service().unwrap();
        assert_eq!(
            sifs_browser_write_options_with_configuration(
                &mut service,
                path,
                &old_candidates,
                Some(&old)
            )
            .unwrap(),
            old_options
        );
    }
    *recorder.view.lock().unwrap() = refreshed;
    recorder.prefixes.lock().unwrap().clear();
    let candidates =
        crate::discover_mounted_sifs_content_replica_candidates_with_control_plane(&recorder)
            .unwrap();
    assert_eq!(candidates, expected_candidates);
    assert!(!candidates.contains(&omitted));
    let mut service = backend.lock_service().unwrap();
    let actual = sifs_browser_write_options(&mut service, path, &candidates).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.policy.min_distinct_zones, 2);
    assert_eq!(
        actual.policy.node_zones[&node("browser_projection_other_a")],
        "updated-zone"
    );
    assert_ne!(actual.policy, old_options.policy);
    assert_no_write_provider_prefix_reads(&recorder);
}
