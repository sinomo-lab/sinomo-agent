use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, StatReplyFault};
use sinomo_core::{DfsObjectPolicy, node::NodeId};
use std::sync::{Arc, Mutex};

struct SplitAuthorities {
    runtime: DfsTestRuntime,
    caller: NetworkedDfsService<DfsNodeTransport>,
    root: DfsMetadataShardConfig,
    services: DfsMetadataShardConfig,
}

impl SplitAuthorities {
    fn new(name: &str) -> Self {
        let (runtime, initial, root) =
            crate::test_support::current_native_metadata_service(name, 3, "/").unwrap();
        drop(initial);
        let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
        let peers = nodes
            .iter()
            .filter(|node| **node != root.primary_node_id)
            .cloned()
            .collect::<Vec<_>>();
        let replicas = |primary: &NodeId| {
            nodes
                .iter()
                .filter(|node| *node != primary)
                .cloned()
                .collect()
        };
        let services = runtime
            .reassign_metadata_shard(
                &root.file_system_id,
                "services",
                peers[0].clone(),
                replicas(&peers[0]),
            )
            .unwrap();
        let users = runtime
            .reassign_metadata_shard(
                &root.file_system_id,
                "users",
                peers[1].clone(),
                replicas(&peers[1]),
            )
            .unwrap();
        assert_ne!(root.primary_node_id, services.primary_node_id);
        assert_ne!(root.primary_node_id, users.primary_node_id);
        assert_ne!(services.primary_node_id, users.primary_node_id);
        let caller = runtime.client(&users.primary_node_id).unwrap();
        assert!(caller.transport.delegates_local_namespace_authority());
        Self {
            runtime,
            caller,
            root,
            services,
        }
    }

    fn deny_services_stat(&self) -> Arc<Mutex<Vec<DfsRoutedMessage>>> {
        let captured = Arc::new(Mutex::new(Vec::new()));
        self.runtime.install_fault(FaultRule {
            source: Some(self.caller.node_id.clone()),
            target: Some(self.services.primary_node_id.clone()),
            route: Some(DFS_DATA_NAMESPACE_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 1,
            action: FaultAction::AlterStatReplyAfterDispatch {
                change: StatReplyFault::Unsupported,
                captured: Arc::clone(&captured),
            },
        });
        captured
    }

    fn close(self) {
        self.runtime.clear_faults();
        drop(self.caller);
        self.runtime.close().unwrap();
    }
}

fn captured_projection(captured: &Arc<Mutex<Vec<DfsRoutedMessage>>>) -> DfsStatProjectionPage {
    let records = captured.lock().unwrap();
    assert_eq!(
        records.len(),
        1,
        "one actual stat reply must exercise the denial"
    );
    let DfsNetworkMessage::NamespaceResponse(reply) = &records[0].message else {
        panic!("the native negative must capture a namespace response");
    };
    let Some(DfsPosixCoordinationResult::MetadataQuery(query)) = &reply.posix_coordination_result
    else {
        panic!("the native negative must capture an authority query response");
    };
    let DfsMetadataQueryResult::StatProjectionV1 { page } = &query.result else {
        panic!("the denial must exercise the original stat projection");
    };
    page.as_ref().clone()
}

#[test]
fn directory_stat_remote_private_mkdir_ignores_unrelated_services_projection() {
    let mut cluster = SplitAuthorities::new("stat-private-mkdir-split-authorities");
    let parent = "/sinomo/agents/stat-private-mkdir";
    let path = "/sinomo/agents/stat-private-mkdir/home";
    cluster.caller.posix_mkdir_all(parent).unwrap();
    let heads = cluster
        .runtime
        .capture_namespace_requests(&cluster.root.primary_node_id, 32)
        .unwrap();
    let captured = cluster.deny_services_stat();
    let attrs = DfsPosixCreateAttrs::new(0o700, 1001, 4242);
    let stat = cluster
        .caller
        .posix_mkdir_with_create_attrs(path, attrs)
        .expect("the actual remote mkdir and its required directory stat must succeed");
    assert_eq!(stat.kind, DfsObjectKind::Directory);
    assert_eq!((stat.mode, stat.uid, stat.gid), (0o700, 1001, 4242));
    assert_eq!(stat.link_count, 2);
    assert!(captured.lock().unwrap().is_empty());
    let commands = heads.records();
    assert_eq!(
        commands
            .iter()
            .filter(|head| head.source_node_id == cluster.caller.node_id
                && matches!(&head.message,
                    DfsNetworkMessage::NamespaceRequest(request)
                        if matches!(&request.operation,
                            DfsNamespaceOperation::MkdirWithAttrs { path: actual, attrs: actual_attrs }
                                if actual == path && *actual_attrs == attrs)))
            .count(),
        1,
        "one original attributed mkdir must reach the actual remote Root owner"
    );
    drop(heads);
    {
        let owner = cluster.runtime.metadata_primary_service(path).unwrap();
        let object = owner
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&sifs_file_system_id(), path)
            .unwrap()
            .unwrap();
        assert_eq!(object.object_id, stat.object_id);
        assert_eq!(object.version, stat.version);
    }

    // Exercise the held denial through the same real Services projection.
    // An empty capture above therefore cannot stand in for a working fault.
    let request = DfsStatProjectionRequest {
        object_id: stat.object_id.clone(),
        object_path: stat.path.clone(),
        object_kind: stat.kind.clone(),
        object_version: stat.version.clone(),
        limit: crate::file_history_query::HISTORY_PAGE_LIMIT,
        after: None,
        expected_prefix: None,
        fence_only: false,
    };
    let catalog = cluster.runtime.metadata_shards().unwrap();
    assert!(
        cluster
            .caller
            .stat_projection_page(&cluster.services, &request, &catalog)
            .is_err()
    );
    assert_eq!(captured_projection(&captured).request, request);
    cluster.close();
}

#[test]
fn directory_stat_root_counts_immediate_children_across_prefix_owners() {
    let mut cluster = SplitAuthorities::new("stat-root-directory-split-authorities");
    for path in [
        "/stat-root-child",
        "/services/stat-inner",
        "/users/stat-inner",
    ] {
        cluster.caller.posix_mkdir_all(path).unwrap();
    }
    let entries = cluster.caller.posix_list_dir_distributed("/").unwrap();
    for boundary in ["/services", "/users"] {
        assert!(
            entries
                .iter()
                .any(|entry| entry.path == boundary && entry.kind == DfsObjectKind::Directory)
        );
    }
    let expected = 2 + entries
        .iter()
        .filter(|entry| entry.kind == DfsObjectKind::Directory)
        .count() as u64;
    assert_eq!(
        cluster
            .caller
            .posix_stat_distributed("/")
            .unwrap()
            .link_count,
        expected
    );
    let captured = cluster.deny_services_stat();
    assert!(cluster.caller.posix_stat_distributed("/").is_err());
    assert!(
        captured_projection(&captured)
            .entries
            .iter()
            .any(|entry| { entry.path == "/services" && entry.kind == DfsObjectKind::Directory })
    );
    cluster.runtime.clear_faults();
    assert_eq!(
        cluster
            .caller
            .posix_stat_distributed("/")
            .unwrap()
            .link_count,
        expected
    );
    cluster.close();
}

#[test]
fn existing_shard_boundary_mkdir_all_is_an_authoritative_noop() {
    let mut cluster = SplitAuthorities::new("mkdir-existing-boundary-authority");
    let boundary = "/services";
    let before = cluster
        .caller
        .posix_namespace_lstat_distributed(boundary)
        .unwrap();
    assert_eq!(before.kind, DfsObjectKind::Directory);
    let root_before = cluster
        .caller
        .posix_namespace_lstat_distributed("/")
        .unwrap();
    let services = cluster
        .runtime
        .capture_namespace_requests(&cluster.services.primary_node_id, 32)
        .unwrap();
    let root = cluster
        .runtime
        .capture_namespace_requests(&cluster.root.primary_node_id, 32)
        .unwrap();
    cluster.caller.posix_mkdir_all(boundary).unwrap();
    cluster
        .caller
        .posix_mkdir_all_without_local_replay(boundary)
        .unwrap();
    let calls = services.records();
    assert_eq!(
        calls
            .iter()
            .filter(|head| {
                head.source_node_id == cluster.caller.node_id
                    && matches!(&head.message,
                        DfsNetworkMessage::NamespaceRequest(request)
                            if matches!(&request.operation,
                                DfsNamespaceOperation::LookupPath { path, .. } if path == boundary)
                            && request.authority_shards == vec![cluster.services.clone()])
            })
            .count(),
        2,
        "both public entries must read the actual exact boundary owner"
    );
    assert!(calls.iter().chain(&root.records()).all(|head| {
        !matches!(&head.message, DfsNetworkMessage::NamespaceRequest(request)
        if head.source_node_id == cluster.caller.node_id
            && matches!(&request.operation,
                DfsNamespaceOperation::MkdirAll { .. }
                    | DfsNamespaceOperation::MetadataQuery {
                        query: DfsMetadataShardQuery {
                            selection: DfsMetadataSelection::StatProjectionV1 { .. }, ..
                        }
                    }))
    }));
    drop((services, root));
    let after = cluster
        .caller
        .posix_namespace_lstat_distributed(boundary)
        .unwrap();
    let root_after = cluster
        .caller
        .posix_namespace_lstat_distributed("/")
        .unwrap();
    assert_eq!(
        (
            &before.object_id,
            &before.version,
            before.mode,
            before.uid,
            before.gid
        ),
        (
            &after.object_id,
            &after.version,
            after.mode,
            after.uid,
            after.gid
        )
    );
    assert_eq!(
        (root_before.object_id, root_before.version),
        (root_after.object_id, root_after.version)
    );

    let original_deadline = cluster.caller.transport.operation_deadline();
    cluster.caller.set_transport_operation_deadline(Some(
        std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    ));
    let denied_operations = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&denied_operations);
    let caller = cluster.caller.node_id.clone();
    let denial =
        crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
            cluster.runtime.data_dir(&cluster.services.primary_node_id),
            Arc::new(
                move |point: crate::runtime_host::DaemonDfsPreDispatchTestPoint<'_>,
                      command: Option<&DfsMessageCommand>,
                      _preflight: Option<
                    &crate::runtime_host::DaemonDfsMetadataAuthorityPreflight,
                >| {
                    if !matches!(
                        point,
                        crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeExecute { .. }
                    ) {
                        return Ok(());
                    }
                    let Some(command) =
                        command.filter(|command| command.routed_message.source_node_id == caller)
                    else {
                        return Ok(());
                    };
                    let DfsNetworkMessage::NamespaceRequest(request) =
                        &command.routed_message.message
                    else {
                        return Ok(());
                    };
                    let mut actual = observed.lock().unwrap();
                    assert!(actual.len() < 8, "bounded actual namespace denial capture");
                    actual.push(request.operation.clone());
                    if matches!(&request.operation,
                    DfsNamespaceOperation::LookupPath { path, .. } if path == boundary)
                    {
                        return Err(DfsError::Metadata {
                            message: "native boundary current-owner read unavailable".into(),
                        });
                    }
                    Ok(())
                },
            ),
        );
    let denied = cluster.caller.posix_mkdir_all(boundary);
    drop(denial);
    cluster
        .caller
        .set_transport_operation_deadline(original_deadline);
    assert!(
        denied.is_err(),
        "unknown current-owner visibility must not become cached no-op success"
    );
    let actual = denied_operations.lock().unwrap();
    assert!(actual.iter().any(|operation| {
        matches!(operation, DfsNamespaceOperation::LookupPath { path, .. } if path == boundary)
    }));
    assert!(
        actual
            .iter()
            .all(|operation| { !matches!(operation, DfsNamespaceOperation::MkdirAll { .. }) })
    );
    drop(actual);
    cluster.caller.posix_mkdir_all(boundary).unwrap();
    cluster.close();
}

#[test]
fn directory_stat_scope_keeps_file_alias_projection_global() {
    let mut cluster = SplitAuthorities::new("stat-file-alias-global-authorities");
    let path = "/stat-global-target";
    // A same-shard alias proves that file stat still needs authoritative
    // absence from every other shard; this does not exercise alias adoption.
    let alias = "/stat-global-alias";
    cluster
        .caller
        .posix_write_file(
            path,
            b"file aliases require every possible authority",
            DfsWriteOptions::replicated_eventual(DfsTestRuntime::ordered_node_ids(
                "stat-file-alias-global-authorities",
                3,
            )),
        )
        .unwrap();
    cluster.caller.posix_hardlink(path, alias).unwrap();
    let original = cluster.caller.posix_stat_distributed(path).unwrap();
    assert_eq!(original.link_count, 2);
    {
        let owner = cluster.runtime.metadata_primary_service(alias).unwrap();
        let row = owner
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&sifs_file_system_id(), alias)
            .unwrap()
            .unwrap();
        assert_eq!(row.kind, DfsObjectKind::Hardlink);
        assert_eq!(
            row.link_target_object_id.as_ref(),
            Some(&original.object_id)
        );
    }
    let captured = cluster.deny_services_stat();
    assert!(
        cluster.caller.posix_stat_distributed(path).is_err(),
        "an unavailable alias authority cannot produce a successful undercount"
    );
    let page = captured_projection(&captured);
    assert_eq!(page.request.object_id, original.object_id);
    assert_eq!(page.request.object_kind, DfsObjectKind::File);
    assert!(
        page.entries.is_empty(),
        "the actual Services projection supplies absence, never a copied alias"
    );
    cluster.runtime.clear_faults();
    assert_eq!(
        cluster
            .caller
            .posix_stat_distributed(path)
            .unwrap()
            .link_count,
        2
    );
    let alias_stat = cluster.caller.posix_lstat_distributed(alias).unwrap();
    assert_eq!(alias_stat.object_id, original.object_id);
    assert_eq!(alias_stat.link_count, 2);
    cluster.close();
}

#[test]
fn directory_stat_selector_preserves_nested_prefix_and_hash_fallback_scopes() {
    // Pure catalog inputs test selection only; no DATA or authority is authored.
    let node = DfsTestRuntime::ordered_node_ids("stat-directory-scope-catalog", 1)[0].clone();
    let config = |id: &str, assignment| DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: id.into(),
        primary_node_id: node.clone(),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    let prefix = |path: &str| DfsMetadataShardAssignment::PathPrefix {
        prefix: path.into(),
    };
    let mut catalog = vec![
        config("root", DfsMetadataShardAssignment::Root),
        config("services", prefix("/services")),
        config("users", prefix("/users")),
        config("nested", prefix("/services/nested")),
        config("deep", prefix("/services/nested/deep")),
        config("elsewhere", prefix("/elsewhere/deeper")),
    ];
    let selected = |path: &str, catalog: &[DfsMetadataShardConfig]| {
        super::super::routing::directory_child_authority_shards(path, catalog)
            .unwrap()
            .iter()
            .map(|shard| shard.shard_id.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(selected("/", &catalog), ["root", "services", "users"]);
    assert_eq!(selected("/services", &catalog), ["services", "nested"]);
    assert_eq!(selected("/services/nested", &catalog), ["nested", "deep"]);
    assert_eq!(selected("/elsewhere", &catalog), ["root", "elsewhere"]);
    assert_eq!(selected("/services/nested/file", &catalog), ["nested"]);
    catalog.push(config(
        "hash",
        DfsMetadataShardAssignment::NameHashRange {
            start_inclusive: 0,
            end_exclusive: u64::MAX,
        },
    ));
    let owner = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
        &sifs_file_system_id(),
        &catalog,
        "/hashed-directory",
    )
    .unwrap()
    .unwrap();
    assert_eq!(owner.shard_id, "hash");
    assert_eq!(selected("/hashed-directory", &catalog), ["root", "hash"]);
    assert_eq!(
        selected("/services", &catalog),
        ["services", "nested", "hash"]
    );
}

mod prefix_concurrency;
