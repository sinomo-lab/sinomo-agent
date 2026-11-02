use super::write_options_tests::{native_browser_policy, native_browser_runtime};
use super::*;
use crate::test_support::{DfsTestRpcOrigin, DfsTestRpcPhase};

const ROOT: &str = "/remote-parent";
const PARENT: &str = "/remote-parent/fresh";
const CHILD: &str = "/remote-parent/fresh/note.md";
const BYTES: &[u8] = b"one exclusive create beneath the actual remote parent";

fn observed_stat(stat: &crate::DfsStat) -> crate::DaemonDfsBrowserStatResponse {
    daemon_dfs_browser_stat_response(stat.clone(), Vec::new(), None, None, None)
}

struct RemoteParent {
    runtime: crate::test_support::DfsTestRuntime,
    nodes: Vec<sinomo_core::node::NodeId>,
    caller: sinomo_core::node::NodeId,
    primary: sinomo_core::node::NodeId,
    backend: Arc<DfsBrowserBackend>,
}

impl RemoteParent {
    fn new(name: &str) -> Self {
        let (runtime, nodes) = native_browser_runtime(name, 3);
        let original = runtime
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|shard| shard.shard_id == "root")
            .unwrap();
        let caller = original.primary_node_id;
        let primary = nodes.iter().find(|node| **node != caller).unwrap().clone();
        native_browser_policy(
            &runtime.shared_control_plane().unwrap(),
            ROOT,
            DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 2, 3)
                .unwrap(),
        );
        let data_dir = Arc::new(runtime.data_dir(&caller).clone());
        let handle = runtime.runtime_handle();
        let backend =
            DfsBrowserBackend::for_component_data_dir(Arc::clone(&data_dir), &handle).unwrap();
        let host = dfs_browser_component_host(&data_dir).unwrap();
        handle.block_on(async {
            tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                host.ensure_write_configuration_ready(&data_dir, &handle),
            )
            .await
            .unwrap()
            .unwrap();
        });
        assert_eq!(
            backend
                .no_links(Self::request(
                    ROOT,
                    crate::DaemonDfsBrowserNoLinksOperation::CreateDirAll
                ))
                .unwrap(),
            crate::DaemonDfsBrowserNoLinksResponse::DirectoryCreated
        );
        let moved = runtime
            .reassign_metadata_shard(
                &sifs_file_system_id(),
                "root",
                primary.clone(),
                nodes
                    .iter()
                    .filter(|node| **node != primary)
                    .cloned()
                    .collect(),
            )
            .unwrap();
        assert_eq!(moved.primary_node_id, primary);
        assert!(moved.epoch > original.epoch);
        handle.block_on(async {
            tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                host.ensure_write_configuration_ready(&data_dir, &handle),
            )
            .await
            .unwrap()
            .unwrap();
        });
        // The same old-primary browser stays open. Only the genuine new
        // metadata owner creates and attributes this previously absent parent.
        let mut producer = runtime.metadata_primary_service(PARENT).unwrap();
        assert_eq!(producer.node_id(), &primary);
        producer
            .posix_mkdir_with_create_attrs(
                PARENT,
                crate::DfsPosixCreateAttrs::new(0o2770, 1001, 4242),
            )
            .unwrap();
        drop(producer);
        assert!(
            backend
                .lock_service()
                .unwrap()
                .lookup_sifs_path(PARENT)
                .unwrap()
                .is_none(),
            "the real caller projection must not materialize the fresh remote directory"
        );
        drop(host);
        Self {
            runtime,
            nodes,
            caller,
            primary,
            backend,
        }
    }

    fn request(
        path: &str,
        operation: crate::DaemonDfsBrowserNoLinksOperation,
    ) -> crate::DaemonDfsBrowserNoLinksRequest {
        crate::DaemonDfsBrowserNoLinksRequest {
            expected_cluster_id: None,
            root: ROOT.into(),
            path: path.into(),
            operation,
        }
    }

    fn opened_parent(&self) -> crate::DfsStat {
        let mut service = self.backend.lock_service().unwrap();
        let parent = service
            .posix_open_beneath_no_links_distributed(ROOT, PARENT)
            .unwrap();
        assert_eq!(parent.kind, crate::DfsObjectKind::Directory);
        assert_eq!(parent.gid, 4242);
        assert_eq!(parent.mode, 0o2770);
        assert!(
            service.lookup_sifs_path(PARENT).unwrap().is_none(),
            "authoritative no-link observation must not become a local metadata row"
        );
        parent
    }

    fn create_at_opened(
        &self,
        opened: &crate::DfsStat,
    ) -> crate::posix_model::Result<crate::DfsStat> {
        let attempt = self
            .backend
            .try_with_configured_mutation_service(&[CHILD], (), |service, configuration, ()| {
                let mut options = self.backend.write_options_for_path_with_configuration(
                    service,
                    CHILD,
                    configuration,
                )?;
                assert_eq!(
                    (
                        options.policy.min_commit_replicas,
                        options.policy.desired_replicas
                    ),
                    (2, 3)
                );
                options.foreground_replica_repair = true;
                Ok(service.posix_create_file_at_opened_directory_no_links(
                    opened, "note.md", BYTES, options,
                ))
            })
            .unwrap();
        match attempt {
            DfsBrowserMutationAttempt::Complete(result) => result,
            DfsBrowserMutationAttempt::ConfigurationPending(_) => {
                panic!("normal owner readiness completed")
            }
        }
    }

    fn write_calls(&self, after: usize) -> Vec<crate::test_runtime::RpcObservation> {
        self.runtime
            .rpc_observations()
            .into_iter()
            .skip(after)
            .filter(|call| {
                call.source == self.caller
                    && call.target == self.primary
                    && call.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                    && call.phase == DfsTestRpcPhase::BeforeDispatch
                    && call.origin == DfsTestRpcOrigin::RegisteredHost
            })
            .collect()
    }

    fn assert_absent(&self) {
        let mut owner = self.runtime.metadata_primary_service(CHILD).unwrap();
        assert!(matches!(
            owner.posix_stat_distributed(CHILD),
            Err(crate::DfsPosixError::NotFound { .. })
        ));
    }

    fn export(&self, outcome: serde_json::Value) {
        let root = self.runtime.data_dir(&self.caller).root();
        std::fs::write(
            root.join("remote-parent-result.json"),
            serde_json::to_vec_pretty(&outcome).unwrap(),
        )
        .unwrap();
        std::fs::write(
            root.join("remote-parent-native-rpc.txt"),
            format!("{:#?}", self.runtime.rpc_observations()),
        )
        .unwrap();
    }

    fn close(self) {
        drop(self.backend);
        self.runtime.close().unwrap();
    }
}

#[test]
fn remote_parent_after_handoff_creates_without_local_mirror_and_inherits_attributes() {
    let case = RemoteParent::new("native-remote-parent-after-handoff");
    let parent = case.opened_parent();
    let before = case.runtime.rpc_observations().len();
    let started = Instant::now();
    let response = case
        .backend
        .no_links(RemoteParent::request(
            CHILD,
            crate::DaemonDfsBrowserNoLinksOperation::CreateNew {
                bytes: BYTES.to_vec(),
            },
        ))
        .unwrap();
    let crate::DaemonDfsBrowserNoLinksResponse::Metadata { metadata } = response else {
        panic!("actual native exclusive create: {response:?}");
    };
    assert!(started.elapsed() < sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
    assert_eq!(metadata.gid, parent.gid);
    assert_eq!(
        metadata.mode,
        crate::DfsPosixCreateAttrs::file_default().mode
    );
    let writes = case.write_calls(before);
    case.export(serde_json::json!({"parent":observed_stat(&parent),"created":metadata,
        "elapsed_us":started.elapsed().as_micros(),"write_dispatches":writes.len(),
        "dispatch_identity_hints":writes.iter().map(|call| (&call.dfs_request_id, &call.dfs_object_id)).collect::<Vec<_>>(),
        "qualification":"streamed BeforeDispatch observations may omit decoded scalar identities"}));
    assert_eq!(writes.len(), 1, "one native semantic create, no replay");
    if let Some(object_id) = writes[0].dfs_object_id.as_ref() {
        assert_eq!(object_id.as_str(), metadata.object_id);
    }
    let mut owner = case.runtime.metadata_primary_service(CHILD).unwrap();
    assert_eq!(owner.posix_read_file_distributed(CHILD).unwrap(), BYTES);
    let object_id = crate::DfsObjectId::new(&metadata.object_id).unwrap();
    let intents = owner
        .metadata_backend()
        .unwrap()
        .intents_for_object(&object_id)
        .unwrap();
    let exact = intents
        .iter()
        .filter(|intent| intent.target_version.as_str() == metadata.version)
        .collect::<Vec<_>>();
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].status, crate::DfsWriteIntentStatus::Committed);
    assert_eq!(exact[0].object_id, object_id);
    assert_eq!(exact[0].coordinator_node_id, case.caller);
    let expected_hash = crate::content_store::content_hash(BYTES);
    assert_eq!(
        metadata.content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(metadata.size_bytes, Some(BYTES.len() as u64));
    assert_eq!(
        exact[0].content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert!(
        exact[0]
            .operation_id
            .as_ref()
            .is_some_and(|id| !id.is_empty())
    );
    assert_eq!(
        (
            exact[0].policy.min_commit_replicas,
            exact[0].policy.desired_replicas
        ),
        (2, 3)
    );
    if let Some(request_id) = writes[0].dfs_request_id.as_deref() {
        assert_eq!(exact[0].operation_id.as_deref(), Some(request_id));
    }
    let stat = owner.posix_stat_distributed(CHILD).unwrap();
    assert_eq!((stat.gid, stat.mode), (parent.gid, metadata.mode));
    case.export(
        serde_json::json!({"parent":observed_stat(&parent),"created":metadata,
        "stored_intent":exact[0],"elapsed_us":started.elapsed().as_micros(),
        "caller":case.caller,"new_primary":case.primary,"local_parent_absent_before":true}),
    );
    drop(owner);
    case.close();
}

#[test]
fn remote_parent_recreated_after_open_rejects_original_identity_before_write() {
    let case = RemoteParent::new("native-remote-parent-changed-identity");
    let opened = case.opened_parent();
    let mut producer = case.runtime.metadata_primary_service(PARENT).unwrap();
    producer
        .posix_rename(PARENT, "/remote-parent/retired")
        .unwrap();
    let replacement = producer.posix_mkdir(PARENT).unwrap();
    assert_ne!(replacement.object_id, opened.object_id);
    drop(producer);
    let before = case.runtime.rpc_observations().len();
    let result = case.create_at_opened(&opened);
    assert!(
        matches!(result, Err(crate::DfsPosixError::Conflict { .. })),
        "{result:?}"
    );
    assert!(case.write_calls(before).is_empty());
    case.assert_absent();
    case.export(
        serde_json::json!({"original_parent":observed_stat(&opened),"replacement":observed_stat(&replacement),
        "result":format!("{result:?}"),"write_dispatches":0}),
    );
    case.close();
}

#[test]
fn remote_parent_changed_version_after_open_rejects_before_write() {
    let case = RemoteParent::new("native-remote-parent-changed-version");
    let opened = case.opened_parent();
    let mut producer = case.runtime.metadata_primary_service(PARENT).unwrap();
    let changed = producer.posix_set_mode(PARENT, 0o2750).unwrap();
    assert_eq!(changed.object_id, opened.object_id);
    assert_ne!(changed.version, opened.version);
    drop(producer);
    let before = case.runtime.rpc_observations().len();
    let result = case.create_at_opened(&opened);
    assert!(
        matches!(result, Err(crate::DfsPosixError::Conflict { .. })),
        "{result:?}"
    );
    assert!(case.write_calls(before).is_empty());
    case.assert_absent();
    case.export(
        serde_json::json!({"original_parent":observed_stat(&opened),"changed":observed_stat(&changed),
        "result":format!("{result:?}"),"write_dispatches":0}),
    );
    case.close();
}

#[test]
fn remote_parent_link_substitution_after_open_rejects_before_write() {
    let case = RemoteParent::new("native-remote-parent-link-substitution");
    let opened = case.opened_parent();
    let mut producer = case.runtime.metadata_primary_service(PARENT).unwrap();
    producer
        .posix_rename(PARENT, "/remote-parent/retired")
        .unwrap();
    producer.posix_mkdir("/remote-parent/outside").unwrap();
    let policy = producer
        .managed_write_replication_policy_for_path(
            PARENT,
            sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            3,
        )
        .unwrap();
    producer
        .posix_symlink(
            "/remote-parent/outside",
            PARENT,
            crate::DfsWriteOptions {
                policy,
                replica_candidates: case.nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    drop(producer);
    let before = case.runtime.rpc_observations().len();
    let result = case.create_at_opened(&opened);
    assert!(
        matches!(result, Err(crate::DfsPosixError::InvalidPath { .. })),
        "{result:?}"
    );
    assert!(case.write_calls(before).is_empty());
    let mut owner = case
        .runtime
        .metadata_primary_service("/remote-parent/outside")
        .unwrap();
    assert!(matches!(
        owner.posix_stat_distributed("/remote-parent/outside/note.md"),
        Err(crate::DfsPosixError::NotFound { .. })
    ));
    drop(owner);
    case.export(serde_json::json!({"original_parent":observed_stat(&opened),
        "result":format!("{result:?}"),"write_dispatches":0,"outside_child_absent":true}));
    case.close();
}
