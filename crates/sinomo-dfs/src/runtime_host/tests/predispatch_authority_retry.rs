//! Real daemon lane/admission regressions for typed pre-execution retry.

use super::super::message_host::{
    DaemonDfsPreDispatchTestHook, DaemonDfsPreDispatchTestPoint,
    install_daemon_dfs_pre_dispatch_test_hook,
};
use super::*;

struct QuietAuthorityHost {
    delegate: Arc<dyn sinomo_dfs::DfsMetadataAuthorityHost>,
    write_configuration: bool,
}

impl sinomo_dfs::DfsMetadataAuthorityHost for QuietAuthorityHost {
    fn read_authoritative_prefix(&self) -> sinomo_dfs::DfsMetadataAuthorityReadFuture<'_> {
        self.delegate.read_authoritative_prefix()
    }
    fn supports_write_configuration_projection(&self) -> bool {
        self.write_configuration && self.delegate.supports_write_configuration_projection()
    }
    fn read_authoritative_configuration_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> sinomo_dfs::DfsMetadataAuthorityReadFuture<'a> {
        self.delegate
            .read_authoritative_configuration_prefix(prefix)
    }
    fn write_configuration_floor(
        &self,
    ) -> Result<sinomo_core::kv::KvLocalRead<Option<KvEntry>>, sinomo_dfs::DfsError> {
        self.delegate.write_configuration_floor()
    }
    fn spawn(&self, task: sinomo_dfs::DfsMetadataAuthorityTask) {
        self.delegate.spawn(task);
    }
    fn authority_changed(&self) {
        // These tests own the exact authority edge. Keep unrelated discovery
        // workers out of the admission-release assertion; reads stay real.
    }
}

struct RetryFixture {
    // Drop the runtime before its temporary files/data-owner guard.
    runtime: tokio::runtime::Runtime,
    data_dir: SinomoDataDir,
    paths: DaemonPaths,
    cluster_paths: ClusterPaths,
    local: NodeId,
    remote: NodeId,
    shard: DfsMetadataShardConfig,
    quiet_host: Arc<dyn sinomo_dfs::DfsMetadataAuthorityHost>,
    _authority: Arc<DaemonDfsMetadataAuthorityGeneration>,
    scheduler: Arc<DaemonDfsMutationScheduler>,
    journal_sequence: u64,
    _owner: Arc<DaemonDataOwnerLock>,
    _root: sinomo_testdir::TestDir,
}

impl RetryFixture {
    fn new(name: &str) -> Self {
        Self::with_write_configuration(name, false)
    }

    fn with_write_configuration(name: &str, write_configuration: bool) -> Self {
        let root = temp_root(name);
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let cluster_paths = ClusterPaths::new(&data_dir);
        let owner = Arc::new(DaemonDataOwnerLock::acquire(&paths).unwrap());
        register_daemon_dfs_data_owner_lock(&paths, &owner).unwrap();
        let local = NodeId::from_legacy_named(format!("{name}-local")).unwrap();
        let remote = NodeId::from_legacy_named(format!("{name}-remote")).unwrap();
        write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
        let mut cluster =
            sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).unwrap();
        let member = cluster
            .members
            .iter_mut()
            .find(|member| member.node_id == local)
            .unwrap();
        member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster).unwrap();
        if write_configuration {
            // Preserve the exact canonical voter/learner/non-voting partition
            // used by native ReadIndex, including the remote NonVoting member.
            sinomo_core::cluster::sync_cluster_state_to_local_raft_unchecked(
                &cluster_paths,
                &cluster,
            )
            .unwrap();
        } else {
            sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(
                &cluster_paths,
                &cluster,
            )
            .unwrap();
        }
        let fs = distributed_sifs(local.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: fs.file_system_id.clone(),
            shard_id: "root".into(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 9,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let configuration_ops = vec![
            sinomo_core::kv::KvOp::put_serde(
                file_system_config_key(&fs.file_system_id),
                &DfsFileSystemConfig::from(&fs),
                None,
            )
            .unwrap(),
            sinomo_core::kv::KvOp::put_serde(
                metadata_shard_key(&fs.file_system_id, "root"),
                &shard,
                None,
            )
            .unwrap(),
        ];
        let committed = append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: configuration_ops,
                failure: Vec::new(),
            }),
        )
        .unwrap();
        let mut backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            default_dfs_metadata_root(&data_dir),
        )
        .unwrap();
        backend.put_file_system(&fs).unwrap();
        for object in distributed_sifs_bootstrap_objects(local.clone()) {
            backend.put_object(&object).unwrap();
        }
        let journal_sequence = backend.journal_last_sequence();
        drop(backend);
        let runtime = daemon_test_runtime(name);
        let peer = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
            .unwrap()
            .unwrap();
        let authority = daemon_dfs_metadata_authority_generation_for_identity(
            &cluster_paths,
            &peer.cluster_id,
            &peer.local_node_id,
            Some(runtime.handle()),
        )
        .unwrap();
        let quiet_host: Arc<dyn sinomo_dfs::DfsMetadataAuthorityHost> =
            Arc::new(QuietAuthorityHost {
                write_configuration,
                delegate: daemon_dfs_metadata_authority_host(
                    &cluster_paths,
                    Some(runtime.handle()),
                )
                .unwrap(),
            });
        authority.attach_host(Arc::clone(&quiet_host));
        if !write_configuration {
            // Metadata-only fixtures seed their real committed DFS proof.
            // Native write fixtures instead let the first real async lookup
            // run the existing cold owner and prove all three scopes. Seeding
            // just DFS would bypass that bootstrap through the retained-prefix
            // fast path and leave the write configuration unpublished.
            assert!(authority.install_authoritative_projection(
                test_dfs_authoritative_prefix_projection(&committed)
            ));
        }
        let scheduler = daemon_dfs_mutation_scheduler(&data_dir).unwrap();
        let fixture = Self {
            runtime,
            data_dir,
            paths,
            cluster_paths,
            local,
            remote,
            shard,
            quiet_host,
            _authority: authority,
            scheduler,
            journal_sequence,
            _owner: owner,
            _root: root,
        };
        // First access initializes global metadata; second proves/warms the
        // exact stamped namespace lane before the injected edge.
        for frame in 1..=2 {
            let command = fixture.command(frame, "pre-dispatch-warm");
            let response = fixture
                .runtime
                .block_on(handle_daemon_dfs_message_async(
                    &fixture.paths,
                    command,
                    None,
                    None,
                    fixture.runtime.handle(),
                ))
                .unwrap();
            assert_eq!(
                namespace_result(&response),
                &sinomo_dfs::DfsNamespaceResult::Applied
            );
        }
        if write_configuration {
            assert!(
                fixture
                    ._authority
                    .write_configuration_projection()
                    .unwrap()
                    .is_some(),
                "real native configuration must prove all three scopes before the race"
            );
        }
        fixture
    }

    fn command(&self, frame: u64, request: &str) -> DfsMessageCommand {
        let mut routed =
            test_dfs_lookup_routed_message(&self.remote, &self.local, frame, "/services");
        let DfsNetworkMessage::NamespaceRequest(namespace) = &mut routed.message else {
            unreachable!();
        };
        namespace.authority_shards = vec![self.shard.clone()];
        test_dfs_message_command(request, routed)
    }

    fn require_unchanged_journal(&self) {
        evict_daemon_dfs_message_service_cache(&self.data_dir)
            .expect("close resident lane before durable journal assertion");
        let backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            default_dfs_metadata_root(&self.data_dir),
        )
        .unwrap();
        assert_eq!(
            backend.journal_last_sequence(),
            self.journal_sequence,
            "read retries cannot create namespace effects"
        );
    }

    fn hook(
        &self,
        expected: DfsMessageCommand,
        changed_attempts: usize,
        change_original_epoch: bool,
        late_error: bool,
        block_retry_admission: bool,
    ) -> (Arc<RetryObservations>, Arc<DaemonDfsPreDispatchTestHook>) {
        let observations = Arc::new(RetryObservations::default());
        let state = Arc::clone(&observations);
        let paths = self.cluster_paths.clone();
        let local = self.local.clone();
        let base_shard = self.shard.clone();
        let quiet = Arc::clone(&self.quiet_host);
        let scheduler = Arc::clone(&self.scheduler);
        let hook = Arc::new(
            move |point,
                  command: Option<&DfsMessageCommand>,
                  preflight: Option<&DaemonDfsMetadataAuthorityPreflight>| {
                match point {
                    DaemonDfsPreDispatchTestPoint::PublishPlanningAuthorityCaptured => {}
                    DaemonDfsPreDispatchTestPoint::ContentStoreCacheAccess
                    | DaemonDfsPreDispatchTestPoint::ExactLaneCacheAccess
                    | DaemonDfsPreDispatchTestPoint::AfterPreflightFence
                    | DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight => {}
                    DaemonDfsPreDispatchTestPoint::BeforePreflightFence => {
                        let attempt = state.selections.fetch_add(1, Ordering::SeqCst);
                        if attempt < changed_attempts {
                            let preflight =
                                preflight.ok_or_else(|| test_error("missing real preflight"))?;
                            let before = preflight.authority.generation();
                            preflight.authority.attach_host(Arc::clone(&quiet));
                            let mut changed = base_shard.clone();
                            if change_original_epoch {
                                changed.epoch += attempt as u64 + 1;
                            } else {
                                changed.shard_id = "retry-unrelated".into();
                                changed.assignment = DfsMetadataShardAssignment::PathPrefix {
                                    prefix: "/retry-unrelated".into(),
                                };
                                changed.epoch = attempt as u64 + 1;
                            }
                            let op = sinomo_core::kv::KvOp::put_serde(
                                metadata_shard_key(&changed.file_system_id, &changed.shard_id),
                                &changed,
                                None,
                            )
                            .map_err(|error| test_error(error.to_string()))?;
                            let committed = append_cluster_raft_command(
                                &paths,
                                &local,
                                sinomo_core::kv::KvCommand::Op(op),
                            )
                            .map_err(sinomo_dfs::DfsError::from_network)?;
                            if !preflight.authority.install_authoritative_projection(
                                test_dfs_authoritative_prefix_projection(&committed),
                            ) || preflight.authority.generation() <= before
                            {
                                return Err(test_error(
                                    "committed authority did not advance after preflight",
                                ));
                            }
                        }
                    }
                    DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh => {
                        require_original(command, &expected)?;
                        state.refreshes.fetch_add(1, Ordering::SeqCst);
                        let admission = scheduler.try_admit_global().ok_or_else(|| {
                            test_error("old admission still held during authority refresh")
                        })?;
                        state.released_before_refresh.fetch_add(1, Ordering::SeqCst);
                        if block_retry_admission {
                            *state.test_admission.lock().unwrap() = Some(admission);
                        } else {
                            drop(admission);
                        }
                        state.refresh_entered.notify_one();
                    }
                    DaemonDfsPreDispatchTestPoint::BeforeExecute => {
                        require_original(command, &expected)?;
                        state.executions.fetch_add(1, Ordering::SeqCst);
                        if scheduler.try_admit_global().is_some() {
                            return Err(test_error("execution did not reacquire admission"));
                        }
                    }
                    DaemonDfsPreDispatchTestPoint::AfterExecute => {
                        require_original(command, &expected)?;
                        state.completed_executions.fetch_add(1, Ordering::SeqCst);
                        if late_error {
                            // Deliberately the same text as the pre-dispatch error:
                            // only the typed unexecuted outcome is safe to retry.
                            return Err(test_error(format!(
                                "DFS metadata authority advanced after async preflight of generation {}",
                                preflight.map(|p| p.generation).unwrap_or(0)
                            )));
                        }
                    }
                }
                Ok(())
            },
        );
        (observations, hook)
    }

    fn provider_change_hook(
        &self,
        expected: DfsMessageCommand,
        changed_attempts: usize,
    ) -> (Arc<RetryObservations>, Arc<DaemonDfsPreDispatchTestHook>) {
        let observations = Arc::new(RetryObservations::default());
        let state = Arc::clone(&observations);
        let paths = self.cluster_paths.clone();
        let local = self.local.clone();
        let quiet = Arc::clone(&self.quiet_host);
        let scheduler = Arc::clone(&self.scheduler);
        let hook = Arc::new(
            move |point,
                  command: Option<&DfsMessageCommand>,
                  preflight: Option<&DaemonDfsMetadataAuthorityPreflight>| {
                match point {
                    DaemonDfsPreDispatchTestPoint::AfterPreflightFence => {
                        let attempt = state.selections.fetch_add(1, Ordering::SeqCst);
                        if attempt < changed_attempts {
                            let preflight = preflight
                                .ok_or_else(|| test_error("missing provider preflight"))?;
                            let before = preflight.authority.generation();
                            let storage = preflight.authority.storage_generation();
                            preflight.authority.attach_host(Arc::clone(&quiet));
                            let mut advertisement =
                                sinomo_dfs::DfsNodeAdvertisement::content(local.clone());
                            advertisement.zone = Some(format!("pre-execution-zone-{attempt}"));
                            // This is a real contiguous committed provider delta.
                            // Its shared subscription updates all three proven
                            // scopes; installing a DFS-only prefix would miss it.
                            append_cluster_raft_command(
                                &paths,
                                &local,
                                sinomo_core::kv::KvCommand::Op(
                                    sinomo_core::kv::KvOp::put_serde(
                                        sinomo_dfs::content_service_key(&local),
                                        &advertisement,
                                        None,
                                    )
                                    .map_err(|error| test_error(error.to_string()))?,
                                ),
                            )
                            .map_err(sinomo_dfs::DfsError::from_network)?;
                            if preflight.authority.generation() <= before
                                || preflight.authority.storage_generation() != storage
                                || preflight
                                    .authority
                                    .write_configuration_projection()?
                                    .is_none()
                            {
                                return Err(test_error(
                                    "provider delta did not advance only the complete authority fence",
                                ));
                            }
                        }
                    }
                    DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh => {
                        require_original(command, &expected)?;
                        state.refreshes.fetch_add(1, Ordering::SeqCst);
                        let admission = scheduler.try_admit_global().ok_or_else(|| {
                            test_error("provider replan retained the old admission")
                        })?;
                        state.released_before_refresh.fetch_add(1, Ordering::SeqCst);
                        drop(admission);
                    }
                    DaemonDfsPreDispatchTestPoint::BeforeExecute => {
                        require_original(command, &expected)?;
                        state.executions.fetch_add(1, Ordering::SeqCst);
                        if scheduler.try_admit_global().is_some() {
                            return Err(test_error("provider replan did not reacquire admission"));
                        }
                    }
                    DaemonDfsPreDispatchTestPoint::AfterExecute => {
                        require_original(command, &expected)?;
                        state.completed_executions.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => {}
                }
                Ok(())
            },
        );
        (observations, hook)
    }
}

#[derive(Default)]
struct RetryObservations {
    selections: AtomicUsize,
    refreshes: AtomicUsize,
    released_before_refresh: AtomicUsize,
    executions: AtomicUsize,
    completed_executions: AtomicUsize,
    test_admission: Mutex<Option<DaemonDfsMutationAdmission>>,
    refresh_entered: tokio::sync::Notify,
}

fn test_error(message: impl Into<String>) -> sinomo_dfs::DfsError {
    sinomo_dfs::DfsError::Network {
        message: message.into(),
    }
}
fn require_original(
    actual: Option<&DfsMessageCommand>,
    expected: &DfsMessageCommand,
) -> Result<(), sinomo_dfs::DfsError> {
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(test_error(
            "retry changed the original request/source/target/frame/shard/epoch payload",
        ))
    }
}
fn namespace_result(response: &DfsDataPlaneResponse) -> &sinomo_dfs::DfsNamespaceResult {
    response
        .response
        .outbound_messages
        .iter()
        .find_map(|message| match &message.message {
            DfsNetworkMessage::NamespaceResponse(response) => Some(&response.result),
            _ => None,
        })
        .expect("namespace response from real service execution")
}

#[test]
fn predispatch_authority_edge_retries_once_with_released_and_reacquired_admission() {
    let f = RetryFixture::new("predispatch-retry-once");
    let command = f.command(50, "predispatch-once");
    let (seen, hook) = f.hook(command.clone(), 1, false, false, false);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let response = f
        .runtime
        .block_on(handle_daemon_dfs_message_async(
            &f.paths,
            command,
            None,
            None,
            f.runtime.handle(),
        ))
        .expect("retry the unchanged unexecuted read");
    assert_eq!(
        namespace_result(&response),
        &sinomo_dfs::DfsNamespaceResult::Applied
    );
    assert_eq!(seen.selections.load(Ordering::SeqCst), 2);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(seen.released_before_refresh.load(Ordering::SeqCst), 1);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 1);
    assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 1);
    f.require_unchanged_journal();
}

fn provider_delta_after_first_fence(changed_attempts: usize) {
    let f = RetryFixture::with_write_configuration(
        if changed_attempts == 1 {
            "postfence-provider-once"
        } else {
            "postfence-provider-churn"
        },
        true,
    );
    let command = f.command(90, "postfence-provider-original");
    let (primary, exact, scan_owner, storage_generation) = {
        let cache = daemon_dfs_message_service_cache().lock().unwrap();
        let entry = cache.get(&f.data_dir.root()).unwrap();
        (
            entry.primary_service.clone(),
            entry.exact_shard_services.clone(),
            Arc::clone(&entry.write_intent_maintenance_scan),
            entry.signature.metadata_storage_generation,
        )
    };
    let opens = daemon_dfs_test_message_service_lane_open_count(&f.data_dir);
    let _ = take_daemon_dfs_test_authoritative_prefix_call_count(&f.cluster_paths);
    let _ = take_daemon_dfs_test_authoritative_exact_set_call_count(&f.cluster_paths);
    let (seen, hook) = f.provider_change_hook(command.clone(), changed_attempts);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let result = f.runtime.block_on(handle_daemon_dfs_message_async(
        &f.paths,
        command,
        None,
        None,
        f.runtime.handle(),
    ));
    if changed_attempts == 1 {
        assert_eq!(
            namespace_result(
                &result.expect(
                    "retry the same command after the second fence detects a provider delta"
                )
            ),
            &sinomo_dfs::DfsNamespaceResult::Applied
        );
        assert_eq!(seen.selections.load(Ordering::SeqCst), 2);
        assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(seen.executions.load(Ordering::SeqCst), 1);
        assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 1);
    } else {
        assert!(
            result.is_err(),
            "continuous provider churn must exhaust the existing replan bound"
        );
        assert_eq!(seen.selections.load(Ordering::SeqCst), 3);
        assert_eq!(seen.refreshes.load(Ordering::SeqCst), 2);
        assert_eq!(seen.executions.load(Ordering::SeqCst), 0);
        assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 0);
    }
    assert_eq!(
        seen.released_before_refresh.load(Ordering::SeqCst),
        seen.refreshes.load(Ordering::SeqCst)
    );
    assert!(f.scheduler.try_admit_global().is_some());
    {
        let cache = daemon_dfs_message_service_cache().lock().unwrap();
        let entry = cache.get(&f.data_dir.root()).unwrap();
        assert_eq!(
            entry.signature.metadata_storage_generation,
            storage_generation
        );
        assert!(Arc::ptr_eq(
            &scan_owner,
            &entry.write_intent_maintenance_scan
        ));
        match (primary.as_ref(), entry.primary_service.as_ref()) {
            (Some(before), Some(after)) => assert!(Arc::ptr_eq(before, after)),
            (None, None) => {}
            _ => panic!("provider-only replan changed the resident global service"),
        }
        assert_eq!(exact.len(), entry.exact_shard_services.len());
        for (key, before) in &exact {
            assert!(Arc::ptr_eq(
                before,
                entry.exact_shard_services.get(key).unwrap()
            ));
        }
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&f.data_dir),
        opens
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&f.cluster_paths),
        0,
        "contiguous provider deltas replan from the complete resident view"
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_exact_set_call_count(&f.cluster_paths),
        0
    );
    // Release cloned owners before proving that no durable read effects exist.
    drop(primary);
    drop(exact);
    f.require_unchanged_journal();
}

#[test]
fn provider_delta_after_first_preflight_fence_replans_without_reopening_stores() {
    provider_delta_after_first_fence(1);
}

#[test]
fn provider_churn_after_first_preflight_fence_is_bounded_without_execution() {
    provider_delta_after_first_fence(usize::MAX);
}

#[test]
fn valid_preflight_content_store_open_failure_is_terminal_without_authority_replan() {
    let f = RetryFixture::with_write_configuration("preflight-real-store-error", true);
    assert!(
        f._authority
            .write_configuration_projection()
            .unwrap()
            .is_some()
    );
    let generation = f._authority.generation();
    evict_daemon_dfs_message_service_cache(&f.data_dir).unwrap();
    let content_root = f.data_dir.dfs_dir().join("content");
    fs::remove_dir_all(&content_root).unwrap();
    fs::write(
        &content_root,
        b"file blocks the real content-store directory",
    )
    .unwrap();
    let store_error = LocalContentStore::new(&content_root)
        .err()
        .expect("the real store rejects the directory conflict");
    assert!(matches!(
        sinomo_dfs::DfsError::from(store_error),
        sinomo_dfs::DfsError::ContentStore { .. }
    ));
    let command = f.command(91, "valid-preflight-real-backend-error");
    let (seen, hook) = f.hook(command.clone(), 0, false, false, false);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let error = f
        .runtime
        .block_on(handle_daemon_dfs_message_async(
            &f.paths,
            command,
            None,
            None,
            f.runtime.handle(),
        ))
        .unwrap_err();
    assert!(
        matches!(&error, super::super::DfsRuntimeError::Parse { .. }),
        "the terminal store failure retains the runtime's typed backend-error wrapper: {error}"
    );
    assert_eq!(f._authority.generation(), generation);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 0);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 0);
    assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 0);
    assert!(f.scheduler.try_admit_global().is_some());
    f.require_unchanged_journal();
}

#[test]
fn predispatch_retry_does_not_rewrite_original_obsolete_shard_epoch() {
    let f = RetryFixture::new("predispatch-original-stamp");
    let command = f.command(51, "predispatch-original-epoch");
    let (seen, hook) = f.hook(command.clone(), 1, true, false, false);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let response = f
        .runtime
        .block_on(handle_daemon_dfs_message_async(
            &f.paths,
            command,
            None,
            None,
            f.runtime.handle(),
        ))
        .expect("service-level structured stale-stamp rejection");
    assert!(matches!(
        namespace_result(&response),
        sinomo_dfs::DfsNamespaceResult::Rejected { .. }
    ));
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 1);
    assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 1);
    f.require_unchanged_journal();
}

#[test]
fn predispatch_churn_stops_after_three_attempts_and_two_refreshes_without_execution() {
    let f = RetryFixture::new("predispatch-retry-bound");
    let command = f.command(52, "predispatch-bounded");
    let (seen, hook) = f.hook(command.clone(), usize::MAX, false, false, false);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    assert!(
        f.runtime
            .block_on(handle_daemon_dfs_message_async(
                &f.paths,
                command,
                None,
                None,
                f.runtime.handle()
            ))
            .is_err()
    );
    assert_eq!(seen.selections.load(Ordering::SeqCst), 3);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 2);
    assert_eq!(seen.released_before_refresh.load(Ordering::SeqCst), 2);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 0);
    assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 0);
    assert!(f.scheduler.try_admit_global().is_some());
    f.require_unchanged_journal();
}

#[test]
fn after_execution_error_matching_stale_text_is_terminal_and_never_replayed() {
    let f = RetryFixture::new("predispatch-late-error");
    let command = f.command(53, "predispatch-terminal-execute");
    let (seen, hook) = f.hook(command.clone(), 0, false, true, false);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let error = f
        .runtime
        .block_on(handle_daemon_dfs_message_async(
            &f.paths,
            command,
            None,
            None,
            f.runtime.handle(),
        ))
        .unwrap_err();
    assert!(error.to_string().contains("advanced after async preflight"));
    assert_eq!(seen.selections.load(Ordering::SeqCst), 1);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 0);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 1);
    assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 1);
    f.require_unchanged_journal();
}

#[test]
fn outer_rpc_timeout_cancels_pending_readmission_without_reset_or_late_execution() {
    let f = RetryFixture::new("predispatch-outer-timeout");
    let command = f.command(54, "predispatch-timeout");
    let (seen, hook) = f.hook(command.clone(), 1, false, false, true);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let result = f.runtime.block_on(async {
        let request = handle_daemon_dfs_message_async(&f.paths, command, None, None, f.runtime.handle());
        tokio::pin!(request);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = seen.refresh_entered.notified() => {},
                result = &mut request => panic!("request completed before retry admission blocker: {result:?}"),
            }
        }).await.expect("reach the forced stale edge without a timing race");
        // The SAME RPC future is now known to be blocked on native admission.
        // A caller deadline must cancel it; no internal retry gets a replacement
        // deadline or detached owner which resumes afterward.
        tokio::time::timeout(Duration::from_millis(25), &mut request).await
    });
    assert!(
        result.is_err(),
        "same outer RPC deadline must expire while retry admission is held"
    );
    assert_eq!(seen.selections.load(Ordering::SeqCst), 1);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(seen.released_before_refresh.load(Ordering::SeqCst), 1);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 0);
    drop(seen.test_admission.lock().unwrap().take());
    assert!(f.scheduler.try_admit_global().is_some());
    f.runtime.block_on(tokio::task::yield_now());
    assert_eq!(
        seen.executions.load(Ordering::SeqCst),
        0,
        "releasing the test blocker must not resurrect the canceled command"
    );
    f.require_unchanged_journal();
}

#[test]
fn cancelled_pre_dispatch_retry_drops_owned_quarantine_without_binding_or_execution() {
    let f = RetryFixture::new("predispatch-quarantine-cancel");
    let mut routed = test_dfs_write_routed_message(
        &f.remote,
        &f.local,
        55,
        "/services/cancel-before-dispatch.bin",
    );
    let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed.message else {
        unreachable!();
    };
    let bytes = std::mem::take(&mut write.bytes);
    let hash = sinomo_dfs::content_hash(&bytes);
    let object = write.request.object_id.clone();
    let version = write.request.target_version.clone();
    write.requester_blob_pre_staged = false;
    write.content_hash = Some(hash.clone());
    write.content_size_bytes = Some(bytes.len() as u64);
    write.authority_shard = Some(f.shard.clone());
    let command = test_dfs_message_command("predispatch-quarantine", routed);
    let (seen, hook) = f.hook(command.clone(), 1, false, false, true);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let (sender, body) =
        sinomo_core::network::ObjectRpcIncomingBody::test_channel(Some(bytes.len() as u64), 1);
    let attachment = DfsInboundAttachment {
        descriptor: sinomo_dfs::DfsRpcAttachmentDescriptor {
            total_size_bytes: bytes.len() as u64,
            content_hash: Some(hash.clone()),
        },
        body,
    };
    f.runtime
        .block_on(sender.send(bytes))
        .expect("buffer bounded attachment");
    drop(sender);
    let temporary_root = f.data_dir.dfs_dir().join("content/blob-tmp");
    let result = f.runtime.block_on(async {
        let request = handle_daemon_dfs_message_async(&f.paths, command, Some(attachment), None, f.runtime.handle());
        tokio::pin!(request);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = seen.refresh_entered.notified() => {},
                result = &mut request => panic!("quarantined request completed before retry blocker: {result:?}"),
            }
        }).await.expect("ingest once and reach retry with owned quarantine");
        assert!(fs::read_dir(&temporary_root).unwrap().next().is_some(), "unexecuted command still owns private bytes while waiting");
        tokio::time::timeout(Duration::from_millis(25), &mut request).await
    });
    assert!(result.is_err());
    assert_eq!(seen.selections.load(Ordering::SeqCst), 1);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 0);
    drop(seen.test_admission.lock().unwrap().take());
    assert!(f.scheduler.try_admit_global().is_some());
    assert!(
        fs::read_dir(&temporary_root).unwrap().next().is_none(),
        "canceling the RPC drops its unchanged quarantine rather than orphaning or binding it"
    );
    let content = LocalContentStore::new(f.data_dir.dfs_dir().join("content")).unwrap();
    assert!(
        content
            .read_manifest(object.as_str(), version.as_str())
            .is_err()
    );
    assert!(
        content
            .content_reservation_roots()
            .unwrap()
            .reservations
            .is_empty()
    );
    assert!(!content.has_blob(&hash).unwrap());
    f.require_unchanged_journal();
}

struct ProgressTestTransport(Option<tokio::io::DuplexStream>);

impl sinomo_core::network::ClusterObjectRpcAsyncHost for ProgressTestTransport {
    fn open_object_rpc_stream_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
        _health: sinomo_core::network::NetworkHealthPolicy,
    ) -> sinomo_core::network::ClusterObjectRpcFuture<
        'a,
        Box<dyn sinomo_core::network::NetworkDuplexStream>,
    > {
        let stream = self.0.take().expect("one test carrier");
        Box::pin(async move {
            Ok(Box::new(stream) as Box<dyn sinomo_core::network::NetworkDuplexStream>)
        })
    }
}

fn rpc_progress_survives_dfs_cache_access(attachment_ingress: bool) {
    use sinomo_core::network::{
        ClusterObjectRpcClient, ClusterObjectRpcOptions, DaemonRpcTimeouts, ObjectFrameIoOptions,
        ObjectRpcBodySource, ObjectRpcCall, ObjectRpcOutgoingResponse, ObjectRpcRequest,
        RpcPeerContext, accept_local_object_rpc_stream_carrier,
        read_local_object_rpc_carrier_prelude, serve_service_rpc_peer_stream,
    };
    const IDLE_TIMEOUT: Duration = Duration::from_millis(80);
    const CACHE_DELAY: Duration = Duration::from_millis(320);
    let name = if attachment_ingress {
        "rpc-progress-content-cache"
    } else {
        "rpc-progress-exact-cache"
    };
    let fixture = RetryFixture::new(name);
    let entered = Arc::new(AtomicUsize::new(0));
    let hook_entered = Arc::clone(&entered);
    let point = if attachment_ingress {
        DaemonDfsPreDispatchTestPoint::ContentStoreCacheAccess
    } else {
        DaemonDfsPreDispatchTestPoint::ExactLaneCacheAccess
    };
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(
        &fixture.data_dir,
        Arc::new(move |observed, _, _| {
            if observed == point {
                hook_entered.fetch_add(1, Ordering::SeqCst);
                // Stall only this root at the actual cache-access boundary.
                // Holding the process-wide mutex would delay unrelated tests.
                std::thread::sleep(CACHE_DELAY);
            }
            Ok(())
        }),
    );
    let (command, bytes) = if attachment_ingress {
        let mut routed = test_dfs_write_routed_message(
            &fixture.remote,
            &fixture.local,
            81,
            "/services/progress-cache.bin",
        );
        let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed.message else {
            unreachable!();
        };
        write.authority_shard = Some(fixture.shard.clone());
        let bytes = std::mem::take(&mut write.bytes);
        (test_dfs_message_command(name, routed), bytes)
    } else {
        (fixture.command(82, name), Vec::new())
    };
    let size = bytes.len() as u64;
    let request = ObjectRpcRequest::encode_body("/test/dfs-cache-progress", "dfs", &command)
        .expect("typed DFS request");
    let call = if attachment_ingress {
        ObjectRpcCall::new(request, ObjectRpcBodySource::bytes(bytes))
    } else {
        ObjectRpcCall::value(request)
    };
    let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
    let client_data_dir = fixture.data_dir.clone();
    // The client clock runs independently. Blocking the server's current-thread
    // runtime therefore produces a genuine client idle timeout in the regression.
    let client = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut transport = ProgressTestTransport(Some(client_stream));
            let mut rpc = ClusterObjectRpcClient::endpoint_only(&mut transport);
            rpc.call_loopback(
                &client_data_dir,
                "127.0.0.1:9",
                call,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts {
                    connect_timeout: Some(Duration::from_secs(3)),
                    idle_timeout: Some(IDLE_TIMEOUT),
                })
                .with_overall_timeout(Duration::from_secs(5)),
            )
            .await?
            .decode_body::<String>()
            .await
        })
    });
    let server_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let server_result = server_runtime.block_on(async {
        let mut server_stream = server_stream;
        let context = read_local_object_rpc_carrier_prelude(&mut server_stream).await?;
        let mut acceptor =
            accept_local_object_rpc_stream_carrier(Box::new(server_stream), &context)?;
        let stream = acceptor.accept().await?.expect("test logical RPC stream");
        let context = RpcPeerContext::unauthenticated_local(
            stream.session_id.clone(),
            stream.channel_binding,
        );
        serve_service_rpc_peer_stream(
            stream,
            &context,
            ObjectFrameIoOptions::idle_timeout(IDLE_TIMEOUT),
            |inbound, _| async {
                let (command, attachment) = if attachment_ingress {
                    let command = inbound.request.decode_body::<DfsMessageCommand>()?;
                    let attachment = DfsInboundAttachment {
                        descriptor: sinomo_dfs::DfsRpcAttachmentDescriptor {
                            total_size_bytes: size,
                            content_hash: Some("deliberately-invalid-test-hash".to_string()),
                        },
                        body: inbound.body,
                    };
                    (command, Some(attachment))
                } else {
                    (inbound.decode_body::<DfsMessageCommand>().await?, None)
                };
                let result = handle_daemon_dfs_message_async(
                    &fixture.paths,
                    command,
                    attachment,
                    None,
                    server_runtime.handle(),
                )
                .await;
                if attachment_ingress {
                    assert!(
                        result.unwrap_err().to_string().contains("hash mismatch"),
                        "attachment must reach storage validation after the stalled lookup"
                    );
                } else {
                    assert_eq!(
                        namespace_result(&result.expect("real stamped namespace lookup")),
                        &sinomo_dfs::DfsNamespaceResult::Applied
                    );
                }
                ObjectRpcOutgoingResponse::encode_value(
                    "/test/dfs-cache-progress",
                    "completed-cache-access".to_string(),
                )
            },
        )
        .await
    });
    let client_result = client.join().expect("client thread");
    assert_eq!(
        client_result.expect("RPC progress must prevent an unknown idle-timeout outcome"),
        "completed-cache-access"
    );
    server_result.expect("server completed framed response");
    assert_eq!(
        entered.load(Ordering::SeqCst),
        1,
        "one stalled cache lookup"
    );
    fixture.require_unchanged_journal();
}

#[test]
fn streamed_dfs_cache_access_keeps_rpc_progress_live() {
    rpc_progress_survives_dfs_cache_access(true);
}

#[test]
fn exact_dfs_cache_access_keeps_rpc_progress_live() {
    rpc_progress_survives_dfs_cache_access(false);
}

struct BlockedIngestOwnerDrop {
    _owner: Arc<DaemonDataOwnerLock>,
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

impl Drop for BlockedIngestOwnerDrop {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        self.release
            .recv_timeout(Duration::from_secs(5))
            .expect("release completed ingest worker");
    }
}

#[test]
fn cancelled_attachment_ingest_retains_os_data_owner_until_worker_finishes() {
    let root = temp_root("cancelled-ingest-data-owner");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().unwrap();
    let paths = DaemonPaths::new(&data_dir);
    let owner = Arc::new(DaemonDataOwnerLock::acquire(&paths).unwrap());
    let (entered, worker_finished) = std::sync::mpsc::sync_channel(1);
    let (release, released) = std::sync::mpsc::sync_channel(1);
    let guard = BlockedIngestOwnerDrop {
        _owner: Arc::clone(&owner),
        entered,
        release: released,
    };
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content")).unwrap();
    let source = NodeId::from_legacy_named("cancelled-ingest-owner-source").unwrap();
    let target = NodeId::from_legacy_named("cancelled-ingest-owner-target").unwrap();
    let mut routed =
        test_dfs_write_routed_message(&source, &target, 83, "/services/cancelled-ingest-owner.bin");
    let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed.message else {
        unreachable!();
    };
    write.bytes.clear();
    let command = test_dfs_message_command("cancelled-ingest-owner", routed);
    let (sender, body) = sinomo_core::network::ObjectRpcIncomingBody::test_channel(Some(100), 1);
    let attachment = DfsInboundAttachment {
        descriptor: sinomo_dfs::DfsRpcAttachmentDescriptor {
            total_size_bytes: 100,
            content_hash: None,
        },
        body,
    };
    let runtime = daemon_test_runtime("cancelled-attachment-owner");
    let ingest = runtime.spawn(
        crate::runtime_message::ingest_dfs_message_attachment_with_owner(
            Some(content),
            command,
            Some(attachment),
            guard,
            None,
        ),
    );
    runtime.block_on(async {
        // Exceed the ingest channel's bounded depth without finishing the body.
        // Producer progress proves the worker began consuming input.
        for _ in 0..32 {
            sender.send(vec![7]).await.expect("partial attachment");
        }
    });
    assert!(
        fs::read_dir(data_dir.dfs_dir().join("content/blob-tmp"))
            .unwrap()
            .next()
            .is_some(),
        "ingest has begun durable staging"
    );
    drop(owner);
    ingest.abort();
    assert!(runtime.block_on(ingest).unwrap_err().is_cancelled());
    worker_finished
        .recv_timeout(Duration::from_secs(3))
        .expect("cancelled feed closed its worker input");
    assert!(
        matches!(
            DaemonDataOwnerLock::acquire(&paths),
            Err(DaemonError::AlreadyRunning { .. })
        ),
        "an abandoned RPC cannot release the OS owner while ingest cleanup runs"
    );
    release.send(()).unwrap();
    // Runtime shutdown waits for the owned blocking worker, proving guard drop.
    drop(runtime);
    DaemonDataOwnerLock::acquire(&paths).expect("worker released the OS owner after cleanup");
}

fn publisher_provider_delta_before_plan(changed_attempts: usize, expire_handoff: bool) {
    let f = RetryFixture::with_write_configuration(
        if expire_handoff {
            "publish-planning-original-deadline"
        } else if changed_attempts == 1 {
            "publish-planning-provider-once"
        } else {
            "publish-planning-provider-churn"
        },
        true,
    );
    let (primary, exact, scan_owner, storage_generation) = {
        let cache = daemon_dfs_message_service_cache().lock().unwrap();
        let entry = cache.get(&f.data_dir.root()).unwrap();
        (
            entry.primary_service.clone(),
            entry.exact_shard_services.clone(),
            Arc::clone(&entry.write_intent_maintenance_scan),
            entry.signature.metadata_storage_generation,
        )
    };
    let opens = daemon_dfs_test_message_service_lane_open_count(&f.data_dir);
    let _ = take_daemon_dfs_test_authoritative_prefix_call_count(&f.cluster_paths);
    let _ = take_daemon_dfs_test_authoritative_exact_set_call_count(&f.cluster_paths);
    let seen = Arc::new(RetryObservations::default());
    let state = Arc::clone(&seen);
    let paths = f.cluster_paths.clone();
    let local = f.local.clone();
    let scheduler = Arc::clone(&f.scheduler);
    let quiet_host = Arc::clone(&f.quiet_host);
    let hook: Arc<DaemonDfsPreDispatchTestHook> = Arc::new(move |point, command, preflight| {
        match point {
            DaemonDfsPreDispatchTestPoint::PublishPlanningAuthorityCaptured => {
                assert!(
                    command.is_none(),
                    "publication planning owns no served request replay"
                );
                let attempt = state.selections.fetch_add(1, Ordering::SeqCst);
                if attempt < changed_attempts {
                    let preflight =
                        preflight.ok_or_else(|| test_error("missing publisher certificate"))?;
                    // Production preflight reattaches its lifecycle host. This
                    // fixture owns the injected edge, so keep unrelated
                    // discovery/publisher owners out of its hook counters while
                    // preserving the real native reads and configuration floor.
                    preflight.authority.attach_host(Arc::clone(&quiet_host));
                    let generation = preflight.generation;
                    let storage = preflight.authority.storage_generation();
                    assert!(
                        scheduler.try_admit_global().is_none(),
                        "planning owns exact admission"
                    );
                    let mut advertisement =
                        sinomo_dfs::DfsNodeAdvertisement::content(local.clone());
                    advertisement.zone = Some(format!("publication-planning-zone-{attempt}"));
                    append_cluster_raft_command(
                        &paths,
                        &local,
                        sinomo_core::kv::KvCommand::Op(
                            sinomo_core::kv::KvOp::put_serde(
                                sinomo_dfs::content_service_key(&local),
                                &advertisement,
                                None,
                            )
                            .map_err(|error| test_error(error.to_string()))?,
                        ),
                    )
                    .map_err(sinomo_dfs::DfsError::from_network)?;
                    assert!(preflight.authority.generation() > generation);
                    assert_eq!(preflight.authority.storage_generation(), storage);
                    assert!(
                        preflight
                            .authority
                            .write_configuration_projection()?
                            .is_some()
                    );
                }
            }
            DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh => {
                assert!(command.is_none());
                state.refreshes.fetch_add(1, Ordering::SeqCst);
                let admission = scheduler.try_admit_global().ok_or_else(|| {
                    test_error("publisher replan retained planning admission or its backend lease")
                })?;
                state.released_before_refresh.fetch_add(1, Ordering::SeqCst);
                if expire_handoff {
                    // The old owner released admission. Retain the independently
                    // acquired test permit so unrelated root-owned publishers
                    // cannot consume this same hook during the handoff window.
                    *state.test_admission.lock().unwrap() = Some(admission);
                    // Hold the exact handoff past the ORIGINAL planning budget.
                    // The successor then waits on ordinary admission, so a
                    // restarted budget cannot complete through a ready fast path.
                    std::thread::sleep(
                        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT + Duration::from_millis(25),
                    );
                } else {
                    drop(admission);
                }
            }
            _ => {}
        }
        Ok(())
    });
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let prepare = super::super::publish_host::prepare_daemon_dfs_metadata_publish_with_replanning(
        Arc::clone(&f.scheduler),
        f.data_dir.clone(),
        f.cluster_paths.clone(),
        DaemonDfsMutationLaneKey {
            file_system_id: f.shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(f.shard.shard_id.clone()),
        },
        f.runtime.handle().clone(),
        Arc::clone(&f._owner),
        daemon_dfs_metadata_replication_options(),
    );
    let result = f.runtime.block_on(async {
        if expire_handoff {
            tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT + Duration::from_secs(2),
                prepare,
            )
            .await
            .expect("the expired original budget must not grant the successor a new budget")
        } else {
            prepare.await
        }
    });
    if expire_handoff {
        let error = result.expect_err("the original planning deadline expired during handoff");
        assert!(error.contains("deadline elapsed"), "{error}");
        assert_eq!(seen.selections.load(Ordering::SeqCst), 1);
        assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
        drop(seen.test_admission.lock().unwrap().take());
    } else if changed_attempts == 1 {
        let plan =
            result.expect("a provider-only rollover can rebind before preparing publication");
        assert!(
            plan.is_some(),
            "the real resident shard produced immutable journal work"
        );
        drop(plan);
        assert_eq!(seen.selections.load(Ordering::SeqCst), 2);
        assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
    } else {
        assert!(
            result.is_err(),
            "continuous rollover must exhaust the existing two-replan bound"
        );
        assert_eq!(seen.selections.load(Ordering::SeqCst), 3);
        assert_eq!(seen.refreshes.load(Ordering::SeqCst), 2);
    }
    assert_eq!(
        seen.released_before_refresh.load(Ordering::SeqCst),
        seen.refreshes.load(Ordering::SeqCst)
    );
    // Join the scheduler's FIFO queue: unrelated work woken by the test
    // permit's release may already own admission. A bounded successful
    // acquisition proves the publisher released its owner without racing it.
    let _released_admission = f.runtime.block_on(async {
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            f.scheduler
                .admit(&DfsMessageExecutionScope::GlobalExclusive),
        )
        .await
        .expect("publisher admission must be released within the existing network deadline")
        .expect("publisher admission can be reacquired after the original owner releases")
    });
    {
        let cache = daemon_dfs_message_service_cache().lock().unwrap();
        let entry = cache.get(&f.data_dir.root()).unwrap();
        assert_eq!(
            entry.signature.metadata_storage_generation,
            storage_generation
        );
        assert!(Arc::ptr_eq(
            &scan_owner,
            &entry.write_intent_maintenance_scan
        ));
        match (primary.as_ref(), entry.primary_service.as_ref()) {
            (Some(before), Some(after)) => assert!(Arc::ptr_eq(before, after)),
            (None, None) => {}
            _ => panic!("provider-only publication replan replaced the resident global service"),
        }
        assert_eq!(exact.len(), entry.exact_shard_services.len());
        for (key, before) in &exact {
            assert!(Arc::ptr_eq(
                before,
                entry.exact_shard_services.get(key).unwrap()
            ));
        }
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&f.data_dir),
        opens
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_prefix_call_count(&f.cluster_paths),
        0
    );
    assert_eq!(
        take_daemon_dfs_test_authoritative_exact_set_call_count(&f.cluster_paths),
        0
    );
    drop(primary);
    drop(exact);
    f.require_unchanged_journal();
}

#[test]
fn publisher_provider_delta_before_plan_replans_without_reopening_stores() {
    publisher_provider_delta_before_plan(1, false);
}

#[test]
fn publisher_provider_churn_exhausts_existing_preeffect_replan_bound() {
    publisher_provider_delta_before_plan(3, false);
}

#[test]
fn publisher_handoff_preserves_the_original_planning_deadline() {
    publisher_provider_delta_before_plan(1, true);
}

#[test]
fn publisher_missing_backend_at_unchanged_generation_stays_terminal() {
    let f = RetryFixture::with_write_configuration("publish-planning-stable-missing", true);
    let seen = Arc::new(RetryObservations::default());
    let state = Arc::clone(&seen);
    let data_dir = f.data_dir.clone();
    let quiet_host = Arc::clone(&f.quiet_host);
    let hook: Arc<DaemonDfsPreDispatchTestHook> = Arc::new(move |point, _, preflight| {
        match point {
            DaemonDfsPreDispatchTestPoint::PublishPlanningAuthorityCaptured => {
                state.selections.fetch_add(1, Ordering::SeqCst);
                let preflight = preflight.unwrap();
                preflight.authority.attach_host(Arc::clone(&quiet_host));
                let mut cache = daemon_dfs_message_service_cache().lock().unwrap();
                let entry = cache.get_mut(&data_dir.root()).unwrap();
                assert_eq!(
                    entry.signature.metadata_authority_generation,
                    preflight.generation
                );
                entry.resident_metadata_backend = None;
            }
            DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh => {
                state.refreshes.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
        Ok(())
    });
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let result = f.runtime.block_on(
        super::super::publish_host::prepare_daemon_dfs_metadata_publish_with_replanning(
            Arc::clone(&f.scheduler),
            f.data_dir.clone(),
            f.cluster_paths.clone(),
            DaemonDfsMutationLaneKey {
                file_system_id: f.shard.file_system_id.clone(),
                shard_id: DfsMetadataShardId::new(f.shard.shard_id.clone()),
            },
            f.runtime.handle().clone(),
            Arc::clone(&f._owner),
            daemon_dfs_metadata_replication_options(),
        ),
    );
    let error =
        result.expect_err("an unchanged-generation backend failure cannot become staleness");
    assert!(
        error.contains("resident backend does not match retained authority")
            || error.contains("metadata publish has no resident backend"),
        "the injected stable missing backend returned: {error}"
    );
    assert_eq!(seen.selections.load(Ordering::SeqCst), 1);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 0);
    assert!(f.scheduler.try_admit_global().is_some());
    f.require_unchanged_journal();
}
