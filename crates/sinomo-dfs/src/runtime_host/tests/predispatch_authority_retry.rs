//! Real daemon lane/admission regressions for typed pre-execution retry.

use super::super::message_host::{
    DaemonDfsPreDispatchTestHook, DaemonDfsPreDispatchTestPoint,
    install_daemon_dfs_pre_dispatch_test_hook,
};
use super::*;

struct QuietAuthorityHost {
    delegate: Arc<dyn sinomo_dfs::DfsMetadataAuthorityHost>,
}

impl sinomo_dfs::DfsMetadataAuthorityHost for QuietAuthorityHost {
    fn read_authoritative_prefix(&self) -> sinomo_dfs::DfsMetadataAuthorityReadFuture<'_> {
        self.delegate.read_authoritative_prefix()
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
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &cluster)
            .unwrap();
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
        let committed = append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![
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
                ],
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
                delegate: daemon_dfs_metadata_authority_host(
                    &cluster_paths,
                    Some(runtime.handle()),
                )
                .unwrap(),
            });
        authority.attach_host(Arc::clone(&quiet_host));
        // Seed from the real committed Raft result, not its observational file.
        assert!(authority.install_authoritative_projection(
            test_dfs_authoritative_prefix_projection(&committed)
        ));
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
