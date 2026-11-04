use super::*;
use sinomo_dfs::test_support::DfsTestRuntime;
use std::time::SystemTime;

const WAIT: Duration = Duration::from_secs(60);

fn backend(native: &DfsTestRuntime, node: &NodeId) -> ClusteredSandboxBackend {
    let context = native
        .component_context(node)
        .expect("live native component context");
    ClusteredSandboxBackend::new(
        Arc::new(native.data_dir(node).clone()),
        context
            .cluster_access()
            .expect("clustered component context"),
        Some(native.runtime_handle()),
    )
}

fn store(backend: &ClusteredSandboxBackend) -> ClusteredInstanceStore {
    let (dfs, candidates) = backend.open_dfs_service_with_candidates().unwrap();
    SifsSandboxInstanceStore::new(dfs).with_replica_candidates(candidates)
}

fn now() -> time::OffsetDateTime {
    time::OffsetDateTime::now_utc()
}

fn create_request(
    sandbox_id: &SandboxId,
    toolchain_id: &SandboxToolchainId,
) -> SandboxCreateInstanceRequest {
    SandboxCreateInstanceRequest {
        sandbox_id: Some(sandbox_id.clone()),
        toolchain_id: toolchain_id.clone(),
        temp_policy: SandboxTempPolicy::ClearOnSessionStart,
        memory_mb: None,
        wall_time_ms: None,
        max_input_bytes: None,
        max_output_bytes: None,
        max_workspace_bytes: None,
        max_written_bytes_per_command: None,
        max_file_count: None,
        mounts: Vec::new(),
        portability: None,
    }
}

/// The exact candidate `ClusteredSandboxBackend::create` stages for `request`.
fn create_candidate(
    backend: &ClusteredSandboxBackend,
    request: &SandboxCreateInstanceRequest,
) -> SandboxInstanceManifest {
    let policy = backend.load_policy_config().unwrap().policy;
    let mut manifest = SandboxInstanceManifest::new(
        request.sandbox_id.clone().unwrap(),
        request.toolchain_id.clone(),
        now(),
    )
    .unwrap();
    manifest.temp_policy = request.temp_policy.clone();
    manifest.limits = sandbox_create_limits(request, &policy.max_limits);
    manifest
}

/// A settled sandbox created through the ordinary authoritative transaction.
fn created(native: &DfsTestRuntime, backend: &mut ClusteredSandboxBackend) -> SandboxId {
    let sandbox_id = SandboxId::generate();
    backend
        .create(create_request(&sandbox_id, &SandboxToolchainId::generate()))
        .expect("ordinary create");
    seal(native);
    sandbox_id
}

/// A create fence left by a writer that stopped before settling it.
fn fenced_create(native: &DfsTestRuntime, backend: &ClusteredSandboxBackend) -> SandboxId {
    let sandbox_id = SandboxId::generate();
    store(backend)
        .create_manifest(&create_candidate(
            backend,
            &create_request(&sandbox_id, &SandboxToolchainId::generate()),
        ))
        .unwrap();
    seal(native);
    sandbox_id
}

fn pending_operation(
    backend: &ClusteredSandboxBackend,
    sandbox_id: &SandboxId,
) -> Option<SandboxAuthoritativeMutationOperation> {
    match store(backend).load_allow_pending_reconciliation(sandbox_id) {
        Ok(manifest) => manifest
            .pending_reconciliation
            .map(|pending| pending.operation),
        Err(SandboxStoreError::NotFound { .. }) => None,
        Err(error) => panic!("unexpected sandbox state for {sandbox_id}: {error}"),
    }
}

fn index_record(
    backend: &ClusteredSandboxBackend,
    sandbox_id: &SandboxId,
) -> Option<SandboxPendingFenceRecord> {
    let key = sandbox_pending_fence_key(sandbox_id);
    backend
        .block_on_cluster(backend.cluster.kv_get_authoritative_async(&key))
        .unwrap()
        .unwrap()
        .value
        .map(|entry| entry.decode().unwrap())
}

/// Seal the fixture's bounded RPC observation window between batches so
/// close-time coverage verification sees every record.
fn seal(native: &DfsTestRuntime) {
    native
        .checkpoint_rpc_observation_window()
        .expect("observation window stayed within its bound");
}

fn lifecycle_events(
    backend: &ClusteredSandboxBackend,
    kind: &str,
    sandbox_id: &SandboxId,
) -> usize {
    let store = backend.event_store();
    if !backend
        .data_dir
        .root()
        .join("events/event-log.jsonl")
        .exists()
    {
        return 0;
    }
    store
        .replay(sinomo_core::events::ReplayQuery::all())
        .unwrap()
        .into_iter()
        .filter(|stored| {
            stored.event.kind.as_str() == kind
                && stored.event.payload["sandbox_id"] == sandbox_id.to_string()
        })
        .count()
}

fn update_request(sandbox_id: &SandboxId) -> SandboxUpdateInstanceRequest {
    SandboxUpdateInstanceRequest {
        sandbox_id: sandbox_id.clone(),
        cwd: None,
        mounts: None,
        env_set: BTreeMap::from([("RECOVERED".to_string(), "1".to_string())]),
        env_unset: Vec::new(),
    }
}

fn write_value(
    native: &DfsTestRuntime,
    backend: &mut ClusteredSandboxBackend,
    sandbox_id: &SandboxId,
    value: &[u8],
) {
    backend
        .write(SandboxWriteRequest {
            sandbox_id: sandbox_id.clone(),
            path: SandboxPath::new("/work/value.txt").unwrap(),
            bytes: value.to_vec(),
        })
        .expect("ordinary workspace write");
    seal(native);
}

fn read_value(
    native: &DfsTestRuntime,
    backend: &mut ClusteredSandboxBackend,
    sandbox_id: &SandboxId,
) -> Vec<u8> {
    let bytes = backend
        .read(SandboxReadRequest {
            sandbox_id: sandbox_id.clone(),
            path: SandboxPath::new("/work/value.txt").unwrap(),
            range: None,
        })
        .expect("ordinary workspace read")
        .bytes;
    seal(native);
    bytes
}

fn checkpoint(
    native: &DfsTestRuntime,
    backend: &mut ClusteredSandboxBackend,
    sandbox_id: &SandboxId,
) -> SandboxCheckpointId {
    let checkpoint_id = backend
        .checkpoint(SandboxCheckpointRequest {
            sandbox_id: sandbox_id.clone(),
        })
        .expect("ordinary checkpoint")
        .checkpoint
        .checkpoint_id;
    seal(native);
    checkpoint_id
}

/// Leave a legacy directory instance mid-migration behind its fence.
fn pending_legacy_migration(
    native: &DfsTestRuntime,
    backend: &ClusteredSandboxBackend,
) -> SandboxId {
    let sandbox_id = SandboxId::generate();
    let mut store = store(backend);
    let stable = sandbox_instance_root(&sandbox_id);
    let mut legacy =
        SandboxInstanceManifest::new(sandbox_id.clone(), SandboxToolchainId::generate(), now())
            .unwrap();
    legacy.incarnation_id = None;
    let options =
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![store.dfs.node_id().clone()]);
    for name in ["root", "home", "tmp/0", "work", "var", "logs", "artifacts"] {
        store
            .dfs
            .posix_mkdir_all(&format!("{stable}/{name}"))
            .unwrap();
    }
    store
        .dfs
        .posix_write_file(
            &format!("{stable}/manifest.sobj"),
            &encode_sandbox_instance_manifest(&legacy).unwrap(),
            options,
        )
        .unwrap();
    store.migrate_legacy_instance(&sandbox_id, now()).unwrap();
    seal(native);
    sandbox_id
}

fn start_worker(
    native: &DfsTestRuntime,
    node: &NodeId,
    contention_retry_delays: Vec<Duration>,
) -> (
    SandboxLifecycleRecoveryScheduler,
    tokio::sync::watch::Sender<bool>,
) {
    let context = native.component_context(node).unwrap();
    let (scheduler, worker) = sandbox_lifecycle_recovery(
        Arc::new(native.data_dir(node).clone()),
        context.cluster_access().unwrap(),
        native.runtime_handle(),
    );
    let (stop, stopped) = tokio::sync::watch::channel(false);
    native.runtime_handle().spawn(
        worker
            .with_contention_retry_delays(contention_retry_delays)
            .run(stopped),
    );
    (scheduler, stop)
}

fn commit(backend: &ClusteredSandboxBackend, success: Vec<sinomo_core::kv::KvOp>) {
    let request = sinomo_core::kv::KvTxnRequest::generated(sinomo_core::kv::KvTxn {
        compares: Vec::new(),
        success,
        failure: Vec::new(),
    });
    backend
        .block_on_cluster(backend.cluster.kv_commit_compact_async(request))
        .unwrap()
        .unwrap();
}

/// Block until a committed delta on `node` deletes `key`.
fn wait_for_committed_delete(
    native: &DfsTestRuntime,
    node: &NodeId,
    key: String,
    action: impl FnOnce(),
) {
    let observed = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let callback = Arc::clone(&observed);
    let _subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_deltas(
        &sinomo_core::network::ClusterPaths::new(native.data_dir(node)),
        move |delta| {
            if delta.events.iter().any(|event| {
                event.key == key && matches!(event.kind, sinomo_core::kv::KvEventKind::Delete)
            }) {
                *callback.0.lock().unwrap() = true;
                callback.1.notify_all();
            }
        },
    );
    action();
    let (deleted, changed) = &*observed;
    let (deleted, _) = changed
        .wait_timeout_while(deleted.lock().unwrap(), WAIT, |deleted| !*deleted)
        .unwrap();
    assert!(*deleted, "the expected committed delete was not observed");
}

#[test]
fn mutation_open_finishes_every_pending_transaction_kind() {
    let (native, nodes) = DfsTestRuntime::start_cluster("sandbox-recover-on-mutation", 1).unwrap();
    let mut backend = backend(&native, &nodes[0]);

    let create = fenced_create(&native, &backend);

    let resume = created(&native, &mut backend);
    write_value(&native, &mut backend, &resume, b"checkpoint");
    let checkpoint_id = checkpoint(&native, &mut backend, &resume);
    write_value(&native, &mut backend, &resume, b"newer");
    store(&backend)
        .resume_checkpoint(&resume, &checkpoint_id, now())
        .unwrap();
    seal(&native);

    let reboot = created(&native, &mut backend);
    let epoch = store(&backend).load(&reboot).unwrap().temp_epoch;
    store(&backend).reboot_session(&reboot, now()).unwrap();
    seal(&native);

    let destroy = created(&native, &mut backend);
    store(&backend).prepare_destroy(&destroy, now()).unwrap();
    seal(&native);

    let migration = pending_legacy_migration(&native, &backend);

    for sandbox_id in [&create, &resume, &reboot, &destroy, &migration] {
        assert!(
            pending_operation(&backend, sandbox_id).is_some(),
            "fixture leaves {sandbox_id} fenced"
        );
        assert!(
            index_record(&backend, sandbox_id).is_none(),
            "fixture fences carry no index entry, like those of older binaries"
        );
    }
    seal(&native);

    for sandbox_id in [&create, &resume, &reboot, &migration] {
        let updated = backend
            .update(update_request(sandbox_id))
            .expect("a mutation finishes the pending transaction and then applies");
        assert!(updated.manifest.pending_reconciliation.is_none());
        assert_eq!(updated.manifest.env["RECOVERED"], "1");
        seal(&native);
    }
    let destroyed = backend.update(update_request(&destroy));
    assert!(
        matches!(
            destroyed,
            Err(SandboxBackendError::Store(
                SandboxStoreError::NotFound { .. }
            ))
        ),
        "a pending destroy finishes before the mutation sees its target: {destroyed:?}"
    );
    seal(&native);

    assert_eq!(read_value(&native, &mut backend, &resume), b"checkpoint");
    assert_eq!(
        store(&backend).load(&reboot).unwrap().temp_epoch,
        epoch + 1,
        "the pending reboot finishes exactly once"
    );
    for (kind, sandbox_id) in [
        ("sandbox.instance.created", &create),
        ("sandbox.instance.resumed", &resume),
        ("sandbox.instance.rebooted", &reboot),
        ("sandbox.instance.destroyed", &destroy),
        ("sandbox.instance.migrated", &migration),
    ] {
        assert_eq!(
            lifecycle_events(&backend, kind, sandbox_id),
            1,
            "{kind} is durably audited once for {sandbox_id}"
        );
        assert!(index_record(&backend, sandbox_id).is_none());
    }
    assert!(
        backend.marked_lifecycle_operations().unwrap().is_empty(),
        "settled recoveries retire the obligations they took over"
    );
    drop(backend);
    native.close().unwrap();
}

#[test]
fn transaction_open_resumes_its_own_pending_operation_and_finishes_others() {
    let (native, nodes) =
        DfsTestRuntime::start_cluster("sandbox-recover-on-transaction", 1).unwrap();
    let mut backend = backend(&native, &nodes[0]);

    // Same operation: the request continues the pending transaction itself.
    let same_create = SandboxId::generate();
    let request = create_request(&same_create, &SandboxToolchainId::generate());
    store(&backend)
        .create_manifest(&create_candidate(&backend, &request))
        .unwrap();
    let resumed_create = backend
        .create(request)
        .expect("a matching create resumes its own pending create");
    assert!(resumed_create.manifest.pending_reconciliation.is_none());
    assert_eq!(
        lifecycle_events(&backend, "sandbox.instance.created", &same_create),
        1
    );
    seal(&native);

    let same_reboot = created(&native, &mut backend);
    let epoch = store(&backend).load(&same_reboot).unwrap().temp_epoch;
    store(&backend).reboot_session(&same_reboot, now()).unwrap();
    seal(&native);
    let rebooted = backend
        .reboot(SandboxRebootRequest {
            sandbox_id: same_reboot.clone(),
        })
        .unwrap();
    seal(&native);
    assert_eq!(
        rebooted.manifest.temp_epoch,
        epoch + 1,
        "a reboot request completes the pending reboot instead of adding another"
    );
    assert_eq!(
        lifecycle_events(&backend, "sandbox.instance.rebooted", &same_reboot),
        1
    );

    let same_resume = created(&native, &mut backend);
    write_value(&native, &mut backend, &same_resume, b"first");
    let first = checkpoint(&native, &mut backend, &same_resume);
    store(&backend)
        .resume_checkpoint(&same_resume, &first, now())
        .unwrap();
    seal(&native);
    backend
        .resume(SandboxResumeRequest {
            sandbox_id: same_resume.clone(),
            checkpoint_id: first,
        })
        .unwrap();
    seal(&native);
    assert_eq!(
        lifecycle_events(&backend, "sandbox.instance.resumed", &same_resume),
        1
    );

    // Different operation: the pending transaction finishes first.
    let other_resume = created(&native, &mut backend);
    let first = checkpoint(&native, &mut backend, &other_resume);
    write_value(&native, &mut backend, &other_resume, b"second");
    let second = checkpoint(&native, &mut backend, &other_resume);
    store(&backend)
        .resume_checkpoint(&other_resume, &first, now())
        .unwrap();
    seal(&native);
    backend
        .resume(SandboxResumeRequest {
            sandbox_id: other_resume.clone(),
            checkpoint_id: second,
        })
        .expect("a different checkpoint resumes after the pending one finishes");
    seal(&native);
    assert_eq!(read_value(&native, &mut backend, &other_resume), b"second");
    assert_eq!(
        lifecycle_events(&backend, "sandbox.instance.resumed", &other_resume),
        2
    );

    let destroy_pending_create = fenced_create(&native, &backend);
    backend
        .destroy(SandboxDestroyRequest {
            sandbox_id: destroy_pending_create.clone(),
        })
        .expect("destroy finishes the pending create, then destroys");
    seal(&native);
    assert_eq!(
        lifecycle_events(
            &backend,
            "sandbox.instance.created",
            &destroy_pending_create
        ),
        1
    );
    assert_eq!(
        lifecycle_events(
            &backend,
            "sandbox.instance.destroyed",
            &destroy_pending_create
        ),
        1
    );
    assert!(matches!(
        store(&backend).load_allow_pending_reconciliation(&destroy_pending_create),
        Err(SandboxStoreError::NotFound { .. })
    ));

    let reboot_pending_destroy = created(&native, &mut backend);
    store(&backend)
        .prepare_destroy(&reboot_pending_destroy, now())
        .unwrap();
    seal(&native);
    let rebooted = backend.reboot(SandboxRebootRequest {
        sandbox_id: reboot_pending_destroy.clone(),
    });
    seal(&native);
    assert!(
        matches!(
            rebooted,
            Err(SandboxBackendError::Store(
                SandboxStoreError::NotFound { .. }
            ))
        ),
        "the pending destroy wins before a reboot can start: {rebooted:?}"
    );
    for sandbox_id in [
        &same_create,
        &same_reboot,
        &same_resume,
        &other_resume,
        &destroy_pending_create,
        &reboot_pending_destroy,
    ] {
        assert!(
            index_record(&backend, sandbox_id).is_none(),
            "no index entry outlives a settled or abandoned transaction for {sandbox_id}"
        );
    }
    assert!(backend.marked_lifecycle_operations().unwrap().is_empty());
    drop(backend);
    native.close().unwrap();
}

#[test]
fn pending_fence_index_entry_follows_the_fence() {
    let (native, nodes) = DfsTestRuntime::start_cluster("sandbox-fence-index", 2).unwrap();
    let mut first = backend(&native, &nodes[0]);
    let mut second = backend(&native, &nodes[1]);

    let settled = created(&native, &mut first);
    assert!(
        index_record(&first, &settled).is_none(),
        "a committed transaction retires its index entry"
    );
    assert!(first.marked_lifecycle_operations().unwrap().is_empty());

    // A rejected transaction leaves no fence and retires what it recorded.
    let mut denied = create_request(&SandboxId::generate(), &SandboxToolchainId::generate());
    denied.memory_mb = Some(u64::MAX);
    let denied_id = denied.sandbox_id.clone().unwrap();
    assert!(first.create(denied).is_err(), "policy rejects the create");
    seal(&native);
    assert!(index_record(&first, &denied_id).is_none());
    assert!(first.marked_lifecycle_operations().unwrap().is_empty());

    // A node that stops between its fence and its settlement leaves both
    // obligations naming itself and its lease session.
    let interrupted = SandboxId::generate();
    let (lease, mut fenced, _) = first
        .open_transaction(&interrupted, SandboxAuthoritativeTransaction::Create)
        .unwrap();
    let session = lease.session_id().to_string();
    fenced
        .create_manifest(&create_candidate(
            &first,
            &create_request(&interrupted, &SandboxToolchainId::generate()),
        ))
        .unwrap();
    drop(fenced);
    drop(lease);
    seal(&native);
    let record = index_record(&second, &interrupted).expect("index entry replicated");
    assert_eq!(
        record,
        SandboxPendingFenceRecord {
            sandbox_id: interrupted.clone(),
            node_id: nodes[0].clone(),
            session_id: session,
        }
    );
    assert!(record.session_id.starts_with("sandbox-lifecycle:"));
    assert_eq!(
        first.marked_lifecycle_operations().unwrap(),
        BTreeSet::from([interrupted.clone()])
    );

    // Another node finishes it on access and retires the shared entry.
    second
        .update(update_request(&interrupted))
        .expect("the remote mutation finishes the interrupted create");
    seal(&native);
    assert!(pending_operation(&second, &interrupted).is_none());
    assert!(index_record(&first, &interrupted).is_none());
    assert!(second.marked_lifecycle_operations().unwrap().is_empty());

    // The original node's own marker is settled under the lease once.
    assert_eq!(
        first.recover_lifecycle_operation(&interrupted).unwrap(),
        ClusteredSandboxRecoveryAttempt::Settled
    );
    seal(&native);
    assert!(first.marked_lifecycle_operations().unwrap().is_empty());
    assert_eq!(
        lifecycle_events(&first, "sandbox.instance.created", &interrupted)
            + lifecycle_events(&second, "sandbox.instance.created", &interrupted),
        1
    );
    drop((first, second));
    native.close().unwrap();
}

#[test]
fn lease_contention_defers_recovery_without_touching_the_fence() {
    let (native, nodes) = DfsTestRuntime::start_cluster("sandbox-recovery-contention", 2).unwrap();
    let holder = backend(&native, &nodes[0]);
    let mut contender = backend(&native, &nodes[1]);
    let sandbox_id = fenced_create(&native, &holder);
    let (lease_dfs, _, candidates) = holder.open_lifecycle_services().unwrap();
    let held = start_clustered_sandbox_lifecycle_lease(lease_dfs, &sandbox_id, &candidates)
        .expect("first node holds the lifecycle lease");

    assert!(matches!(
        contender.recover_lifecycle_operation(&sandbox_id).unwrap(),
        ClusteredSandboxRecoveryAttempt::Deferred {
            retry_after_unix_nanos: None
        }
    ));
    assert!(
        contender.update(update_request(&sandbox_id)).is_err(),
        "a mutation cannot open while another node holds the lease"
    );
    seal(&native);
    let (scheduler, stop) = start_worker(
        &native,
        &nodes[1],
        vec![Duration::from_millis(20), Duration::from_millis(20)],
    );
    scheduler.recover_observed(sandbox_id.clone());
    assert!(
        scheduler.wait_idle(WAIT),
        "the follow-up schedule is bounded while the lease stays held"
    );
    seal(&native);
    assert_eq!(
        pending_operation(&contender, &sandbox_id),
        Some(SandboxAuthoritativeMutationOperation::Create),
        "deferred recovery leaves the fence to its holder"
    );
    assert!(contender.marked_lifecycle_operations().unwrap().is_empty());

    drop(held);
    scheduler.recover_observed(sandbox_id.clone());
    assert!(scheduler.wait_idle(WAIT));
    seal(&native);
    assert!(pending_operation(&contender, &sandbox_id).is_none());
    assert_eq!(
        lifecycle_events(&contender, "sandbox.instance.created", &sandbox_id),
        1
    );
    assert_eq!(
        lifecycle_events(&holder, "sandbox.instance.created", &sandbox_id),
        0
    );
    stop.send(true).unwrap();
    drop((holder, contender));
    native.close().unwrap();
}

#[test]
fn membership_drop_hands_an_orphaned_fence_to_one_live_node() {
    let name = "sandbox-orphaned-fence";
    let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
    let mut native = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        native.define_node(node.clone()).unwrap();
    }
    native
        .configure_component_factories(vec![Arc::new(crate::component_factory().unwrap())])
        .unwrap();
    native.start_nodes().unwrap();
    let backends = nodes
        .iter()
        .map(|node| backend(&native, node))
        .collect::<Vec<_>>();

    // A member that published a fence and then left the cluster.
    let departed = NodeId::from_legacy_named(format!("{name}-departed")).unwrap();
    let mut member: sinomo_core::ClusterMemberRecord = backends[0]
        .cluster
        .kv_get_local(&sinomo_core::node::cluster_member_key(&nodes[2]))
        .unwrap()
        .value
        .expect("fixture member record")
        .decode()
        .unwrap();
    member.node_id = departed.clone();
    member.display_name = "departed".to_string();
    commit(
        &backends[0],
        vec![
            sinomo_core::kv::KvOp::put_serde(
                sinomo_core::node::cluster_member_key(&departed),
                &member,
                None,
            )
            .unwrap(),
        ],
    );
    let orphan = fenced_create(&native, &backends[1]);
    backends[1]
        .publish_pending_fence_index(&SandboxPendingFenceRecord {
            sandbox_id: orphan.clone(),
            node_id: departed.clone(),
            session_id: "sandbox-lifecycle:departed".to_string(),
        })
        .unwrap();
    // A fence whose publisher is still a member stays with that member.
    let owned = fenced_create(&native, &backends[1]);
    backends[1]
        .publish_pending_fence_index(&SandboxPendingFenceRecord {
            sandbox_id: owned.clone(),
            node_id: nodes[2].clone(),
            session_id: "sandbox-lifecycle:live".to_string(),
        })
        .unwrap();
    assert!(
        backends
            .iter()
            .all(|backend| backend.orphaned_lifecycle_operations().unwrap().is_empty()),
        "no fence is orphaned while its publisher is a member"
    );
    seal(&native);

    wait_for_committed_delete(
        &native,
        &nodes[0],
        sandbox_pending_fence_key(&orphan),
        || {
            commit(
                &backends[0],
                vec![sinomo_core::kv::KvOp::Delete {
                    key: sinomo_core::node::cluster_member_key(&departed),
                }],
            )
        },
    );
    seal(&native);

    assert!(pending_operation(&backends[0], &orphan).is_none());
    assert_eq!(
        backends
            .iter()
            .map(|backend| lifecycle_events(backend, "sandbox.instance.created", &orphan))
            .sum::<usize>(),
        1,
        "exactly one live node recovers the orphaned fence"
    );
    assert_eq!(
        pending_operation(&backends[0], &owned),
        Some(SandboxAuthoritativeMutationOperation::Create),
        "a member's fence is not taken over by a membership drop"
    );
    assert_eq!(
        index_record(&backends[0], &owned).map(|record| record.node_id),
        Some(nodes[2].clone())
    );
    drop(backends);
    native.close().unwrap();
}

#[test]
fn host_ready_recovers_only_marked_work_and_observed_fences_recover_on_access() {
    let name = "sandbox-startup-recovery";
    let nodes = DfsTestRuntime::ordered_node_ids(name, 2);
    let mut native = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    native.define_node(nodes[1].clone()).unwrap();
    native
        .configure_component_factories(vec![Arc::new(crate::component_factory().unwrap())])
        .unwrap();
    native.start_nodes().unwrap();
    let node = nodes[1].clone();
    let restarting = backend(&native, &node);

    let marked = SandboxId::generate();
    let (lease, mut fenced, _) = restarting
        .open_transaction(&marked, SandboxAuthoritativeTransaction::Create)
        .unwrap();
    fenced
        .create_manifest(&create_candidate(
            &restarting,
            &create_request(&marked, &SandboxToolchainId::generate()),
        ))
        .unwrap();
    drop((fenced, lease));
    seal(&native);
    // A fence from an older binary has neither marker nor index entry.
    let legacy = fenced_create(&native, &restarting);
    drop(restarting);

    native.stop_node(&node).unwrap();
    native.reopen_node(node.clone()).unwrap();
    let scheduler = test_recovery_scheduler(&native.data_dir(&node).root())
        .expect("remounted Sandbox component scheduler");
    assert!(scheduler.wait_idle(WAIT));
    seal(&native);
    let mut restarted = backend(&native, &node).with_lifecycle_recovery(scheduler.clone());
    assert!(pending_operation(&restarted, &marked).is_none());
    assert!(restarted.marked_lifecycle_operations().unwrap().is_empty());
    assert!(index_record(&restarted, &marked).is_none());
    assert_eq!(
        pending_operation(&restarted, &legacy),
        Some(SandboxAuthoritativeMutationOperation::Create),
        "startup reads only the marker folder; it never scans sandbox state"
    );

    let observed = restarted.status(SandboxStatusRequest {
        sandbox_id: legacy.clone(),
    });
    assert!(matches!(
        observed,
        Err(SandboxBackendError::Store(
            SandboxStoreError::ReconciliationPending { .. }
        ))
    ));
    assert!(scheduler.wait_idle(WAIT));
    seal(&native);
    assert!(
        restarted
            .status(SandboxStatusRequest {
                sandbox_id: legacy.clone()
            })
            .unwrap()
            .manifest
            .pending_reconciliation
            .is_none(),
        "the observed legacy fence recovers in the background"
    );
    assert_eq!(
        lifecycle_events(&restarted, "sandbox.instance.created", &legacy),
        1
    );
    drop(restarted);
    native.close().unwrap();
}

#[test]
fn obligation_free_recovery_triggers_do_not_rebind_a_current_primary() {
    let name = "sandbox-native-quiet-recovery";
    let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
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
    drop(writer);
    let data_dir = native.data_dir(&primary).clone();
    let marker = data_dir
        .dfs_dir()
        .join("metadata")
        .join(&current.shard_id)
        .join("journal/membership-handoff-write-freeze.sobj");
    let before = std::fs::read(&marker).unwrap();
    let before_mark = std::fs::metadata(&marker).unwrap().modified().unwrap();
    #[cfg(unix)]
    let content_shard = data_dir.dfs_dir().join("content/content-reservations/00");
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
    let (scheduler, stop) = start_worker(&native, &primary, Vec::new());
    scheduler.recover_marked();
    scheduler.recover_orphaned();
    assert!(scheduler.wait_idle(WAIT));
    stop.send(true).unwrap();
    #[cfg(unix)]
    let content_after = std::fs::metadata(&content_shard)
        .unwrap()
        .modified()
        .unwrap();
    let after = std::fs::read(&marker).unwrap();
    let after_mark = std::fs::metadata(&marker).unwrap().modified().unwrap();
    native.close().unwrap();
    assert_eq!(
        after, before,
        "recovery triggers without obligations must not advance an activated primary's durable rebind checkpoint"
    );
    assert_eq!(after_mark, before_mark);
    #[cfg(unix)]
    assert_eq!(
        content_after, content_mark,
        "recovery triggers without obligations must not stage a content sentinel"
    );
}

#[test]
fn orphan_evaluation_reads_the_committed_index_and_rejects_malformed_entries() {
    let (native, nodes) = DfsTestRuntime::start_cluster("sandbox-orphan-index", 2).unwrap();
    let writer = backend(&native, &nodes[0]);
    let reader = backend(&native, &nodes[1]);
    let departed = NodeId::from_legacy_named("sandbox-orphan-index-departed").unwrap();
    let orphan = SandboxId::generate();
    let owned = SandboxId::generate();
    for (sandbox_id, node_id) in [(&orphan, &departed), (&owned, &nodes[0])] {
        writer
            .publish_pending_fence_index(&SandboxPendingFenceRecord {
                sandbox_id: sandbox_id.clone(),
                node_id: node_id.clone(),
                session_id: "sandbox-lifecycle:fixture".to_string(),
            })
            .unwrap();
    }
    assert_eq!(
        reader.orphaned_lifecycle_operations().unwrap(),
        vec![orphan.clone()]
    );
    assert_eq!(
        writer.orphaned_lifecycle_operations().unwrap(),
        vec![orphan.clone()]
    );
    reader
        .recovery_state()
        .mark_authoritative_recovery_pending(&owned)
        .unwrap();

    // A malformed index entry is uncertainty, never an empty result.
    commit(
        &writer,
        vec![sinomo_core::kv::KvOp::Put {
            key: sandbox_pending_fence_key(&SandboxId::generate()),
            value: b"not a pending fence record".to_vec(),
            lease_id: None,
        }],
    );
    let unknown = reader.orphaned_lifecycle_operations();
    assert!(
        unknown.is_err(),
        "invalid index data cannot become an empty result: {unknown:?}"
    );
    assert_eq!(
        reader.marked_lifecycle_operations().unwrap(),
        BTreeSet::from([owned])
    );
    drop((writer, reader));
    native.close().unwrap();
}
