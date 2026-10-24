use super::*;

pub(crate) fn daemon_dfs_maintenance_notify(
    data_dir: &SinomoDataDir,
) -> Result<Arc<tokio::sync::Notify>, DaemonError> {
    let host = dfs_runtime_host(data_dir)?;
    Ok(Arc::clone(&host.maintenance_notify))
}

pub(crate) fn notify_daemon_dfs_maintenance_work(data_dir: &SinomoDataDir) {
    let Ok(notify) = daemon_dfs_maintenance_notify(data_dir) else {
        return;
    };
    metrics::counter!("sinomo.dfs.background.wakes", "worker" => "maintenance").increment(1);
    #[cfg(test)]
    DAEMON_DFS_MAINTENANCE_WAKE_COUNT.fetch_add(1, Ordering::Relaxed);
    notify.notify_one();
}

pub(crate) fn notify_daemon_dfs_maintenance_work_for_paths(paths: &DaemonPaths) {
    if let Ok(data_dir) = paths.data_dir() {
        notify_daemon_dfs_maintenance_work(&data_dir);
    }
}

static RECONCILIATION_COUNTERS: std::sync::OnceLock<Mutex<BTreeMap<PathBuf, u64>>> =
    std::sync::OnceLock::new();

#[derive(Clone, Debug)]
pub(super) struct DaemonDfsReconciliationRequest {
    generation: u64,
    reconciled_authority_generation: Option<u64>,
    compact_namespace_jobs: Vec<String>,
    namespace_work: BTreeMap<String, Arc<NamespaceReconciliationWork>>,
}

struct NamespaceReconciliationWork {
    owner: Arc<DfsDataOwnerLock>,
    running: AtomicBool,
    blocks_ordinary_maintenance_while_pending: bool,
    action: Arc<dyn Fn() -> Result<bool, DfsRuntimeError> + Send + Sync>,
}
impl std::fmt::Debug for NamespaceReconciliationWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceReconciliationWork")
            .finish_non_exhaustive()
    }
}

/// Retain bounded current namespace work in the existing maintenance request.
/// This schedules work only; its action still needs normal effect authorization.
pub fn queue_namespace_reconciliation_work(
    data_dir: &SinomoDataDir,
    owner: Arc<DfsDataOwnerLock>,
    binding: String,
    blocks_ordinary_maintenance_while_pending: bool,
    action: Arc<dyn Fn() -> Result<bool, DfsRuntimeError> + Send + Sync>,
) -> Result<(), DfsRuntimeError> {
    let paths = DfsRuntimePaths::new(data_dir);
    let current = require_daemon_dfs_data_owner_lock(&paths)?;
    let host = registered_dfs_runtime_host(data_dir)?.ok_or_else(|| DfsRuntimeError::Parse {
        detail: "namespace reconciliation has no registered runtime".into(),
    })?;
    if !Arc::ptr_eq(&current, &owner)
        || host.shutting_down.is_cancelled()
        || binding.is_empty()
        || binding.len() > 512
    {
        return Err(DfsRuntimeError::Parse {
            detail: "namespace reconciliation does not own the current live scope".into(),
        });
    }
    request_reconciliation(
        &paths,
        Vec::new(),
        Some((
            binding,
            Arc::new(NamespaceReconciliationWork {
                owner,
                running: AtomicBool::new(false),
                blocks_ordinary_maintenance_while_pending,
                action,
            }),
        )),
    )?;
    Ok(())
}

/// Wake ordinary namespace maintenance after current durable job publication.
/// A wake grants no effect permission and retains no fabricated task or vote.
pub fn wake_namespace_reconciliation(
    data_dir: &SinomoDataDir,
    owner: &Arc<DfsDataOwnerLock>,
) -> Result<(), DfsRuntimeError> {
    let paths = DfsRuntimePaths::new(data_dir);
    let current = require_daemon_dfs_data_owner_lock(&paths)?;
    if !Arc::ptr_eq(&current, owner) {
        return Err(DfsRuntimeError::Parse {
            detail: "namespace wake lost its actual owner".into(),
        });
    }
    request_daemon_dfs_maintenance_reconciliation(&paths)?;
    Ok(())
}

pub(super) struct NamespaceReconciliationRun {
    pub(super) complete: bool,
    pub(super) blocks_ordinary_maintenance: bool,
}
pub(super) fn run_namespace_reconciliation_work(
    paths: &DaemonPaths,
) -> Result<NamespaceReconciliationRun, DaemonError> {
    let root = daemon_dfs_maintenance_reconciliation_data_root(paths)?;
    let work = DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&root)
        .map(|request| request.namespace_work.clone())
        .unwrap_or_default();
    let mut complete = true;
    let mut blocks_ordinary_maintenance = false;
    for (binding, work) in work {
        let current = require_daemon_dfs_data_owner_lock(paths)?;
        if !Arc::ptr_eq(&current, &work.owner) {
            return Err(DaemonError::Parse {
                detail: "namespace work lost actual storage custody".into(),
            });
        }
        if work.running.swap(true, Ordering::AcqRel) {
            blocks_ordinary_maintenance |= work.blocks_ordinary_maintenance_while_pending;
            complete = false;
            continue;
        }
        struct Running(Arc<NamespaceReconciliationWork>);
        impl Drop for Running {
            fn drop(&mut self) {
                self.0.running.store(false, Ordering::Release);
            }
        }
        let _running = Running(Arc::clone(&work));
        let finished = (work.action)()?;
        if finished {
            let mut requests = DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS
                .get_or_init(Default::default)
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(request) = requests.get_mut(&root)
                && request
                    .namespace_work
                    .get(&binding)
                    .is_some_and(|current| Arc::ptr_eq(current, &work))
            {
                request.namespace_work.remove(&binding);
            }
        } else {
            blocks_ordinary_maintenance |= work.blocks_ordinary_maintenance_while_pending;
            complete = false;
        }
    }
    Ok(NamespaceReconciliationRun {
        complete,
        blocks_ordinary_maintenance,
    })
}

fn daemon_dfs_maintenance_reconciliation_data_root(
    paths: &DaemonPaths,
) -> Result<PathBuf, DaemonError> {
    daemon_data_dir(paths).map(|path| path.to_path_buf())
}

/// Request one authoritative metadata reconciliation before the next bounded
/// repair pass. Requests are generation-counted so a manual request arriving
/// during a running pass cannot be cleared by the older completion.
pub(crate) fn request_daemon_dfs_maintenance_reconciliation(
    paths: &DaemonPaths,
) -> Result<u64, DaemonError> {
    request_daemon_dfs_maintenance_reconciliation_with_namespace_jobs(paths, Vec::new())
}

pub(crate) fn request_daemon_dfs_maintenance_reconciliation_with_namespace_jobs(
    paths: &DaemonPaths,
    compact_namespace_jobs: Vec<String>,
) -> Result<u64, DaemonError> {
    request_reconciliation(paths, compact_namespace_jobs, None)
}

fn request_reconciliation(
    paths: &DaemonPaths,
    mut compact_namespace_jobs: Vec<String>,
    work: Option<(String, Arc<NamespaceReconciliationWork>)>,
) -> Result<u64, DaemonError> {
    if compact_namespace_jobs.len() > 32
        || compact_namespace_jobs
            .iter()
            .any(|job| job.is_empty() || job.len() > 512 || job.chars().any(char::is_control))
    {
        return Err(DaemonError::Parse {
            detail:
                "namespace job compaction requires at most 32 nonempty job ids of at most 512 bytes"
                    .into(),
        });
    }
    let data_root = daemon_dfs_maintenance_reconciliation_data_root(paths)?;
    let generation = {
        let mut requests = DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut namespace_work = BTreeMap::new();
        if let Some(previous) = requests.get(&data_root) {
            compact_namespace_jobs.extend(previous.compact_namespace_jobs.iter().cloned());
            namespace_work = previous.namespace_work.clone();
        }
        if let Some((binding, work)) = work {
            namespace_work.entry(binding).or_insert(work);
            if namespace_work.len() > 32 {
                return Err(DaemonError::Parse {
                    detail: "too many retained namespace reconciliation requests".into(),
                });
            }
        }
        compact_namespace_jobs.sort();
        compact_namespace_jobs.dedup();
        if compact_namespace_jobs.len() > 128 {
            return Err(DaemonError::Parse {
                detail: "too many pending namespace job compaction requests".into(),
            });
        }
        let mut counters = RECONCILIATION_COUNTERS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let generation = counters
            .get(&data_root)
            .copied()
            .unwrap_or_default()
            .checked_add(1)
            .ok_or_else(|| DaemonError::Parse {
                detail: "DFS reconciliation generation exhausted".into(),
            })?;
        counters.insert(data_root.clone(), generation);
        requests.insert(
            data_root,
            DaemonDfsReconciliationRequest {
                generation,
                reconciled_authority_generation: None,
                compact_namespace_jobs,
                namespace_work,
            },
        );
        metrics::gauge!("sinomo.dfs.background.reconciliation_roots_pending")
            .set(requests.len() as f64);
        generation
    };
    notify_daemon_dfs_maintenance_work_for_paths(paths);
    Ok(generation)
}

pub(super) fn daemon_dfs_requested_namespace_job_compaction(
    paths: &DaemonPaths,
) -> Option<(u64, Vec<String>)> {
    let root = daemon_dfs_maintenance_reconciliation_data_root(paths).ok()?;
    let requests = DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS
        .get()?
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let request = requests.get(&root)?;
    (!request.compact_namespace_jobs.is_empty())
        .then(|| (request.generation, request.compact_namespace_jobs.clone()))
}

pub(super) fn complete_daemon_dfs_namespace_job_compaction(
    paths: &DaemonPaths,
    generation: u64,
) -> Result<(), DaemonError> {
    let root = daemon_dfs_maintenance_reconciliation_data_root(paths)?;
    if let Some(requests) = DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS.get() {
        let mut requests = requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(request) = requests.get_mut(&root)
            && request.generation == generation
        {
            request.compact_namespace_jobs.clear();
        }
    }
    Ok(())
}

pub(crate) fn daemon_dfs_maintenance_reconciliation_generation(paths: &DaemonPaths) -> Option<u64> {
    let data_root = daemon_dfs_maintenance_reconciliation_data_root(paths).ok()?;
    DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS
        .get()
        .and_then(|requests| {
            requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&data_root)
                .map(|request| request.generation)
        })
}

pub(super) fn daemon_dfs_reconciliation_checkpoint_matches(
    paths: &DaemonPaths,
    request_generation: Option<u64>,
    authority_generation: u64,
) -> bool {
    let Some(request_generation) = request_generation else {
        return false;
    };
    let Ok(data_root) = daemon_dfs_maintenance_reconciliation_data_root(paths) else {
        return false;
    };
    DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS
        .get()
        .is_some_and(|requests| {
            requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&data_root)
                .is_some_and(|request| {
                    request.generation == request_generation
                        && request.reconciled_authority_generation == Some(authority_generation)
                })
        })
}

/// A startup/activation edge reconciles metadata-owned repair obligations with
/// the node-local queue. A previous run's marker cannot prove that a later
/// process stopped after persisting every committed queue handoff. This is not
/// scheduled by reads, status queries, or ordinary idle maintenance wakes.
pub(crate) fn schedule_daemon_dfs_startup_repair_reconciliation(
    paths: &DaemonPaths,
) -> Result<u64, DaemonError> {
    request_daemon_dfs_maintenance_reconciliation(paths)
}

pub(super) fn complete_daemon_dfs_maintenance_reconciliation(
    paths: &DaemonPaths,
    generation: u64,
) -> Result<(), DaemonError> {
    let data_root = daemon_dfs_maintenance_reconciliation_data_root(paths)?;
    if let Some(requests) = DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS.get() {
        let mut requests = requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if requests.get(&data_root).is_some_and(|request| {
            request.generation == generation && request.namespace_work.is_empty()
        }) {
            requests.remove(&data_root);
        }
        metrics::gauge!("sinomo.dfs.background.reconciliation_roots_pending")
            .set(requests.len() as f64);
    }
    Ok(())
}

pub(super) fn record_daemon_dfs_repair_queue_nonempty(data_dir: &SinomoDataDir, nonempty: bool) {
    DAEMON_DFS_REPAIR_QUEUE_NONEMPTY
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(data_dir.root(), nonempty);
}

pub(super) fn refresh_daemon_dfs_repair_queue_nonempty(
    data_dir: &SinomoDataDir,
) -> Result<bool, sinomo_dfs::DfsError> {
    let nonempty =
        sinomo_dfs::maintenance::read_native_repair_attempt_snapshot(data_dir)?
            .is_some_and(|snapshot| !snapshot.records.is_empty());
    record_daemon_dfs_repair_queue_nonempty(data_dir, nonempty);
    Ok(nonempty)
}

/// Return the process-local queue state sampled at startup, a committed repair
/// edge, or the end of the previous bounded maintenance pass.
///
/// Connectivity generation changes use this to wake only a repair executor
/// whose external route blocker may have cleared. An empty cluster must not
/// turn routine route refreshes into broad DFS maintenance passes.
pub(crate) fn daemon_dfs_repair_queue_work_is_known(paths: &DaemonPaths) -> bool {
    let Some(data_root) = paths.root.parent() else {
        return false;
    };
    DAEMON_DFS_REPAIR_QUEUE_NONEMPTY
        .get()
        .and_then(|states| {
            states
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(data_root)
                .copied()
        })
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn daemon_dfs_maintenance_wake_count() -> u64 {
    DAEMON_DFS_MAINTENANCE_WAKE_COUNT.load(Ordering::Relaxed)
}

#[cfg(test)]
pub(super) fn record_daemon_dfs_allocator_cleanup_request(data_dir: &SinomoDataDir) {
    *DAEMON_DFS_TEST_ALLOCATOR_CLEANUP_REQUESTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(data_dir.root())
        .or_default() += 1;
}

#[cfg(test)]
pub(super) fn daemon_dfs_allocator_cleanup_request_count(data_dir: &SinomoDataDir) -> u64 {
    DAEMON_DFS_TEST_ALLOCATOR_CLEANUP_REQUESTS
        .get()
        .and_then(|counts| {
            counts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&data_dir.root())
                .copied()
        })
        .unwrap_or_default()
}

#[cfg(test)]
pub(super) fn record_daemon_dfs_allocator_trim_completion(data_dir: &SinomoDataDir) {
    *DAEMON_DFS_TEST_ALLOCATOR_TRIM_COMPLETIONS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(data_dir.root())
        .or_default() += 1;
}

#[cfg(test)]
pub(super) fn daemon_dfs_allocator_trim_completion_count(data_dir: &SinomoDataDir) -> u64 {
    DAEMON_DFS_TEST_ALLOCATOR_TRIM_COMPLETIONS
        .get()
        .and_then(|counts| {
            counts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&data_dir.root())
                .copied()
        })
        .unwrap_or_default()
}

#[cfg(test)]
pub(super) fn daemon_dfs_local_membership_handoff_keys(
    metadata_shard_statuses: &[DfsMetadataShardStatus],
    local_node_id: &NodeId,
) -> Vec<String> {
    metadata_shard_statuses
        .iter()
        .filter(|status| status.primary_node_id.as_ref() == Some(local_node_id))
        .filter_map(|status| {
            status.file_system_id.as_ref().map(|file_system_id| {
                sinomo_dfs::metadata_shard_membership_handoff_key(
                    file_system_id,
                    &status.shard_id.0,
                )
            })
        })
        .collect()
}

#[cfg(test)]
pub(super) fn daemon_dfs_node_is_offline_for_maintenance(
    availability: &ClusterNodeAvailability,
    connectivity: &ClusterConnectivityGraph,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) -> bool {
    sinomo_dfs::dfs_node_is_offline_for_maintenance(
        availability,
        connectivity,
        local_node_id,
        target_node_id,
    )
}

pub(super) fn daemon_dfs_offline_node_ids(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
) -> BTreeSet<NodeId> {
    let host = cluster_paths
        .root
        .parent()
        .and_then(|root| {
            registered_dfs_runtime_host(&SinomoDataDir::new(root))
                .ok()
                .flatten()
        })
        .filter(|host| !host.shutting_down.is_cancelled());
    sinomo_dfs::maintenance::dfs_offline_node_ids_with_transport_availability(
        cluster_paths,
        state,
        &|target| {
            host.as_ref()
                .map(|host| host.object_rpc.availability(target))
                .unwrap_or(sinomo_core::network::RoutedAvailability::Unknown)
        },
    )
}

pub(super) fn daemon_dfs_maintenance_execution_exclusions(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
) -> DfsMaintenanceExecutionExclusions {
    DfsMaintenanceExecutionExclusions::new(daemon_dfs_offline_node_ids(cluster_paths, state))
}

/// Add newly committed repair identities to the bounded durable queue.
///
/// This runs at the mutation's committed edge, before its live service lane is
/// released. Existing retry/backoff state wins for an already queued target;
/// maintenance remains the sole owner of advancing or clearing that state.
/// The framed append is proportional only to this message's newly committed
/// targets and never reads or rewrites the accumulated queue.
pub(super) fn persist_daemon_dfs_repair_queue_records(
    data_dir: &SinomoDataDir,
    committed_records: Vec<DfsRepairAttemptRecord>,
) -> Result<usize, sinomo_dfs::DfsError> {
    let appended =
        sinomo_dfs::persist_default_dfs_repair_queue_records(data_dir, committed_records)?;
    if appended > 0 {
        record_daemon_dfs_repair_queue_nonempty(data_dir, true);
    }
    Ok(appended)
}

/// Transfer a completed local recovery scan to durable queue ownership before
/// any cancellable network phase. Retrying placement or repair must not replay
/// the whole inventory and requeue its commit promotions on every preemption.
#[tracing::instrument(target = "sinomo.perf", level = "trace", name = "dfs.recovery.handoff",
    skip_all, fields(stage = "dfs.recovery.handoff", outcome = tracing::field::Empty))]
pub(super) fn checkpoint_daemon_dfs_reconciliation(
    data_dir: &SinomoDataDir,
    authority: &DaemonDfsMetadataAuthorityGeneration,
    authority_generation: u64,
    request_generation: Option<u64>,
    records: Vec<DfsRepairAttemptRecord>,
) -> Result<bool, DaemonError> {
    if authority
        .retained_complete_prefix_revision_for_generation(authority_generation)
        .is_none()
    {
        return Ok(false);
    }
    // The existing enqueue protocol preserves retry/backoff for known targets
    // and fsyncs new identities before the volatile request can be completed.
    for chunk in records.chunks(256) {
        persist_daemon_dfs_repair_queue_records(data_dir, chunk.to_vec())?;
    }
    if authority
        .retained_complete_prefix_revision_for_generation(authority_generation)
        .is_none()
    {
        return Ok(false);
    }
    if let Some(generation) = request_generation
        && let Some(requests) = DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS.get()
    {
        let mut requests = requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(request) = requests.get_mut(&data_dir.root())
            && request.generation == generation
        {
            // Keep the request runnable until its later bounded phases finish.
            // Only the expensive scan is checkpointed here, under its exact
            // authority generation. Cancellation can neither erase remaining
            // work nor force this completed inventory scan to repeat.
            request.reconciled_authority_generation = Some(authority_generation);
        }
    }
    metrics::counter!("sinomo.dfs.background.reconciliations_handed_off").increment(1);
    tracing::Span::current().record("outcome", "ok");
    Ok(true)
}

#[cfg(test)]
pub(super) fn fail_next_daemon_dfs_mounted_mutation_after_repair_persist(data_dir: &SinomoDataDir) {
    DAEMON_DFS_TEST_MOUNTED_MUTATION_FAIL_AFTER_REPAIR_PERSIST
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
        .expect("mounted-mutation crash-cut hooks")
        .insert(data_dir.root());
}

#[cfg(test)]
pub(super) fn inject_daemon_dfs_mounted_mutation_crash_cut_after_repair_persist(
    data_dir: &SinomoDataDir,
) -> Result<(), DaemonError> {
    let should_fail = DAEMON_DFS_TEST_MOUNTED_MUTATION_FAIL_AFTER_REPAIR_PERSIST
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
        .expect("mounted-mutation crash-cut hooks")
        .remove(&data_dir.root());
    if should_fail {
        return Err(DaemonError::Io {
            path: data_dir.root().display().to_string(),
            detail: "injected mounted-mutation crash after repair queue persistence".to_string(),
        });
    }
    Ok(())
}

pub(super) struct DaemonMountedSifsMutationHandoffHost<'a> {
    pub(super) data_dir: &'a SinomoDataDir,
    pub(super) cluster_paths: &'a ClusterPaths,
    pub(super) runtime: &'a tokio::runtime::Handle,
    pub(super) traceparent: Option<String>,
}

impl sinomo_dfs::MountedSifsMutationHandoffHost for DaemonMountedSifsMutationHandoffHost<'_> {
    fn repair_queue_persisted(&self, appended_record_count: usize) {
        if appended_record_count > 0 {
            record_daemon_dfs_repair_queue_nonempty(self.data_dir, true);
        }
    }

    fn after_repair_queue_persisted(&self) -> Result<(), sinomo_dfs::DfsError> {
        #[cfg(test)]
        inject_daemon_dfs_mounted_mutation_crash_cut_after_repair_persist(self.data_dir).map_err(
            |error| sinomo_dfs::DfsError::Metadata {
                message: error.to_string(),
            },
        )?;
        Ok(())
    }

    fn committed_work(&self, committed_work: DfsMessageCommittedWork) {
        emit_daemon_dfs_committed_edges(
            self.data_dir,
            self.cluster_paths,
            &DfsMessageExecutionScope::GlobalExclusive,
            self.runtime,
            self.traceparent.clone(),
            committed_work,
            &[],
        );
    }

    fn maintenance_needed(&self) {
        notify_daemon_dfs_maintenance_work(self.data_dir);
    }
}

/// Main-daemon endpoint for the crash-safe mounted-filesystem mutation
/// handoff. Only Arm authors a precondition. Every post-mutation request first
/// attests the adapter's exact fsynced marker phase and payload.
pub(crate) fn handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
    request: DaemonDfsMountedMutationHandoffRequest,
) -> Result<DaemonDfsMountedMutationHandoffResponse, DaemonError> {
    if paths.is_shadow() {
        return Err(DaemonError::Parse {
            detail: "mounted-mutation durability handoff is owned only by the main daemon"
                .to_string(),
        });
    }
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    sinomo_dfs::handle_mounted_sifs_mutation_handoff(
        &data_dir,
        &DaemonMountedSifsMutationHandoffHost {
            data_dir: &data_dir,
            cluster_paths: &cluster_paths,
            runtime,
            traceparent,
        },
        request,
    )
    .map_err(Into::into)
}

/// Count exact completed primary transfers whose durable local marker has not
/// yet been consumed by the promoted router generation.
///
/// This reads one bounded marker and one compact proof per current local
/// primary. It never opens, recovers, or scans a metadata backend. Successful
/// rebind advances `activated_epoch`, durably clearing this retry projection.
#[cfg(test)]
pub(super) fn daemon_dfs_completed_local_primary_handoff_rebind_count(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    pending_handoffs: &[DfsMetadataShardMembershipHandoff],
    active_shards: &[DfsMetadataShardConfig],
    authority_entries: &[KvEntry],
) -> Result<usize, DaemonError> {
    sinomo_dfs::maintenance::dfs_completed_local_primary_handoff_rebind_count_with_layout(
        data_dir, local_node_id, pending_handoffs, active_shards, authority_entries,
        sinomo_dfs::maintenance::selected_native_layout_for_maintenance(data_dir)?.as_deref(),
    )
    .map_err(Into::into)
}

/// Inspect only the bounded durable queues owned by normal reactive healing.
///
/// This function is safe on daemon startup and notification handling: it does
/// not open a DFS metadata backend, enumerate object/shard indexes, or inspect
/// content. Full metadata reconstruction belongs to explicit disaster
/// recovery, not to the automatic scheduler.
pub(super) fn daemon_dfs_mounted_mutation_pending_work(
    data_dir: &SinomoDataDir,
    now_unix_nanos: u64,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    sinomo_dfs::mounted_sifs_mutation_pending_maintenance_work(data_dir, now_unix_nanos)
        .map_err(Into::into)
}

pub(super) fn daemon_dfs_pending_reactive_maintenance_work_from_authority_entries(
    paths: &DaemonPaths,
    authority_entries: &[KvEntry],
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now_unix_nanos = daemon_dfs_now_unix_nanos();
    if !cluster_paths.state.exists() {
        return daemon_dfs_mounted_mutation_pending_work(&data_dir, now_unix_nanos);
    }
    let repair_attempt_records =
        sinomo_dfs::maintenance::read_native_repair_attempt_snapshot(&data_dir)?
            .map(|snapshot| snapshot.records)
            .unwrap_or_default();
    record_daemon_dfs_repair_queue_nonempty(&data_dir, !repair_attempt_records.is_empty());
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DaemonError::from)?;
    sinomo_dfs::dfs_pending_reactive_maintenance_work_from_authority_entries(
        &data_dir,
        &state.local_node_id,
        authority_entries,
        &repair_attempt_records,
        now_unix_nanos,
    )
    .map_err(Into::into)
}

#[tracing::instrument(
    target = "sinomo.perf",
    level = "trace",
    name = "dfs.background.probe",
    skip_all,
    fields(stage = "dfs.background.probe", worker = "maintenance")
)]
pub(crate) fn daemon_dfs_pending_reactive_maintenance_work_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return daemon_dfs_pending_reactive_maintenance_work_from_authority_entries(paths, &[]);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let authority = daemon_dfs_metadata_authority_generation_for_identity(
        &cluster_paths,
        &state.cluster_id,
        &state.local_node_id,
        Some(runtime),
    )?;
    let projection =
        authority
            .current_complete_prefix()
            .ok_or_else(|| sinomo_dfs::DfsError::Network {
                message: "reactive DFS maintenance awaits a complete retained authority projection"
                    .to_string(),
            })?;
    let work = daemon_dfs_pending_reactive_maintenance_work_from_authority_entries(
        paths,
        projection.entries(),
    )?;
    tracing::trace!(target: "sinomo.perf", ?work, "DFS background work classified");
    Ok(work)
}

#[cfg(test)]
pub(crate) fn daemon_dfs_pending_reactive_maintenance_work(
    paths: &DaemonPaths,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let entries = if cluster_paths.state.exists() {
        sinomo_core::cluster::read_cluster_kv_latest_observed_prefix_entries(
            &cluster_paths,
            DAEMON_DFS_METADATA_AUTHORITY_PREFIX,
        )?
    } else {
        Vec::new()
    };
    daemon_dfs_pending_reactive_maintenance_work_from_authority_entries(paths, &entries)
}

/// Full metadata classifier retained for authoritative reconciliation tests.
///
/// Unlike [`daemon_dfs_pending_reactive_maintenance_work`], this opens the
/// cluster-aware metadata image and is therefore O(total metadata). Production
/// daemon scheduling must never call it.
#[cfg(test)]
pub(crate) fn daemon_dfs_pending_maintenance_work(
    paths: &DaemonPaths,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    daemon_dfs_pending_maintenance_work_with_topology(paths, None)
}

/// The same full diagnostic classifier, using the live owner's retained
/// complete configuration instead of reopening its owned Raft files.
#[cfg(test)]
pub(crate) fn daemon_dfs_pending_maintenance_work_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let authority = daemon_dfs_metadata_authority_generation_for_identity(
        &cluster_paths,
        &state.cluster_id,
        &state.local_node_id,
        Some(runtime),
    )?;
    let topology = authority
        .current_complete_prefix()
        .ok_or_else(|| DaemonError::Parse {
            detail: "full native diagnostic awaits a complete retained authority projection".into(),
        })?;
    daemon_dfs_pending_maintenance_work_with_topology(paths, Some(topology))
}

#[cfg(test)]
fn daemon_dfs_pending_maintenance_work_with_topology(
    paths: &DaemonPaths,
    topology: Option<crate::DfsAuthoritativePrefixProjection>,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(DaemonDfsPendingMaintenanceWork::default());
    }
    let now_unix_nanos = daemon_dfs_now_unix_nanos();
    // Routine scheduling observes only indexed metadata work queues and their
    // bounded retry records. Content-store inventory (reservations, manifests,
    // versions, blobs, and orphan observations) is deliberately excluded:
    // those O(total-content) scans belong to authoritative reconciliation, never
    // to daemon startup, notification handling, or an idle safety poll.
    let DaemonDfsMaintenanceMetadataObservation {
        summary,
        metadata_shard_statuses,
        write_intents,
        repair_views,

        receipt_records,
        lookups,
    } = daemon_dfs_maintenance_metadata_observation(&data_dir, &cluster_paths, topology.clone())?;
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DaemonError::from)?;
    let authority_entries = match topology {
        Some(topology) => topology.entries().to_vec(),
        None => sinomo_core::cluster::read_cluster_kv_latest_observed_prefix_entries(
            &cluster_paths,
            DAEMON_DFS_METADATA_AUTHORITY_PREFIX,
        )?,
    };
    let pending_local_membership_handoff_count =
        daemon_dfs_local_membership_handoff_keys(&metadata_shard_statuses, &state.local_node_id)
            .into_iter()
            .filter(|key| authority_entries.iter().any(|entry| &entry.key == key))
            .count();
    let membership_handoffs = authority_entries
        .iter()
        .filter(|entry| crate::dfs_metadata_shard_membership_handoff_key_is_exact(&entry.key))
        .map(|entry| entry.decode::<crate::DfsMetadataShardMembershipHandoff>())
        .collect::<Result<Vec<_>, _>>()?;
    let active_shards = authority_entries
        .iter()
        .filter(|entry| daemon_dfs_metadata_shard_config_key_is_exact(&entry.key))
        .map(|entry| entry.decode::<DfsMetadataShardConfig>())
        .collect::<Result<Vec<_>, _>>()?;
    let completed_local_primary_handoff_rebind_count =
        daemon_dfs_completed_local_primary_handoff_rebind_count(
            &data_dir,
            &state.local_node_id,
            &membership_handoffs,
            &active_shards,
            &authority_entries,
        )?;
    let repair_attempt_records =
        sinomo_dfs::maintenance::read_native_repair_attempt_snapshot(&data_dir)?
            .map(|snapshot| snapshot.records)
            .unwrap_or_default();
    let placement_prefix =
        sinomo_dfs::placement_reconciliations_prefix(&sinomo_dfs::sifs_file_system_id());
    let placement_reconciliation_jobs = authority_entries
        .iter()
        .filter(|entry| entry.key.starts_with(&placement_prefix))
        .map(|entry| entry.decode::<DfsPlacementReconciliationJob>())
        .collect::<Result<Vec<_>, _>>()?;
    let actionable_placement_reconciliation_count =
        daemon_dfs_actionable_placement_reconciliation_count(
            &placement_reconciliation_jobs,
            summary.pending_repair_count,
        );
    let offline_node_ids = daemon_dfs_offline_node_ids(&cluster_paths, &state);
    let intents_without_recovery_source = daemon_dfs_intents_without_recovery_source(
        &data_dir,
        &lookups,
        &write_intents,
        &receipt_records,
        &offline_node_ids,
        &state.local_node_id,
    );
    let repairs_without_recovery_source = daemon_dfs_repairs_without_recovery_source(
        &data_dir,
        &lookups,
        &repair_views,
        &receipt_records,
        &offline_node_ids,
        &state.local_node_id,
    );
    let mut work = daemon_dfs_pending_work_from_records(
        summary.pending_write_count,
        summary.pending_repair_count,
        summary.pending_namespace_job_count,
        actionable_placement_reconciliation_count,
        &write_intents,
        &receipt_records,
        &metadata_shard_statuses,
        &repair_views,
        &repair_attempt_records,
        &intents_without_recovery_source,
        &repairs_without_recovery_source,
        &offline_node_ids,
        &state.local_node_id,
        pending_local_membership_handoff_count,
        now_unix_nanos,
    );
    work.completed_local_primary_handoff_rebind_count =
        completed_local_primary_handoff_rebind_count;
    Ok(work)
}
