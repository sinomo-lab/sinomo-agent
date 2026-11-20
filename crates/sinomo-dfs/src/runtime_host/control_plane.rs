use super::*;

#[cfg(test)]
#[derive(Debug)]
pub(super) enum DaemonDfsTestAuthoritativeExactSetOutcome {
    Error(KvError),
    Entries(Vec<Option<KvEntry>>),
}

/// What one daemon client can borrow from the process-wide publication.
enum DaemonDfsPublishedConfiguration {
    /// This client has no retained owner; every read is a ReadIndex request.
    Unowned,
    /// A coherent publication is not available yet.
    Pending,
    Ready(Arc<sinomo_dfs::DfsConfigurationSnapshot>),
}

#[derive(Clone, Debug)]
pub(crate) struct DaemonDfsControlPlaneKvClient {
    pub(super) cluster_paths: ClusterPaths,
    pub(super) runtime: tokio::runtime::Handle,
    /// Daemon-owned DFS clients read the process-wide published configuration
    /// snapshot for its complete scopes. Every other key is one bounded
    /// ReadIndex request, and nothing read through this client is retained.
    pub(super) retained_metadata_authority: Option<Arc<DaemonDfsMetadataAuthorityGeneration>>,
    pub(super) write_configuration_projection_required: bool,
    /// Highest revision this client or its clones observed through its own
    /// ReadIndex reads or commits. A publication older than it cannot answer
    /// this client's later reads, so they stay monotonic.
    pub(super) observed_revision: Arc<AtomicU64>,
    /// Cached and short-lived services normally retain only a weak capability;
    /// their surrounding blocking task owns the strong guard. This prevents a
    /// static message cache from pinning an old daemon's OS lock after shutdown.
    pub(super) data_owner_lock: Option<Weak<DaemonDataOwnerLock>>,
    /// Sandbox services may outlive the handler which opened them, so those
    /// services explicitly retain the strong capability for their lifetime.
    #[cfg(test)]
    pub(super) authoritative_exact_set_calls: Arc<AtomicUsize>,
    #[cfg(test)]
    pub(super) authoritative_prefix_calls: Arc<AtomicUsize>,
    #[cfg(test)]
    pub(super) authoritative_exact_set_deadlines: Arc<Mutex<Vec<Instant>>>,
    #[cfg(test)]
    pub(super) authoritative_exact_set_outcomes:
        Arc<Mutex<VecDeque<DaemonDfsTestAuthoritativeExactSetOutcome>>>,
}

impl DaemonDfsControlPlaneKvClient {
    pub(crate) fn new_on_runtime(
        cluster_paths: &ClusterPaths,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let data_owner_lock = Self::discover_data_owner_lock(cluster_paths);
        Self {
            cluster_paths: cluster_paths.clone(),
            runtime: runtime.clone(),
            retained_metadata_authority: None,
            write_configuration_projection_required: false,
            observed_revision: Arc::new(AtomicU64::new(0)),
            data_owner_lock: data_owner_lock.as_ref().map(Arc::downgrade),
            #[cfg(test)]
            authoritative_exact_set_calls: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            authoritative_prefix_calls: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            authoritative_exact_set_deadlines: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            authoritative_exact_set_outcomes: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub(super) fn new_daemon_client_on_runtime(
        cluster_paths: &ClusterPaths,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let mut client = Self::new_on_runtime(cluster_paths, runtime);
        client.retained_metadata_authority =
            Self::discover_retained_metadata_authority(cluster_paths, runtime);
        client.write_configuration_projection_required = true;
        client
    }

    pub(super) fn discover_retained_metadata_authority(
        cluster_paths: &ClusterPaths,
        runtime: &tokio::runtime::Handle,
    ) -> Option<Arc<DaemonDfsMetadataAuthorityGeneration>> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths).ok()?;
        daemon_dfs_metadata_authority_generation_for_identity(
            cluster_paths,
            &state.cluster_id,
            &state.local_node_id,
            Some(runtime),
        )
        .ok()
    }

    pub(super) fn discover_data_owner_lock(
        cluster_paths: &ClusterPaths,
    ) -> Option<Arc<DaemonDataOwnerLock>> {
        cluster_paths
            .root
            .parent()
            .map(SinomoDataDir::new)
            .map(|data_dir| DaemonPaths::new(&data_dir))
            .and_then(|paths| daemon_dfs_data_owner_lock(&paths).ok().flatten())
    }

    pub(super) fn data_owner_lock(&self) -> Option<Arc<DaemonDataOwnerLock>> {
        self.data_owner_lock.as_ref().and_then(Weak::upgrade)
    }

    fn cluster_access(&self) -> Result<Arc<dyn sinomo_core::service::ClusterAccess>, KvError> {
        dfs_runtime_host_for_cluster_paths(&self.cluster_paths)
            .map(|host| Arc::clone(&host.cluster))
            .map_err(|error| KvError::AuthorityUnavailable {
                detail: error.to_string(),
            })
    }

    /// Highest revision this client or one of its clones read or committed.
    pub(super) fn observed_revision(&self) -> KvRevision {
        KvRevision(self.observed_revision.load(Ordering::Acquire))
    }

    fn observe_revision(&self, revision: KvRevision) {
        self.observed_revision
            .fetch_max(revision.0, Ordering::AcqRel);
    }

    fn authority_unavailable(error: impl std::fmt::Display) -> KvError {
        KvError::AuthorityUnavailable {
            detail: error.to_string(),
        }
    }

    /// sinomo:kv-configuration: Lend the process-wide published configuration only after its native authority floor check.
    fn published_configuration(&self) -> Result<DaemonDfsPublishedConfiguration, KvError> {
        let Some(authority) = self.retained_metadata_authority.as_ref() else {
            return Ok(DaemonDfsPublishedConfiguration::Unowned);
        };
        let snapshot = if self.write_configuration_projection_required {
            authority
                .checked_configuration_snapshot()
                .map_err(Self::authority_unavailable)?
        } else {
            Some(authority.configuration_snapshot())
        };
        Ok(match snapshot {
            Some(snapshot) if snapshot.revision().is_some() => {
                DaemonDfsPublishedConfiguration::Ready(snapshot)
            }
            _ => DaemonDfsPublishedConfiguration::Pending,
        })
    }

    /// Wait for the shared publication under one network deadline. Every edge
    /// that withdrew it already requested the shared refresh; this client
    /// never starts one.
    async fn wait_for_published_configuration(
        &self,
    ) -> Result<Option<Arc<sinomo_dfs::DfsConfigurationSnapshot>>, KvError> {
        let Some(authority) = self.retained_metadata_authority.as_ref() else {
            return Ok(None);
        };
        let mut published = authority.subscribe_configuration_snapshot();
        let wait = async {
            loop {
                match self.published_configuration()? {
                    DaemonDfsPublishedConfiguration::Unowned => return Ok(None),
                    DaemonDfsPublishedConfiguration::Ready(snapshot) => return Ok(Some(snapshot)),
                    DaemonDfsPublishedConfiguration::Pending => {}
                }
                if published.changed().await.is_err() {
                    return Err(KvError::AuthorityUnavailable {
                        detail: "daemon DFS configuration publication closed".into(),
                    });
                }
            }
        };
        let host = dfs_runtime_host_for_cluster_paths(&self.cluster_paths).ok();
        let stopped = async {
            match host.as_ref() {
                Some(host) => host.wait_for_shutdown().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            ready = tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, wait) => {
                ready.map_err(|_| KvError::AuthorityUnavailable {
                    detail: format!(
                        "daemon DFS retained metadata authority generation {} is not ready: refresh deadline elapsed",
                        authority.generation(),
                    ),
                })?
            }
            () = stopped => Err(KvError::AuthorityUnavailable {
                detail: "daemon DFS component is stopping".into(),
            }),
        }
    }

    fn snapshot_for_read(
        &self,
    ) -> Result<Option<Arc<sinomo_dfs::DfsConfigurationSnapshot>>, KvError> {
        match self.published_configuration()? {
            DaemonDfsPublishedConfiguration::Unowned => Ok(None),
            DaemonDfsPublishedConfiguration::Ready(snapshot) => Ok(Some(snapshot)),
            // Browser/restore workers are synchronous, but a filesystem read
            // must wait for the shared publication. No lock is held here.
            DaemonDfsPublishedConfiguration::Pending => {
                block_on_dfs_cluster_future(&self.runtime, async {
                    Ok(self.wait_for_published_configuration().await)
                })?
            }
        }
    }

    async fn snapshot_for_read_async(
        &self,
    ) -> Result<Option<Arc<sinomo_dfs::DfsConfigurationSnapshot>>, KvError> {
        match self.published_configuration()? {
            DaemonDfsPublishedConfiguration::Unowned => Ok(None),
            DaemonDfsPublishedConfiguration::Ready(snapshot) => Ok(Some(snapshot)),
            DaemonDfsPublishedConfiguration::Pending => {
                self.wait_for_published_configuration().await
            }
        }
    }

    fn may_cover(&self, prefix: &str) -> bool {
        self.retained_metadata_authority.is_some()
            && if self.write_configuration_projection_required {
                crate::write_configuration_projection::DFS_WRITE_CONFIGURATION_PREFIXES
                    .iter()
                    .any(|scope| prefix.starts_with(scope))
            } else {
                prefix.starts_with(DAEMON_DFS_METADATA_AUTHORITY_PREFIX)
            }
    }

    fn covers(&self, snapshot: &sinomo_dfs::DfsConfigurationSnapshot, prefix: &str) -> bool {
        snapshot.entries(prefix).is_some()
            && snapshot
                .revision()
                .is_some_and(|revision| revision >= self.observed_revision())
    }

    /// This client already observed a newer committed revision than the
    /// shared publication. For the filesystem scope that is a possible
    /// catch-up edge for the publication. Either way this read is answered by
    /// its own ReadIndex.
    fn behind_observed_revision(
        &self,
        snapshot: &sinomo_dfs::DfsConfigurationSnapshot,
        prefix: &str,
    ) -> bool {
        let observed = self.observed_revision();
        if snapshot
            .revision()
            .is_none_or(|revision| revision >= observed)
        {
            return false;
        }
        if prefix.starts_with(DAEMON_DFS_METADATA_AUTHORITY_PREFIX)
            && let Some(authority) = self.retained_metadata_authority.as_ref()
        {
            authority.observe_client_revision(observed);
        }
        true
    }

    /// The publication answering a read inside its complete scopes. A
    /// filesystem read waits for a pending publication; other scopes use an
    /// already ready one and otherwise issue their own ReadIndex request.
    fn covering_snapshot(
        &self,
        prefix: &str,
    ) -> Result<Option<Arc<sinomo_dfs::DfsConfigurationSnapshot>>, KvError> {
        if !self.may_cover(prefix) {
            return Ok(None);
        }
        let snapshot = if prefix.starts_with(DAEMON_DFS_METADATA_AUTHORITY_PREFIX) {
            self.snapshot_for_read()?
        } else {
            match self.published_configuration()? {
                DaemonDfsPublishedConfiguration::Ready(snapshot) => Some(snapshot),
                _ => None,
            }
        };
        Ok(snapshot.filter(|snapshot| {
            !self.behind_observed_revision(snapshot, prefix) && self.covers(snapshot, prefix)
        }))
    }

    async fn covering_snapshot_async(
        &self,
        prefix: &str,
    ) -> Result<Option<Arc<sinomo_dfs::DfsConfigurationSnapshot>>, KvError> {
        if !self.may_cover(prefix) {
            return Ok(None);
        }
        let snapshot = if prefix.starts_with(DAEMON_DFS_METADATA_AUTHORITY_PREFIX) {
            self.snapshot_for_read_async().await?
        } else {
            match self.published_configuration()? {
                DaemonDfsPublishedConfiguration::Ready(snapshot) => Some(snapshot),
                _ => None,
            }
        };
        Ok(snapshot.filter(|snapshot| {
            !self.behind_observed_revision(snapshot, prefix) && self.covers(snapshot, prefix)
        }))
    }

    fn published_entries(
        snapshot: &sinomo_dfs::DfsConfigurationSnapshot,
        keys: &[String],
    ) -> Vec<Option<KvEntry>> {
        keys.iter()
            .map(|key| snapshot.entry(key).flatten().cloned())
            .collect()
    }

    #[cfg(any(test, feature = "test-support"))]
    fn injected_operation_floor_read_error(&self, key: &str) -> Option<KvError> {
        self.retained_metadata_authority
            .as_ref()
            .is_some_and(|authority| {
                crate::test_runtime::native_operation_floor_read_is_denied(authority.as_ref(), key)
            })
            .then(|| KvError::Io {
                path: self.cluster_paths.root.display().to_string(),
                detail: "resident Raft view changed after ReadIndex quorum; retry the read (injected native operation-floor return)".into(),
            })
    }

    fn checked_exact_set(
        keys: &[String],
        read: sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        if keys.len() != read.value.len() {
            return Err(KvError::Serialization {
                detail: format!(
                    "daemon DFS exact-key authority returned {} values for {} keys",
                    read.value.len(),
                    keys.len(),
                ),
            });
        }
        for (key, entry) in keys.iter().zip(&read.value) {
            if entry.as_ref().is_some_and(|entry| entry.key != *key) {
                return Err(KvError::Serialization {
                    detail: format!(
                        "daemon DFS exact-key authority returned an entry outside requested key {key:?}"
                    ),
                });
            }
        }
        Ok(read.value)
    }

    fn checked_prefix(
        prefix: &str,
        read: &sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>,
    ) -> Result<(), KvError> {
        if let Some(entry) = read
            .value
            .iter()
            .find(|entry| !entry.key.starts_with(prefix))
        {
            return Err(KvError::Serialization {
                detail: format!(
                    "daemon DFS prefix authority for {prefix:?} returned entry {:?}",
                    entry.key,
                ),
            });
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn take_test_authoritative_exact_set_outcome(
        &self,
        deadline: Instant,
    ) -> Option<Result<Vec<Option<KvEntry>>, KvError>> {
        self.authoritative_exact_set_deadlines
            .lock()
            .expect("test authoritative exact-set deadlines")
            .push(deadline);
        self.authoritative_exact_set_outcomes
            .lock()
            .expect("test authoritative exact-set outcomes")
            .pop_front()
            .map(|outcome| match outcome {
                DaemonDfsTestAuthoritativeExactSetOutcome::Error(error) => Err(error),
                DaemonDfsTestAuthoritativeExactSetOutcome::Entries(entries) => Ok(entries),
            })
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.control_plane.entries_fetch",
        skip_all,
        fields(stage = "dfs.control_plane.entries_fetch", key_count = keys.len())
    )]
    pub(super) fn fetch_authoritative_entries(
        &self,
        keys: &[String],
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_exact_set_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_exact_set_call(&self.cluster_paths);
        }
        let cluster = self.cluster_access()?;
        let read = block_on_dfs_cluster_future(
            &self.runtime,
            cluster.kv_get_authoritative_exact_set_async(keys.to_vec()),
        )?;
        self.observe_revision(read.authority.revision);
        Ok(read)
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.control_plane.entries_fetch",
        skip_all,
        fields(stage = "dfs.control_plane.entries_fetch", phase = "deadline", key_count = keys.len())
    )]
    pub(super) fn fetch_authoritative_entries_before(
        &self,
        keys: &[String],
        deadline: Instant,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_exact_set_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_exact_set_call(&self.cluster_paths);
        }
        let cluster = self.cluster_access()?;
        let read = block_on_dfs_cluster_future(&self.runtime, async move {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                cluster.kv_get_authoritative_exact_set_async(keys.to_vec()),
            )
            .await
            .map_err(|_| {
                sinomo_core::service::ServiceError::Service(
                    "DFS authoritative exact-set read exceeded its deadline".to_string(),
                )
            })?
        })?;
        self.observe_revision(read.authority.revision);
        Ok(read)
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.control_plane.prefix_fetch",
        skip_all,
        fields(
            stage = "dfs.control_plane.prefix_fetch",
            prefix,
            outcome = "unknown",
            error = tracing::field::Empty
        )
    )]
    pub(super) fn fetch_authoritative_prefix(
        &self,
        prefix: &str,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_prefix_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_prefix_call(&self.cluster_paths);
        }
        let result = (|| {
            let cluster = self.cluster_access()?;
            block_on_dfs_cluster_future(
                &self.runtime,
                cluster.kv_range_prefix_authoritative_async(prefix.to_string()),
            )
        })();
        let span = tracing::Span::current();
        span.record("outcome", if result.is_ok() { "ok" } else { "error" });
        if let Err(error) = &result {
            span.record("error", tracing::field::display(error));
        }
        let read = result?;
        Self::checked_prefix(prefix, &read)?;
        self.observe_revision(read.authority.revision);
        Ok(read)
    }

    async fn fetch_authoritative_entries_async(
        &self,
        keys: Vec<String>,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_exact_set_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_exact_set_call(&self.cluster_paths);
        }
        let read = self
            .cluster_access()?
            .kv_get_authoritative_exact_set_async(keys)
            .await
            .map_err(dfs_cluster_service_error)?;
        self.observe_revision(read.authority.revision);
        Ok(read)
    }

    async fn fetch_authoritative_entries_before_async(
        &self,
        keys: Vec<String>,
        deadline: Instant,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_exact_set_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_exact_set_call(&self.cluster_paths);
        }
        let cluster = self.cluster_access()?;
        let read = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            cluster.kv_get_authoritative_exact_set_async(keys),
        )
        .await
        .map_err(|_| KvError::AuthorityUnavailable {
            detail: "DFS authoritative exact-set read exceeded its deadline".to_string(),
        })?
        .map_err(dfs_cluster_service_error)?;
        self.observe_revision(read.authority.revision);
        Ok(read)
    }

    async fn fetch_authoritative_prefix_async(
        &self,
        prefix: String,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>, KvError> {
        #[cfg(test)]
        {
            self.authoritative_prefix_calls
                .fetch_add(1, Ordering::SeqCst);
            record_daemon_dfs_test_authoritative_prefix_call(&self.cluster_paths);
        }
        let read = self
            .cluster_access()?
            .kv_range_prefix_authoritative_async(prefix.clone())
            .await
            .map_err(dfs_cluster_service_error)?;
        Self::checked_prefix(&prefix, &read)?;
        self.observe_revision(read.authority.revision);
        Ok(read)
    }
}

pub(super) fn require_dfs_placement_authority_admin_owner(
    paths: &DaemonPaths,
) -> Result<Arc<DaemonDataOwnerLock>, DaemonError> {
    daemon_dfs_data_owner_lock(paths)?.ok_or_else(|| DaemonError::Parse {
        detail: "DFS placement-authority administration requires the live exclusive main-daemon data-owner capability"
            .to_string(),
    })
}

/// sinomo:kv-consistency: Inspect the current global placement permit and exact recovery token for an explicit administrator request.
pub(crate) fn inspect_dfs_placement_authority_permit(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    file_system_id: &DfsFileSystemId,
) -> Result<Option<sinomo_dfs::DfsPlacementAuthorityPermitStatus>, DaemonError> {
    let _data_owner_lock = require_dfs_placement_authority_admin_owner(paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .placement_authority_permit_status(file_system_id)
        .map_err(Into::into)
}

/// sinomo:kv-consistency: Recover an exact abandoned placement permit after proving global removal of its former owning node.
pub(crate) fn recover_removed_node_dfs_placement_authority_permit(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    file_system_id: &DfsFileSystemId,
    expected_recovery_token: &str,
    acknowledge_owner_stopped: bool,
) -> Result<sinomo_dfs::DfsPlacementAuthorityRecoveryOutcome, DaemonError> {
    let _data_owner_lock = require_dfs_placement_authority_admin_owner(paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .recover_removed_node_placement_authority_permit(
            file_system_id,
            &state.cluster_id.to_string(),
            &state.local_node_id,
            expected_recovery_token,
            acknowledge_owner_stopped,
        )
        .map_err(Into::into)
}

/// sinomo:kv-consistency: Perform the explicitly authorized recovery of an exact abandoned global placement permit.
pub(crate) fn break_glass_recover_dfs_placement_authority_permit(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    file_system_id: &DfsFileSystemId,
    expected_recovery_token: &str,
    acknowledgement: &str,
) -> Result<sinomo_dfs::DfsPlacementAuthorityRecoveryOutcome, DaemonError> {
    let _data_owner_lock = require_dfs_placement_authority_admin_owner(paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .break_glass_recover_placement_authority_permit(
            file_system_id,
            &state.cluster_id.to_string(),
            expected_recovery_token,
            acknowledgement,
        )
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn recover_prior_process_default_dfs_placement_authority(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<bool, DaemonError> {
    recover_prior_process_default_dfs_placement_authority_from_observation(paths, runtime, true)
}

/// Recover only a prior-process placement permit owned by this main daemon's
/// local cluster node. Callers are required to hold `DaemonLock` and
/// `DaemonDataOwnerLock`; the control loop acquires both for its full lifetime
/// before this helper is reachable. Exact-value CAS in the DFS store prevents
/// deleting a concurrently replaced holder.
pub(super) fn recover_prior_process_default_dfs_placement_authority_from_observation(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    refresh_latest: bool,
) -> Result<bool, DaemonError> {
    let _data_owner_lock = daemon_dfs_data_owner_lock(paths)?.ok_or_else(|| {
        DaemonError::Parse {
            detail: "refusing placement-authority recovery without a live exclusive main-daemon data-owner lock"
                .to_string(),
        }
    })?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    #[cfg(test)]
    if let Some(hook) = super::message_host::daemon_dfs_pre_dispatch_test_hook(&data_dir) {
        hook(super::message_host::DaemonDfsPreDispatchTestPoint::PlacementRecoveryOwnerAcquired, None, None)?;
    }
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(false);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    let current_process =
        sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(&client)
            .ok_or_else(|| DaemonError::Parse {
                detail: "main daemon has no DFS placement-authority process identity".to_string(),
            })?;
    if refresh_latest {
        sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    }
    let recovered = sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .recover_prior_process_placement_authority_permit_for_local_node(
            &sinomo_dfs::sifs_file_system_id(),
            &state.local_node_id,
            &current_process,
        )?;
    if recovered {
        tracing::info!(
            target: "sinomo.dfs.placement",
            node_id = %state.local_node_id,
            "recovered exact prior-process SIFS placement authority permit"
        );
    }
    Ok(recovered)
}

pub(super) fn daemon_dfs_txn_puts_local_membership_handoff(
    cluster_paths: &ClusterPaths,
    txn: &KvTxn,
) -> bool {
    if !txn.success.iter().any(
        |op| matches!(op, KvOp::Put { key, .. } if daemon_dfs_membership_handoff_key_is_exact(key)),
    ) {
        return false;
    }
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths) else {
        return false;
    };
    daemon_dfs_txn_puts_membership_handoff_for_node(txn, &state.local_node_id)
}

pub(super) fn daemon_dfs_membership_handoff_key_is_exact(key: &str) -> bool {
    sinomo_dfs::dfs_metadata_shard_membership_handoff_key_is_exact(key)
}

pub(super) fn daemon_dfs_txn_puts_membership_handoff_for_node(
    txn: &KvTxn,
    local_node_id: &NodeId,
) -> bool {
    txn.success.iter().any(|op| {
        let KvOp::Put { key, value, .. } = op else {
            return false;
        };
        if !daemon_dfs_membership_handoff_key_is_exact(key) {
            return false;
        }
        sinomo_core::kv::decode_kv_value::<DfsMetadataShardMembershipHandoff>(value)
            .is_ok_and(|handoff| handoff.retiring_shard.primary_node_id == *local_node_id)
    })
}

/// A requester's wait on the process-wide published configuration.
struct DaemonDfsConfigurationChangeWatch {
    published: tokio::sync::watch::Receiver<Arc<sinomo_dfs::DfsConfigurationSnapshot>>,
    runtime: tokio::runtime::Handle,
    wake: Arc<tokio::sync::Notify>,
}

impl sinomo_dfs::DfsConfigurationChangeWatch for DaemonDfsConfigurationChangeWatch {
    fn wait_for_change(&mut self, wait: std::time::Duration) -> Result<bool, KvError> {
        let published = &mut self.published;
        let wake = Arc::clone(&self.wake);
        block_on_dfs_cluster_future(&self.runtime, async move {
            tokio::select! {
                changed = tokio::time::timeout(wait, published.changed()) => match changed {
                    Ok(Ok(())) => Ok(true),
                    Ok(Err(_)) => Err(sinomo_core::service::ServiceError::Service(
                        "daemon DFS configuration publication closed".into(),
                    )),
                    Err(_) => Ok(false),
                },
                () = wake.notified() => Ok(false),
            }
        })
    }

    fn waker(&self) -> Arc<dyn Fn() + Send + Sync> {
        let wake = Arc::clone(&self.wake);
        Arc::new(move || wake.notify_one())
    }
}

impl sinomo_dfs::DfsControlPlaneKvClient for DaemonDfsControlPlaneKvClient {
    fn async_client(&mut self) -> Option<&mut dyn sinomo_dfs::AsyncDfsControlPlaneKvClient> {
        Some(self)
    }

    fn configuration_snapshot(
        &self,
    ) -> Result<Option<Arc<sinomo_dfs::DfsConfigurationSnapshot>>, KvError> {
        match self.published_configuration()? {
            DaemonDfsPublishedConfiguration::Unowned => Ok(None),
            DaemonDfsPublishedConfiguration::Ready(snapshot) => Ok(Some(snapshot)),
            DaemonDfsPublishedConfiguration::Pending => Err(KvError::AuthorityUnavailable {
                detail: "DFS write configuration is awaiting a coherent authoritative projection"
                    .into(),
            }),
        }
    }

    fn write_configuration_projection(
        &self,
    ) -> Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, KvError> {
        match self.write_configuration_readiness()? {
            crate::DfsWriteConfigurationReadiness::Ready(view) => Ok(view),
            crate::DfsWriteConfigurationReadiness::Pending => Err(KvError::AuthorityUnavailable {
                detail: "DFS write configuration is awaiting a coherent authoritative projection"
                    .into(),
            }),
        }
    }

    fn write_configuration_readiness(
        &self,
    ) -> Result<crate::DfsWriteConfigurationReadiness, KvError> {
        let Some(authority) = self.retained_metadata_authority.as_ref() else {
            if !self.write_configuration_projection_required {
                return Ok(crate::DfsWriteConfigurationReadiness::Ready(None));
            }
            return Err(KvError::AuthorityUnavailable {
                detail: "daemon write configuration owner is unavailable".into(),
            });
        };
        let Some(snapshot) = authority
            .checked_configuration_snapshot()
            .map_err(Self::authority_unavailable)?
        else {
            return Ok(crate::DfsWriteConfigurationReadiness::Pending);
        };
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(view), Some(root)) = (
            snapshot.write_configuration(),
            self.cluster_paths.root.parent(),
        ) {
            crate::native_shard_trace::record_configuration_cut(
                root,
                self.data_owner_lock().as_ref(),
                view,
                snapshot.generation(),
            );
        }
        Ok(crate::DfsWriteConfigurationReadiness::Ready(
            snapshot.write_configuration().cloned(),
        ))
    }

    fn cached_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<Vec<crate::DfsMetadataShardConfig>>, KvError> {
        let prefix = crate::metadata_shards_prefix(file_system_id);
        let Some(snapshot) = self.covering_snapshot(&prefix)? else {
            return Ok(None);
        };
        snapshot.metadata_shards(file_system_id)
    }

    /// Nothing read through this client is retained, so it has nothing to
    /// refresh: a requester waits for the shared publication to be replaced.
    fn watch_configuration_changes(
        &self,
    ) -> Option<Box<dyn sinomo_dfs::DfsConfigurationChangeWatch>> {
        let authority = self.retained_metadata_authority.as_ref()?;
        Some(Box::new(DaemonDfsConfigurationChangeWatch {
            published: authority.subscribe_configuration_snapshot(),
            runtime: self.runtime.clone(),
            wake: Arc::new(tokio::sync::Notify::new()),
        }))
    }

    fn placement_authority_process_identity(
        &self,
    ) -> Option<sinomo_dfs::DfsPlacementAuthorityProcessIdentity> {
        // The shadow daemon shares cluster state but never owns local data.
        // Only a process which currently retains the main daemon's exclusive
        // data-owner lock may advertise placement mutation authority.
        let _data_owner_lock = self.data_owner_lock()?;
        let state =
            sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths).ok()?;
        Some(sinomo_dfs::DfsPlacementAuthorityProcessIdentity {
            cluster_id: state.cluster_id.to_string(),
            process_session_id: DAEMON_DFS_PLACEMENT_AUTHORITY_SESSION_ID
                .get_or_init(|| sinomo_core::Id::generate().to_string())
                .clone(),
        })
    }

    fn authorizes_prior_process_placement_authority_recovery(
        &self,
        local_node_id: &NodeId,
        current_process: &sinomo_dfs::DfsPlacementAuthorityProcessIdentity,
    ) -> bool {
        let Some(_data_owner_lock) = self.data_owner_lock() else {
            return false;
        };
        let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)
        else {
            return false;
        };
        state.local_node_id == *local_node_id
            && sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(self)
                .as_ref()
                == Some(current_process)
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        let keys = [key.to_string()];
        let value = match self.covering_snapshot(key)? {
            Some(snapshot) => Self::published_entries(&snapshot, &keys),
            None => Self::checked_exact_set(&keys, self.fetch_authoritative_entries(&keys)?)?,
        };
        #[cfg(any(test, feature = "test-support"))]
        if let Some(error) = self.injected_operation_floor_read_error(key) {
            return Err(error);
        }
        Ok(value.into_iter().next().flatten())
    }

    fn get_consistent(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        // One publication or one exact-set ReadIndex pins every key together.
        let Some(first) = keys.first() else {
            return Ok(Vec::new());
        };
        if let Some(snapshot) = self.covering_snapshot(first)?
            && keys.iter().all(|key| self.covers(&snapshot, key))
        {
            return Ok(Self::published_entries(&snapshot, keys));
        }
        Self::checked_exact_set(keys, self.fetch_authoritative_entries(keys)?)
    }

    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        // An operation-bound authority check is one exact-key-set ReadIndex,
        // returned directly to its caller.
        Self::checked_exact_set(keys, self.fetch_authoritative_entries(keys)?)
    }

    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        deadline: Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        if Instant::now() >= deadline {
            return Err(KvError::AuthorityUnavailable {
                detail: "metadata shard authority read deadline elapsed".to_string(),
            });
        }
        #[cfg(test)]
        if let Some(result) = self.take_test_authoritative_exact_set_outcome(deadline) {
            return result;
        }
        let read = self.fetch_authoritative_entries_before(keys, deadline)?;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(data_root) = self.cluster_paths.root.parent() {
            crate::native_shard_trace::record_authoritative_read(
                data_root,
                self.data_owner_lock().as_ref(),
                keys,
                &read,
                self.retained_metadata_authority
                    .as_ref()
                    .map_or(0, |authority| authority.generation()),
            );
        }
        Self::checked_exact_set(keys, read)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        if let Some(snapshot) = self.covering_snapshot(prefix)? {
            return Ok(snapshot.entries(prefix).unwrap_or_default().to_vec());
        }
        Ok(self.fetch_authoritative_prefix(prefix)?.value)
    }

    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<sinomo_dfs::DfsAuthoritativePrefixProjection, KvError> {
        // Destructive callers never inherit a published prefix certificate.
        // Exactly one bounded prefix ReadIndex is returned to the caller.
        let read = self.fetch_authoritative_prefix(prefix)?;
        sinomo_dfs::DfsAuthoritativePrefixProjection::complete(
            prefix,
            read.value,
            read.authority.revision,
        )
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        let response = self.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.revision)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let notify_handoff =
            daemon_dfs_txn_puts_local_membership_handoff(&self.cluster_paths, &txn);
        let cluster = self.cluster_access()?;
        let request = sinomo_core::kv::KvTxnRequest::generated(txn);
        #[cfg(any(test, feature = "test-support"))]
        let native_request = self.cluster_paths.root.parent().and_then(|root| {
            let owner = self.data_owner_lock()?;
            crate::native_shard_trace::has_root_interest(root, &owner).then(|| request.clone())
        });
        let result =
            block_on_dfs_cluster_future(&self.runtime, cluster.kv_commit_compact_async(request))?;
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(root), Some(request)) =
            (self.cluster_paths.root.parent(), native_request.as_ref())
        {
            crate::native_shard_trace::record_native_control_commit(
                root,
                self.data_owner_lock().as_ref(),
                request,
                &result,
            );
        }
        let response = result.txn_response.ok_or_else(|| KvError::Serialization {
            detail: "component cluster commit returned no transaction response".into(),
        })?;
        if response.succeeded && notify_handoff {
            if let Some(root) = self.cluster_paths.root.parent() {
                notify_daemon_dfs_maintenance_work(&SinomoDataDir::new(root));
            }
        }
        self.observe_revision(response.revision);
        Ok(response)
    }

    fn txn_with_native_authority(
        &mut self,
        txn: KvTxn,
    ) -> Result<(KvTxnResponse, Option<crate::DfsNativeTxnCommit>), KvError> {
        let notify_handoff =
            daemon_dfs_txn_puts_local_membership_handoff(&self.cluster_paths, &txn);
        let cluster = self.cluster_access()?;
        let request = sinomo_core::kv::KvTxnRequest::generated(txn);
        let result = block_on_dfs_cluster_future(
            &self.runtime,
            cluster.kv_commit_compact_async(request.clone()),
        )?;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(root) = self.cluster_paths.root.parent() {
            crate::native_shard_trace::record_native_control_commit(
                root,
                self.data_owner_lock().as_ref(),
                &request,
                &result,
            );
        }
        let response = result
            .txn_response
            .clone()
            .ok_or_else(|| KvError::Serialization {
                detail: "component cluster commit returned no transaction response".into(),
            })?;
        if response.succeeded && notify_handoff {
            if let Some(root) = self.cluster_paths.root.parent() {
                notify_daemon_dfs_maintenance_work(&SinomoDataDir::new(root));
            }
        }
        self.observe_revision(response.revision);
        Ok((
            response,
            Some(crate::DfsNativeTxnCommit { request, result }),
        ))
    }

    fn txn_request(
        &mut self,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> Result<KvTxnResponse, KvError> {
        let notify_handoff =
            daemon_dfs_txn_puts_local_membership_handoff(&self.cluster_paths, &request.txn);
        let cluster = self.cluster_access()?;
        let result =
            block_on_dfs_cluster_future(&self.runtime, cluster.kv_commit_compact_async(request))?;
        let response = result.txn_response.ok_or_else(|| KvError::Serialization {
            detail: "component cluster commit returned no transaction response".into(),
        })?;
        if response.succeeded
            && notify_handoff
            && let Some(root) = self.cluster_paths.root.parent()
        {
            notify_daemon_dfs_maintenance_work(&SinomoDataDir::new(root));
        }
        self.observe_revision(response.revision);
        Ok(response)
    }

    fn request_outcome(
        &mut self,
        request_id: &sinomo_core::kv::KvRequestId,
    ) -> Result<sinomo_core::kv::KvRequestOutcome, KvError> {
        let cluster = self.cluster_access()?;
        let read = block_on_dfs_cluster_future(
            &self.runtime,
            cluster.kv_request_outcome_authoritative_async(request_id.clone()),
        )?;
        sinomo_core::kv::validate_cluster_kv_request_outcome_read(&read)?;
        Ok(read.outcome)
    }
}

impl sinomo_dfs::AsyncDfsControlPlaneKvClient for DaemonDfsControlPlaneKvClient {
    fn get_async<'a>(
        &'a self,
        key: &'a str,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Option<KvEntry>> {
        Box::pin(async move {
            let keys = vec![key.to_string()];
            let value = match self.covering_snapshot_async(key).await? {
                Some(snapshot) => Self::published_entries(&snapshot, &keys),
                None => Self::checked_exact_set(
                    &keys,
                    self.fetch_authoritative_entries_async(keys.clone()).await?,
                )?,
            };
            Ok(value.into_iter().next().flatten())
        })
    }

    fn get_consistent_async<'a>(
        &'a self,
        keys: Vec<String>,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        Box::pin(async move {
            let Some(first) = keys.first() else {
                return Ok(Vec::new());
            };
            if let Some(snapshot) = self.covering_snapshot_async(first).await?
                && keys.iter().all(|key| self.covers(&snapshot, key))
            {
                return Ok(Self::published_entries(&snapshot, &keys));
            }
            let read = self.fetch_authoritative_entries_async(keys.clone()).await?;
            Self::checked_exact_set(&keys, read)
        })
    }

    fn get_authoritative_consistent_async<'a>(
        &'a mut self,
        keys: Vec<String>,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        Box::pin(async move {
            let read = self.fetch_authoritative_entries_async(keys.clone()).await?;
            Self::checked_exact_set(&keys, read)
        })
    }

    fn get_authoritative_consistent_before_async<'a>(
        &'a mut self,
        keys: Vec<String>,
        deadline: Instant,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        if Instant::now() >= deadline {
            return Box::pin(async {
                Err(KvError::AuthorityUnavailable {
                    detail: "metadata shard authority read deadline elapsed".to_string(),
                })
            });
        }
        Box::pin(async move {
            #[cfg(test)]
            if let Some(result) = self.take_test_authoritative_exact_set_outcome(deadline) {
                return result;
            }
            let read = self
                .fetch_authoritative_entries_before_async(keys.clone(), deadline)
                .await?;
            Self::checked_exact_set(&keys, read)
        })
    }

    fn range_prefix_async<'a>(
        &'a self,
        prefix: &'a str,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<KvEntry>> {
        Box::pin(async move {
            if let Some(snapshot) = self.covering_snapshot_async(prefix).await? {
                return Ok(snapshot.entries(prefix).unwrap_or_default().to_vec());
            }
            Ok(self
                .fetch_authoritative_prefix_async(prefix.to_string())
                .await?
                .value)
        })
    }

    fn put_async<'a>(
        &'a mut self,
        key: String,
        value: Vec<u8>,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, KvRevision> {
        Box::pin(async move {
            let response = self
                .txn_async(KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Put {
                        key,
                        value,
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                })
                .await?;
            Ok(response.revision)
        })
    }

    fn txn_async<'a>(
        &'a mut self,
        txn: KvTxn,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, KvTxnResponse> {
        Box::pin(async move {
            let notify_handoff =
                daemon_dfs_txn_puts_local_membership_handoff(&self.cluster_paths, &txn);
            let request = sinomo_core::kv::KvTxnRequest::generated(txn);
            #[cfg(any(test, feature = "test-support"))]
            let native_request = self.cluster_paths.root.parent().and_then(|root| {
                let owner = self.data_owner_lock()?;
                crate::native_shard_trace::has_root_interest(root, &owner).then(|| request.clone())
            });
            let result = self
                .cluster_access()?
                .kv_commit_compact_async(request)
                .await
                .map_err(dfs_cluster_service_error)?;
            #[cfg(any(test, feature = "test-support"))]
            if let (Some(root), Some(request)) =
                (self.cluster_paths.root.parent(), native_request.as_ref())
            {
                crate::native_shard_trace::record_native_control_commit(
                    root,
                    self.data_owner_lock().as_ref(),
                    request,
                    &result,
                );
            }
            let response = result.txn_response.ok_or_else(|| KvError::Serialization {
                detail: "component cluster commit returned no transaction response".to_string(),
            })?;
            if response.succeeded && notify_handoff {
                if let Some(root) = self.cluster_paths.root.parent() {
                    notify_daemon_dfs_maintenance_work(&SinomoDataDir::new(root));
                }
            }
            self.observe_revision(response.revision);
            Ok(response)
        })
    }

    fn txn_request_async<'a>(
        &'a mut self,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, KvTxnResponse> {
        Box::pin(async move {
            let notify_handoff =
                daemon_dfs_txn_puts_local_membership_handoff(&self.cluster_paths, &request.txn);
            let result = self
                .cluster_access()?
                .kv_commit_compact_async(request)
                .await
                .map_err(dfs_cluster_service_error)?;
            let response = result.txn_response.ok_or_else(|| KvError::Serialization {
                detail: "component cluster commit returned no transaction response".to_string(),
            })?;
            if response.succeeded
                && notify_handoff
                && let Some(root) = self.cluster_paths.root.parent()
            {
                notify_daemon_dfs_maintenance_work(&SinomoDataDir::new(root));
            }
            self.observe_revision(response.revision);
            Ok(response)
        })
    }

    fn request_outcome_async<'a>(
        &'a mut self,
        request_id: sinomo_core::kv::KvRequestId,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, sinomo_core::kv::KvRequestOutcome> {
        Box::pin(async move {
            let read = self
                .cluster_access()?
                .kv_request_outcome_authoritative_async(request_id)
                .await
                .map_err(dfs_cluster_service_error)?;
            sinomo_core::kv::validate_cluster_kv_request_outcome_read(&read)?;
            Ok(read.outcome)
        })
    }
}

#[cfg(test)]
pub(crate) async fn test_retained_client_configuration_sync(
    authority: Arc<DaemonDfsMetadataAuthorityGeneration>,
    paths: &ClusterPaths,
) -> Result<(), KvError> {
    let mut client =
        DaemonDfsControlPlaneKvClient::new_on_runtime(paths, &tokio::runtime::Handle::current());
    client.retained_metadata_authority = Some(authority);
    client.write_configuration_projection_required = true;
    client.wait_for_published_configuration().await?;
    sinomo_dfs::DfsControlPlaneKvClient::write_configuration_projection(&client).map(|_| ())
}

#[cfg(test)]
pub(crate) fn test_retained_client_configuration_projection(
    authority: Arc<DaemonDfsMetadataAuthorityGeneration>,
    paths: &ClusterPaths,
) -> Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, KvError> {
    let mut client =
        DaemonDfsControlPlaneKvClient::new_on_runtime(paths, &tokio::runtime::Handle::current());
    client.retained_metadata_authority = Some(authority);
    client.write_configuration_projection_required = true;
    sinomo_dfs::DfsControlPlaneKvClient::write_configuration_projection(&client)
}
