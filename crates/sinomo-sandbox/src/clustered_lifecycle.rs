use crate::*;
use sinomo_core::{EventEnvelope, node::NodeId};
use sinomo_dfs::{
    DfsAdvisoryLockSessionId, DfsMessageTransport, DfsNodeAdvertisement, DfsObjectKind,
    DfsPosixError, DfsRenewableExclusiveLease, DfsRenewableLeaseAnchor, DfsRenewableLeaseConfig,
    DfsRenewableLeaseError, DfsWriteOptions, NetworkedDfsService,
};
use std::collections::BTreeSet;
use std::fmt::Display;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;

const SANDBOX_LOCK_ANCHOR_CONTENT: &[u8] = b"sinomo-sandbox-lifecycle-lock-v1\n";

/// Builds the content-replica set used by clustered sandbox lifecycle writes.
/// The local DFS node remains a candidate even before its service
/// advertisement converges; remote nodes must explicitly accept content.
pub fn clustered_sandbox_replica_candidates(
    local_node_id: &NodeId,
    advertisements: impl IntoIterator<Item = DfsNodeAdvertisement>,
) -> Vec<NodeId> {
    let mut candidates = BTreeSet::from([local_node_id.clone()]);
    candidates.extend(
        advertisements
            .into_iter()
            .filter(|advertisement| advertisement.accepts_content)
            .map(|advertisement| advertisement.node_id),
    );
    candidates.into_iter().collect()
}

#[derive(Debug, thiserror::Error)]
pub enum ClusteredSandboxLeaseError {
    #[error(transparent)]
    Posix(#[from] DfsPosixError),
    #[error(
        "failed to create sandbox lifecycle lock anchor {path}: {write_error}; post-write stat also failed: {stat_error}"
    )]
    AnchorCreate {
        path: String,
        write_error: String,
        stat_error: String,
    },
    #[error("sandbox lifecycle lock anchor is not a regular file: {path} ({kind})")]
    InvalidAnchor { path: String, kind: String },
    #[error("sandbox {sandbox_id} is busy or its lifecycle lease is unavailable: {source}")]
    Acquire {
        sandbox_id: SandboxId,
        #[source]
        source: DfsRenewableLeaseError,
    },
    #[error("start sandbox lifecycle lease loss monitor: {source}")]
    LossMonitor {
        #[source]
        source: std::io::Error,
    },
    #[error("sandbox {sandbox_id} lifecycle lease lost {phase}: {source}")]
    Lost {
        sandbox_id: SandboxId,
        phase: String,
        #[source]
        source: DfsRenewableLeaseError,
    },
}

impl ClusteredSandboxLeaseError {
    /// Classify an acquisition that failed only because another session holds
    /// the lease or the lease authority is warming. The fence is untouched in
    /// both cases; warming carries the authority's own retry time.
    pub fn deferral(&self) -> Option<ClusteredSandboxRecoveryAttempt> {
        let Self::Acquire { source, .. } = self else {
            return None;
        };
        match source {
            DfsRenewableLeaseError::WouldBlock { .. } | DfsRenewableLeaseError::Busy { .. } => {
                Some(ClusteredSandboxRecoveryAttempt::Deferred {
                    retry_after_unix_nanos: None,
                })
            }
            DfsRenewableLeaseError::AuthorityWarming {
                retry_after_unix_nanos,
            } => Some(ClusteredSandboxRecoveryAttempt::Deferred {
                retry_after_unix_nanos: Some(*retry_after_unix_nanos),
            }),
            _ => None,
        }
    }
}

/// Outcome of one event-triggered lifecycle recovery attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusteredSandboxRecoveryAttempt {
    /// Any pending fence is finished and this attempt retired its obligations.
    Settled,
    /// Another session holds the lifecycle lease, or its authority is still
    /// warming. The fence and its obligations are untouched.
    Deferred { retry_after_unix_nanos: Option<u64> },
}

/// Renewable DFS lease plus the cancellation fence observed by long-running
/// sandbox store mutations.
pub struct ClusteredSandboxExecutionLease<T: DfsMessageTransport + Send + 'static> {
    lease: Option<DfsRenewableExclusiveLease<T>>,
    loss_monitor: Option<std::thread::JoinHandle<()>>,
    cancellation: SandboxCancellationToken,
    session_id: String,
}

impl<T: DfsMessageTransport + Send + 'static> ClusteredSandboxExecutionLease<T> {
    pub fn start(
        lease: DfsRenewableExclusiveLease<T>,
        cancellation: SandboxCancellationToken,
    ) -> Result<Self, ClusteredSandboxLeaseError> {
        let status = lease.status();
        let session_id = lease.session_id().0.clone();
        let monitor_cancellation = cancellation.clone();
        let loss_monitor = std::thread::Builder::new()
            .name("sinomo-sandbox-lease-loss".to_string())
            .spawn(move || {
                if status.wait_until_lost() {
                    monitor_cancellation.fence_lifecycle_mutation();
                }
            })
            .map_err(|source| ClusteredSandboxLeaseError::LossMonitor { source })?;
        Ok(Self {
            lease: Some(lease),
            loss_monitor: Some(loss_monitor),
            cancellation,
            session_id,
        })
    }

    /// DFS advisory-lock session holding this lifecycle lease.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn ensure_held(&self) -> Result<(), DfsRenewableLeaseError> {
        self.lease
            .as_ref()
            .ok_or(DfsRenewableLeaseError::Released)?
            .ensure_held()
    }

    pub fn cancellation_token(&self) -> SandboxCancellationToken {
        self.cancellation.clone()
    }

    pub fn ensure_held_for(
        &self,
        sandbox_id: &SandboxId,
        phase: &str,
    ) -> Result<(), ClusteredSandboxLeaseError> {
        self.ensure_held()
            .map_err(|source| ClusteredSandboxLeaseError::Lost {
                sandbox_id: sandbox_id.clone(),
                phase: phase.to_string(),
                source,
            })
    }
}

impl<T: DfsMessageTransport + Send + 'static> Drop for ClusteredSandboxExecutionLease<T> {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take()
            && let Err(error) = lease.release()
        {
            tracing::debug!(error = %error, "sandbox lifecycle lease release failed");
        }
        if let Some(monitor) = self.loss_monitor.take()
            && monitor.join().is_err()
        {
            tracing::warn!("sandbox lifecycle lease loss monitor panicked");
        }
    }
}

impl<T, E> ClusteredSandboxLifecycleLease<E> for ClusteredSandboxExecutionLease<T>
where
    T: DfsMessageTransport + Send + 'static,
    E: From<ClusteredSandboxLeaseError>,
{
    fn ensure_held(&self, sandbox_id: &SandboxId, phase: &str) -> Result<(), E> {
        self.ensure_held_for(sandbox_id, phase).map_err(Into::into)
    }

    fn cancellation_token(&self) -> SandboxCancellationToken {
        self.cancellation_token()
    }

    fn session_id(&self) -> &str {
        self.session_id()
    }
}

/// Acquire the exclusive lifecycle lease and start its loss monitor.
pub fn start_clustered_sandbox_lifecycle_lease<T>(
    lease_dfs: NetworkedDfsService<T>,
    sandbox_id: &SandboxId,
    replica_candidates: &[NodeId],
) -> Result<ClusteredSandboxExecutionLease<T>, ClusteredSandboxLeaseError>
where
    T: DfsMessageTransport + Send + 'static,
{
    let raw_lease =
        acquire_clustered_sandbox_lifecycle_lease(lease_dfs, sandbox_id, replica_candidates)?;
    ensure_clustered_sandbox_lifecycle_lease(&raw_lease, sandbox_id, "before pending settlement")?;
    ClusteredSandboxExecutionLease::start(raw_lease, SandboxCancellationToken::new())
}

pub fn acquire_clustered_sandbox_lifecycle_lease<T>(
    mut dfs: NetworkedDfsService<T>,
    sandbox_id: &SandboxId,
    replica_candidates: &[NodeId],
) -> Result<DfsRenewableExclusiveLease<T>, ClusteredSandboxLeaseError>
where
    T: DfsMessageTransport + Send + 'static,
{
    let lock_root = sandbox_lock_root();
    dfs.posix_mkdir_all(&lock_root)?;
    let anchor_path = sandbox_lock_anchor_path(sandbox_id);
    let stat = match dfs.posix_stat_distributed(&anchor_path) {
        Ok(stat) => stat,
        Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => match dfs
            .posix_write_file(
                &anchor_path,
                SANDBOX_LOCK_ANCHOR_CONTENT,
                DfsWriteOptions::replicated_eventual(replica_candidates.to_vec()),
            ) {
            Ok(stat) => stat,
            Err(write_error) => match dfs.posix_stat_distributed(&anchor_path) {
                Ok(stat) => stat,
                Err(stat_error) => {
                    return Err(ClusteredSandboxLeaseError::AnchorCreate {
                        path: anchor_path,
                        write_error: write_error.to_string(),
                        stat_error: stat_error.to_string(),
                    });
                }
            },
        },
        Err(error) => return Err(error.into()),
    };
    if matches!(
        stat.kind,
        DfsObjectKind::Directory | DfsObjectKind::Tombstone
    ) {
        return Err(ClusteredSandboxLeaseError::InvalidAnchor {
            path: anchor_path,
            kind: format!("{:?}", stat.kind),
        });
    }
    let anchor = DfsRenewableLeaseAnchor::new(anchor_path, stat.object_id);
    let session_id = DfsAdvisoryLockSessionId::new(format!(
        "sandbox-lifecycle:{}:{}:{}",
        sandbox_id,
        std::process::id(),
        sinomo_core::Id::generate()
    ));
    DfsRenewableExclusiveLease::try_acquire(
        Arc::new(Mutex::new(dfs)),
        anchor,
        session_id,
        Some(std::process::id()),
        DfsRenewableLeaseConfig::default(),
    )
    .map_err(|source| ClusteredSandboxLeaseError::Acquire {
        sandbox_id: sandbox_id.clone(),
        source,
    })
}

pub fn ensure_clustered_sandbox_lifecycle_lease<T>(
    lease: &DfsRenewableExclusiveLease<T>,
    sandbox_id: &SandboxId,
    phase: &str,
) -> Result<(), ClusteredSandboxLeaseError>
where
    T: DfsMessageTransport + Send + 'static,
{
    lease
        .ensure_held()
        .map_err(|source| ClusteredSandboxLeaseError::Lost {
            sandbox_id: sandbox_id.clone(),
            phase: phase.to_string(),
            source,
        })
}

/// Persistence strength required for a sandbox audit event.
///
/// Ordinary mutations may use the event store's normal append contract. An
/// authoritative lifecycle transaction must durably append its deterministic
/// audit event before its reconciliation fence is cleared.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxAuditDurability {
    Buffered,
    Durable,
}

/// Host effects used by [`ClusteredSandboxLifecycleRuntime`].
///
/// The sandbox crate owns state-transition ordering and policy semantics. The
/// embedding runtime supplies only the cross-module effects whose concrete
/// storage and authority live outside this crate.
pub trait ClusteredSandboxLifecycleHooks {
    type Error: Display
        + From<SandboxStoreError>
        + From<SandboxFsError>
        + From<DfsPosixError>
        + From<ClusteredSandboxLeaseError>
        + From<SandboxRecordedHttpFetchError>
        + From<SandboxEventEmissionError>;

    fn load_policy_config(&mut self) -> Result<SandboxClusterPolicyConfig, Self::Error>;

    fn append_event(
        &mut self,
        event: EventEnvelope,
        durability: SandboxAuditDurability,
    ) -> Result<(), Self::Error>;

    /// Retire the node-local wake marker and the cluster-KV pending-fence
    /// index entry after authoritative state has settled. Implementations
    /// intentionally make this best effort: a stale obligation causes a safe
    /// recovery recheck and must not make a committed mutation appear
    /// retryable.
    fn clear_authoritative_recovery_pending(
        &mut self,
        sandbox_id: &SandboxId,
        context: &'static str,
    );

    /// Durably record an authoritative transaction's obligations before its
    /// fence can exist: the node-local wake marker and the cluster-KV index
    /// entry naming this node and lease session. Failure aborts the
    /// transaction before any fence is written.
    fn mark_authoritative_recovery_pending(
        &mut self,
        record: &SandboxPendingFenceRecord,
    ) -> Result<(), Self::Error>;

    /// Take over the obligations of a pending fence this lease holder is about
    /// to finish, so a crash during recovery leaves it with this node. Best
    /// effort: the previous index entry remains a valid recovery trigger.
    fn claim_authoritative_recovery(&mut self, record: &SandboxPendingFenceRecord);

    fn invalid(&mut self, detail: String) -> Self::Error;

    fn now(&mut self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

/// Lease capability required by clustered sandbox mutations.
///
/// This keeps DFS lease acquisition and renewal replaceable while ensuring the
/// owner runtime fences every authoritative transition at the same phases.
pub trait ClusteredSandboxLifecycleLease<E> {
    fn ensure_held(&self, sandbox_id: &SandboxId, phase: &str) -> Result<(), E>;
    fn cancellation_token(&self) -> SandboxCancellationToken;
    fn session_id(&self) -> &str;
}

/// Owner-side clustered sandbox state machine.
pub struct ClusteredSandboxLifecycleRuntime<'a, H> {
    hooks: &'a mut H,
}

/// Lifecycle operation requested under the sandbox lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusteredSandboxOperationKind {
    /// Ordinary lease-holding mutation. Any pending authoritative transaction
    /// is finished first.
    Mutation,
    /// Authoritative transaction that publishes a reconciliation fence.
    AuthoritativeTransaction(SandboxAuthoritativeTransaction),
}

/// Authoritative lifecycle transaction requested by a caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SandboxAuthoritativeTransaction {
    Create,
    ResumeCheckpoint { checkpoint_id: SandboxCheckpointId },
    Reboot,
    Destroy,
}

impl SandboxAuthoritativeTransaction {
    /// Whether this request continues `pending` through its own idempotent
    /// resume path. Any other pending operation is finished before it starts.
    pub fn resumes(&self, pending: &SandboxAuthoritativeMutationOperation) -> bool {
        match (self, pending) {
            (Self::Create, SandboxAuthoritativeMutationOperation::Create)
            | (Self::Reboot, SandboxAuthoritativeMutationOperation::Reboot { .. })
            | (Self::Destroy, SandboxAuthoritativeMutationOperation::Destroy) => true,
            (
                Self::ResumeCheckpoint { checkpoint_id },
                SandboxAuthoritativeMutationOperation::ResumeCheckpoint {
                    checkpoint_id: pending,
                },
            ) => checkpoint_id == pending,
            _ => false,
        }
    }
}

impl<'a, H> ClusteredSandboxLifecycleRuntime<'a, H>
where
    H: ClusteredSandboxLifecycleHooks,
{
    pub fn new(hooks: &'a mut H) -> Self {
        Self { hooks }
    }

    /// Take the sandbox lease and settle any pending authoritative
    /// transaction before the requested operation starts. A mutation finishes
    /// whatever is pending; a transaction finishes only a different pending
    /// operation and otherwise continues its own through its idempotent path.
    pub fn open_operation<T>(
        &mut self,
        lease_dfs: NetworkedDfsService<T>,
        dfs: NetworkedDfsService<T>,
        replica_candidates: Vec<NodeId>,
        sandbox_id: &SandboxId,
        kind: &ClusteredSandboxOperationKind,
    ) -> Result<
        (
            ClusteredSandboxExecutionLease<T>,
            SifsSandboxInstanceStore<T>,
            Vec<NodeId>,
        ),
        H::Error,
    >
    where
        T: DfsMessageTransport + Send + 'static,
    {
        let lease =
            start_clustered_sandbox_lifecycle_lease(lease_dfs, sandbox_id, &replica_candidates)?;
        let mut store =
            SifsSandboxInstanceStore::new(dfs).with_replica_candidates(replica_candidates.clone());
        let transaction = match kind {
            ClusteredSandboxOperationKind::Mutation => None,
            ClusteredSandboxOperationKind::AuthoritativeTransaction(transaction) => {
                Some(transaction)
            }
        };
        self.recover_authoritative_except(&lease, &mut store, sandbox_id, transaction)?;
        lease.ensure_held_for(sandbox_id, "after pending settlement")?;
        if transaction.is_some() {
            self.hooks
                .mark_authoritative_recovery_pending(&SandboxPendingFenceRecord {
                    sandbox_id: sandbox_id.clone(),
                    node_id: store.dfs.node_id().clone(),
                    session_id: lease.session_id().to_string(),
                })?;
        }
        Ok((lease, store, replica_candidates))
    }

    /// One event-triggered recovery: take the lease and finish whatever is
    /// pending. Lease contention defers without touching the fence.
    pub fn recover_operation<T>(
        &mut self,
        lease_dfs: NetworkedDfsService<T>,
        dfs: NetworkedDfsService<T>,
        replica_candidates: Vec<NodeId>,
        sandbox_id: &SandboxId,
    ) -> Result<ClusteredSandboxRecoveryAttempt, H::Error>
    where
        T: DfsMessageTransport + Send + 'static,
    {
        let lease = match start_clustered_sandbox_lifecycle_lease(
            lease_dfs,
            sandbox_id,
            &replica_candidates,
        ) {
            Ok(lease) => lease,
            Err(error) => return error.deferral().ok_or_else(|| error.into()),
        };
        let mut store =
            SifsSandboxInstanceStore::new(dfs).with_replica_candidates(replica_candidates);
        self.recover_authoritative(&lease, &mut store, sandbox_id)?;
        Ok(ClusteredSandboxRecoveryAttempt::Settled)
    }

    pub fn http_fetch_to_file<T, L, N>(
        &mut self,
        lease: &L,
        mut store: SifsSandboxInstanceStore<T>,
        replica_candidates: Vec<NodeId>,
        transport: &N,
        request: SandboxHttpFetchToFileRequest,
    ) -> Result<SandboxHttpFetchFileResult, H::Error>
    where
        T: DfsMessageTransport,
        L: ClusteredSandboxLifecycleLease<H::Error>,
        N: SandboxHttpTransport,
    {
        let config = self.hooks.load_policy_config()?;
        let execution_node_id = store.dfs.node_id().clone();
        let manifest = store.load(&request.sandbox_id)?;
        if let Err(denial) = config
            .policy
            .validate_manifest(&manifest, &execution_node_id)
        {
            self.hooks.append_event(
                sandbox_policy_denied_event(
                    execution_node_id,
                    Some(&request.sandbox_id),
                    "sandbox-fetch",
                    &denial,
                ),
                SandboxAuditDurability::Buffered,
            )?;
            return Err(self.hooks.invalid(denial.to_string()));
        }
        if let Err(denial) = config.policy.validate_http_request(&request.request) {
            self.hooks.append_event(
                sandbox_policy_denied_event(
                    execution_node_id,
                    Some(&request.sandbox_id),
                    "sandbox-fetch",
                    &denial,
                ),
                SandboxAuditDurability::Buffered,
            )?;
            return Err(self.hooks.invalid(denial.to_string()));
        }
        let mut manifest = manifest;
        manifest
            .advance_generation()
            .map_err(|source| self.hooks.invalid(source.to_string()))?;
        manifest.updated_at = self.hooks.now();
        lease.ensure_held(&request.sandbox_id, "before fetch generation fence")?;
        store.save_manifest(&manifest)?;
        lease.ensure_held(&request.sandbox_id, "after fetch generation fence")?;

        let command_id = SandboxCommandId::generate();
        let mut fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest)
            .with_replica_candidates(replica_candidates);
        let fetch_result = sandbox_http_fetch_to_file(
            &config.policy,
            transport,
            &mut fs,
            manifest.sandbox_id.clone(),
            command_id.clone(),
            &request.request,
            &request.output_path,
            &manifest.limits,
        );
        lease.ensure_held(&request.sandbox_id, "after fetch workspace mutation")?;
        let result = match fetch_result {
            Ok(result) => result,
            Err(error) => {
                if let SandboxRecordedHttpFetchError::Fs(SandboxFsError::QuotaExceeded {
                    kind,
                    actual,
                    limit,
                }) = &error
                {
                    self.hooks.append_event(
                        sandbox_resource_exhausted_event(
                            execution_node_id.clone(),
                            &manifest.sandbox_id,
                            "sandbox-fetch",
                            kind.clone(),
                            *actual,
                            *limit,
                        ),
                        SandboxAuditDurability::Buffered,
                    )?;
                }
                if let Some(record) = self.http_exchange_record_for_command(&mut fs, &command_id)? {
                    self.hooks.append_event(
                        sandbox_http_exchange_event(execution_node_id, &record),
                        SandboxAuditDurability::Buffered,
                    )?;
                }
                return Err(error.into());
            }
        };
        self.hooks.append_event(
            sandbox_file_written_event_with_context(
                execution_node_id.clone(),
                &manifest.sandbox_id,
                &request.output_path,
                result.response_bytes,
                "sandbox-fetch",
                Some(&result.record.command_id),
                Some(&result.request_id),
            ),
            SandboxAuditDurability::Buffered,
        )?;
        self.hooks.append_event(
            sandbox_http_exchange_event(execution_node_id, &result.record),
            SandboxAuditDurability::Buffered,
        )?;
        Ok(result)
    }

    pub fn create<T, L>(
        &mut self,
        lease: &L,
        store: &mut SifsSandboxInstanceStore<T>,
        sandbox_id: SandboxId,
        request: SandboxCreateInstanceRequest,
    ) -> Result<SandboxCreateInstanceResponse, H::Error>
    where
        T: DfsMessageTransport,
        L: ClusteredSandboxLifecycleLease<H::Error>,
    {
        let config = self.hooks.load_policy_config()?;
        let create_limits = sandbox_create_limits(&request, &config.policy.max_limits);
        let execution_node_id = store.dfs.node_id().clone();
        lease.ensure_held(&sandbox_id, "before create")?;
        let mut manifest =
            SandboxInstanceManifest::new(sandbox_id, request.toolchain_id, self.hooks.now())
                .map_err(|source| self.hooks.invalid(source.to_string()))?;
        manifest.temp_policy = request.temp_policy;
        manifest.limits = create_limits;
        manifest.mounts = request.mounts;
        self.validate_manifest_policy(
            &config.policy,
            &execution_node_id,
            &manifest,
            "sandbox-create",
        )?;
        self.validate_resolved_mount_policy(
            &config.policy,
            &mut store.dfs,
            &manifest,
            &execution_node_id,
            "sandbox-create-resolved-source",
        )?;
        let mut manifest = store.create_manifest(&manifest).map_err(|source| {
            self.hooks
                .invalid(format!("create sandbox SIFS state: {source}"))
        })?;
        lease.ensure_held(&manifest.sandbox_id, "after create")?;
        let transaction_id = manifest
            .pending_reconciliation
            .as_ref()
            .ok_or_else(|| {
                self.hooks.invalid(format!(
                    "sandbox {} create lost its durable fence",
                    manifest.sandbox_id
                ))
            })?
            .transaction_id
            .clone();
        self.hooks.append_event(
            sandbox_instance_created_event(execution_node_id.clone(), &manifest),
            SandboxAuditDurability::Durable,
        )?;
        self.hooks.append_event(
            sandbox_toolchain_pinned_event(execution_node_id.clone(), &manifest),
            SandboxAuditDurability::Durable,
        )?;
        for mount in &manifest.mounts {
            self.hooks.append_event(
                sandbox_mount_added_event(execution_node_id.clone(), &manifest, mount),
                SandboxAuditDurability::Durable,
            )?;
        }
        lease.ensure_held(&manifest.sandbox_id, "after durable create audit")?;
        let sandbox_id = manifest.sandbox_id.clone();
        manifest = store.clear_reconciliation_transaction(
            &sandbox_id,
            &transaction_id,
            self.hooks.now(),
        )?;
        self.hooks
            .clear_authoritative_recovery_pending(&manifest.sandbox_id, "sandbox create committed");
        Ok(SandboxCreateInstanceResponse { manifest })
    }

    pub fn status<T>(
        &mut self,
        store: &mut SifsSandboxInstanceStore<T>,
        request: SandboxStatusRequest,
    ) -> Result<SandboxStatusResponse, H::Error>
    where
        T: DfsMessageTransport,
    {
        let manifest = store.load(&request.sandbox_id)?;
        Ok(SandboxStatusResponse { manifest })
    }

    pub fn update<T, L>(
        &mut self,
        lease: &L,
        mut store: SifsSandboxInstanceStore<T>,
        replica_candidates: Vec<NodeId>,
        request: SandboxUpdateInstanceRequest,
    ) -> Result<SandboxUpdateInstanceResponse, H::Error>
    where
        T: DfsMessageTransport,
        L: ClusteredSandboxLifecycleLease<H::Error>,
    {
        let SandboxUpdateInstanceRequest {
            sandbox_id,
            cwd,
            mounts,
            env_set,
            env_unset,
        } = request;
        let config = self.hooks.load_policy_config()?;
        let node_id = store.dfs.node_id().clone();
        let mut manifest = store.load(&sandbox_id)?;
        let cwd_changed = cwd.is_some();
        let mut env_set_keys = env_set.keys().cloned().collect::<Vec<_>>();
        let mut env_unset_keys = env_unset.clone();
        for key in env_set_keys.iter().chain(env_unset_keys.iter()) {
            if !sandbox_env_name_is_valid(key) {
                return Err(self
                    .hooks
                    .invalid(format!("invalid sandbox env name `{key}`")));
            }
        }
        for (key, value) in &env_set {
            if value.contains('\0') {
                return Err(self
                    .hooks
                    .invalid(format!("sandbox env `{key}` contains an embedded NUL byte")));
            }
        }
        env_set_keys.sort();
        env_unset_keys.sort();
        env_unset_keys.dedup();
        let mounts_reconciliation = reconcile_sandbox_mounts(&mut manifest.mounts, mounts);
        if let Some(cwd) = cwd {
            manifest.cwd = cwd;
        }
        for key in env_unset {
            manifest.env.remove(&key);
        }
        manifest.env.extend(env_set);
        self.validate_manifest_policy(&config.policy, &node_id, &manifest, "sandbox-update")?;
        if mounts_reconciliation.replacement_requested {
            let mut dfs = store.into_inner();
            self.validate_resolved_mount_policy(
                &config.policy,
                &mut dfs,
                &manifest,
                &node_id,
                "sandbox-update-resolved-source",
            )?;
            store = SifsSandboxInstanceStore::new(dfs)
                .with_replica_candidates(replica_candidates.clone());
        }
        if cwd_changed || mounts_reconciliation.replacement_requested {
            let mut fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
            validate_sandbox_working_directory(&mut fs, &manifest.cwd)?;
            store = SifsSandboxInstanceStore::new(fs.into_inner())
                .with_replica_candidates(replica_candidates);
        }
        manifest
            .advance_generation()
            .map_err(|source| self.hooks.invalid(source.to_string()))?;
        manifest.updated_at = self.hooks.now();
        lease.ensure_held(&sandbox_id, "before update")?;
        store.save_manifest(&manifest)?;
        lease.ensure_held(&sandbox_id, "after update")?;
        self.hooks.append_event(
            sandbox_instance_updated_event(
                node_id.clone(),
                &manifest,
                cwd_changed,
                &env_set_keys,
                &env_unset_keys,
                mounts_reconciliation.replacement_requested,
                mounts_reconciliation.changed,
                &mounts_reconciliation.added,
                &mounts_reconciliation.removed,
            ),
            SandboxAuditDurability::Buffered,
        )?;
        for mount in &mounts_reconciliation.removed {
            self.hooks.append_event(
                sandbox_mount_removed_event(node_id.clone(), &manifest, mount),
                SandboxAuditDurability::Buffered,
            )?;
        }
        for mount in &mounts_reconciliation.added {
            self.hooks.append_event(
                sandbox_mount_added_event(node_id.clone(), &manifest, mount),
                SandboxAuditDurability::Buffered,
            )?;
        }
        lease.ensure_held(&sandbox_id, "after update audit")?;
        Ok(SandboxUpdateInstanceResponse { manifest })
    }

    pub fn checkpoint<T, L>(
        &mut self,
        lease: &L,
        store: &mut SifsSandboxInstanceStore<T>,
        request: SandboxCheckpointRequest,
    ) -> Result<SandboxCheckpointResponse, H::Error>
    where
        T: DfsMessageTransport,
        L: ClusteredSandboxLifecycleLease<H::Error>,
    {
        let config = self.hooks.load_policy_config()?;
        let node_id = store.dfs.node_id().clone();
        let manifest = store.load(&request.sandbox_id)?;
        self.validate_manifest_policy(&config.policy, &node_id, &manifest, "sandbox-checkpoint")?;
        lease.ensure_held(&request.sandbox_id, "before checkpoint")?;
        let cancellation = lease.cancellation_token();
        let now = self.hooks.now();
        let checkpoint =
            store.create_checkpoint_with_checkpoints(&request.sandbox_id, now, || {
                mutation_checkpoint(
                    lease,
                    &cancellation,
                    &request.sandbox_id,
                    "during checkpoint",
                )
            })?;
        lease.ensure_held(&request.sandbox_id, "after checkpoint")?;
        let manifest = store.load(&request.sandbox_id)?;
        self.hooks.append_event(
            sandbox_instance_checkpointed_event(node_id, &manifest, &checkpoint),
            SandboxAuditDurability::Buffered,
        )?;
        Ok(SandboxCheckpointResponse { checkpoint })
    }

    pub fn resume<T, L>(
        &mut self,
        lease: &L,
        store: &mut SifsSandboxInstanceStore<T>,
        request: SandboxResumeRequest,
    ) -> Result<SandboxResumeResponse, H::Error>
    where
        T: DfsMessageTransport,
        L: ClusteredSandboxLifecycleLease<H::Error>,
    {
        let config = self.hooks.load_policy_config()?;
        let node_id = store.dfs.node_id().clone();
        let current = store.load_allow_pending_reconciliation(&request.sandbox_id)?;
        if let Some(pending) = current.pending_reconciliation.as_ref()
            && !matches!(
                &pending.operation,
                SandboxAuthoritativeMutationOperation::ResumeCheckpoint { checkpoint_id }
                    if checkpoint_id == &request.checkpoint_id
            )
        {
            return Err(SandboxStoreError::ReconciliationPending {
                sandbox_id: request.sandbox_id.clone(),
                transaction_id: pending.transaction_id.clone(),
            }
            .into());
        }
        self.validate_manifest_policy(&config.policy, &node_id, &current, "sandbox-resume")?;
        lease.ensure_held(&request.sandbox_id, "before resume")?;
        let cancellation = lease.cancellation_token();
        let now = self.hooks.now();
        let pending_manifest = store.resume_checkpoint_with_checkpoints(
            &request.sandbox_id,
            &request.checkpoint_id,
            now,
            || mutation_checkpoint(lease, &cancellation, &request.sandbox_id, "during resume"),
        )?;
        lease.ensure_held(&request.sandbox_id, "after resume")?;
        let transaction_id = pending_manifest
            .pending_reconciliation
            .as_ref()
            .ok_or_else(|| {
                self.hooks.invalid(format!(
                    "sandbox {} resume committed without an authoritative transaction fence",
                    request.sandbox_id
                ))
            })?
            .transaction_id
            .clone();
        self.hooks.append_event(
            sandbox_instance_resumed_event(node_id, &pending_manifest, &request.checkpoint_id),
            SandboxAuditDurability::Durable,
        )?;
        lease.ensure_held(&request.sandbox_id, "after durable resume event")?;
        let manifest = store.clear_reconciliation_transaction(
            &request.sandbox_id,
            &transaction_id,
            self.hooks.now(),
        )?;
        self.hooks
            .clear_authoritative_recovery_pending(&request.sandbox_id, "sandbox resume committed");
        Ok(SandboxResumeResponse { manifest })
    }

    pub fn reboot<T, L>(
        &mut self,
        lease: &L,
        store: &mut SifsSandboxInstanceStore<T>,
        request: SandboxRebootRequest,
    ) -> Result<SandboxRebootResponse, H::Error>
    where
        T: DfsMessageTransport,
        L: ClusteredSandboxLifecycleLease<H::Error>,
    {
        let config = self.hooks.load_policy_config()?;
        let node_id = store.dfs.node_id().clone();
        let manifest = store.load_allow_pending_reconciliation(&request.sandbox_id)?;
        if let Some(pending) = manifest.pending_reconciliation.as_ref()
            && !matches!(
                pending.operation,
                SandboxAuthoritativeMutationOperation::Reboot { .. }
            )
        {
            return Err(SandboxStoreError::ReconciliationPending {
                sandbox_id: request.sandbox_id.clone(),
                transaction_id: pending.transaction_id.clone(),
            }
            .into());
        }
        self.validate_manifest_policy(&config.policy, &node_id, &manifest, "sandbox-reboot")?;
        lease.ensure_held(&request.sandbox_id, "before reboot")?;
        let cancellation = lease.cancellation_token();
        let now = self.hooks.now();
        let (pending_manifest, cleared_tmp) =
            store.reboot_session_with_checkpoints(&request.sandbox_id, now, || {
                mutation_checkpoint(lease, &cancellation, &request.sandbox_id, "during reboot")
            })?;
        lease.ensure_held(&request.sandbox_id, "after reboot")?;
        let transaction_id = pending_manifest
            .pending_reconciliation
            .as_ref()
            .ok_or_else(|| {
                self.hooks.invalid(format!(
                    "sandbox {} reboot committed without an authoritative transaction fence",
                    request.sandbox_id
                ))
            })?
            .transaction_id
            .clone();
        self.hooks.append_event(
            sandbox_instance_rebooted_event(node_id, &pending_manifest, cleared_tmp),
            SandboxAuditDurability::Durable,
        )?;
        lease.ensure_held(&request.sandbox_id, "after durable reboot event")?;
        let manifest = store.clear_reconciliation_transaction(
            &request.sandbox_id,
            &transaction_id,
            self.hooks.now(),
        )?;
        self.hooks
            .clear_authoritative_recovery_pending(&request.sandbox_id, "sandbox reboot committed");
        Ok(SandboxRebootResponse {
            manifest,
            cleared_tmp,
        })
    }

    pub fn destroy<T, L>(
        &mut self,
        lease: &L,
        store: &mut SifsSandboxInstanceStore<T>,
        request: SandboxDestroyRequest,
    ) -> Result<SandboxDestroyResponse, H::Error>
    where
        T: DfsMessageTransport,
        L: ClusteredSandboxLifecycleLease<H::Error>,
    {
        let node_id = store.dfs.node_id().clone();
        lease.ensure_held(&request.sandbox_id, "before destroy")?;
        let pending_manifest = store.prepare_destroy(&request.sandbox_id, self.hooks.now())?;
        let transaction_id = pending_manifest
            .pending_reconciliation
            .as_ref()
            .expect("destroy fence")
            .transaction_id
            .clone();
        let cancellation = lease.cancellation_token();
        store.apply_destroy_with_checkpoints(&request.sandbox_id, &transaction_id, || {
            mutation_checkpoint(lease, &cancellation, &request.sandbox_id, "during destroy")
        })?;
        lease.ensure_held(&request.sandbox_id, "after destroy")?;
        self.hooks.append_event(
            sandbox_instance_destroyed_transaction_event(node_id, &pending_manifest),
            SandboxAuditDurability::Durable,
        )?;
        lease.ensure_held(&request.sandbox_id, "after durable destroy event")?;
        store.finalize_destroy_with_checkpoints(&request.sandbox_id, &transaction_id, || {
            mutation_checkpoint(
                lease,
                &cancellation,
                &request.sandbox_id,
                "during destroy finalization",
            )
        })?;
        self.hooks
            .clear_authoritative_recovery_pending(&request.sandbox_id, "sandbox destroy committed");
        Ok(SandboxDestroyResponse {
            sandbox_id: request.sandbox_id,
        })
    }

    pub fn list<T>(
        &mut self,
        mut store: SifsSandboxInstanceStore<T>,
        request: SandboxListRequest,
    ) -> Result<SandboxListResponse, H::Error>
    where
        T: DfsMessageTransport,
    {
        let manifest = store.load(&request.sandbox_id)?;
        let mut fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
        let entries = fs.list_mut(&request.path)?;
        Ok(SandboxListResponse { entries })
    }

    pub fn read<T>(
        &mut self,
        mut store: SifsSandboxInstanceStore<T>,
        request: SandboxReadRequest,
    ) -> Result<SandboxReadResponse, H::Error>
    where
        T: DfsMessageTransport,
    {
        let manifest = store.load(&request.sandbox_id)?;
        let mut fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
        let bytes = fs.read_mut(&request.path, request.range)?;
        Ok(SandboxReadResponse { bytes })
    }

    pub fn write<T, L>(
        &mut self,
        lease: &L,
        mut store: SifsSandboxInstanceStore<T>,
        replica_candidates: Vec<NodeId>,
        request: SandboxWriteRequest,
    ) -> Result<SandboxWriteResponse, H::Error>
    where
        T: DfsMessageTransport,
        L: ClusteredSandboxLifecycleLease<H::Error>,
    {
        let config = self.hooks.load_policy_config()?;
        let node_id = store.dfs.node_id().clone();
        let manifest = store.load(&request.sandbox_id)?;
        self.validate_manifest_policy(&config.policy, &node_id, &manifest, "sandbox-write")?;
        let mut fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest)
            .with_replica_candidates(replica_candidates.clone());
        if let Err(error) = validate_sandbox_write_resource_limits(
            &mut fs,
            &request.path,
            request.bytes.len() as u64,
            &manifest.limits,
        ) {
            if let SandboxFsError::QuotaExceeded {
                kind,
                actual,
                limit,
            } = &error
            {
                self.hooks.append_event(
                    sandbox_resource_exhausted_event(
                        node_id.clone(),
                        &request.sandbox_id,
                        "sandbox-write",
                        kind.clone(),
                        *actual,
                        *limit,
                    ),
                    SandboxAuditDurability::Buffered,
                )?;
            }
            self.append_write_denial_event(&node_id, &request.sandbox_id, &request.path, &error)?;
            return Err(error.into());
        }
        let mut manifest = manifest;
        manifest
            .advance_generation()
            .map_err(|source| self.hooks.invalid(source.to_string()))?;
        manifest.updated_at = self.hooks.now();
        let mut store = SifsSandboxInstanceStore::new(fs.into_inner())
            .with_replica_candidates(replica_candidates.clone());
        lease.ensure_held(&request.sandbox_id, "before write generation fence")?;
        store.save_manifest(&manifest)?;
        lease.ensure_held(&request.sandbox_id, "after write generation fence")?;
        let mut fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest)
            .with_replica_candidates(replica_candidates);
        let write_result = fs.write(&request.path, &request.bytes);
        lease.ensure_held(&request.sandbox_id, "after write workspace mutation")?;
        if let Err(error) = write_result {
            self.append_write_denial_event(&node_id, &request.sandbox_id, &request.path, &error)?;
            return Err(error.into());
        }
        if !sandbox_path_is_virtual_device_sink(&request.path) {
            self.hooks.append_event(
                sandbox_file_written_event(
                    node_id,
                    &request.sandbox_id,
                    &request.path,
                    request.bytes.len() as u64,
                ),
                SandboxAuditDurability::Buffered,
            )?;
        }
        Ok(SandboxWriteResponse {
            path: request.path,
            bytes_written: request.bytes.len() as u64,
        })
    }

    pub fn recover_authoritative<T, L>(
        &mut self,
        lease: &L,
        store: &mut SifsSandboxInstanceStore<T>,
        sandbox_id: &SandboxId,
    ) -> Result<(), H::Error>
    where
        T: DfsMessageTransport,
        L: ClusteredSandboxLifecycleLease<H::Error>,
    {
        self.recover_authoritative_except(lease, store, sandbox_id, None)
    }

    /// After a failed transaction, retire the obligations it recorded when no
    /// fence was left behind. A remaining fence keeps them for its recovery.
    pub fn retire_unfenced_obligations<T, L>(
        &mut self,
        lease: &L,
        store: &mut SifsSandboxInstanceStore<T>,
        sandbox_id: &SandboxId,
    ) where
        T: DfsMessageTransport,
        L: ClusteredSandboxLifecycleLease<H::Error>,
    {
        if lease
            .ensure_held(sandbox_id, "before failed transaction settlement")
            .is_err()
        {
            return;
        }
        match store.load_allow_pending_reconciliation(sandbox_id) {
            Ok(manifest) if manifest.pending_reconciliation.is_some() => {}
            Ok(_)
            | Err(SandboxStoreError::NotFound { .. })
            | Err(SandboxStoreError::LegacyLayout { .. }) => self
                .hooks
                .clear_authoritative_recovery_pending(sandbox_id, "transaction left no fence"),
            Err(error) => tracing::debug!(
                sandbox_id = %sandbox_id,
                %error,
                "failed sandbox transaction keeps its recovery obligations"
            ),
        }
    }

    /// Finish the pending authoritative transaction unless `requested`
    /// resumes it itself. The caller holds the sandbox lease.
    fn recover_authoritative_except<T, L>(
        &mut self,
        lease: &L,
        store: &mut SifsSandboxInstanceStore<T>,
        sandbox_id: &SandboxId,
        requested: Option<&SandboxAuthoritativeTransaction>,
    ) -> Result<(), H::Error>
    where
        T: DfsMessageTransport,
        L: ClusteredSandboxLifecycleLease<H::Error>,
    {
        lease.ensure_held(sandbox_id, "before authoritative transaction recovery")?;
        let node_id = store.dfs.node_id().clone();
        let manifest = match store.load_allow_pending_reconciliation(sandbox_id) {
            Ok(manifest) => manifest,
            Err(SandboxStoreError::NotFound { .. })
            | Err(SandboxStoreError::LegacyLayout { .. }) => {
                self.hooks
                    .clear_authoritative_recovery_pending(sandbox_id, "recovery target absent");
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        let Some(pending) = manifest.pending_reconciliation.clone() else {
            self.hooks
                .clear_authoritative_recovery_pending(sandbox_id, "recovery already settled");
            return Ok(());
        };
        if requested.is_some_and(|requested| requested.resumes(&pending.operation)) {
            return Ok(());
        }
        if !matches!(
            pending.operation,
            SandboxAuthoritativeMutationOperation::UnsupportedLegacyCommandTransaction
        ) {
            self.hooks
                .claim_authoritative_recovery(&SandboxPendingFenceRecord {
                    sandbox_id: sandbox_id.clone(),
                    node_id: node_id.clone(),
                    session_id: lease.session_id().to_string(),
                });
        }
        match pending.operation {
            SandboxAuthoritativeMutationOperation::UnsupportedLegacyCommandTransaction => {
                return Err(self.hooks.invalid(format!(
                    "sandbox {sandbox_id} has obsolete command transaction {}; recreate the internal-development sandbox",
                    pending.transaction_id
                )));
            }
            SandboxAuthoritativeMutationOperation::Create => {
                let manifest = store.recover_create(sandbox_id, self.hooks.now())?;
                self.hooks.append_event(
                    sandbox_instance_created_event(node_id.clone(), &manifest),
                    SandboxAuditDurability::Durable,
                )?;
                self.hooks.append_event(
                    sandbox_toolchain_pinned_event(node_id.clone(), &manifest),
                    SandboxAuditDurability::Durable,
                )?;
                for mount in &manifest.mounts {
                    self.hooks.append_event(
                        sandbox_mount_added_event(node_id.clone(), &manifest, mount),
                        SandboxAuditDurability::Durable,
                    )?;
                }
                lease.ensure_held(sandbox_id, "after recovered create audit")?;
                store.clear_reconciliation_transaction(
                    sandbox_id,
                    &pending.transaction_id,
                    self.hooks.now(),
                )?;
            }
            SandboxAuthoritativeMutationOperation::ResumeCheckpoint { checkpoint_id } => {
                let manifest =
                    store.resume_checkpoint(sandbox_id, &checkpoint_id, self.hooks.now())?;
                self.hooks.append_event(
                    sandbox_instance_resumed_event(node_id, &manifest, &checkpoint_id),
                    SandboxAuditDurability::Durable,
                )?;
                lease.ensure_held(sandbox_id, "after recovered resume audit")?;
                store.clear_reconciliation_transaction(
                    sandbox_id,
                    &pending.transaction_id,
                    self.hooks.now(),
                )?;
            }
            SandboxAuthoritativeMutationOperation::Reboot { .. } => {
                let (manifest, cleared_tmp) = store.reboot_session(sandbox_id, self.hooks.now())?;
                self.hooks.append_event(
                    sandbox_instance_rebooted_event(node_id, &manifest, cleared_tmp),
                    SandboxAuditDurability::Durable,
                )?;
                lease.ensure_held(sandbox_id, "after recovered reboot audit")?;
                store.clear_reconciliation_transaction(
                    sandbox_id,
                    &pending.transaction_id,
                    self.hooks.now(),
                )?;
            }
            SandboxAuthoritativeMutationOperation::LegacyMigration => {
                let cancellation = lease.cancellation_token();
                let now = self.hooks.now();
                let manifest =
                    store.migrate_legacy_instance_with_checkpoints(sandbox_id, now, || {
                        mutation_checkpoint(
                            lease,
                            &cancellation,
                            sandbox_id,
                            "during legacy migration",
                        )
                    })?;
                self.hooks.append_event(
                    sandbox_instance_migrated_event(node_id, &manifest),
                    SandboxAuditDurability::Durable,
                )?;
                lease.ensure_held(sandbox_id, "after recovered migration audit")?;
                store.clear_reconciliation_transaction(
                    sandbox_id,
                    &pending.transaction_id,
                    self.hooks.now(),
                )?;
            }
            SandboxAuthoritativeMutationOperation::Destroy => {
                let cancellation = lease.cancellation_token();
                store.apply_destroy_with_checkpoints(
                    sandbox_id,
                    &pending.transaction_id,
                    || {
                        mutation_checkpoint(
                            lease,
                            &cancellation,
                            sandbox_id,
                            "during destroy recovery",
                        )
                    },
                )?;
                self.hooks.append_event(
                    sandbox_instance_destroyed_transaction_event(node_id, &manifest),
                    SandboxAuditDurability::Durable,
                )?;
                lease.ensure_held(sandbox_id, "after recovered destroy audit")?;
                store.finalize_destroy_with_checkpoints(
                    sandbox_id,
                    &pending.transaction_id,
                    || {
                        mutation_checkpoint(
                            lease,
                            &cancellation,
                            sandbox_id,
                            "during destroy finalization recovery",
                        )
                    },
                )?;
            }
        }
        self.hooks
            .clear_authoritative_recovery_pending(sandbox_id, "authoritative recovery settled");
        Ok(())
    }

    fn validate_manifest_policy(
        &mut self,
        policy: &SandboxPolicy,
        node_id: &NodeId,
        manifest: &SandboxInstanceManifest,
        phase: &str,
    ) -> Result<(), H::Error> {
        if let Err(denial) = policy.validate_manifest(manifest, node_id) {
            self.hooks.append_event(
                sandbox_policy_denied_event(
                    node_id.clone(),
                    Some(&manifest.sandbox_id),
                    phase,
                    &denial,
                ),
                SandboxAuditDurability::Buffered,
            )?;
            if denial.code.starts_with("mount-") {
                self.hooks.append_event(
                    sandbox_mount_denied_event(
                        node_id.clone(),
                        Some(&manifest.sandbox_id),
                        phase,
                        &denial,
                    ),
                    SandboxAuditDurability::Buffered,
                )?;
            }
            return Err(self.hooks.invalid(denial.to_string()));
        }
        Ok(())
    }

    fn http_exchange_record_for_command<F>(
        &mut self,
        sandbox_fs: &mut F,
        command_id: &SandboxCommandId,
    ) -> Result<Option<SandboxHttpExchangeRecord>, H::Error>
    where
        F: SandboxFileSystem,
    {
        let logs_path = SandboxPath::new("/logs").expect("static logs path");
        let mut entries = sandbox_fs.list_mut(&logs_path)?;
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        let prefix = format!("/logs/{command_id}.");
        for entry in entries {
            let path = entry.path;
            let path_text = path.as_str();
            if path_text.starts_with(&prefix) && path_text.ends_with(".http.sobj") {
                let bytes = sandbox_fs.read_mut(&path, None)?;
                return Ok(Some(decode_http_exchange_record(&bytes)?));
            }
        }
        Ok(None)
    }

    fn validate_resolved_mount_policy<T: DfsMessageTransport>(
        &mut self,
        policy: &SandboxPolicy,
        dfs: &mut NetworkedDfsService<T>,
        manifest: &SandboxInstanceManifest,
        node_id: &NodeId,
        phase: &str,
    ) -> Result<(), H::Error> {
        for mount in &manifest.mounts {
            let resolved = dfs.posix_resolve_path_distributed(&mount.source_sifs_path, true)?;
            let mut resolved_mount = mount.clone();
            resolved_mount.source_sifs_path = resolved;
            if let Err(denial) = policy.validate_mounts(&[resolved_mount], node_id) {
                self.hooks.append_event(
                    sandbox_mount_denied_event(
                        node_id.clone(),
                        Some(&manifest.sandbox_id),
                        phase,
                        &denial,
                    ),
                    SandboxAuditDurability::Buffered,
                )?;
                return Err(self.hooks.invalid(denial.to_string()));
            }
        }
        Ok(())
    }

    fn append_write_denial_event(
        &mut self,
        node_id: &NodeId,
        sandbox_id: &SandboxId,
        request_path: &SandboxPath,
        error: &SandboxFsError,
    ) -> Result<(), H::Error> {
        let SandboxFsError::ReadOnly { path } = error else {
            return Ok(());
        };
        let denial = SandboxPolicyDenial::new(
            "mount-read-only-denied",
            format!(
                "write to read-only sandbox path {path} denied for request path {request_path}"
            ),
        );
        self.hooks.append_event(
            sandbox_policy_denied_event(
                node_id.clone(),
                Some(sandbox_id),
                "sandbox-write",
                &denial,
            ),
            SandboxAuditDurability::Buffered,
        )?;
        self.hooks.append_event(
            sandbox_mount_denied_event(node_id.clone(), Some(sandbox_id), "sandbox-write", &denial),
            SandboxAuditDurability::Buffered,
        )
    }
}

pub fn sandbox_create_limits(
    request: &SandboxCreateInstanceRequest,
    policy_max: &SandboxResourceLimits,
) -> SandboxResourceLimits {
    SandboxResourceLimits {
        memory_mb: request.memory_mb.unwrap_or(policy_max.memory_mb),
        wall_time_ms: request.wall_time_ms.unwrap_or(policy_max.wall_time_ms),
        max_input_bytes: request
            .max_input_bytes
            .unwrap_or(policy_max.max_input_bytes),
        max_output_bytes: request
            .max_output_bytes
            .unwrap_or(policy_max.max_output_bytes),
        max_workspace_bytes: request
            .max_workspace_bytes
            .unwrap_or(policy_max.max_workspace_bytes),
        max_written_bytes_per_command: request
            .max_written_bytes_per_command
            .unwrap_or(policy_max.max_written_bytes_per_command),
        max_file_count: request.max_file_count.unwrap_or(policy_max.max_file_count),
    }
}

fn mutation_checkpoint<L, E>(
    lease: &L,
    cancellation: &SandboxCancellationToken,
    sandbox_id: &SandboxId,
    phase: &str,
) -> Result<(), SandboxStoreError>
where
    L: ClusteredSandboxLifecycleLease<E>,
    E: Display,
{
    if cancellation.lifecycle_mutation_is_fenced() {
        return Err(SandboxStoreError::Io {
            path: sandbox_manifest_path(sandbox_id),
            detail: format!("sandbox lifecycle lease was lost {phase}"),
        });
    }
    lease
        .ensure_held(sandbox_id, phase)
        .map_err(|error| SandboxStoreError::Io {
            path: sandbox_manifest_path(sandbox_id),
            detail: error.to_string(),
        })
}

#[cfg(test)]
mod replica_candidate_tests {
    use super::*;

    #[test]
    fn candidates_include_local_and_advertised_content_services() {
        let local = NodeId::from_legacy_named("node_local").expect("local node");
        let replica = NodeId::from_legacy_named("node_replica").expect("replica node");
        let metadata_only = NodeId::from_legacy_named("node_metadata").expect("metadata node");
        let candidates = clustered_sandbox_replica_candidates(
            &local,
            [
                DfsNodeAdvertisement::metadata(metadata_only),
                DfsNodeAdvertisement::content(replica.clone()),
                DfsNodeAdvertisement::content(local.clone()),
            ],
        );

        assert!(candidates.contains(&local));
        assert!(candidates.contains(&replica));
        assert_eq!(candidates.len(), 2);
    }
}
