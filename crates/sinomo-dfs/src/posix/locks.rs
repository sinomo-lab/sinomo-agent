//! Strong, whole-file POSIX advisory-lock coordination.

use std::time::Duration;

use crate::advisory_lock::observe_advisory_lock_outcome_at;
use crate::posix_model::{DfsPosixError, Result, posix_error_for_namespace_rejection};
use crate::*;

/// Locks and blocking waiters are renewed by clients before this lease ends.
/// The lease bounds how long an unreachable former primary can obstruct a new
/// authority epoch; the epoch itself fences old tokens immediately.
// Mounted clients serialize DFS calls through one service instance, and a
// foreground fsync may legitimately occupy that lane for up to 50 seconds.
// Keep the lease comfortably above that bound so a healthy holder is not
// fenced merely because its renewal waits behind its own durability barrier.
pub const DEFAULT_DFS_ADVISORY_LOCK_LEASE: Duration = Duration::from_secs(120);

/// Network budget for one lock-manager round trip, including exact metadata
/// replication of any state transition.
pub const DEFAULT_DFS_ADVISORY_LOCK_RPC_TIMEOUT: Duration = Duration::from_secs(45);

/// Bounded clock-difference assumption used by the epoch warm-up fence. This
/// is deliberately much larger than normal NTP error because Sinomo nodes may
/// be intermittently connected mobile devices. Deployments whose clocks can
/// differ by more than this bound must not enable renewable strong locks.
pub const DEFAULT_DFS_ADVISORY_LOCK_MAX_CLOCK_SKEW: Duration = Duration::from_secs(300);

const POSIX_LOCK_AUTHORITY_REDIRECT_LIMIT: usize = 3;
const POSIX_LOCK_REMOTE_RESPONSE_MARGIN: Duration = Duration::from_secs(1);

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Apply one idempotent advisory-lock command to the stable object visible
    /// at `path`. The current metadata primary durably prepares the resulting
    /// lock journal prefix in DFS. Visibility requires either exact
    /// acknowledgement by the complete shard or the DFS recovery quorum plus
    /// an operation-bound cluster-Raft permit.
    pub fn posix_advisory_lock(
        &mut self,
        path: &str,
        command: DfsAdvisoryLockCommand,
    ) -> Result<DfsAdvisoryLockOutcome> {
        let (path, object) = self.advisory_lock_object_for_path(path)?;
        self.posix_advisory_lock_for_object(&path, &object.object_id, command)
    }

    /// Same operation with the caller's already-open stable object identity.
    /// The path is only a routing hint: same-shard renames must not detach a
    /// POSIX lock from the inode represented by `object_id`.
    pub fn posix_advisory_lock_for_object(
        &mut self,
        path: &str,
        object_id: &DfsObjectId,
        command: DfsAdvisoryLockCommand,
    ) -> Result<DfsAdvisoryLockOutcome> {
        self.posix_advisory_lock_for_object_with_timeout(
            path,
            object_id,
            command,
            DEFAULT_DFS_ADVISORY_LOCK_RPC_TIMEOUT,
        )
    }

    /// Bounded variant used by mounted-client renewal lanes. Callers must keep
    /// their maximum number of live grants consistent with this timeout and
    /// the configured lease duration.
    pub fn posix_advisory_lock_for_object_with_timeout(
        &mut self,
        path: &str,
        object_id: &DfsObjectId,
        command: DfsAdvisoryLockCommand,
        timeout: Duration,
    ) -> Result<DfsAdvisoryLockOutcome> {
        if timeout.is_zero() {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: "advisory-lock RPC timeout must be greater than zero".to_string(),
            }));
        }
        let command = scope_advisory_lock_command(command, &self.node_id)?;
        let object = self.advisory_lock_object_by_id(object_id, path)?;
        let authority_path = object.path.clone();

        let operation = DfsNamespaceOperation::AdvisoryLock {
            path: authority_path.clone(),
            object_id: object_id.clone(),
            command,
            timeout_millis: timeout
                .saturating_sub(POSIX_LOCK_REMOTE_RESPONSE_MARGIN)
                .max(Duration::from_millis(1))
                .as_millis()
                .min(u64::MAX as u128) as u64,
        };
        let file_system_id = object.file_system_id.clone();
        let mut primary = match self.routed_namespace_primary_for_path(&authority_path)? {
            Some(primary) => primary,
            None => {
                return self.posix_apply_advisory_lock_local(
                    &authority_path,
                    object_id,
                    match &operation {
                        DfsNamespaceOperation::AdvisoryLock { command, .. } => command,
                        _ => unreachable!(),
                    },
                    timeout,
                );
            }
        };

        for _ in 0..POSIX_LOCK_AUTHORITY_REDIRECT_LIMIT {
            let settled = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    timeout,
                    false,
                    false,
                )
                .map_err(DfsPosixError::from)?;
            match settled.result {
                DfsNamespaceResult::Applied => {
                    return match settled.posix_coordination_result {
                        Some(DfsPosixCoordinationResult::AdvisoryLock(outcome)) => Ok(outcome),
                        _ => Err(DfsPosixError::Dfs(DfsError::Distributed {
                            message: "advisory-lock response omitted its typed outcome".to_string(),
                        })),
                    };
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current == self.node_id => {
                    return self.posix_apply_advisory_lock_local(
                        &authority_path,
                        object_id,
                        match &operation {
                            DfsNamespaceOperation::AdvisoryLock { command, .. } => command,
                            _ => unreachable!(),
                        },
                        timeout,
                    );
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current != primary => {
                    primary = self
                        .refreshed_primary_after_authority_redirect(
                            &file_system_id,
                            &operation,
                            &primary,
                            current,
                        )
                        .map_err(DfsPosixError::from)?;
                }
                DfsNamespaceResult::Rejected {
                    reason,
                    posix_error,
                    ..
                } => return Err(posix_error_for_namespace_rejection(reason, posix_error)),
            }
        }
        Err(DfsPosixError::Dfs(DfsError::Distributed {
            message: format!(
                "advisory-lock operation exceeded {POSIX_LOCK_AUTHORITY_REDIRECT_LIMIT} authority redirects"
            ),
        }))
    }

    pub(crate) fn posix_apply_advisory_lock_local(
        &mut self,
        path: &str,
        expected_object_id: &DfsObjectId,
        command: &DfsAdvisoryLockCommand,
        timeout: Duration,
    ) -> Result<DfsAdvisoryLockOutcome> {
        // Namespace migration holds this path guard across bundle capture,
        // destination adoption, and source eviction. Lock transitions must
        // serialize with that cutover or a source-side renew/grant could be
        // omitted from the transferred aggregate.
        self.ensure_no_running_cross_shard_move_for_path(path)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(path)?;
        let object = self.advisory_lock_object_by_id(expected_object_id, path)?;
        if object.path != path {
            self.ensure_no_running_cross_shard_move_for_path(&object.path)?;
            self.ensure_resolved_path_not_in_cross_shard_flight(&object.path)?;
        }

        // Capture the committed configuration epoch without a Raft write. The
        // exact-prefix barrier below is the authorization point: every
        // configured shard member must acknowledge the operation.
        let authority_epoch = self.local_metadata_owner_epoch_hint_for_object(&object)?;
        let context = DfsAdvisoryLockTransitionContext {
            authority_epoch,
            now_unix_nanos: dfs_now_unix_nanos(),
            lease_duration_nanos: DEFAULT_DFS_ADVISORY_LOCK_LEASE
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            max_clock_skew_nanos: DEFAULT_DFS_ADVISORY_LOCK_MAX_CLOCK_SKEW
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        };
        let applied = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "distributed advisory locks require a durable metadata backend"
                        .to_string(),
                })
            })?
            .apply_advisory_lock_command(
                &object.file_system_id,
                &object.object_id,
                context,
                command,
            )?;

        // Replicate and freshly verify the journal prefix containing the
        // transition before exposing its outcome. This also covers unchanged
        // queries and idempotent retries: they must not trust a stale isolated
        // primary merely because no new journal entry was appended.
        let metadata_target = self.capture_metadata_quorum_sync_target(&object)?;
        if metadata_target.shard_id != applied.shard_id
            || metadata_target.journal_sequence < applied.durable_through_sequence
        {
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: format!(
                    "advisory-lock transition durable at {}:{} but sync captured {}:{}",
                    applied.shard_id.0,
                    applied.durable_through_sequence,
                    metadata_target.shard_id.0,
                    metadata_target.journal_sequence
                ),
            }));
        }
        self.sync_metadata_shard_to_quorum(&metadata_target, timeout)?;
        let mut outcome = applied.outcome;
        observe_advisory_lock_outcome_at(&mut outcome, dfs_now_unix_nanos());
        Ok(outcome)
    }

    fn advisory_lock_object_for_path(&mut self, path: &str) -> Result<(String, DfsObjectMetadata)> {
        let resolved = self.resolve_existing_posix_path_distributed(path, true)?;
        let object = self
            .lookup_sifs_path(&resolved)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: resolved.clone(),
            })?;
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory || object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::InvalidPath { path: resolved });
        }
        Ok((object.path.clone(), object))
    }

    fn advisory_lock_object_by_id(
        &self,
        object_id: &DfsObjectId,
        path_hint: &str,
    ) -> Result<DfsObjectMetadata> {
        let object = self
            .current_read_object_metadata(object_id)
            .map_err(DfsPosixError::from)?;
        if object.kind == DfsObjectKind::Directory || object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::InvalidPath {
                path: path_hint.to_string(),
            });
        }
        Ok(object)
    }
}

fn advisory_lock_node_scope(node_id: &sinomo_core::node::NodeId) -> String {
    let node = node_id.to_string();
    format!("node:{}:{node}:", node.len())
}

fn scoped_lock_session_id(
    node_id: &sinomo_core::node::NodeId,
    session_id: DfsAdvisoryLockSessionId,
) -> DfsAdvisoryLockSessionId {
    DfsAdvisoryLockSessionId::new(format!(
        "{}{}",
        advisory_lock_node_scope(node_id),
        session_id.0
    ))
}

fn scoped_lock_request_id(
    node_id: &sinomo_core::node::NodeId,
    request_id: DfsAdvisoryLockRequestId,
) -> DfsAdvisoryLockRequestId {
    DfsAdvisoryLockRequestId::new(format!(
        "{}{}",
        advisory_lock_node_scope(node_id),
        request_id.0
    ))
}

fn lock_session_belongs_to_node(
    node_id: &sinomo_core::node::NodeId,
    session_id: &DfsAdvisoryLockSessionId,
) -> bool {
    session_id.0.starts_with(&advisory_lock_node_scope(node_id))
}

fn lock_request_belongs_to_node(
    node_id: &sinomo_core::node::NodeId,
    request_id: &DfsAdvisoryLockRequestId,
) -> bool {
    request_id.0.starts_with(&advisory_lock_node_scope(node_id))
}

fn scope_advisory_lock_command(
    command: DfsAdvisoryLockCommand,
    node_id: &sinomo_core::node::NodeId,
) -> Result<DfsAdvisoryLockCommand> {
    let command = match command {
        DfsAdvisoryLockCommand::Acquire {
            request_id,
            session_id,
            mode,
            blocking,
            process_id,
        } => DfsAdvisoryLockCommand::Acquire {
            request_id: scoped_lock_request_id(node_id, request_id),
            session_id: scoped_lock_session_id(node_id, session_id),
            mode,
            blocking,
            process_id,
        },
        DfsAdvisoryLockCommand::Query {
            request_id,
            session_id,
            mode,
        } => DfsAdvisoryLockCommand::Query {
            request_id: scoped_lock_request_id(node_id, request_id),
            session_id: scoped_lock_session_id(node_id, session_id),
            mode,
        },
        DfsAdvisoryLockCommand::Renew { request_id, token } => {
            validate_advisory_lock_token_node(node_id, &token)?;
            DfsAdvisoryLockCommand::Renew {
                request_id: scoped_lock_request_id(node_id, request_id),
                token,
            }
        }
        DfsAdvisoryLockCommand::Convert {
            request_id,
            token,
            mode,
        } => {
            validate_advisory_lock_token_node(node_id, &token)?;
            DfsAdvisoryLockCommand::Convert {
                request_id: scoped_lock_request_id(node_id, request_id),
                token,
                mode,
            }
        }
        DfsAdvisoryLockCommand::Unlock { request_id, token } => {
            validate_advisory_lock_token_node(node_id, &token)?;
            DfsAdvisoryLockCommand::Unlock {
                request_id: scoped_lock_request_id(node_id, request_id),
                token,
            }
        }
        DfsAdvisoryLockCommand::Close {
            request_id,
            session_id,
        } => DfsAdvisoryLockCommand::Close {
            request_id: scoped_lock_request_id(node_id, request_id),
            session_id: scoped_lock_session_id(node_id, session_id),
        },
        DfsAdvisoryLockCommand::Cancel {
            request_id,
            session_id,
            acquire_request_id,
        } => DfsAdvisoryLockCommand::Cancel {
            request_id: scoped_lock_request_id(node_id, request_id),
            session_id: scoped_lock_session_id(node_id, session_id),
            acquire_request_id: scoped_lock_request_id(node_id, acquire_request_id),
        },
    };
    Ok(command)
}

fn validate_advisory_lock_token_node(
    node_id: &sinomo_core::node::NodeId,
    token: &DfsAdvisoryLockToken,
) -> Result<()> {
    if lock_session_belongs_to_node(node_id, &token.session_id)
        && lock_request_belongs_to_node(node_id, &token.grant_id)
    {
        return Ok(());
    }
    Err(DfsPosixError::Dfs(DfsError::Distributed {
        message: format!("advisory-lock token is not owned by authenticated node {node_id}"),
    }))
}

pub(crate) fn advisory_lock_command_belongs_to_node(
    command: &DfsAdvisoryLockCommand,
    node_id: &sinomo_core::node::NodeId,
) -> bool {
    let request_owned = lock_request_belongs_to_node(node_id, command.request_id());
    request_owned
        && match command {
            DfsAdvisoryLockCommand::Acquire { session_id, .. }
            | DfsAdvisoryLockCommand::Query { session_id, .. }
            | DfsAdvisoryLockCommand::Close { session_id, .. } => {
                lock_session_belongs_to_node(node_id, session_id)
            }
            DfsAdvisoryLockCommand::Renew { token, .. }
            | DfsAdvisoryLockCommand::Convert { token, .. }
            | DfsAdvisoryLockCommand::Unlock { token, .. } => {
                lock_session_belongs_to_node(node_id, &token.session_id)
                    && lock_request_belongs_to_node(node_id, &token.grant_id)
            }
            DfsAdvisoryLockCommand::Cancel {
                session_id,
                acquire_request_id,
                ..
            } => {
                lock_session_belongs_to_node(node_id, session_id)
                    && lock_request_belongs_to_node(node_id, acquire_request_id)
            }
        }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, VecDeque};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sinomo_core::kv::{InProcessKvStore, KvEntry, KvError, KvRevision, KvTxn, KvTxnResponse};
    use sinomo_core::network::NetworkError;

    use super::*;

    #[derive(Debug)]
    struct LockAckTransport {
        local_node_id: sinomo_core::node::NodeId,
        acknowledged_nodes: BTreeSet<sinomo_core::node::NodeId>,
        incoming: VecDeque<DfsRoutedMessage>,
        next_frame_id: u64,
    }

    impl DfsTransportIdentity for LockAckTransport {
        fn local_node_id(&self) -> &sinomo_core::node::NodeId {
            &self.local_node_id
        }
    }

    impl DfsMessageTransport for LockAckTransport {
        fn send_routed_call(
            &mut self,
            target_node_id: &sinomo_core::node::NodeId,
            call: DfsRoutedCall,
        ) -> std::result::Result<(), NetworkError> {
            let message = crate::materialize_dfs_routed_call_sync(call)?;
            if !self.acknowledged_nodes.contains(target_node_id) {
                return Ok(());
            }
            let frame_id = self.next_frame_id;
            self.next_frame_id = self.next_frame_id.saturating_add(1);
            let response = match message.message {
                DfsNetworkMessage::MetadataJournalBatch(command) => {
                    let applied_entries = command
                        .durable_replication
                        .as_ref()
                        .map(|replication| replication.journal_frames.len())
                        .or_else(|| {
                            command.authority_probe.as_ref().and_then(|probe| {
                                probe
                                    .durable_journal_ack
                                    .then_some(probe.journal_frames.len())
                            })
                        })
                        .unwrap_or(command.batch.entries.len());
                    let verified_authority_probe_id = command
                        .durable_replication
                        .as_ref()
                        .map(|replication| replication.confirmation_id.clone())
                        .or_else(|| {
                            command.authority_probe.as_ref().and_then(
                                crate::service_replica::metadata_authority_probe_confirmation_id,
                            )
                        });
                    DfsNetworkMessage::MetadataJournalBatchResponse(
                        DfsMetadataJournalBatchResponse {
                            request_id: command.request_id,
                            shard_id: command.batch.shard_id,
                            next_after_sequence: command.batch.next_after_sequence,
                            applied_entries,
                            verified_prefix_only: command.verify_prefix_only,
                            verified_authority_probe_id,
                            result: DfsMetadataJournalBatchResult::Applied,
                        },
                    )
                }
                DfsNetworkMessage::MetadataSnapshotInstall(command) => {
                    DfsNetworkMessage::MetadataSnapshotInstallResponse(
                        DfsMetadataSnapshotInstallResponse {
                            request_id: command.request_id,
                            shard_id: command.shard_id,
                            next_after_sequence: command.next_after_sequence,
                            result: DfsMetadataSnapshotInstallResult::Installed {
                                archived_existing: false,
                            },
                        },
                    )
                }
                _ => return Ok(()),
            };
            self.incoming.push_back(build_dfs_routed_message(
                target_node_id,
                &self.local_node_id,
                frame_id,
                frame_id,
                response,
            ));
            Ok(())
        }

        fn drain_incoming_messages(
            &mut self,
        ) -> std::result::Result<Vec<DfsRoutedMessage>, NetworkError> {
            Ok(self.incoming.drain(..).collect())
        }

        fn pending_incoming_count(&self) -> std::result::Result<Option<usize>, NetworkError> {
            Ok(Some(self.incoming.len()))
        }
    }

    #[derive(Debug)]
    struct LockCountingControlPlane {
        inner: InProcessKvStore,
        txn_calls: Arc<AtomicUsize>,
    }

    impl DfsControlPlaneKvClient for LockCountingControlPlane {
        fn get(&self, key: &str) -> std::result::Result<Option<KvEntry>, KvError> {
            DfsControlPlaneKvClient::get(&self.inner, key)
        }

        fn range_prefix(&self, prefix: &str) -> std::result::Result<Vec<KvEntry>, KvError> {
            DfsControlPlaneKvClient::range_prefix(&self.inner, prefix)
        }

        fn put(&mut self, key: String, value: Vec<u8>) -> std::result::Result<KvRevision, KvError> {
            DfsControlPlaneKvClient::put(&mut self.inner, key, value)
        }

        fn txn(&mut self, txn: KvTxn) -> std::result::Result<KvTxnResponse, KvError> {
            self.txn_calls.fetch_add(1, Ordering::SeqCst);
            DfsControlPlaneKvClient::txn(&mut self.inner, txn)
        }
    }

    fn node(name: &str) -> sinomo_core::node::NodeId {
        sinomo_core::node::NodeId::from_legacy_named(name).expect("test node")
    }

    fn clustered_lock_fixture(
        replica_node_ids: Vec<sinomo_core::node::NodeId>,
        acknowledged_nodes: Vec<sinomo_core::node::NodeId>,
    ) -> (
        NetworkedDfsService<LockAckTransport>,
        DfsObjectMetadata,
        Arc<AtomicUsize>,
    ) {
        let root = sinomo_testdir::TestDir::new("dfs-posix-lock-authority").keep();
        let primary = node("node_lock_authority_primary");
        let shard = DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids,
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut inner = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut inner)
            .put_metadata_shard(&shard)
            .expect("metadata shard config");
        let txn_calls = Arc::new(AtomicUsize::new(0));
        let control_plane = LockCountingControlPlane {
            inner,
            txn_calls: Arc::clone(&txn_calls),
        };
        let object = DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named("object_lock_authority").expect("object"),
            file_system_id: sifs_file_system_id(),
            path: "/lock-authority.txt".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: primary.clone(),
            current_locations: vec![primary.clone()],
            content_hash: Some("blake3:lock-authority".to_string()),
            content_size_bytes: Some(1),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: DfsVersion::from_legacy_named("version_lock_authority").expect("version"),
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        let mut backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
                .expect("metadata backend");
        backend
            .put_file_system(&DfsFileSystem::sifs(primary.clone()))
            .expect("filesystem");
        backend.put_object(&object).expect("object metadata");
        let service = NetworkedDfsService::new(
            LockAckTransport {
                local_node_id: primary,
                acknowledged_nodes: acknowledged_nodes.into_iter().collect(),
                incoming: VecDeque::new(),
                next_frame_id: 1,
            },
            LocalContentStore::new(root.join("content")).expect("content store"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend(backend)
        .expect("metadata-backed service");
        (service, object, txn_calls)
    }

    #[test]
    fn command_scope_binds_sessions_and_requests_to_the_authenticated_node() {
        let owner = node("node_lock_scope_owner");
        let attacker = node("node_lock_scope_attacker");
        let scoped = scope_advisory_lock_command(
            DfsAdvisoryLockCommand::Acquire {
                request_id: DfsAdvisoryLockRequestId::new("acquire"),
                session_id: DfsAdvisoryLockSessionId::new("session"),
                mode: DfsAdvisoryLockMode::Exclusive,
                blocking: false,
                process_id: Some(7),
            },
            &owner,
        )
        .expect("scope command");

        assert!(advisory_lock_command_belongs_to_node(&scoped, &owner));
        assert!(!advisory_lock_command_belongs_to_node(&scoped, &attacker));
        assert!(!advisory_lock_command_belongs_to_node(
            &DfsAdvisoryLockCommand::Close {
                request_id: DfsAdvisoryLockRequestId::new("forged-close"),
                session_id: DfsAdvisoryLockSessionId::new("session"),
            },
            &owner,
        ));
    }

    #[test]
    fn clustered_lock_full_shard_path_uses_no_control_plane_transaction() {
        let replica_a = node("node_lock_authority_replica_a");
        let replica_b = node("node_lock_authority_replica_b");
        let (mut service, object, txn_calls) = clustered_lock_fixture(
            vec![replica_a.clone(), replica_b.clone()],
            vec![replica_a, replica_b],
        );

        let outcome = service
            .posix_apply_advisory_lock_local(
                &object.path,
                &object.object_id,
                &DfsAdvisoryLockCommand::Acquire {
                    request_id: DfsAdvisoryLockRequestId::new("full-shard-acquire"),
                    session_id: DfsAdvisoryLockSessionId::new("full-shard-session"),
                    mode: DfsAdvisoryLockMode::Exclusive,
                    blocking: false,
                    process_id: Some(7),
                },
                Duration::ZERO,
            )
            .expect("full-shard lock grant");

        assert!(matches!(outcome, DfsAdvisoryLockOutcome::Acquired { .. }));
        assert_eq!(txn_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn clustered_lock_partial_shard_uses_operation_bound_quorum_fallback() {
        let replica_a = node("node_lock_fallback_replica_a");
        let replica_b = node("node_lock_fallback_replica_b");
        let (mut service, object, txn_calls) =
            clustered_lock_fixture(vec![replica_a.clone(), replica_b], vec![replica_a]);

        let outcome = service
            .posix_apply_advisory_lock_local(
                &object.path,
                &object.object_id,
                &DfsAdvisoryLockCommand::Acquire {
                    request_id: DfsAdvisoryLockRequestId::new("fallback-acquire"),
                    session_id: DfsAdvisoryLockSessionId::new("fallback-session"),
                    mode: DfsAdvisoryLockMode::Exclusive,
                    blocking: false,
                    process_id: Some(8),
                },
                Duration::ZERO,
            )
            .expect("operation-bound fallback lock grant");

        assert!(matches!(outcome, DfsAdvisoryLockOutcome::Acquired { .. }));
        assert_eq!(txn_calls.load(Ordering::SeqCst), 1);
    }
}
