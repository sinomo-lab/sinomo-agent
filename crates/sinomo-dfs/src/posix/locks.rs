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
    /// lock journal prefix in DFS. Visibility requires exact acknowledgement
    /// by every configured metadata participant; a KV quorum or permit cannot
    /// replace a missing shard member.
    pub fn posix_advisory_lock(
        &mut self,
        path: &str,
        command: DfsAdvisoryLockCommand,
    ) -> Result<DfsAdvisoryLockOutcome> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        if timeout.is_zero() {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: "advisory-lock RPC timeout must be greater than zero".to_string(),
            }));
        }
        let command = scope_advisory_lock_command(command, &self.node_id)?;
        let object = self.advisory_lock_object_by_id_distributed(object_id, path)?;
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
                } if current == self.node_id
                    && !self.transport.delegates_local_namespace_authority() =>
                {
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let scope = self.metadata_effect_scope_for_object(
            &object,
            format!("advisory-lock:{}", sinomo_core::Id::generate()),
        )?;
        let _execution = self.acquire_prepared_execution(&scope)?;
        let backend = self.metadata_backend.as_mut().ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Metadata {
                message: "distributed advisory locks require current durable metadata".into(),
            })
        })?;
        // The prefix, lock aggregate and anchor predicates must describe one
        // refreshed immutable cut. A different local handle may have published
        // since this handle's last export; mixing that export with fresh point
        // reads creates a stale preparation even without concurrent mutation.
        backend.begin_consistent_read_snapshot_for_path(&object.file_system_id, &object.path)?;
        let plan: Result<_> = (|| {
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
            let before = backend
                .export_shard_table_snapshot(&scope.shard_id)?
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: "advisory-lock publication has no exact current shard cut".into(),
                    })
                })?;
            let existing =
                backend.get_advisory_lock_state(&object.file_system_id, &object.object_id)?;
            let transition = crate::transition_advisory_lock_state(
                existing.as_ref(),
                &object.file_system_id,
                &object.object_id,
                context,
                &command,
            )
            .map_err(|error| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: error.to_string(),
                })
            })?;
            let payload = if transition.changed {
                let current = backend.get_object_on_shard(&scope.shard_id, &object.object_id)?;
                let occupant = backend.get_object_by_path_on_shard(
                    &scope.shard_id,
                    &object.file_system_id,
                    &object.path,
                )?;
                let mut payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
                    scope.clone(),
                    vec![crate::DfsNamespaceObjectVersionExpectation {
                        object_id: object.object_id.clone(),
                        expected_version: current.map(|row| row.version),
                    }],
                    vec![crate::DfsNamespacePathOccupantExpectation {
                        path: object.path.clone(),
                        expected_object_id: occupant.map(|row| row.object_id),
                    }],
                    vec![crate::DfsPreparedMetadataEffect::PutAdvisoryLockState {
                        state: transition.state,
                    }],
                    time::OffsetDateTime::now_utc(),
                )?;
                payload.expected_journal_sequence = Some(before.prefix_proof.journal_sequence);
                Some(payload)
            } else {
                None
            };
            Ok((transition.outcome, payload))
        })();
        backend.end_consistent_read_snapshot_for_path(&object.file_system_id, &object.path);
        let (outcome, payload) = plan?;
        if let Some(payload) = payload {
            self.publish_prepared_operation(payload)?;
        }
        let applied_shard_id = scope.shard_id;
        let durable_through_sequence = self
            .metadata_backend
            .as_ref()
            .expect("lock backend")
            .export_shard_table_snapshot(&applied_shard_id)?
            .expect("actual lock shard")
            .prefix_proof
            .journal_sequence;

        // Replicate and freshly verify the journal prefix containing the
        // transition before exposing its outcome. This also covers unchanged
        // queries and idempotent retries: they must not trust a stale isolated
        // primary merely because no new journal entry was appended.
        let metadata_target = self.capture_metadata_quorum_sync_target(&object)?;
        if metadata_target.shard_id != applied_shard_id
            || metadata_target.journal_sequence < durable_through_sequence
        {
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: format!(
                    "advisory-lock transition durable at {}:{} but sync captured {}:{}",
                    applied_shard_id.0,
                    durable_through_sequence,
                    metadata_target.shard_id.0,
                    metadata_target.journal_sequence
                ),
            }));
        }
        self.sync_metadata_shard_to_quorum(&metadata_target, timeout)?;
        let mut outcome = outcome;
        observe_advisory_lock_outcome_at(&mut outcome, dfs_now_unix_nanos());
        Ok(outcome)
    }

    fn advisory_lock_object_for_path(&mut self, path: &str) -> Result<(String, DfsObjectMetadata)> {
        let resolved = self.resolve_existing_posix_path_distributed(path, true)?;
        let object = self
            .lookup_sifs_path_direct_distributed_synced(&resolved)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: resolved.clone(),
            })?;
        let object = self.resolve_hardlink_target_distributed(&object, None)?;
        let object = advisory_lock_target(object, &resolved)?;
        Ok((object.path.clone(), object))
    }

    /// The requester resolves the opened identity through its authority; a
    /// caller delegating to its Main keeps no local metadata view.
    fn advisory_lock_object_by_id_distributed(
        &mut self,
        object_id: &DfsObjectId,
        path_hint: &str,
    ) -> Result<DfsObjectMetadata> {
        let object = self
            .current_read_object_metadata_distributed(object_id)
            .map_err(DfsPosixError::from)?;
        advisory_lock_target(object, path_hint)
    }

    fn advisory_lock_object_by_id(
        &self,
        object_id: &DfsObjectId,
        path_hint: &str,
    ) -> Result<DfsObjectMetadata> {
        let object = self
            .current_read_object_metadata(object_id)
            .map_err(DfsPosixError::from)?;
        advisory_lock_target(object, path_hint)
    }
}

fn advisory_lock_target(object: DfsObjectMetadata, path_hint: &str) -> Result<DfsObjectMetadata> {
    if object.kind == DfsObjectKind::Directory || object.kind == DfsObjectKind::Tombstone {
        return Err(DfsPosixError::InvalidPath {
            path: path_hint.to_string(),
        });
    }
    Ok(object)
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
    use crate::test_runtime::{DfsTestControlPlaneProbe, DfsTestRuntime};

    use super::*;

    fn node(name: &str) -> sinomo_core::node::NodeId {
        sinomo_core::node::NodeId::from_legacy_named(name).expect("test node")
    }

    fn clustered_lock_fixture(
        primary: sinomo_core::node::NodeId,
        replica_node_ids: Vec<sinomo_core::node::NodeId>,
        acknowledged_nodes: Vec<sinomo_core::node::NodeId>,
    ) -> (
        DfsTestRuntime,
        NetworkedDfsService<crate::DfsNodeTransport>,
        DfsObjectMetadata,
        DfsTestControlPlaneProbe,
    ) {
        let mut runtime = DfsTestRuntime::new("dfs-posix-lock-authority", primary.clone())
            .expect("normal DFS lock runtime");
        for replica in &replica_node_ids {
            runtime
                .define_node(replica.clone())
                .expect("define lock replica");
        }
        runtime.start_nodes().expect("mount normal DFS receivers");
        let mut setup = runtime.client(&primary).expect("normal fixture writer");
        let stat = setup
            .posix_write_file(
                "/lock-authority.txt",
                b"lock bytes",
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
                    replica_candidates: vec![primary.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .expect("normal immutable publication");
        let object = runtime
            .node_metadata(&primary)
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap();
        drop(setup);
        for replica in &replica_node_ids {
            if !acknowledged_nodes.contains(replica) {
                runtime
                    .stop_node(replica)
                    .expect("stop unavailable lock replica");
            }
        }
        // The local lock authority prepares and commits grants in the
        // metadata owner's store; the delegating client keeps no store.
        let (service, txn_calls) = runtime
            .metadata_primary_service_with_control_plane_probe(&object.path)
            .expect("normal foreground DFS lock authority");
        assert_eq!(service.node_id(), &primary);
        (runtime, service, object, txn_calls)
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
        let nodes = DfsTestRuntime::ordered_node_ids("lock-full-shard", 3);
        let primary = nodes[0].clone();
        let replica_a = nodes[1].clone();
        let replica_b = nodes[2].clone();
        let (runtime, mut service, object, txn_calls) = clustered_lock_fixture(
            primary,
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
                Duration::from_secs(5),
            )
            .expect("full-shard lock grant");

        assert!(matches!(outcome, DfsAdvisoryLockOutcome::Acquired { .. }));
        assert_eq!(txn_calls.txn_calls(), 0);
        drop(service);
        runtime
            .close()
            .expect("close every DFS lock task and producer");
    }

    #[test]
    fn clustered_lock_waits_for_all_metadata_replicas_without_kv_fallback() {
        let nodes = DfsTestRuntime::ordered_node_ids("lock-partial-shard", 3);
        let primary = nodes[0].clone();
        let replica_a = nodes[1].clone();
        let replica_b = nodes[2].clone();
        let (mut runtime, mut service, object, txn_calls) = clustered_lock_fixture(
            primary,
            vec![replica_a.clone(), replica_b.clone()],
            vec![replica_a],
        );
        let command = DfsAdvisoryLockCommand::Acquire {
            request_id: DfsAdvisoryLockRequestId::new("fallback-acquire"),
            session_id: DfsAdvisoryLockSessionId::new("fallback-session"),
            mode: DfsAdvisoryLockMode::Exclusive,
            blocking: false,
            process_id: Some(8),
        };
        let error = service
            .posix_apply_advisory_lock_local(
                &object.path,
                &object.object_id,
                &command,
                Duration::from_secs(5),
            )
            .expect_err("two acknowledgements cannot publish a three-member lock grant");
        assert!(
            matches!(
                error,
                DfsPosixError::Dfs(DfsError::MetadataCommitOutcomeUnknown {
                    cause: crate::DfsCommitOutcomeUnknownCause::AcknowledgementsMissing {
                        required: 3,
                        acknowledged: 2,
                        ..
                    },
                    ..
                })
            ),
            "{error}"
        );
        assert_eq!(txn_calls.txn_calls(), 0);
        let backend = service.metadata_backend().unwrap();
        assert!(
            backend
                .get_advisory_lock_state(&object.file_system_id, &object.object_id)
                .unwrap()
                .is_none()
        );
        let pending = backend.list_prepared_operations().unwrap().into_iter().filter(|record| {
            record.metadata_effects.iter().any(|effect| matches!(effect,
                crate::DfsPreparedMetadataEffect::PutAdvisoryLockState { state } if state.object_id == object.object_id))
        }).collect::<Vec<_>>();
        assert_eq!(pending.len(), 1);
        let original = &pending[0];
        assert!(original.expected_journal_sequence.is_some());
        runtime.reopen_node(replica_b).unwrap();
        // Reconcile the exact saved body, including its original grant clock
        // and read set, before retrying the caller's unchanged lock command.
        let recovered = service
            .commit_stored_prepared_operation(&original.digest)
            .unwrap();
        assert_eq!(recovered.payload, original.payload);
        let before = service
            .metadata_backend()
            .unwrap()
            .export_shard_table_snapshot(&original.shard_id)
            .unwrap()
            .unwrap();
        let outcome = service
            .posix_apply_advisory_lock_local(
                &object.path,
                &object.object_id,
                &command,
                Duration::from_secs(5),
            )
            .expect("full-participant exact lock replay");
        assert!(matches!(outcome, DfsAdvisoryLockOutcome::Acquired { .. }));
        assert_eq!(txn_calls.txn_calls(), 0);
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .export_shard_table_snapshot(&original.shard_id)
                .unwrap()
                .unwrap(),
            before
        );
        let committed = service
            .metadata_backend()
            .unwrap()
            .prepared_operation_record(&original.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(committed.digest, original.digest);
        assert_eq!(committed.payload, original.payload);
        assert_eq!(
            committed.status,
            crate::DfsPreparedOperationStatus::Committed
        );
        drop(service);
        runtime
            .close()
            .expect("close every DFS lock task and producer");
    }
}
