//! Durable whole-file advisory-lock state and atomic transitions.
//!
//! The lock manager deliberately operates on stable DFS object identifiers,
//! not paths. One aggregate is owned by the authoritative metadata shard for
//! an object. Callers supply that shard's current authority epoch; advancing
//! the epoch atomically fences every older grant and waiter before applying the
//! requested command.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::metadata_records::{DfsMetadataJournalEntry, DfsMetadataShardId};
use crate::{DfsFileSystemId, DfsObjectId};

/// Completed outcomes currently remain durable for the lifetime of the lock
/// aggregate. Evicting an old acquire id would let a delayed duplicate
/// resurrect a grant, so a bounded implementation requires a future monotone
/// per-session request watermark rather than cache-style trimming.
pub const DFS_ADVISORY_LOCK_REQUEST_OUTCOME_LIMIT: usize = usize::MAX;

/// Stable lock-owner identity supplied by the POSIX/session adapter.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DfsAdvisoryLockSessionId(pub String);

impl DfsAdvisoryLockSessionId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Idempotency key for one lock-manager command.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DfsAdvisoryLockRequestId(pub String);

impl DfsAdvisoryLockRequestId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Whole-file POSIX advisory-lock mode.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsAdvisoryLockMode {
    Shared,
    Exclusive,
}

/// Exact capability returned by a successful lock acquisition.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct DfsAdvisoryLockToken {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub session_id: DfsAdvisoryLockSessionId,
    pub authority_epoch: u64,
    /// The acquisition request id is also the unique grant id.
    pub grant_id: DfsAdvisoryLockRequestId,
}

/// A currently live grant.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAdvisoryLockGrant {
    pub token: DfsAdvisoryLockToken,
    pub mode: DfsAdvisoryLockMode,
    /// Process id reported by `F_GETLK`, when the client has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<u32>,
    pub lease_deadline_unix_nanos: u64,
    /// Remaining validity as observed by the authority immediately before it
    /// returns this grant. Clients turn this relative duration into a local
    /// monotonic deadline; they must not compare the authority's Unix
    /// deadline with their own wall clock.
    #[serde(default)]
    pub lease_remaining_nanos: u64,
}

/// One FIFO blocking acquisition.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAdvisoryLockWaiter {
    pub request_id: DfsAdvisoryLockRequestId,
    pub session_id: DfsAdvisoryLockSessionId,
    pub mode: DfsAdvisoryLockMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<u32>,
    pub authority_epoch: u64,
    pub queue_sequence: u64,
    pub lease_deadline_unix_nanos: u64,
}

/// The conflicting holder or queued request returned by query/try-lock.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAdvisoryLockConflict {
    pub session_id: DfsAdvisoryLockSessionId,
    pub mode: DfsAdvisoryLockMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<u32>,
}

/// Transport-neutral command applied atomically to one object aggregate.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DfsAdvisoryLockCommand {
    Acquire {
        request_id: DfsAdvisoryLockRequestId,
        session_id: DfsAdvisoryLockSessionId,
        mode: DfsAdvisoryLockMode,
        blocking: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        process_id: Option<u32>,
    },
    Query {
        request_id: DfsAdvisoryLockRequestId,
        session_id: DfsAdvisoryLockSessionId,
        mode: DfsAdvisoryLockMode,
    },
    Renew {
        request_id: DfsAdvisoryLockRequestId,
        token: DfsAdvisoryLockToken,
    },
    /// Atomically change the mode of an exact current grant.
    ///
    /// A conflicting upgrade returns `WouldBlock` while retaining the old
    /// grant. Retry the logical conversion with a fresh request id.
    Convert {
        request_id: DfsAdvisoryLockRequestId,
        token: DfsAdvisoryLockToken,
        mode: DfsAdvisoryLockMode,
    },
    Unlock {
        request_id: DfsAdvisoryLockRequestId,
        token: DfsAdvisoryLockToken,
    },
    /// Release the grant and cancel every waiter owned by a session.
    Close {
        request_id: DfsAdvisoryLockRequestId,
        session_id: DfsAdvisoryLockSessionId,
    },
    /// Cancel one exact blocking acquisition. If promotion won the race, the
    /// exact grant created by that acquisition is released atomically too.
    Cancel {
        request_id: DfsAdvisoryLockRequestId,
        session_id: DfsAdvisoryLockSessionId,
        acquire_request_id: DfsAdvisoryLockRequestId,
    },
}

impl DfsAdvisoryLockCommand {
    pub fn request_id(&self) -> &DfsAdvisoryLockRequestId {
        match self {
            Self::Acquire { request_id, .. }
            | Self::Query { request_id, .. }
            | Self::Renew { request_id, .. }
            | Self::Convert { request_id, .. }
            | Self::Unlock { request_id, .. }
            | Self::Close { request_id, .. }
            | Self::Cancel { request_id, .. } => request_id,
        }
    }
}

/// Durable result of a lock-manager command.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DfsAdvisoryLockOutcome {
    Acquired {
        grant: DfsAdvisoryLockGrant,
    },
    Waiting {
        acquire_request_id: DfsAdvisoryLockRequestId,
        position: usize,
    },
    WouldBlock {
        conflict: DfsAdvisoryLockConflict,
    },
    QueryResult {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conflict: Option<DfsAdvisoryLockConflict>,
    },
    Renewed {
        grant: DfsAdvisoryLockGrant,
    },
    Converted {
        grant: DfsAdvisoryLockGrant,
    },
    Unlocked,
    Closed {
        released_grant: bool,
        cancelled_waiter_count: usize,
    },
    Cancelled {
        acquire_request_id: DfsAdvisoryLockRequestId,
        was_waiting: bool,
    },
    /// The blocking request stopped being renewed before it was promoted.
    WaitExpired {
        acquire_request_id: DfsAdvisoryLockRequestId,
    },
    /// The blocking request was invalidated by a newer authority epoch.
    Fenced {
        authority_epoch: u64,
    },
    /// A previously granted lock expired or was fenced.
    LockLost,
    /// A new authority must wait out leases issued by the prior epoch.
    AuthorityWarming {
        retry_after_unix_nanos: u64,
    },
    /// An unlock/renew token is not the exact current grant capability.
    StaleToken,
    /// The session already owns a grant or a blocking request for this object.
    SessionBusy,
    /// The request id was previously used with a different command payload.
    RequestMismatch,
    /// The caller reached an authority older than the persisted aggregate.
    StaleAuthority {
        requested_epoch: u64,
        state_epoch: u64,
    },
}

/// A retained idempotency record.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAdvisoryLockRequestOutcomeRecord {
    pub request_id: DfsAdvisoryLockRequestId,
    pub command: DfsAdvisoryLockCommand,
    pub outcome: DfsAdvisoryLockOutcome,
    pub recorded_at_unix_nanos: u64,
}

/// Durable cancellation fence for an Acquire that may arrive after Cancel.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAdvisoryLockAcquireTombstone {
    pub acquire_request_id: DfsAdvisoryLockRequestId,
    pub session_id: DfsAdvisoryLockSessionId,
}

/// One journaled lock aggregate per stable DFS object.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAdvisoryLockStateRecord {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub authority_epoch: u64,
    /// Set only while an aggregate copied to another shard awaits rebinding
    /// to that destination's independently-numbered authority epoch.
    #[serde(default)]
    pub authority_rebind_required: bool,
    /// New grants are fenced until all possibly-live prior-epoch leases end.
    #[serde(default)]
    pub grant_not_before_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<DfsAdvisoryLockGrant>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waiters: Vec<DfsAdvisoryLockWaiter>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub request_outcomes: Vec<DfsAdvisoryLockRequestOutcomeRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cancelled_acquires: Vec<DfsAdvisoryLockAcquireTombstone>,
    /// Exact source-state identities this aggregate has durably adopted while
    /// moving between metadata shards. Retaining lineage makes a lost-reply
    /// retry safe even after destination epoch rebinding advanced the state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub migration_source_fingerprints: Vec<String>,
    #[serde(default)]
    pub next_queue_sequence: u64,
}

/// Stable wire identity for a complete lock aggregate copied during a
/// cross-shard move. The destination echoes this digest only after it has
/// installed the exact rebinding-marked state and durably synced that state to
/// metadata quorum.
pub(crate) fn advisory_lock_state_fingerprint(
    state: &DfsAdvisoryLockStateRecord,
) -> Result<String, serde_json::Error> {
    let encoded = serde_json::to_vec(state)?;
    Ok(format!("blake3:{}", blake3::hash(&encoded).to_hex()))
}

impl DfsAdvisoryLockStateRecord {
    pub fn empty(
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        authority_epoch: u64,
    ) -> Self {
        Self {
            file_system_id,
            object_id,
            authority_epoch,
            authority_rebind_required: false,
            grant_not_before_unix_nanos: 0,
            grants: Vec::new(),
            waiters: Vec::new(),
            request_outcomes: Vec::new(),
            cancelled_acquires: Vec::new(),
            migration_source_fingerprints: Vec::new(),
            next_queue_sequence: 1,
        }
    }
}

pub(crate) fn prepare_advisory_lock_state_for_migration(
    source: &DfsAdvisoryLockStateRecord,
) -> Result<(DfsAdvisoryLockStateRecord, String), serde_json::Error> {
    let mut prepared = source.clone();
    prepared.authority_rebind_required = true;
    let source_fingerprint = advisory_lock_state_fingerprint(&prepared)?;
    if !prepared
        .migration_source_fingerprints
        .contains(&source_fingerprint)
    {
        prepared
            .migration_source_fingerprints
            .push(source_fingerprint.clone());
    }
    Ok((prepared, source_fingerprint))
}

/// Shard-authority and lease inputs for one atomic transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsAdvisoryLockTransitionContext {
    pub authority_epoch: u64,
    pub now_unix_nanos: u64,
    pub lease_duration_nanos: u64,
    /// Maximum absolute wall-clock difference permitted between consecutive
    /// metadata authorities. Epoch warm-up adds this bound to every prior
    /// lease deadline before issuing a new grant.
    pub max_clock_skew_nanos: u64,
}

/// State and result produced by the pure transition engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsAdvisoryLockTransition {
    pub state: DfsAdvisoryLockStateRecord,
    pub outcome: DfsAdvisoryLockOutcome,
    pub changed: bool,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DfsAdvisoryLockTransitionError {
    #[error("advisory-lock lease duration must be greater than zero")]
    ZeroLeaseDuration,
    #[error(
        "advisory-lock aggregate scope mismatch: expected {expected_file_system_id}/{expected_object_id}, found {actual_file_system_id}/{actual_object_id}"
    )]
    ScopeMismatch {
        expected_file_system_id: DfsFileSystemId,
        expected_object_id: DfsObjectId,
        actual_file_system_id: DfsFileSystemId,
        actual_object_id: DfsObjectId,
    },
}

/// Apply one command to an object-scoped lock aggregate.
///
/// Expiry, epoch fencing, FIFO promotion, a mutating command, and its
/// idempotency outcome are all part of the returned single-state transition.
/// Queries observe the post-maintenance state but do not append durable
/// idempotency history. Persistence backends must journal a changed `state`
/// atomically before returning `outcome`.
pub fn transition_advisory_lock_state(
    existing: Option<&DfsAdvisoryLockStateRecord>,
    file_system_id: &DfsFileSystemId,
    object_id: &DfsObjectId,
    context: DfsAdvisoryLockTransitionContext,
    command: &DfsAdvisoryLockCommand,
) -> Result<DfsAdvisoryLockTransition, DfsAdvisoryLockTransitionError> {
    if context.lease_duration_nanos == 0 {
        return Err(DfsAdvisoryLockTransitionError::ZeroLeaseDuration);
    }

    let original = existing.cloned();
    let mut state = existing.cloned().unwrap_or_else(|| {
        DfsAdvisoryLockStateRecord::empty(
            file_system_id.clone(),
            object_id.clone(),
            context.authority_epoch,
        )
    });
    validate_scope(&state, file_system_id, object_id)?;

    if state.authority_rebind_required {
        fence_previous_epoch(&mut state, context);
    } else if context.authority_epoch < state.authority_epoch {
        return Ok(DfsAdvisoryLockTransition {
            changed: original.as_ref() != Some(&state),
            state,
            outcome: DfsAdvisoryLockOutcome::StaleAuthority {
                requested_epoch: context.authority_epoch,
                state_epoch: existing.expect("state epoch checked above").authority_epoch,
            },
        });
    }

    if context.authority_epoch > state.authority_epoch {
        fence_previous_epoch(&mut state, context);
    }
    expire_dead_state(
        &mut state,
        context.now_unix_nanos,
        context.max_clock_skew_nanos,
    );
    if state.grant_not_before_unix_nanos <= context.now_unix_nanos {
        state.grant_not_before_unix_nanos = 0;
    }
    promote_fifo_waiters(&mut state, context);

    if let Some(record_index) = state
        .request_outcomes
        .iter()
        .position(|record| record.request_id == *command.request_id())
    {
        let record = &state.request_outcomes[record_index];
        let outcome = if record.command != *command {
            DfsAdvisoryLockOutcome::RequestMismatch
        } else {
            current_idempotent_outcome(&state, record)
        };
        if record.command == *command
            && matches!(
                command,
                DfsAdvisoryLockCommand::Acquire { blocking: true, .. }
            )
            && let Some(waiter) = state
                .waiters
                .iter_mut()
                .find(|waiter| waiter.request_id == *command.request_id())
        {
            let remaining = waiter
                .lease_deadline_unix_nanos
                .saturating_sub(context.now_unix_nanos);
            if remaining <= (context.lease_duration_nanos / 2).max(1) {
                waiter.lease_deadline_unix_nanos = lease_deadline(context);
            }
        }
        let changed = original.as_ref() != Some(&state);
        return Ok(DfsAdvisoryLockTransition {
            state,
            outcome,
            changed,
        });
    }

    let outcome = apply_new_command(&mut state, context, command);
    if matches!(command, DfsAdvisoryLockCommand::Query { .. }) {
        return Ok(DfsAdvisoryLockTransition {
            changed: original.as_ref() != Some(&state),
            state,
            outcome,
        });
    }
    if matches!(outcome, DfsAdvisoryLockOutcome::AuthorityWarming { .. }) {
        return Ok(DfsAdvisoryLockTransition {
            changed: original.as_ref() != Some(&state),
            state,
            outcome,
        });
    }
    state
        .request_outcomes
        .push(DfsAdvisoryLockRequestOutcomeRecord {
            request_id: command.request_id().clone(),
            command: command.clone(),
            outcome: outcome.clone(),
            recorded_at_unix_nanos: context.now_unix_nanos,
        });
    trim_request_outcomes(&mut state);

    Ok(DfsAdvisoryLockTransition {
        changed: original.as_ref() != Some(&state),
        state,
        outcome,
    })
}

fn validate_scope(
    state: &DfsAdvisoryLockStateRecord,
    file_system_id: &DfsFileSystemId,
    object_id: &DfsObjectId,
) -> Result<(), DfsAdvisoryLockTransitionError> {
    if state.file_system_id == *file_system_id && state.object_id == *object_id {
        return Ok(());
    }
    Err(DfsAdvisoryLockTransitionError::ScopeMismatch {
        expected_file_system_id: file_system_id.clone(),
        expected_object_id: object_id.clone(),
        actual_file_system_id: state.file_system_id.clone(),
        actual_object_id: state.object_id.clone(),
    })
}

fn apply_new_command(
    state: &mut DfsAdvisoryLockStateRecord,
    context: DfsAdvisoryLockTransitionContext,
    command: &DfsAdvisoryLockCommand,
) -> DfsAdvisoryLockOutcome {
    match command {
        DfsAdvisoryLockCommand::Acquire {
            request_id,
            session_id,
            mode,
            blocking,
            process_id,
        } => {
            // Session identifiers are one-shot open-handle identities. Close
            // is retained in the durable request history, so it also acts as
            // a session-generation fence for an Acquire that was delayed in
            // transport and arrives after the close operation.
            if session_was_closed(state, session_id) {
                return DfsAdvisoryLockOutcome::LockLost;
            }
            if let Some(tombstone) = state
                .cancelled_acquires
                .iter()
                .find(|tombstone| tombstone.acquire_request_id == *request_id)
            {
                return if tombstone.session_id == *session_id {
                    DfsAdvisoryLockOutcome::Cancelled {
                        acquire_request_id: request_id.clone(),
                        was_waiting: false,
                    }
                } else {
                    DfsAdvisoryLockOutcome::RequestMismatch
                };
            }
            if state.grant_not_before_unix_nanos > context.now_unix_nanos {
                return DfsAdvisoryLockOutcome::AuthorityWarming {
                    retry_after_unix_nanos: state.grant_not_before_unix_nanos,
                };
            }
            if session_is_busy(state, session_id) {
                return DfsAdvisoryLockOutcome::SessionBusy;
            }
            if let Some(conflict) = first_conflict(state, session_id, *mode, true) {
                if !blocking {
                    return DfsAdvisoryLockOutcome::WouldBlock { conflict };
                }
                let queue_sequence = state.next_queue_sequence.max(1);
                state.next_queue_sequence = queue_sequence.saturating_add(1);
                state.waiters.push(DfsAdvisoryLockWaiter {
                    request_id: request_id.clone(),
                    session_id: session_id.clone(),
                    mode: *mode,
                    process_id: *process_id,
                    authority_epoch: state.authority_epoch,
                    queue_sequence,
                    lease_deadline_unix_nanos: lease_deadline(context),
                });
                return DfsAdvisoryLockOutcome::Waiting {
                    acquire_request_id: request_id.clone(),
                    position: state.waiters.len(),
                };
            }
            let grant = grant_for(
                state,
                request_id.clone(),
                session_id.clone(),
                *mode,
                *process_id,
                context,
            );
            state.grants.push(grant.clone());
            DfsAdvisoryLockOutcome::Acquired { grant }
        }
        DfsAdvisoryLockCommand::Query {
            session_id, mode, ..
        } => {
            if state.grant_not_before_unix_nanos > context.now_unix_nanos {
                DfsAdvisoryLockOutcome::AuthorityWarming {
                    retry_after_unix_nanos: state.grant_not_before_unix_nanos,
                }
            } else {
                DfsAdvisoryLockOutcome::QueryResult {
                    conflict: first_conflict(state, session_id, *mode, false),
                }
            }
        }
        DfsAdvisoryLockCommand::Renew { token, .. } => {
            if !token_scope_matches(state, token) || token.authority_epoch != state.authority_epoch
            {
                return DfsAdvisoryLockOutcome::LockLost;
            }
            if let Some(grant) = state.grants.iter_mut().find(|grant| grant.token == *token) {
                grant.lease_deadline_unix_nanos = lease_deadline(context);
                grant.lease_remaining_nanos = context.lease_duration_nanos;
                return DfsAdvisoryLockOutcome::Renewed {
                    grant: grant.clone(),
                };
            }
            if request_outcome_is_lock_lost(state, &token.grant_id) {
                DfsAdvisoryLockOutcome::LockLost
            } else {
                DfsAdvisoryLockOutcome::StaleToken
            }
        }
        DfsAdvisoryLockCommand::Convert { token, mode, .. } => {
            if !token_scope_matches(state, token) || token.authority_epoch != state.authority_epoch
            {
                return DfsAdvisoryLockOutcome::StaleToken;
            }
            let Some(grant_index) = state.grants.iter().position(|grant| grant.token == *token)
            else {
                return if request_outcome_is_lock_lost(state, &token.grant_id) {
                    DfsAdvisoryLockOutcome::LockLost
                } else {
                    DfsAdvisoryLockOutcome::StaleToken
                };
            };
            if *mode == DfsAdvisoryLockMode::Exclusive
                && state.grant_not_before_unix_nanos > context.now_unix_nanos
            {
                return DfsAdvisoryLockOutcome::AuthorityWarming {
                    retry_after_unix_nanos: state.grant_not_before_unix_nanos,
                };
            }
            if *mode == DfsAdvisoryLockMode::Exclusive
                && state.grants[grant_index].mode != DfsAdvisoryLockMode::Exclusive
                && let Some(waiter) = state.waiters.first()
            {
                // A shared holder may not jump an earlier queued request by
                // upgrading in place. It retains the shared grant and must
                // retry after the FIFO head is served or cancelled.
                return DfsAdvisoryLockOutcome::WouldBlock {
                    conflict: DfsAdvisoryLockConflict {
                        session_id: waiter.session_id.clone(),
                        mode: waiter.mode,
                        process_id: waiter.process_id,
                    },
                };
            }
            if let Some(conflict) = state.grants.iter().enumerate().find_map(|(index, grant)| {
                (index != grant_index && modes_conflict(*mode, grant.mode)).then(|| {
                    DfsAdvisoryLockConflict {
                        session_id: grant.token.session_id.clone(),
                        mode: grant.mode,
                        process_id: grant.process_id,
                    }
                })
            }) {
                return DfsAdvisoryLockOutcome::WouldBlock { conflict };
            }
            let grant = &mut state.grants[grant_index];
            grant.mode = *mode;
            grant.lease_deadline_unix_nanos = lease_deadline(context);
            grant.lease_remaining_nanos = context.lease_duration_nanos;
            DfsAdvisoryLockOutcome::Converted {
                grant: grant.clone(),
            }
        }
        DfsAdvisoryLockCommand::Unlock { token, .. } => {
            if !token_scope_matches(state, token) || token.authority_epoch != state.authority_epoch
            {
                return DfsAdvisoryLockOutcome::StaleToken;
            }
            let Some(index) = state.grants.iter().position(|grant| grant.token == *token) else {
                return if request_outcome_is_lock_lost(state, &token.grant_id) {
                    DfsAdvisoryLockOutcome::LockLost
                } else {
                    DfsAdvisoryLockOutcome::StaleToken
                };
            };
            state.grants.remove(index);
            mark_acquire_terminal(state, &token.grant_id, DfsAdvisoryLockOutcome::LockLost);
            promote_fifo_waiters(state, context);
            DfsAdvisoryLockOutcome::Unlocked
        }
        DfsAdvisoryLockCommand::Close { session_id, .. } => {
            let grant_ids = state
                .grants
                .iter()
                .filter(|grant| grant.token.session_id == *session_id)
                .map(|grant| grant.token.grant_id.clone())
                .collect::<Vec<_>>();
            let waiter_ids = state
                .waiters
                .iter()
                .filter(|waiter| waiter.session_id == *session_id)
                .map(|waiter| waiter.request_id.clone())
                .collect::<Vec<_>>();
            state
                .grants
                .retain(|grant| grant.token.session_id != *session_id);
            state
                .waiters
                .retain(|waiter| waiter.session_id != *session_id);
            for grant_id in &grant_ids {
                mark_acquire_terminal(state, grant_id, DfsAdvisoryLockOutcome::LockLost);
            }
            for waiter_id in &waiter_ids {
                mark_acquire_terminal(
                    state,
                    waiter_id,
                    DfsAdvisoryLockOutcome::Cancelled {
                        acquire_request_id: waiter_id.clone(),
                        was_waiting: true,
                    },
                );
            }
            promote_fifo_waiters(state, context);
            DfsAdvisoryLockOutcome::Closed {
                released_grant: !grant_ids.is_empty(),
                cancelled_waiter_count: waiter_ids.len(),
            }
        }
        DfsAdvisoryLockCommand::Cancel {
            session_id,
            acquire_request_id,
            ..
        } => {
            if let Some(tombstone) = state
                .cancelled_acquires
                .iter()
                .find(|tombstone| tombstone.acquire_request_id == *acquire_request_id)
                && tombstone.session_id != *session_id
            {
                return DfsAdvisoryLockOutcome::RequestMismatch;
            }
            if !state
                .cancelled_acquires
                .iter()
                .any(|tombstone| tombstone.acquire_request_id == *acquire_request_id)
            {
                state
                    .cancelled_acquires
                    .push(DfsAdvisoryLockAcquireTombstone {
                        acquire_request_id: acquire_request_id.clone(),
                        session_id: session_id.clone(),
                    });
            }
            let waiting_index = state.waiters.iter().position(|waiter| {
                waiter.session_id == *session_id && waiter.request_id == *acquire_request_id
            });
            let was_waiting = waiting_index.is_some();
            if let Some(index) = waiting_index {
                state.waiters.remove(index);
                mark_acquire_terminal(
                    state,
                    acquire_request_id,
                    DfsAdvisoryLockOutcome::Cancelled {
                        acquire_request_id: acquire_request_id.clone(),
                        was_waiting: true,
                    },
                );
                promote_fifo_waiters(state, context);
            } else if let Some(index) = state.grants.iter().position(|grant| {
                grant.token.session_id == *session_id && grant.token.grant_id == *acquire_request_id
            }) {
                state.grants.remove(index);
                mark_acquire_terminal(
                    state,
                    acquire_request_id,
                    DfsAdvisoryLockOutcome::Cancelled {
                        acquire_request_id: acquire_request_id.clone(),
                        was_waiting: false,
                    },
                );
                promote_fifo_waiters(state, context);
            }
            DfsAdvisoryLockOutcome::Cancelled {
                acquire_request_id: acquire_request_id.clone(),
                was_waiting,
            }
        }
    }
}

fn fence_previous_epoch(
    state: &mut DfsAdvisoryLockStateRecord,
    context: DfsAdvisoryLockTransitionContext,
) {
    let prior_lease_deadline = state
        .grants
        .iter()
        .map(|grant| grant.lease_deadline_unix_nanos)
        .chain(
            state
                .waiters
                .iter()
                .map(|waiter| waiter.lease_deadline_unix_nanos),
        )
        .max()
        .map(|deadline| deadline.saturating_add(context.max_clock_skew_nanos))
        .filter(|deadline| *deadline > context.now_unix_nanos)
        .unwrap_or(0);
    let grant_ids = state
        .grants
        .iter()
        .map(|grant| grant.token.grant_id.clone())
        .collect::<Vec<_>>();
    let waiter_ids = state
        .waiters
        .iter()
        .map(|waiter| waiter.request_id.clone())
        .collect::<Vec<_>>();
    state.grants.clear();
    state.waiters.clear();
    state.authority_epoch = context.authority_epoch;
    state.authority_rebind_required = false;
    state.grant_not_before_unix_nanos = state.grant_not_before_unix_nanos.max(prior_lease_deadline);
    for grant_id in grant_ids {
        mark_acquire_terminal(state, &grant_id, DfsAdvisoryLockOutcome::LockLost);
    }
    for waiter_id in waiter_ids {
        mark_acquire_terminal(
            state,
            &waiter_id,
            DfsAdvisoryLockOutcome::Fenced {
                authority_epoch: context.authority_epoch,
            },
        );
    }
    for record in &mut state.request_outcomes {
        if matches!(
            record.outcome,
            DfsAdvisoryLockOutcome::LockLost | DfsAdvisoryLockOutcome::Fenced { .. }
        ) {
            record.recorded_at_unix_nanos = context.now_unix_nanos;
        }
    }
}

fn expire_dead_state(
    state: &mut DfsAdvisoryLockStateRecord,
    now_unix_nanos: u64,
    max_clock_skew_nanos: u64,
) {
    // A client converts the authority-reported duration to a monotonic local
    // deadline. The authority must therefore not replace the grant merely
    // because its wall clock steps forward within the declared skew bound.
    let is_effectively_expired =
        |deadline: u64| deadline.saturating_add(max_clock_skew_nanos) <= now_unix_nanos;
    let expired_grant_ids = state
        .grants
        .iter()
        .filter(|grant| is_effectively_expired(grant.lease_deadline_unix_nanos))
        .map(|grant| grant.token.grant_id.clone())
        .collect::<Vec<_>>();
    let expired_waiter_ids = state
        .waiters
        .iter()
        .filter(|waiter| is_effectively_expired(waiter.lease_deadline_unix_nanos))
        .map(|waiter| waiter.request_id.clone())
        .collect::<Vec<_>>();
    state
        .grants
        .retain(|grant| !is_effectively_expired(grant.lease_deadline_unix_nanos));
    state
        .waiters
        .retain(|waiter| !is_effectively_expired(waiter.lease_deadline_unix_nanos));
    for grant_id in expired_grant_ids {
        mark_acquire_terminal(state, &grant_id, DfsAdvisoryLockOutcome::LockLost);
    }
    for waiter_id in expired_waiter_ids {
        mark_acquire_terminal(
            state,
            &waiter_id,
            DfsAdvisoryLockOutcome::WaitExpired {
                acquire_request_id: waiter_id.clone(),
            },
        );
    }
}

fn promote_fifo_waiters(
    state: &mut DfsAdvisoryLockStateRecord,
    context: DfsAdvisoryLockTransitionContext,
) {
    state.waiters.sort_by_key(|waiter| waiter.queue_sequence);
    while let Some(waiter) = state.waiters.first().cloned() {
        if waiter.authority_epoch != state.authority_epoch
            || first_grant_conflict(state, &waiter.session_id, waiter.mode).is_some()
        {
            break;
        }
        state.waiters.remove(0);
        let grant = grant_for(
            state,
            waiter.request_id.clone(),
            waiter.session_id,
            waiter.mode,
            waiter.process_id,
            context,
        );
        state.grants.push(grant.clone());
        mark_acquire_terminal(
            state,
            &waiter.request_id,
            DfsAdvisoryLockOutcome::Acquired { grant },
        );

        // Consecutive shared waiters may be co-granted. An exclusive grant or
        // the next exclusive waiter ends this promotion pass.
        if waiter.mode == DfsAdvisoryLockMode::Exclusive
            || state
                .waiters
                .first()
                .is_some_and(|next| next.mode == DfsAdvisoryLockMode::Exclusive)
        {
            break;
        }
    }
}

fn grant_for(
    state: &DfsAdvisoryLockStateRecord,
    grant_id: DfsAdvisoryLockRequestId,
    session_id: DfsAdvisoryLockSessionId,
    mode: DfsAdvisoryLockMode,
    process_id: Option<u32>,
    context: DfsAdvisoryLockTransitionContext,
) -> DfsAdvisoryLockGrant {
    DfsAdvisoryLockGrant {
        token: DfsAdvisoryLockToken {
            file_system_id: state.file_system_id.clone(),
            object_id: state.object_id.clone(),
            session_id,
            authority_epoch: state.authority_epoch,
            grant_id,
        },
        mode,
        process_id,
        lease_deadline_unix_nanos: lease_deadline(context),
        lease_remaining_nanos: context.lease_duration_nanos,
    }
}

/// Refresh response-relative lease validity after every required durability
/// barrier has completed. Persisted idempotency records retain the absolute
/// authority deadline; this projection prevents a delayed retry response from
/// granting a fresh client-side lease.
pub(crate) fn observe_advisory_lock_outcome_at(
    outcome: &mut DfsAdvisoryLockOutcome,
    authority_now_unix_nanos: u64,
) {
    let grant = match outcome {
        DfsAdvisoryLockOutcome::Acquired { grant }
        | DfsAdvisoryLockOutcome::Renewed { grant }
        | DfsAdvisoryLockOutcome::Converted { grant } => grant,
        _ => return,
    };
    grant.lease_remaining_nanos = grant
        .lease_deadline_unix_nanos
        .saturating_sub(authority_now_unix_nanos);
}

fn lease_deadline(context: DfsAdvisoryLockTransitionContext) -> u64 {
    context
        .now_unix_nanos
        .saturating_add(context.lease_duration_nanos)
}

fn session_is_busy(
    state: &DfsAdvisoryLockStateRecord,
    session_id: &DfsAdvisoryLockSessionId,
) -> bool {
    state
        .grants
        .iter()
        .any(|grant| grant.token.session_id == *session_id)
        || state
            .waiters
            .iter()
            .any(|waiter| waiter.session_id == *session_id)
}

fn session_was_closed(
    state: &DfsAdvisoryLockStateRecord,
    session_id: &DfsAdvisoryLockSessionId,
) -> bool {
    state.request_outcomes.iter().any(|record| {
        matches!(
            &record.command,
            DfsAdvisoryLockCommand::Close {
                session_id: closed_session,
                ..
            } if closed_session == session_id
        )
    })
}

fn first_conflict(
    state: &DfsAdvisoryLockStateRecord,
    requester: &DfsAdvisoryLockSessionId,
    requested_mode: DfsAdvisoryLockMode,
    honor_fifo_waiters: bool,
) -> Option<DfsAdvisoryLockConflict> {
    first_grant_conflict(state, requester, requested_mode).or_else(|| {
        if honor_fifo_waiters {
            state.waiters.first().map(|waiter| DfsAdvisoryLockConflict {
                session_id: waiter.session_id.clone(),
                mode: waiter.mode,
                process_id: waiter.process_id,
            })
        } else {
            None
        }
    })
}

fn first_grant_conflict(
    state: &DfsAdvisoryLockStateRecord,
    requester: &DfsAdvisoryLockSessionId,
    requested_mode: DfsAdvisoryLockMode,
) -> Option<DfsAdvisoryLockConflict> {
    state.grants.iter().find_map(|grant| {
        (grant.token.session_id != *requester && modes_conflict(requested_mode, grant.mode)).then(
            || DfsAdvisoryLockConflict {
                session_id: grant.token.session_id.clone(),
                mode: grant.mode,
                process_id: grant.process_id,
            },
        )
    })
}

fn modes_conflict(requested: DfsAdvisoryLockMode, held: DfsAdvisoryLockMode) -> bool {
    requested == DfsAdvisoryLockMode::Exclusive || held == DfsAdvisoryLockMode::Exclusive
}

fn token_scope_matches(state: &DfsAdvisoryLockStateRecord, token: &DfsAdvisoryLockToken) -> bool {
    token.file_system_id == state.file_system_id && token.object_id == state.object_id
}

fn current_idempotent_outcome(
    state: &DfsAdvisoryLockStateRecord,
    record: &DfsAdvisoryLockRequestOutcomeRecord,
) -> DfsAdvisoryLockOutcome {
    match &record.command {
        DfsAdvisoryLockCommand::Acquire { .. } => {
            if let Some(grant) = state
                .grants
                .iter()
                .find(|grant| grant.token.grant_id == record.request_id)
            {
                return DfsAdvisoryLockOutcome::Acquired {
                    grant: grant.clone(),
                };
            }
            if let Some(position) = state
                .waiters
                .iter()
                .position(|waiter| waiter.request_id == record.request_id)
            {
                return DfsAdvisoryLockOutcome::Waiting {
                    acquire_request_id: record.request_id.clone(),
                    position: position + 1,
                };
            }
        }
        DfsAdvisoryLockCommand::Renew { token, .. } => {
            return state
                .grants
                .iter()
                .find(|grant| grant.token == *token)
                .cloned()
                .map(|grant| DfsAdvisoryLockOutcome::Renewed { grant })
                .unwrap_or(DfsAdvisoryLockOutcome::LockLost);
        }
        DfsAdvisoryLockCommand::Convert { token, mode, .. } => {
            return match state.grants.iter().find(|grant| grant.token == *token) {
                Some(grant) if grant.mode == *mode => DfsAdvisoryLockOutcome::Converted {
                    grant: grant.clone(),
                },
                Some(_) => DfsAdvisoryLockOutcome::StaleToken,
                None => DfsAdvisoryLockOutcome::LockLost,
            };
        }
        _ => {}
    }
    record.outcome.clone()
}

fn mark_acquire_terminal(
    state: &mut DfsAdvisoryLockStateRecord,
    acquire_request_id: &DfsAdvisoryLockRequestId,
    outcome: DfsAdvisoryLockOutcome,
) {
    if let Some(record) = state
        .request_outcomes
        .iter_mut()
        .find(|record| record.request_id == *acquire_request_id)
    {
        record.outcome = outcome;
    }
}

fn request_outcome_is_lock_lost(
    state: &DfsAdvisoryLockStateRecord,
    acquire_request_id: &DfsAdvisoryLockRequestId,
) -> bool {
    state.request_outcomes.iter().any(|record| {
        record.request_id == *acquire_request_id
            && matches!(record.outcome, DfsAdvisoryLockOutcome::LockLost)
    })
}

fn trim_request_outcomes(state: &mut DfsAdvisoryLockStateRecord) {
    let _ = state;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> sinomo_core::Id {
        sinomo_core::Id::from_legacy_named(value).expect("test id")
    }

    fn session(value: &str) -> DfsAdvisoryLockSessionId {
        DfsAdvisoryLockSessionId::new(value)
    }

    fn request(value: &str) -> DfsAdvisoryLockRequestId {
        DfsAdvisoryLockRequestId::new(value)
    }

    fn context(epoch: u64, now: u64) -> DfsAdvisoryLockTransitionContext {
        DfsAdvisoryLockTransitionContext {
            authority_epoch: epoch,
            now_unix_nanos: now,
            lease_duration_nanos: 10,
            max_clock_skew_nanos: 0,
        }
    }

    fn acquire(
        request_id: &str,
        session_id: &str,
        mode: DfsAdvisoryLockMode,
        blocking: bool,
    ) -> DfsAdvisoryLockCommand {
        DfsAdvisoryLockCommand::Acquire {
            request_id: request(request_id),
            session_id: session(session_id),
            mode,
            blocking,
            process_id: None,
        }
    }

    fn transition(
        state: Option<&DfsAdvisoryLockStateRecord>,
        context: DfsAdvisoryLockTransitionContext,
        command: &DfsAdvisoryLockCommand,
    ) -> DfsAdvisoryLockTransition {
        transition_advisory_lock_state(state, &id("fs"), &id("object"), context, command)
            .expect("transition")
    }

    #[test]
    fn shared_grants_coexist_and_exclusive_try_lock_conflicts() {
        let first = transition(
            None,
            context(1, 1),
            &acquire("a1", "a", DfsAdvisoryLockMode::Shared, false),
        );
        let second = transition(
            Some(&first.state),
            context(1, 2),
            &acquire("b1", "b", DfsAdvisoryLockMode::Shared, false),
        );
        assert_eq!(second.state.grants.len(), 2);

        let exclusive = transition(
            Some(&second.state),
            context(1, 3),
            &acquire("c1", "c", DfsAdvisoryLockMode::Exclusive, false),
        );
        assert!(matches!(
            exclusive.outcome,
            DfsAdvisoryLockOutcome::WouldBlock { .. }
        ));
        assert!(exclusive.state.waiters.is_empty());
    }

    #[test]
    fn exact_unlock_promotes_fifo_waiters_without_bypass() {
        let holder = transition(
            None,
            context(2, 1),
            &acquire("holder", "a", DfsAdvisoryLockMode::Exclusive, false),
        );
        let token = match &holder.outcome {
            DfsAdvisoryLockOutcome::Acquired { grant } => grant.token.clone(),
            other => panic!("unexpected outcome {other:?}"),
        };
        let first_waiter = transition(
            Some(&holder.state),
            context(2, 2),
            &acquire("wait-b", "b", DfsAdvisoryLockMode::Exclusive, true),
        );
        let second_waiter = transition(
            Some(&first_waiter.state),
            context(2, 3),
            &acquire("wait-c", "c", DfsAdvisoryLockMode::Shared, true),
        );
        let newcomer = transition(
            Some(&second_waiter.state),
            context(2, 4),
            &acquire("try-d", "d", DfsAdvisoryLockMode::Shared, false),
        );
        assert!(matches!(
            newcomer.outcome,
            DfsAdvisoryLockOutcome::WouldBlock { .. }
        ));

        let unlocked = transition(
            Some(&newcomer.state),
            context(2, 5),
            &DfsAdvisoryLockCommand::Unlock {
                request_id: request("unlock-a"),
                token,
            },
        );
        assert_eq!(unlocked.state.grants.len(), 1);
        assert_eq!(unlocked.state.grants[0].token.session_id, session("b"));
        assert_eq!(unlocked.state.waiters[0].session_id, session("c"));
    }

    #[test]
    fn advancing_authority_fences_waiter_and_rejects_old_token() {
        let holder = transition(
            None,
            context(4, 1),
            &acquire("holder", "a", DfsAdvisoryLockMode::Exclusive, false),
        );
        let token = match &holder.outcome {
            DfsAdvisoryLockOutcome::Acquired { grant } => grant.token.clone(),
            other => panic!("unexpected outcome {other:?}"),
        };
        let waiting = transition(
            Some(&holder.state),
            context(4, 2),
            &acquire("waiter", "b", DfsAdvisoryLockMode::Exclusive, true),
        );
        let advanced = transition(
            Some(&waiting.state),
            context(5, 3),
            &DfsAdvisoryLockCommand::Unlock {
                request_id: request("stale-unlock"),
                token,
            },
        );
        assert_eq!(advanced.outcome, DfsAdvisoryLockOutcome::StaleToken);
        assert!(advanced.state.grants.is_empty());
        assert!(advanced.state.waiters.is_empty());

        let waiter_retry = transition(
            Some(&advanced.state),
            context(5, 4),
            &acquire("waiter", "b", DfsAdvisoryLockMode::Exclusive, true),
        );
        assert_eq!(
            waiter_retry.outcome,
            DfsAdvisoryLockOutcome::Fenced { authority_epoch: 5 }
        );
    }

    #[test]
    fn identical_waiting_request_renews_then_observes_promotion() {
        let holder = transition(
            None,
            context(1, 1),
            &acquire("holder", "a", DfsAdvisoryLockMode::Exclusive, false),
        );
        let waiting_command = acquire("waiter", "b", DfsAdvisoryLockMode::Exclusive, true);
        let waiting = transition(Some(&holder.state), context(1, 2), &waiting_command);
        let early_poll = transition(Some(&waiting.state), context(1, 5), &waiting_command);
        assert_eq!(early_poll.state.waiters[0].lease_deadline_unix_nanos, 12);
        assert!(!early_poll.changed);
        let renewed = transition(Some(&early_poll.state), context(1, 8), &waiting_command);
        assert_eq!(renewed.state.waiters[0].lease_deadline_unix_nanos, 18);

        let promoted = transition(Some(&renewed.state), context(1, 12), &waiting_command);
        assert!(matches!(
            promoted.outcome,
            DfsAdvisoryLockOutcome::Acquired { .. }
        ));
        assert_eq!(promoted.state.grants[0].token.session_id, session("b"));
    }

    #[test]
    fn request_id_reuse_with_different_payload_is_rejected() {
        let first = transition(
            None,
            context(1, 1),
            &acquire("same", "a", DfsAdvisoryLockMode::Shared, false),
        );
        let mismatch = transition(
            Some(&first.state),
            context(1, 2),
            &DfsAdvisoryLockCommand::Query {
                request_id: request("same"),
                session_id: session("b"),
                mode: DfsAdvisoryLockMode::Exclusive,
            },
        );
        assert_eq!(mismatch.outcome, DfsAdvisoryLockOutcome::RequestMismatch);
        assert_eq!(mismatch.state.grants, first.state.grants);
    }

    #[test]
    fn queries_do_not_grow_durable_idempotency_history() {
        let holder = transition(
            None,
            context(1, 1),
            &acquire("holder", "a", DfsAdvisoryLockMode::Exclusive, false),
        );
        let outcome_count = holder.state.request_outcomes.len();
        let queried = transition(
            Some(&holder.state),
            context(1, 2),
            &DfsAdvisoryLockCommand::Query {
                request_id: request("query-b"),
                session_id: session("b"),
                mode: DfsAdvisoryLockMode::Exclusive,
            },
        );

        assert!(matches!(
            queried.outcome,
            DfsAdvisoryLockOutcome::QueryResult { conflict: Some(_) }
        ));
        assert_eq!(queried.state.request_outcomes.len(), outcome_count);
        assert!(!queried.changed);
    }

    #[test]
    fn exact_cancel_releases_a_grant_that_won_the_promotion_race() {
        let holder = transition(
            None,
            context(1, 1),
            &acquire("holder", "a", DfsAdvisoryLockMode::Exclusive, false),
        );
        let holder_token = holder.state.grants[0].token.clone();
        let waiting = transition(
            Some(&holder.state),
            context(1, 2),
            &acquire("waiter", "b", DfsAdvisoryLockMode::Exclusive, true),
        );
        let promoted = transition(
            Some(&waiting.state),
            context(1, 3),
            &DfsAdvisoryLockCommand::Unlock {
                request_id: request("unlock-holder"),
                token: holder_token,
            },
        );
        assert_eq!(promoted.state.grants[0].token.grant_id, request("waiter"));

        let cancelled = transition(
            Some(&promoted.state),
            context(1, 4),
            &DfsAdvisoryLockCommand::Cancel {
                request_id: request("cancel-waiter"),
                session_id: session("b"),
                acquire_request_id: request("waiter"),
            },
        );
        assert_eq!(
            cancelled.outcome,
            DfsAdvisoryLockOutcome::Cancelled {
                acquire_request_id: request("waiter"),
                was_waiting: false,
            }
        );
        assert!(cancelled.state.grants.is_empty());
        assert!(cancelled.state.waiters.is_empty());
    }

    #[test]
    fn cancel_before_delayed_acquire_is_a_durable_tombstone() {
        let cancelled = transition(
            None,
            context(1, 1),
            &DfsAdvisoryLockCommand::Cancel {
                request_id: request("cancel-first"),
                session_id: session("a"),
                acquire_request_id: request("delayed-acquire"),
            },
        );
        let delayed = transition(
            Some(&cancelled.state),
            context(1, 2),
            &acquire("delayed-acquire", "a", DfsAdvisoryLockMode::Exclusive, true),
        );

        assert_eq!(
            delayed.outcome,
            DfsAdvisoryLockOutcome::Cancelled {
                acquire_request_id: request("delayed-acquire"),
                was_waiting: false,
            }
        );
        assert!(delayed.state.grants.is_empty());
        assert!(delayed.state.waiters.is_empty());
    }

    #[test]
    fn close_before_delayed_acquire_fences_the_one_shot_session() {
        let closed = transition(
            None,
            context(1, 1),
            &DfsAdvisoryLockCommand::Close {
                request_id: request("close-first"),
                session_id: session("a"),
            },
        );
        let delayed = transition(
            Some(&closed.state),
            context(1, 2),
            &acquire("delayed-acquire", "a", DfsAdvisoryLockMode::Exclusive, true),
        );

        assert_eq!(delayed.outcome, DfsAdvisoryLockOutcome::LockLost);
        assert!(delayed.state.grants.is_empty());
        assert!(delayed.state.waiters.is_empty());
    }

    #[test]
    fn conversion_is_atomic_and_conflicting_upgrade_keeps_shared_grant() {
        let first = transition(
            None,
            context(1, 1),
            &acquire("a", "a", DfsAdvisoryLockMode::Shared, false),
        );
        let second = transition(
            Some(&first.state),
            context(1, 2),
            &acquire("b", "b", DfsAdvisoryLockMode::Shared, false),
        );
        let token = second
            .state
            .grants
            .iter()
            .find(|grant| grant.token.session_id == session("a"))
            .expect("first shared grant")
            .token
            .clone();
        let blocked = transition(
            Some(&second.state),
            context(1, 3),
            &DfsAdvisoryLockCommand::Convert {
                request_id: request("upgrade-a"),
                token: token.clone(),
                mode: DfsAdvisoryLockMode::Exclusive,
            },
        );
        assert!(matches!(
            blocked.outcome,
            DfsAdvisoryLockOutcome::WouldBlock { .. }
        ));
        assert_eq!(
            blocked
                .state
                .grants
                .iter()
                .find(|grant| grant.token == token)
                .expect("old grant retained")
                .mode,
            DfsAdvisoryLockMode::Shared
        );
    }

    #[test]
    fn delayed_conversion_retry_cannot_report_a_superseded_mode() {
        let holder = transition(
            None,
            context(1, 1),
            &acquire("holder", "a", DfsAdvisoryLockMode::Shared, false),
        );
        let token = holder.state.grants[0].token.clone();
        let upgrade = DfsAdvisoryLockCommand::Convert {
            request_id: request("upgrade"),
            token: token.clone(),
            mode: DfsAdvisoryLockMode::Exclusive,
        };
        let upgraded = transition(Some(&holder.state), context(1, 2), &upgrade);
        let downgraded = transition(
            Some(&upgraded.state),
            context(1, 3),
            &DfsAdvisoryLockCommand::Convert {
                request_id: request("downgrade"),
                token,
                mode: DfsAdvisoryLockMode::Shared,
            },
        );
        let delayed = transition(Some(&downgraded.state), context(1, 4), &upgrade);

        assert_eq!(delayed.outcome, DfsAdvisoryLockOutcome::StaleToken);
        assert_eq!(delayed.state.grants[0].mode, DfsAdvisoryLockMode::Shared);
    }

    #[test]
    fn shared_holder_cannot_upgrade_ahead_of_fifo_waiter() {
        let holder = transition(
            None,
            context(1, 1),
            &acquire("holder", "a", DfsAdvisoryLockMode::Shared, false),
        );
        let token = holder.state.grants[0].token.clone();
        let waiting = transition(
            Some(&holder.state),
            context(1, 2),
            &acquire("waiter", "b", DfsAdvisoryLockMode::Exclusive, true),
        );
        let upgrade = transition(
            Some(&waiting.state),
            context(1, 3),
            &DfsAdvisoryLockCommand::Convert {
                request_id: request("upgrade-holder"),
                token: token.clone(),
                mode: DfsAdvisoryLockMode::Exclusive,
            },
        );
        assert!(matches!(
            upgrade.outcome,
            DfsAdvisoryLockOutcome::WouldBlock { ref conflict }
                if conflict.session_id == session("b")
        ));
        assert_eq!(upgrade.state.grants[0].token, token);
        assert_eq!(upgrade.state.grants[0].mode, DfsAdvisoryLockMode::Shared);
        assert_eq!(upgrade.state.waiters[0].session_id, session("b"));
    }

    #[test]
    fn new_epoch_waits_out_max_prior_lease_deadline() {
        let holder = transition(
            None,
            context(7, 1),
            &acquire("holder", "a", DfsAdvisoryLockMode::Exclusive, false),
        );
        let old_token = holder.state.grants[0].token.clone();
        let waiting = transition(
            Some(&holder.state),
            context(7, 5),
            &acquire("waiter", "b", DfsAdvisoryLockMode::Exclusive, true),
        );
        assert_eq!(waiting.state.waiters[0].lease_deadline_unix_nanos, 15);

        let new_epoch_command = acquire("new-epoch", "c", DfsAdvisoryLockMode::Exclusive, true);
        let warming = transition(Some(&waiting.state), context(8, 6), &new_epoch_command);
        assert_eq!(warming.state.authority_epoch, 8);
        assert_eq!(warming.state.grant_not_before_unix_nanos, 15);
        assert!(warming.state.grants.is_empty());
        assert!(warming.state.waiters.is_empty());
        assert_eq!(
            warming.outcome,
            DfsAdvisoryLockOutcome::AuthorityWarming {
                retry_after_unix_nanos: 15
            }
        );

        let old_renew = transition(
            Some(&warming.state),
            context(8, 7),
            &DfsAdvisoryLockCommand::Renew {
                request_id: request("renew-old"),
                token: old_token,
            },
        );
        assert_eq!(old_renew.outcome, DfsAdvisoryLockOutcome::LockLost);

        let granted = transition(Some(&old_renew.state), context(8, 15), &new_epoch_command);
        assert!(matches!(
            granted.outcome,
            DfsAdvisoryLockOutcome::Acquired { .. }
        ));
        assert_eq!(granted.state.grant_not_before_unix_nanos, 0);
    }

    #[test]
    fn new_epoch_adds_the_configured_clock_skew_fence() {
        let holder = transition(
            None,
            context(3, 10),
            &acquire("holder", "a", DfsAdvisoryLockMode::Exclusive, false),
        );
        let mut new_epoch = context(4, 12);
        new_epoch.max_clock_skew_nanos = 7;
        let warming = transition(
            Some(&holder.state),
            new_epoch,
            &acquire("next", "b", DfsAdvisoryLockMode::Exclusive, false),
        );
        assert_eq!(warming.state.grant_not_before_unix_nanos, 27);
        assert_eq!(
            warming.outcome,
            DfsAdvisoryLockOutcome::AuthorityWarming {
                retry_after_unix_nanos: 27,
            }
        );
    }

    #[test]
    fn same_epoch_forward_clock_step_within_skew_cannot_replace_a_grant() {
        let mut initial = context(3, 1);
        initial.max_clock_skew_nanos = 7;
        let holder = transition(
            None,
            initial,
            &acquire("holder", "a", DfsAdvisoryLockMode::Exclusive, false),
        );
        assert_eq!(holder.state.grants[0].lease_deadline_unix_nanos, 11);

        let mut stepped = context(3, 15);
        stepped.max_clock_skew_nanos = 7;
        let blocked = transition(
            Some(&holder.state),
            stepped,
            &acquire("replacement", "b", DfsAdvisoryLockMode::Exclusive, false),
        );
        assert!(matches!(
            blocked.outcome,
            DfsAdvisoryLockOutcome::WouldBlock { .. }
        ));

        let mut beyond_bound = context(3, 18);
        beyond_bound.max_clock_skew_nanos = 7;
        let replaced = transition(
            Some(&blocked.state),
            beyond_bound,
            &acquire("replacement-2", "b", DfsAdvisoryLockMode::Exclusive, false),
        );
        assert!(matches!(
            replaced.outcome,
            DfsAdvisoryLockOutcome::Acquired { .. }
        ));
    }

    #[test]
    fn response_projects_absolute_deadline_to_relative_validity() {
        let granted = transition(
            None,
            context(1, 10),
            &acquire("holder", "a", DfsAdvisoryLockMode::Exclusive, false),
        );
        let mut outcome = granted.outcome;
        observe_advisory_lock_outcome_at(&mut outcome, 16);
        let DfsAdvisoryLockOutcome::Acquired { grant } = outcome else {
            panic!("expected grant");
        };
        assert_eq!(grant.lease_deadline_unix_nanos, 20);
        assert_eq!(grant.lease_remaining_nanos, 4);
    }

    #[test]
    fn migration_rebind_fences_even_when_destination_epoch_number_matches() {
        let holder = transition(
            None,
            context(4, 1),
            &acquire("holder", "a", DfsAdvisoryLockMode::Exclusive, false),
        );
        let mut copied = holder.state;
        copied.authority_rebind_required = true;
        let rebound = transition(
            Some(&copied),
            context(4, 2),
            &acquire("destination", "b", DfsAdvisoryLockMode::Exclusive, false),
        );
        assert_eq!(rebound.state.authority_epoch, 4);
        assert!(!rebound.state.authority_rebind_required);
        assert!(rebound.state.grants.is_empty());
        assert_eq!(rebound.state.grant_not_before_unix_nanos, 11);
        assert_eq!(
            rebound.outcome,
            DfsAdvisoryLockOutcome::AuthorityWarming {
                retry_after_unix_nanos: 11,
            }
        );
    }
}
