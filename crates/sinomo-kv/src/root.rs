use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_base::{
    ClusterBootstrapState, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState, Id,
};
use sinomo_node::NodeId;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_storage::durable_file::{self, file_ino};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;
use time::OffsetDateTime;

pub const KV_OBJECT_VERSION_ID: u16 = 1;
pub const KV_WAL_OBJECT_VERSION_ID: u16 = 2;
const MAX_RETAINED_EVENTS: usize = 4096;
const MAX_PENDING_EVENTS_PER_WATCH: usize = MAX_RETAINED_EVENTS;
const MAX_ACTIVE_WATCHES: usize = 1024;
const MAX_ACTIVE_LEASES: usize = 4096;
pub const MAX_KV_TXN_COMPARES: usize = 1024;
pub const MAX_KV_TXN_OPS_PER_BRANCH: usize = 4096;
pub const MAX_KV_MUTATION_EVENTS_PER_COMMAND: usize = 4096;
pub const MAX_DELETE_PREFIX_KEYS: usize = MAX_KV_MUTATION_EVENTS_PER_COMMAND;
pub const MAX_KEYS_PER_LEASE: usize = MAX_KV_MUTATION_EVENTS_PER_COMMAND;
pub const MAX_KV_VALUE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_KV_COMMAND_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_COMMITTED_KV_REQUESTS: usize = 4096;
pub const MAX_COMMITTED_KV_REQUEST_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_EXPIRED_KV_REQUEST_TOMBSTONES: usize = 4096;
pub const CLUSTER_KV_RPC_CATEGORY: &str = "cluster.kv";
pub const CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE: &str = "/cluster/kv/local/snapshot";
pub const CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE: &str = "/cluster/kv/authoritative/snapshot";
pub const CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE: &str =
    "/cluster/kv/authoritative/range-prefix";
pub const CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE: &str =
    "/cluster/kv/authoritative/watch-start";
pub const CLUSTER_KV_TXN_ROUTE: &str = "/cluster/kv/txn";
pub const CLUSTER_KV_REQUEST_OUTCOME_ROUTE: &str = "/cluster/kv/request-outcome";
pub const CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE: &str = "/cluster/kv/local/range-prefix";
pub const CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE: &str = "/cluster/kv/local/watch-prefix";
pub const CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE: &str = "/cluster/kv/local/watch-drain";
pub const CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE: &str = "/cluster/kv/local/watch-cancel";
pub const CLUSTER_KV_WATCH_EVENTS_ROUTE: &str = "/cluster/kv/watch-events";
pub const CLUSTER_RAFT_RPC_CATEGORY: &str = "cluster.raft";
pub const CLUSTER_RAFT_APPEND_ROUTE: &str = "/cluster/raft/append";
pub const CLUSTER_RAFT_VOTE_ROUTE: &str = "/cluster/raft/vote";
pub const CLUSTER_KV_READ_CAPABILITY: &str = "cluster.kv.read";
pub const CLUSTER_KV_WRITE_CAPABILITY: &str = "cluster.kv.write";
pub const CLUSTER_RAFT_APPEND_CAPABILITY: &str = "cluster.raft.append";
pub const CLUSTER_RAFT_VOTE_CAPABILITY: &str = "cluster.raft.vote";
pub const CLUSTER_KV_LOCAL_READ_WATCH_ROUTES: &[&str] = &[
    CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE,
    CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE,
    CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE,
    CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE,
];
pub const CLUSTER_KV_RPC_ROUTES: &[&str] = &[
    CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE,
    CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
    CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE,
    CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE,
    CLUSTER_KV_TXN_ROUTE,
    CLUSTER_KV_REQUEST_OUTCOME_ROUTE,
    CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE,
    CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE,
    CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE,
    CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE,
    CLUSTER_KV_WATCH_EVENTS_ROUTE,
];
pub const CLUSTER_RAFT_RPC_ROUTES: &[&str] = &[CLUSTER_RAFT_APPEND_ROUTE, CLUSTER_RAFT_VOTE_ROUTE];
pub const CLUSTER_KV_RPC_ROUTE_METADATA: &[(&str, &str)] = &[
    (CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (
        CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
        CLUSTER_KV_RPC_CATEGORY,
    ),
    (
        CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE,
        CLUSTER_KV_RPC_CATEGORY,
    ),
    (
        CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE,
        CLUSTER_KV_RPC_CATEGORY,
    ),
    (CLUSTER_KV_TXN_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_REQUEST_OUTCOME_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_WATCH_EVENTS_ROUTE, CLUSTER_KV_RPC_CATEGORY),
];
pub const CLUSTER_RAFT_RPC_ROUTE_METADATA: &[(&str, &str)] = &[
    (CLUSTER_RAFT_APPEND_ROUTE, CLUSTER_RAFT_RPC_CATEGORY),
    (CLUSTER_RAFT_VOTE_ROUTE, CLUSTER_RAFT_RPC_CATEGORY),
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterKvRouteAuth {
    Authenticated,
    LocalOnly,
    LocalOrClusterMember,
    LocalOrRaftVoter,
}

impl ClusterKvRouteAuth {
    pub fn as_scope(self) -> &'static str {
        match self {
            Self::Authenticated => "authenticated",
            Self::LocalOnly => "local-command",
            Self::LocalOrClusterMember => "local-or-cluster-member",
            Self::LocalOrRaftVoter => "local-or-cluster-voter",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRpcRoutePolicy {
    pub route: &'static str,
    pub category: &'static str,
    pub auth: ClusterKvRouteAuth,
    pub required_capabilities: &'static [&'static str],
}

pub const CLUSTER_KV_RPC_ROUTE_POLICIES: &[ClusterKvRpcRoutePolicy] = &[
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::Authenticated,
        required_capabilities: &[CLUSTER_KV_READ_CAPABILITY],
    },
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrClusterMember,
        required_capabilities: &[CLUSTER_KV_READ_CAPABILITY],
    },
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrClusterMember,
        required_capabilities: &[CLUSTER_KV_READ_CAPABILITY],
    },
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrClusterMember,
        required_capabilities: &[CLUSTER_KV_READ_CAPABILITY],
    },
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_KV_TXN_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrClusterMember,
        required_capabilities: &[CLUSTER_KV_WRITE_CAPABILITY],
    },
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_KV_REQUEST_OUTCOME_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrClusterMember,
        required_capabilities: &[CLUSTER_KV_READ_CAPABILITY],
    },
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrClusterMember,
        required_capabilities: &[CLUSTER_KV_READ_CAPABILITY],
    },
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrClusterMember,
        required_capabilities: &[CLUSTER_KV_READ_CAPABILITY],
    },
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrClusterMember,
        required_capabilities: &[CLUSTER_KV_READ_CAPABILITY],
    },
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrClusterMember,
        required_capabilities: &[CLUSTER_KV_READ_CAPABILITY],
    },
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_KV_WATCH_EVENTS_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrClusterMember,
        required_capabilities: &[CLUSTER_KV_READ_CAPABILITY],
    },
];

pub const CLUSTER_RAFT_RPC_ROUTE_POLICIES: &[ClusterKvRpcRoutePolicy] = &[
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_RAFT_APPEND_ROUTE,
        category: CLUSTER_RAFT_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrRaftVoter,
        required_capabilities: &[CLUSTER_RAFT_APPEND_CAPABILITY],
    },
    ClusterKvRpcRoutePolicy {
        route: CLUSTER_RAFT_VOTE_ROUTE,
        category: CLUSTER_RAFT_RPC_CATEGORY,
        auth: ClusterKvRouteAuth::LocalOrRaftVoter,
        required_capabilities: &[CLUSTER_RAFT_VOTE_CAPABILITY],
    },
];

pub fn cluster_kv_rpc_route_policy(route: &str) -> Option<&'static ClusterKvRpcRoutePolicy> {
    CLUSTER_KV_RPC_ROUTE_POLICIES
        .iter()
        .chain(CLUSTER_RAFT_RPC_ROUTE_POLICIES)
        .find(|policy| policy.route == route)
}
pub const MAX_CLUSTER_KV_WATCH_EVENTS: usize = 256;
pub const CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS: usize = MAX_CLUSTER_KV_WATCH_EVENTS;
pub const CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES: usize = MAX_KV_COMMAND_PAYLOAD_BYTES;
pub const CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_REVISION_LAG: u64 =
    (MAX_CLUSTER_KV_WATCH_EVENTS / 2) as u64;
pub const CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_EVENTS: usize = MAX_CLUSTER_KV_WATCH_EVENTS / 2;
pub const CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_AGE: Duration = Duration::from_secs(60);
pub const MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS: usize = MAX_CLUSTER_KV_WATCH_EVENTS * 2;
pub const MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES: u64 = 1024 * 1024;
pub const MAX_CLUSTER_KV_WATCH_EVENT_RECORD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CLUSTER_KV_ACTIVE_WATCHES: usize = 1024;
pub const MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES: usize = MAX_KV_COMMAND_PAYLOAD_BYTES;
pub const MAX_CLUSTER_KV_WATCH_DRAIN_WAIT: Duration = Duration::from_secs(30);
pub const CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES: usize = 4096;
pub const CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES: usize = MAX_KV_COMMAND_PAYLOAD_BYTES;
pub const MAX_CLUSTER_KV_MATERIALIZED_EVENTS: usize = 64;
pub const CLUSTER_KV_DEFERRED_MATERIALIZED_MAX_REVISION_LAG: u64 = 64;
pub const CLUSTER_KV_DEFERRED_MATERIALIZED_MIN_INTERVAL: Duration = Duration::from_secs(300);
pub const MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS: usize = 64;
/// Committed log entries kept in memory after a local snapshot compaction. A
/// follower that has fallen at most this many entries behind can be caught up
/// with small incremental appends instead of a full snapshot install. The tail is
/// redundant with the snapshot (it is dropped on restart) and only the leader's
/// in-process log carries it, so it never changes the wire protocol or on-disk
/// recovery semantics.
///
/// Compaction triggers must stay *above* this value: compaction only drops entries
/// older than this tail, so triggering at or below it rewrites the WAL to the same
/// tail every time (a busy-idle loop). See `compact_cluster_raft_node_if_needed`.
pub const RAFT_COMPACTION_RETAINED_ENTRIES: u64 = 256;
pub const CLUSTER_KV_RAFT_LEADER_ELECTION_ATTEMPTS: usize = 3;
pub const CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP: u64 = 64;

mod config_change;
mod in_process;
mod keyspace;
mod model;
mod paths;
mod raft;
mod request;
mod rpc;
mod snapshot;
mod wal;
mod watch;
#[cfg(any(test, feature = "test-support"))]
mod watch_store;

mod internal {
    pub(crate) use super::in_process::*;
    pub(crate) use super::keyspace::*;
    pub(crate) use super::model::*;
    pub(crate) use super::paths::*;
    pub(crate) use super::raft::*;
    pub(crate) use super::rpc::*;
    pub(crate) use super::snapshot::*;
    pub(crate) use super::wal::*;
    pub(crate) use super::watch::*;
    pub(crate) use super::*;
}

pub use config_change::{
    ClusterKvRaftCatchUpProof, ClusterKvRaftConfigChange, ClusterKvRaftConfigChangeEvidence,
    ClusterKvRaftConfigChangeKind, ClusterKvRaftConfigChangePhase, ClusterKvRaftPeerProgress,
    MAX_CLUSTER_KV_RAFT_CONFIG_CHANGE_EVIDENCE, MAX_CLUSTER_KV_RAFT_PEER_PROGRESS_HINTS,
    apply_cluster_kv_raft_config_change, begin_cluster_kv_raft_config_change,
    clear_cluster_kv_raft_peer_progress, cluster_kv_raft_config_change_key,
    cluster_kv_raft_peer_progress_next_index_hint,
    cluster_kv_raft_peer_progress_next_index_hint_async, finish_cluster_kv_raft_config_change,
    read_cluster_kv_raft_peer_progress, record_cluster_kv_raft_catch_up_proof,
    record_cluster_kv_raft_peer_progress, record_cluster_kv_raft_peer_progress_batch,
};
pub use in_process::{InProcessKvStore, InProcessRaftKvCluster};
pub use keyspace::{
    ClusterKvKeyspaceClass, cluster_kv_default_read_authority_for_key,
    cluster_kv_key_may_defer_materialized_projection, cluster_kv_keyspace_class,
};
pub type InProcessKvCluster = InProcessRaftKvCluster;
pub use model::{
    ClusterKvRequestOutcomeRead, KvAuthoritativeRead, KvAuthorityBasis, KvAuthorityDiagnostics,
    KvAuthorityFailure, KvAuthorityFailureReason, KvAuthorityProof, KvCommand, KvCommandDigest,
    KvCommandRequest, KvCommittedRequestRecord, KvCompare, KvEntry, KvError, KvEvent, KvEventKind,
    KvFreshness, KvLease, KvLocalRead, KvOp, KvOpResponse, KvReadAuthority, KvRecoveryAction,
    KvRequestAttemptRecord, KvRequestAttemptState, KvRequestId, KvRequestOutcome, KvRevision,
    KvSnapshot, KvTxn, KvTxnRequest, KvTxnResponse, KvVersion, KvWatch, LeaseId, WatchId,
    decode_kv_value, encode_kv_value, kv_command_digest, push_bounded_txn_success_op,
};
pub use paths::{
    CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT, ClusterKvPaths, lock_cluster_kv_commit_sequence,
    register_storage_paths,
};
pub use raft::{
    ClusterKvRaftAppendReplicationAsyncHost, ClusterKvRaftAppendReplicationHost,
    ClusterKvRaftAppendReplicationOptions, ClusterKvRaftAppendReplicationOutcome,
    ClusterKvRaftAppendRequest, ClusterKvRaftCommandStorage, ClusterKvRaftCommitResult,
    ClusterKvRaftCompactionPolicy, ClusterKvRaftConfigRecord, ClusterKvRaftLeaderElectionAsyncHost,
    ClusterKvRaftLeaderElectionHost, ClusterKvRaftLeaderElectionOptions,
    ClusterKvRaftLocalCommandStorage, ClusterKvRaftLocalVoteMetadata, ClusterKvRaftObservedAppend,
    ClusterKvRaftOperationGuard, ClusterKvRaftQuorumCommitAsyncHost, ClusterKvRaftQuorumCommitHost,
    ClusterKvRaftQuorumCommitOptions, ClusterKvRaftQuorumCommitResult, ClusterKvRaftQuorumFuture,
    ClusterKvRaftStore, ClusterKvRaftTargetReplicationAttempt,
    ClusterKvRaftTargetReplicationAttempts, ClusterKvRaftTargetReplicationStatus,
    ClusterKvRaftUncommittedAppend, ClusterKvTxnCommitResult, ClusterKvTxnQuorumAsyncHost,
    ClusterKvTxnQuorumDriverOptions, ClusterKvTxnQuorumHost, DurableRaftKvNode, KvLogEntry,
    RaftAppendEntriesApplyOutcome, RaftAppendEntriesRequest, RaftAppendEntriesResponse,
    RaftHardState, RaftRequestVoteRequest, RaftRequestVoteResponse, RaftSnapshot,
    cluster_kv_member_has_raft_vote, cluster_kv_raft_candidate_is_authorized_voter,
    cluster_kv_raft_config_key, cluster_kv_raft_config_member_role,
    cluster_kv_raft_config_record_stable_eq, cluster_kv_raft_local_is_voter,
    cluster_kv_raft_next_index_after_rejection, cluster_kv_raft_node_is_voter,
    cluster_kv_raft_node_should_compact, cluster_kv_raft_quorum_size,
    cluster_kv_raft_quorum_size_for_state, cluster_kv_raft_snapshot_byte_limit,
    cluster_kv_raft_voter_ids, cluster_kv_store_member_is_raft_voter,
    commit_cluster_kv_raft_command_with_quorum, commit_cluster_kv_raft_command_with_quorum_async,
    commit_cluster_kv_txn_with_quorum_async_with_host, commit_cluster_kv_txn_with_quorum_with_host,
    compact_cluster_kv_raft_node_if_needed, default_cluster_kv_raft_compaction_policy,
    elect_cluster_kv_raft_leader_async_with_host, elect_cluster_kv_raft_leader_with_host,
    lock_cluster_kv_raft_operation_for_paths, lock_cluster_kv_raft_operation_for_root,
    lock_cluster_kv_raft_operation_for_wal_paths,
    read_cluster_kv_authoritative_snapshot_async_with_host,
    read_cluster_kv_authoritative_snapshot_with_host,
    replicate_cluster_kv_raft_to_target_async_with_host,
    replicate_cluster_kv_raft_to_target_with_host, validate_cluster_kv_raft_config_transition,
};
pub use request::{
    MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES, MAX_CLUSTER_KV_REQUEST_ATTEMPT_TOMBSTONES,
    MAX_CLUSTER_KV_REQUEST_ATTEMPTS, mark_cluster_kv_request_rejected_before_commit,
    mark_cluster_kv_request_submitted, mark_cluster_kv_request_unknown,
    read_cluster_kv_request_attempt, record_cluster_kv_request_attempt,
    resolve_cluster_kv_request_outcome,
};
pub use rpc::{
    ClusterKvAuthoritativeRangePrefixRequest, ClusterKvAuthoritativeSnapshotRequest,
    ClusterKvAuthoritativeWatchStartRequest, ClusterKvLocalReadWatchHost,
    ClusterKvReplicationRequest, ClusterKvReplicationResponse, ClusterKvRequestOutcomeRequest,
    ClusterKvRpcPayload, ClusterKvStateAuthorityVoteRequest, ClusterKvStateAuthorityVoteResponse,
    ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse, ClusterRaftVoteRequest, ClusterRaftVoteResponse,
    cluster_kv_authoritative_range_prefix_response_rpc_payload,
    cluster_kv_authoritative_range_prefix_rpc_payload,
    cluster_kv_authoritative_snapshot_response_rpc_payload,
    cluster_kv_authoritative_snapshot_rpc_payload,
    cluster_kv_authoritative_watch_start_response_rpc_payload,
    cluster_kv_authoritative_watch_start_rpc_payload,
    cluster_kv_local_range_prefix_response_rpc_payload, cluster_kv_local_range_prefix_rpc_payload,
    cluster_kv_local_snapshot_response_rpc_payload, cluster_kv_local_snapshot_rpc_payload,
    cluster_kv_local_watch_cancel_rpc_payload, cluster_kv_local_watch_drain_rpc_payload,
    cluster_kv_request_outcome_response_rpc_payload, cluster_kv_request_outcome_rpc_payload,
    cluster_kv_txn_response_rpc_payload, cluster_kv_txn_rpc_payload,
    cluster_kv_watch_events_noop_response, cluster_kv_watch_events_response_rpc_payload,
    cluster_kv_watch_events_rpc_payload, cluster_raft_append_response_rpc_payload,
    cluster_raft_append_rpc_payload, cluster_raft_vote_response_rpc_payload,
    cluster_raft_vote_rpc_payload, decode_cluster_kv_authoritative_range_prefix_rpc_body,
    decode_cluster_kv_authoritative_range_prefix_rpc_request_body,
    decode_cluster_kv_authoritative_snapshot_rpc_body,
    decode_cluster_kv_authoritative_snapshot_rpc_request_body,
    decode_cluster_kv_authoritative_watch_start_rpc_body,
    decode_cluster_kv_authoritative_watch_start_rpc_request_body,
    decode_cluster_kv_local_range_prefix_rpc_body, decode_cluster_kv_local_snapshot_rpc_body,
    decode_cluster_kv_local_snapshot_rpc_request_body,
    decode_cluster_kv_local_watch_cancel_rpc_body, decode_cluster_kv_local_watch_drain_rpc_body,
    decode_cluster_kv_request_outcome_rpc_body, decode_cluster_kv_request_outcome_rpc_request_body,
    decode_cluster_kv_txn_rpc_body, decode_cluster_kv_txn_rpc_request_body,
    decode_cluster_kv_watch_events_rpc_request_body,
    decode_cluster_kv_watch_events_rpc_response_body, decode_cluster_raft_append_rpc_request_body,
    decode_cluster_raft_append_rpc_response_body, decode_cluster_raft_vote_rpc_request_body,
    decode_cluster_raft_vote_rpc_response_body, handle_cluster_kv_local_read_watch_rpc_payload,
    handle_cluster_kv_request_outcome_rpc_payload, handle_cluster_kv_watch_events,
    handle_cluster_raft_rpc_payload, project_cluster_kv_authoritative_range_prefix,
    validate_cluster_kv_current_term_authority_proof,
};
pub use snapshot::{
    KvLocalCache, KvMaterializedCache, compact_kv_snapshot_event_history, kv_snapshot_durable_eq,
    read_cluster_kv_cached_snapshot, read_cluster_kv_current_snapshot_arc_for_node,
    read_cluster_kv_current_snapshot_for_node, read_cluster_kv_entry, read_cluster_kv_local_entry,
    read_cluster_kv_local_prefix, read_cluster_kv_local_snapshot, read_cluster_kv_prefix_entries,
    read_cluster_kv_prefix_entries_limited,
    read_cluster_kv_prefix_entries_limited_by_payload_bytes, read_cluster_kv_store_for_node,
    write_cluster_kv_materialized_snapshot_if_changed, write_cluster_kv_snapshot,
    write_cluster_kv_snapshot_if_changed, write_imported_cluster_kv_snapshot_if_changed,
};
pub use wal::KvWalPaths;
pub use watch::{
    ClusterKvActiveWatchRegistrationReport, ClusterKvWatchAllocatorState,
    ClusterKvWatchCancelRequest, ClusterKvWatchCancelResponse, ClusterKvWatchDrainCursorAction,
    ClusterKvWatchDrainCursorPersistence, ClusterKvWatchDrainRegistrationReport,
    ClusterKvWatchDrainRequest, ClusterKvWatchDrainResponse, ClusterKvWatchEventAppendCacheView,
    ClusterKvWatchEventAppendReport, ClusterKvWatchEventFanoutBatchLimit,
    ClusterKvWatchEventLogRead, ClusterKvWatchEventsRead, ClusterKvWatchObservedGeneration,
    ClusterKvWatchPrefixRequest, ClusterKvWatchRegistration,
    ClusterKvWatchRegistrationCreateReport, ClusterKvWatchRegistrationRemovalReport,
    ClusterKvWatchScanRegistration, ClusterKvWatchScanResult, EncodedClusterKvWatchEventLogBatch,
    append_cluster_kv_watch_events_for_active_watches_for_node,
    append_cluster_kv_watch_events_to_storage,
    append_cluster_kv_watch_events_to_storage_with_observers, cancel_cluster_kv_watch,
    cluster_kv_active_watch_registration_report, cluster_kv_events_can_defer_materialized_snapshot,
    cluster_kv_events_contain, cluster_kv_events_contain_all,
    cluster_kv_key_can_defer_materialized_snapshot,
    cluster_kv_materialized_snapshot_is_fresh_enough_against, cluster_kv_snapshot_compare,
    cluster_kv_snapshot_delta_only_deferable,
    cluster_kv_watch_allocator_after_registration_removed, cluster_kv_watch_allocator_after_scan,
    cluster_kv_watch_allocator_needs_scan, cluster_kv_watch_event_append_should_checkpoint,
    cluster_kv_watch_event_fanout_batches, cluster_kv_watch_event_fanout_batches_with_limits,
    cluster_kv_watch_event_payload_bytes, cluster_kv_watch_events_matching_registrations,
    cluster_kv_watch_events_payload_bytes, cluster_kv_watch_generation,
    cluster_kv_watch_id_from_file_name, cluster_kv_watch_ids_matching_events,
    cluster_kv_watch_observed_generation, cluster_kv_watch_registration_cursor_should_persist,
    cluster_raft_append_request_needs_watch_event_fanout, collect_cluster_kv_watch_drain_events,
    compact_cluster_kv_watch_events, compact_cluster_kv_watch_events_incremental,
    create_cluster_kv_watch_for_node, create_cluster_kv_watch_registration,
    drain_cluster_kv_watch_for_node, drain_cluster_kv_watch_registration,
    list_cluster_kv_watch_registrations, lock_cluster_kv_watch_allocator,
    notify_cluster_kv_watchers, notify_cluster_kv_watchers_for_events,
    notify_cluster_kv_watchers_for_watch_ids, persist_cluster_kv_watch_drain_cursor,
    read_cluster_kv_watch_allocator_state, read_cluster_kv_watch_event_checkpoint,
    read_cluster_kv_watch_event_log, read_cluster_kv_watch_event_log_from_file_with_observer,
    read_cluster_kv_watch_event_log_with_observer, read_cluster_kv_watch_events,
    read_cluster_kv_watch_events_with_observer, read_cluster_kv_watch_registration_file,
    remove_cluster_kv_watch_registration_file_if_exists,
    remove_cluster_kv_watch_registration_with_allocator, scan_cluster_kv_watch_registrations,
    validate_cluster_kv_watch_event_fanout_batch,
    validate_cluster_kv_watch_event_fanout_batch_with_limits, wait_for_cluster_kv_watch_change,
    wait_for_cluster_kv_watch_change_for_watch, write_cluster_kv_watch_allocator_state,
    write_cluster_kv_watch_event_checkpoint, write_cluster_kv_watch_registration_file,
    write_cluster_kv_watch_registration_file_create_new,
};
#[cfg(any(test, feature = "test-support"))]
pub use watch::{
    cluster_kv_watch_notifier_capacity_for_test, cluster_kv_watch_notifier_has_key_for_test,
    cluster_kv_watch_notifier_has_raw_key_for_test,
    cluster_kv_watch_notifier_waiter_count_for_test, cluster_kv_watch_unchanged_wakeups_for_test,
    record_cluster_kv_watch_notifier_waiter_key_for_test,
    remove_cluster_kv_watch_notifier_entries_under_root_for_test,
    reset_cluster_kv_watch_unchanged_wakeups_for_test, touch_cluster_kv_watch_notifier_for_test,
};
#[cfg(any(test, feature = "test-support"))]
pub use watch_store::{
    append_cluster_kv_watch_events, append_cluster_kv_watch_events_for_active_watches,
    cached_cluster_kv_watch_events_contain_all, cached_cluster_kv_watch_events_for_test,
    cached_cluster_kv_watch_registration_for_test,
    cluster_kv_watch_allocator_trust_contains_for_test, cluster_kv_watch_dir_scans_for_test,
    cluster_kv_watch_drain_once_calls_for_test, cluster_kv_watch_drain_read_passes_for_test,
    cluster_kv_watch_event_cache_capacity_for_test, cluster_kv_watch_event_cache_contains_for_test,
    cluster_kv_watch_event_log_read_bytes_for_test,
    cluster_kv_watch_event_log_write_bytes_for_test, cluster_kv_watch_events,
    cluster_kv_watch_registration_cache_capacity_for_test,
    cluster_kv_watch_registration_cache_contains_for_test, create_cluster_kv_watch,
    drain_cluster_kv_watch, drain_cluster_kv_watch_once_limited_by_payload_bytes,
    fail_next_cluster_kv_watch_allocator_persist_for_test,
    record_cluster_kv_watch_allocator_trust_for_test, record_cluster_kv_watch_event_cache_for_test,
    record_cluster_kv_watch_registration_cache_for_test,
    refresh_cluster_kv_watch_allocator_trust_for_test,
    refresh_cluster_kv_watch_event_cache_for_test,
    refresh_cluster_kv_watch_registration_cache_for_test,
    remove_cluster_kv_watch_allocator_trust_entries_under_root_for_test,
    remove_cluster_kv_watch_event_cache_entries_under_root_for_test,
    remove_cluster_kv_watch_registration_cache_entries_under_root_for_test,
    reset_cluster_kv_watch_dir_scans_for_test, reset_cluster_kv_watch_drain_once_calls_for_test,
    reset_cluster_kv_watch_drain_read_passes_for_test,
    reset_cluster_kv_watch_event_log_read_bytes_for_test,
    reset_cluster_kv_watch_event_log_write_bytes_for_test,
};
