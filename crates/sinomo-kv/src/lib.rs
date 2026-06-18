//! In-process cluster KV core.
//!
//! This crate provides the first etcd-like control-plane KV surface for
//! Sinomo: monotonic revisions, compare-and-swap transactions, leases,
//! watches, snapshots, and a deterministic Raft replication core for tests,
//! local development, and daemon integration.
//!
//! This crate owns KV storage and Raft leader/election/log semantics. Cross-node
//! transmission should be supplied by network-owned transport capabilities; KV
//! code should not decide routes or interpret cluster connectivity topology. KV
//! can persist connectivity records as ordinary key/value entries, but those
//! records remain network-domain facts. KV RPC route/category/body payload specs
//! and local read/watch route dispatch live here; the cluster network wraps them
//! in its generic object-RPC envelope when transmitting between peers.

use fs2::FileExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_core::{
    ClusterBootstrapState, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState, Id,
};
use sinomo_node::NodeId;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_storage::durable_file::{self, file_ino};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
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
pub const CLUSTER_KV_RPC_CATEGORY: &str = "cluster.kv";
pub const CLUSTER_KV_SNAPSHOT_ROUTE: &str = "/cluster/kv/snapshot";
pub const CLUSTER_KV_TXN_ROUTE: &str = "/cluster/kv/txn";
pub const CLUSTER_KV_RANGE_PREFIX_ROUTE: &str = "/cluster/kv/range-prefix";
pub const CLUSTER_KV_WATCH_PREFIX_ROUTE: &str = "/cluster/kv/watch-prefix";
pub const CLUSTER_KV_WATCH_DRAIN_ROUTE: &str = "/cluster/kv/watch-drain";
pub const CLUSTER_KV_WATCH_CANCEL_ROUTE: &str = "/cluster/kv/watch-cancel";
pub const CLUSTER_KV_WATCH_EVENTS_ROUTE: &str = "/cluster/kv/watch-events";
pub const CLUSTER_RAFT_RPC_CATEGORY: &str = "cluster.raft";
pub const CLUSTER_RAFT_APPEND_ROUTE: &str = "/cluster/raft/append";
pub const CLUSTER_RAFT_VOTE_ROUTE: &str = "/cluster/raft/vote";
pub const CLUSTER_KV_LOCAL_READ_WATCH_ROUTES: &[&str] = &[
    CLUSTER_KV_RANGE_PREFIX_ROUTE,
    CLUSTER_KV_WATCH_PREFIX_ROUTE,
    CLUSTER_KV_WATCH_DRAIN_ROUTE,
    CLUSTER_KV_WATCH_CANCEL_ROUTE,
];
pub const CLUSTER_KV_RPC_ROUTES: &[&str] = &[
    CLUSTER_KV_SNAPSHOT_ROUTE,
    CLUSTER_KV_TXN_ROUTE,
    CLUSTER_KV_RANGE_PREFIX_ROUTE,
    CLUSTER_KV_WATCH_PREFIX_ROUTE,
    CLUSTER_KV_WATCH_DRAIN_ROUTE,
    CLUSTER_KV_WATCH_CANCEL_ROUTE,
    CLUSTER_KV_WATCH_EVENTS_ROUTE,
];
pub const CLUSTER_KV_RPC_ROUTE_METADATA: &[(&str, &str)] = &[
    (CLUSTER_KV_SNAPSHOT_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_TXN_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_RANGE_PREFIX_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_WATCH_PREFIX_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_WATCH_DRAIN_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_WATCH_CANCEL_ROUTE, CLUSTER_KV_RPC_CATEGORY),
    (CLUSTER_KV_WATCH_EVENTS_ROUTE, CLUSTER_KV_RPC_CATEGORY),
];
pub const CLUSTER_RAFT_RPC_ROUTE_METADATA: &[(&str, &str)] = &[
    (CLUSTER_RAFT_APPEND_ROUTE, CLUSTER_RAFT_RPC_CATEGORY),
    (CLUSTER_RAFT_VOTE_ROUTE, CLUSTER_RAFT_RPC_CATEGORY),
];
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
pub const CLUSTER_KV_SNAPSHOT_RPC_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
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

mod in_process;
mod model;
mod paths;
mod raft;
mod rpc;
mod snapshot;
#[cfg(test)]
mod test_support;
mod wal;
mod watch;

pub use in_process::*;
pub use model::*;
pub use paths::*;
pub use raft::*;
pub use rpc::*;
pub use snapshot::*;
pub use wal::*;
pub use watch::*;
