//! Wire contract for the daemon's published DFS configuration.
//!
//! The DFS component publishes one immutable configuration snapshot. Out of
//! process clients learn that it changed from a typed event on the daemon
//! event stream and then fetch the current snapshot from the local daemon.
//! The event names generations only; the snapshot route is the only source of
//! configuration content.

use serde::{Deserialize, Serialize};
use sinomo_core::kv::{KvAuthorityProof, KvEntry, KvLocalRead, KvRevision};

/// Emitted by the local DFS component whenever its published configuration
/// content changes. Its payload is a [`DfsConfigurationPublication`].
pub const DFS_CONFIGURATION_PUBLISHED_EVENT_KIND: &str = "dfs.configuration.published";
pub const DFS_CONFIGURATION_EVENT_SCHEMA_ID: &str = "sinomo.dfs.configuration";
pub const DFS_CONFIGURATION_EVENT_SCHEMA_VERSION: &str = "1.0.0";

/// Local-node read of the current floor-checked configuration snapshot.
pub const DFS_CONFIGURATION_SNAPSHOT_ROUTE: &str = "/dfs/configuration/snapshot";
pub const DFS_CONFIGURATION_READ_CAPABILITY: &str = "dfs.configuration.read";

/// Upper bound for one encoded snapshot response: the bounded write
/// configuration, the filesystem scope it repeats, and framing.
pub const DFS_CONFIGURATION_SNAPSHOT_MAX_RESPONSE_BYTES: usize =
    sinomo_core::kv::CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_PAYLOAD_BYTES
        .saturating_mul(2)
        .saturating_add(1024 * 1024);

/// Stable detail of the typed error a client reports when its local daemon
/// predates [`DFS_CONFIGURATION_SNAPSHOT_ROUTE`].
pub const DFS_CONFIGURATION_SNAPSHOT_UNSUPPORTED_DETAIL: &str =
    "local daemon does not serve the published DFS configuration route /dfs/configuration/snapshot";

/// Identity of one publication within a daemon process.
///
/// Every generation is monotonic for the lifetime of the publishing daemon;
/// a restarted daemon starts again, so a client compares publications only
/// within one event-stream session. `revision` is the committed revision of
/// the filesystem scope and is `None` while that scope is withdrawn.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsConfigurationPublication {
    pub generation: u64,
    pub projection_generation: u64,
    pub storage_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<KvRevision>,
}

impl DfsConfigurationPublication {
    /// Whether this publication was made after `held` by the same daemon.
    pub fn advances_past(&self, held: &Self) -> bool {
        self.generation > held.generation
            || self.projection_generation > held.projection_generation
            || self.storage_generation > held.storage_generation
            || self.revision.is_some_and(|revision| {
                held.revision
                    .is_none_or(|held_revision| revision > held_revision)
            })
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsConfigurationSnapshotRequest {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum DfsConfigurationSnapshotResponse {
    Published {
        snapshot: DfsConfigurationSnapshotWire,
    },
    /// The daemon has no coherent publication yet. Its next publication
    /// emits [`DFS_CONFIGURATION_PUBLISHED_EVENT_KIND`].
    Pending {
        publication: DfsConfigurationPublication,
    },
}

/// One complete publication: the filesystem scope and, on a write-capable
/// host, the four-scope write configuration with its native proofs and the
/// consensus floor it was checked against.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsConfigurationSnapshotWire {
    pub publication: DfsConfigurationPublication,
    pub filesystem_entries: Vec<KvEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<DfsWriteConfigurationWire>,
}

/// Scopes and proofs are ordered as filesystems, content services, cluster
/// services and members.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsWriteConfigurationWire {
    pub revision: KvRevision,
    pub scopes: Vec<Vec<KvEntry>>,
    pub authority: Vec<KvAuthorityProof>,
    pub floor: KvLocalRead<Option<KvEntry>>,
}
