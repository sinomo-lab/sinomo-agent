//! Cluster network, peer transport, and routing primitives.
//!
//! This crate owns route choice, endpoint selection, relay decisions, peer
//! transport contracts, and interpretation of the cluster connectivity graph.
//! Connectivity records are network-domain facts even when they are persisted
//! in KV. This crate interprets those records for routing through narrow query
//! adapters; it must not own KV transactions, watches, compaction, snapshots, or
//! Raft leaders/elections/storage layout. When cluster routes carry KV traffic,
//! this crate wraps KV-owned payload specs in the generic object-RPC envelope and
//! chooses how that envelope reaches a peer. KV local read/watch RPC dispatch
//! and Raft election semantics belong to `sinomo-kv`; network may temporarily
//! host filesystem and transport adapters while cluster-aware validation remains
//! here.

mod error;
mod invite;
mod lan;
mod object;
mod peer;
mod roaming;
mod route;
mod rpc;
mod storage;

pub use error::*;
pub use invite::*;
pub use lan::*;
pub use object::*;
pub use peer::*;
pub use roaming::*;
pub use route::*;
pub use rpc::*;
pub use storage::*;

pub type ClusterId = sinomo_core::Id;
pub type ClusterEndpointId = sinomo_core::Id;
pub type ClusterInviteId = sinomo_core::Id;
pub type ClusterCertificateId = sinomo_core::Id;

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::{ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    layout.register_sinomo_object(
        "sinomo-network",
        StorageRing::R1,
        "cluster/state.sobj",
        ObjectSchema::new("ClusterBootstrapState", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("cluster_id", "ClusterId")
            .field("local_node_id", "NodeId")
            .field("members", "Vec<ClusterMemberRecord>"),
    )?;
    layout.register_sinomo_object(
        "sinomo-network",
        StorageRing::R1,
        "cluster/connectivity-local.sobj",
        ObjectSchema::new(
            "Vec<ClusterConnectivityRecord>",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("source_node_id", "NodeId")
        .field("target_node_id", "NodeId")
        .field("endpoint_id", "ClusterEndpointId"),
    )?;
    layout.register_escape_path(
        "sinomo-network",
        StorageRing::R1,
        "cluster/iroh",
        "dynamic iroh key and endpoint object files keyed by node id",
    )
}

pub(crate) mod prelude {
    pub(crate) use crate::{ClusterEndpointId, ClusterId, ClusterInviteId};
    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use sinomo_core::{
        CapabilityName, ClusterBootstrapState, ClusterCertificate, ClusterEndpointPrivacy,
        ClusterEndpointRecord, ClusterEndpointSource, ClusterInvitationChannel, ClusterJoinInvite,
        ClusterMemberRecord, ClusterMemberRole, ClusterMemberState, ClusterNodeAvailability,
        ClusterNodeRole, ClusterNodeStatusRecord, ClusterTransportKind, EventEnvelope,
        LatencyClass, RoutingScope, TrustDomain,
    };
    pub(crate) use sinomo_events::{
        EventId, EventSequence, EventStore, EventStoreError, SessionId, StoredEvent,
    };
    pub(crate) use sinomo_kv::{
        ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, DurableRaftKvNode,
        InProcessKvStore, KvCommand, KvCompare, KvEntry, KvError, KvEvent, KvLocalCache,
        KvLogEntry, KvOp, KvRevision, KvSnapshot, KvTxn, KvTxnResponse, KvWalPaths, LeaseId,
        RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftRequestVoteRequest,
        RaftRequestVoteResponse, RaftSnapshot, WatchId,
    };
    pub(crate) use sinomo_local::SinomoDataDir;
    pub(crate) use sinomo_node::{
        NodeAvailability, NodeHealth, NodeId, NodeProfile, daemon_lan_endpoint_id,
        daemon_loopback_endpoint_id, main_daemon_capabilities, shadow_daemon_capabilities,
        shadow_daemon_node_id,
    };
    pub(crate) use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
    pub(crate) use std::fs;
    pub(crate) use std::io::{self, Write};
    pub(crate) use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
    #[cfg(feature = "iroh-transport")]
    pub(crate) use std::net::{Shutdown, TcpStream};
    pub(crate) use std::path::PathBuf;
    #[cfg(feature = "iroh-transport")]
    pub(crate) use std::sync::mpsc;
    pub(crate) use std::sync::{Arc, Mutex};
    #[cfg(feature = "iroh-transport")]
    pub(crate) use std::thread;
    pub(crate) use std::time::Duration;
    pub(crate) use thiserror::Error;
    pub(crate) use time::OffsetDateTime;
}

#[cfg(test)]
mod tests;
