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
mod health;
mod invite;
mod lan;
mod object;
mod peer;
mod roaming;
mod route;
mod rpc;
mod storage;

pub use error::*;
pub use health::*;
pub use invite::*;
pub use lan::*;
pub use object::*;
pub use peer::{
    AsyncInProcessNetwork, AsyncInProcessTransport, AsyncNetworkSendOptions,
    AsyncNetworkServiceCommands, AsyncNetworkServiceHandle, AsyncNetworkTransport,
    AsyncNetworkTransportFuture, DeliveryAck, DeliveryLedger, DeliveryRecord, DeliveryStatus,
    EndpointCandidateStatus, Heartbeat, HeartbeatPolicy, InProcessNetwork, InProcessRouter,
    InProcessTransport, InboundFrame, InboundOutcome, LinkState, MeshMessage, MeshMessageBody,
    MeshMessageId, NetworkFrame, NetworkFrameId, NetworkService, NetworkServiceActorOptions,
    NetworkServiceCommandBackpressure, NetworkServiceCommandOptions, NetworkStreamChunkId,
    NetworkStreamFrame, NetworkStreamId, NetworkStreamOpen, NodeEndpoint, PeerEndpointCandidate,
    PeerRegistry, PeerState, PeerStatus, ProtocolMessage, RetryPolicy, RouteDelivery,
    RouteDeliveryId, TransportKind,
};
pub use roaming::*;
pub(crate) use route::cluster_route_next_hop;
pub use route::{
    CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_CATEGORY, CLUSTER_ROUTING_RPC_ROUTE_METADATA,
    ClusterObjectRpcClient, ClusterObjectRpcOptions, ClusterRelayAsyncHost, ClusterRelayFuture,
    ClusterRelayOptions, ClusterSignedEndpointRpcAsyncHost, ClusterSignedEndpointRpcFuture,
    ClusterSignedRelayRpcAsyncHost, ClusterSignedRelayRpcFuture, ClusterSignedRelayRpcOptions,
    RelayEnvelope, call_object_rpc_endpoint_async_with_host,
    call_object_rpc_endpoint_from_paths_async_with_host, cluster_endpoint_prefers_relay,
    cluster_endpoint_prefers_relay_with_graph, cluster_relay_forward_plan,
    cluster_relay_next_hop_endpoints, cluster_relay_next_hops_with_direct_target_fallback,
    handle_cluster_relay_async_with_host, relay_envelope_hop_transport_timeouts,
    relay_request_for_signed_rpc, relay_response_is_routing_error,
    relay_rpc_to_node_via_peer_async_with_host,
};
pub use rpc::*;
pub use storage::*;

pub type ClusterId = sinomo_core::Id;
pub type ClusterEndpointId = sinomo_core::Id;
pub type ClusterInviteId = sinomo_core::Id;
pub type ClusterCertificateId = sinomo_core::Id;

pub const NETWORK_PROTOCOL: &str = "sinomo.network.v1";
pub const SINOMO_NETWORK_MODULE_LEGACY_ID: &str = "module.network";
pub const SINOMO_NETWORK_MODULE_OWNER: &str = "sinomo-network";
pub const SINOMO_NETWORK_NATIVE_REGISTRATION_KEY: &str = "sinomo-network";
pub const NETWORK_OBJECT_RPC_ROUTES: &[&str] = &[CLUSTER_HEALTH_ROUTE, CLUSTER_NETWORK_PING_ROUTE];
pub const CLUSTER_HEALTH_READ_CAPABILITY: &str = "cluster.health.read";
pub const CLUSTER_NETWORK_PING_CAPABILITY: &str = "cluster.network.ping";
pub const NETWORK_CAPABILITY_IMPORTS: &[&str] = &[
    "storage.local.read",
    "storage.local.write",
    "cluster.kv.read",
];
pub const NETWORK_CAPABILITY_EXPORTS: &[&str] = &[
    CLUSTER_HEALTH_READ_CAPABILITY,
    CLUSTER_NETWORK_PING_CAPABILITY,
];
pub const CLUSTER_NETWORK_INSPECTION_SKILL_ID: &str = "skill.cluster-network.inspect";
pub const CLUSTER_NETWORK_INSPECTION_SKILL_PATH: &str = "skills/cluster-network/SKILL.md";
pub const CLUSTER_NETWORK_INSPECTION_PROMPT_ID: &str = "prompt.cluster-network.inspect";
pub const CLUSTER_NETWORK_INSPECTION_PROMPT_PATH: &str = "prompts/cluster-network-inspection.md";

pub const CLUSTER_NETWORK_INSPECTION_SKILL_MARKDOWN: &str = r#"# Cluster Network Inspection

Use the `module_rpc_cluster_health_cluster_health_read` module tool with the
`cluster.health.read` capability before reasoning about live cluster
connectivity, membership reachability, peer transport, or rollout availability.

Treat the cluster health rows as point-in-time evidence. Compare node id,
availability, role, daemon endpoint, health status, and last observation fields
before recommending operator action. Do not infer membership changes from a
single stale row.

Prefer read-only inspection. Do not use relay, membership mutation, rollout, or
DFS repair tools unless the user explicitly asks for remediation and the current
health evidence supports that action.
"#;

pub const CLUSTER_NETWORK_INSPECTION_PROMPT_MARKDOWN: &str = r#"# Cluster Network Inspection Prompt

Inspect the current Sinomo cluster network state with the native network module
health route. Summarize reachable and unreachable nodes, stale observations,
route or endpoint symptoms, and the narrowest next diagnostic command or tool.
"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkModuleRouteAuth {
    LocalCommand,
    LocalOrClusterMember,
    ClusterMember,
    Admin,
}

impl NetworkModuleRouteAuth {
    pub fn as_scope(self) -> &'static str {
        match self {
            Self::LocalCommand => "local-command",
            Self::LocalOrClusterMember => "local-or-cluster-member",
            Self::ClusterMember => "cluster-member",
            Self::Admin => "admin",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkObjectRpcToolSurface {
    pub route: &'static str,
    pub category: &'static str,
    pub operation: &'static str,
    pub auth: NetworkModuleRouteAuth,
}

pub const NETWORK_OBJECT_RPC_TOOL_SURFACES: &[NetworkObjectRpcToolSurface] = &[
    NetworkObjectRpcToolSurface {
        route: CLUSTER_HEALTH_ROUTE,
        category: CLUSTER_HEALTH_RPC_CATEGORY,
        operation: CLUSTER_HEALTH_READ_CAPABILITY,
        auth: NetworkModuleRouteAuth::LocalOrClusterMember,
    },
    NetworkObjectRpcToolSurface {
        route: CLUSTER_NETWORK_PING_ROUTE,
        category: CLUSTER_NETWORK_RPC_CATEGORY,
        operation: CLUSTER_NETWORK_PING_CAPABILITY,
        auth: NetworkModuleRouteAuth::LocalOrClusterMember,
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkDeclarativeResourceKind {
    Skill,
    Prompt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkDeclarativeResourceSurface {
    pub id: &'static str,
    pub kind: NetworkDeclarativeResourceKind,
    pub path: &'static str,
    pub description: &'static str,
    pub model_families: &'static [&'static str],
    pub activation_tags: &'static [&'static str],
    pub required_capabilities: &'static [&'static str],
}

pub const NETWORK_DECLARATIVE_RESOURCE_SURFACES: &[NetworkDeclarativeResourceSurface] = &[
    NetworkDeclarativeResourceSurface {
        id: CLUSTER_NETWORK_INSPECTION_SKILL_ID,
        kind: NetworkDeclarativeResourceKind::Skill,
        path: CLUSTER_NETWORK_INSPECTION_SKILL_PATH,
        description: "Guide agents through read-only Sinomo cluster network inspection.",
        model_families: &["gpt"],
        activation_tags: &["cluster", "network"],
        required_capabilities: &[CLUSTER_HEALTH_READ_CAPABILITY],
    },
    NetworkDeclarativeResourceSurface {
        id: CLUSTER_NETWORK_INSPECTION_PROMPT_ID,
        kind: NetworkDeclarativeResourceKind::Prompt,
        path: CLUSTER_NETWORK_INSPECTION_PROMPT_PATH,
        description: "Prompt fragment for summarizing cluster network health evidence.",
        model_families: &["gpt"],
        activation_tags: &["cluster", "network"],
        required_capabilities: &[CLUSTER_HEALTH_READ_CAPABILITY],
    },
];

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
    #[cfg(test)]
    pub(crate) use sinomo_kv::KvLogEntry;
    pub(crate) use sinomo_kv::{
        ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, DurableRaftKvNode,
        InProcessKvStore, KvCommand, KvCompare, KvEntry, KvError, KvEvent, KvLocalCache, KvOp,
        KvSnapshot, KvTxn, KvTxnResponse, LeaseId, RaftAppendEntriesRequest,
        RaftAppendEntriesResponse, RaftRequestVoteRequest, RaftRequestVoteResponse, RaftSnapshot,
    };
    #[cfg(test)]
    pub(crate) use sinomo_kv::{KvWalPaths, WatchId};
    pub(crate) use sinomo_local::SinomoDataDir;
    pub(crate) use sinomo_node::{
        NodeAvailability, NodeHealth, NodeId, NodeProfile, daemon_lan_endpoint_id,
        daemon_loopback_endpoint_id, main_daemon_capabilities, shadow_daemon_node_id,
    };
    pub(crate) use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
    pub(crate) use std::fs;
    pub(crate) use std::io;
    pub(crate) use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
    pub(crate) use std::path::PathBuf;
    pub(crate) use std::sync::{Arc, Mutex};
    #[cfg(feature = "iroh-transport")]
    pub(crate) use std::thread;
    pub(crate) use std::time::Duration;
    pub(crate) use thiserror::Error;
    pub(crate) use time::OffsetDateTime;
}

#[cfg(test)]
mod tests;
