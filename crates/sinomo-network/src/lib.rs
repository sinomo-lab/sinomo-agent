//! Peer registry, mesh contracts, and cluster networking primitives.

mod error;
mod invite;
mod lan;
mod object;
mod peer;
mod roaming;
mod rpc;
mod storage;

pub use error::*;
pub use invite::*;
pub use lan::*;
pub use object::*;
pub use peer::*;
pub use roaming::*;
pub use rpc::*;
pub use storage::*;

pub(crate) mod prelude {
    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use sinomo_core::{
        CapabilityName, ClusterBootstrapState, ClusterCertificate, ClusterEndpointId,
        ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterId,
        ClusterInvitationChannel, ClusterInviteId, ClusterJoinInvite, ClusterMemberRecord,
        ClusterMemberRole, ClusterMemberState, ClusterNodeRole, ClusterNodeStatusRecord,
        ClusterTransportKind, EventEnvelope, EventId, LatencyClass, NodeId, RoutingScope,
        SessionId, TrustDomain,
    };
    pub(crate) use sinomo_events::{EventSequence, EventStore, EventStoreError, StoredEvent};
    pub(crate) use sinomo_kv::{
        DurableRaftKvNode, InProcessKvStore, KvCommand, KvCompare, KvEntry, KvError, KvEvent,
        KvLogEntry, KvOp, KvRevision, KvSnapshot, KvTxn, KvTxnResponse, KvWalPaths, LeaseId,
        RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftRequestVoteRequest,
        RaftRequestVoteResponse, RaftSnapshot, WatchId,
    };
    pub(crate) use sinomo_local::SinomoDataDir;
    pub(crate) use sinomo_node::{
        NodeAvailability, NodeHealth, NodeProfile, daemon_lan_endpoint_id,
        daemon_loopback_endpoint_id, main_daemon_capabilities,
    };
    pub(crate) use std::cell::RefCell;
    pub(crate) use std::collections::{BTreeMap, BTreeSet, VecDeque};
    pub(crate) use std::fs;
    pub(crate) use std::io::{self, Write};
    pub(crate) use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
    #[cfg(feature = "iroh-transport")]
    pub(crate) use std::net::{Shutdown, TcpStream, ToSocketAddrs};
    pub(crate) use std::path::PathBuf;
    pub(crate) use std::rc::Rc;
    #[cfg(feature = "iroh-transport")]
    pub(crate) use std::sync::mpsc;
    #[cfg(feature = "iroh-transport")]
    pub(crate) use std::thread;
    pub(crate) use std::time::Duration;
    pub(crate) use thiserror::Error;
    pub(crate) use time::OffsetDateTime;
}

#[cfg(test)]
mod tests;
