//! Peer registry, mesh contracts, and cluster networking primitives.

mod error;
mod lan;
mod object;
mod peer;
mod roaming;
mod storage;

pub use error::*;
pub use lan::*;
pub use object::*;
pub use peer::*;
pub use roaming::*;
pub use storage::*;

pub(crate) mod prelude {
    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use sinomo_core::{
        CapabilityName, ClusterBootstrapState, ClusterCertificate, ClusterEndpointId,
        ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterId,
        ClusterInviteId, ClusterMemberRecord, ClusterMemberRole, ClusterNodeStatusRecord,
        ClusterTransportKind, EventEnvelope, EventId, LatencyClass, NodeId, RoutingScope,
        SessionId,
    };
    pub(crate) use sinomo_events::{EventSequence, EventStore, EventStoreError, StoredEvent};
    pub(crate) use sinomo_kv::{
        DurableRaftKvNode, InProcessKvStore, KvCommand, KvEntry, KvError, KvEvent, KvLogEntry,
        KvOp, KvRevision, KvSnapshot, KvTxn, KvTxnResponse, KvWalPaths, LeaseId,
        RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftRequestVoteRequest,
        RaftRequestVoteResponse, RaftSnapshot, WatchId,
    };
    pub(crate) use sinomo_local::SinomoDataDir;
    pub(crate) use sinomo_node::{NodeAvailability, NodeHealth, NodeProfile};
    pub(crate) use std::cell::RefCell;
    pub(crate) use std::collections::{BTreeMap, BTreeSet, VecDeque};
    pub(crate) use std::fs;
    #[cfg(feature = "iroh-transport")]
    pub(crate) use std::io::Read;
    pub(crate) use std::io::{self, BufRead, BufReader, Write};
    pub(crate) use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
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
