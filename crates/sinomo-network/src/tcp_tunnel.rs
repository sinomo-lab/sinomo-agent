use serde::{Deserialize, Serialize};

pub const CLUSTER_TCP_TUNNEL_OPEN_ROUTE: &str = "/cluster/tcp-tunnel/open";
pub const CLUSTER_TCP_TUNNEL_RPC_CATEGORY: &str = "cluster.tcp-tunnel";
pub const CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION: u16 = 1;
pub const CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX: &str = "tcp-tunnel://";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterTcpTunnelOpenRequest {
    pub protocol_version: u16,
}

impl Default for ClusterTcpTunnelOpenRequest {
    fn default() -> Self {
        Self {
            protocol_version: CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterTcpTunnelOpenResponse {
    pub protocol_version: u16,
    pub accepted: bool,
}

impl ClusterTcpTunnelOpenResponse {
    pub fn accepted() -> Self {
        Self {
            protocol_version: CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
            accepted: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ClusterTcpTunnelFrame {
    /// Opens one canonical full-duplex object-RPC byte stream inside the
    /// persistent tunnel. Stream identifiers are allocated from disjoint
    /// odd/even spaces by the two physical tunnel endpoints.
    StreamOpen {
        id: u64,
    },
    /// Confirms that the peer created the logical stream and attached its
    /// canonical daemon session handler. Callers do not consume one-pass body
    /// sources until this acknowledgement arrives.
    StreamAccept {
        id: u64,
    },
    /// Carries an ordered byte-stream fragment for one logical session.
    StreamData {
        id: u64,
        bytes: Vec<u8>,
    },
    /// Half-closes the sender-to-receiver direction of one logical session.
    StreamFinish {
        id: u64,
    },
    /// Aborts both directions of one logical session without closing the
    /// persistent tunnel or disturbing other multiplexed sessions.
    StreamReset {
        id: u64,
        detail: String,
    },
    Close {
        detail: String,
    },
}
