use crate::{ObjectRpcRequest, ObjectRpcResponse};
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
    Request {
        id: u64,
        request: ObjectRpcRequest,
    },
    Response {
        id: u64,
        response: ObjectRpcResponse,
    },
    StreamChunk {
        id: u64,
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    },
    StreamEnd {
        id: u64,
    },
    StreamError {
        id: u64,
        detail: String,
    },
    Close {
        detail: String,
    },
}
