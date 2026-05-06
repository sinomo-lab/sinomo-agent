use crate::error::NetworkError;
use crate::object::SinomoObjectCodec;
use crate::peer::{NetworkFrame, PeerEndpointCandidate};
use crate::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanQuicEndpointConfig {
    pub bind_address: String,
    pub alpn: String,
    pub max_frame_bytes: usize,
}

impl Default for LanQuicEndpointConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:21355".to_string(),
            alpn: "sinomo-cluster-v1".to_string(),
            max_frame_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanQuicFrameCodec {
    pub max_frame_bytes: usize,
}

impl Default for LanQuicFrameCodec {
    fn default() -> Self {
        Self {
            max_frame_bytes: LanQuicEndpointConfig::default().max_frame_bytes,
        }
    }
}

impl LanQuicFrameCodec {
    pub fn encode(&self, frame: &NetworkFrame) -> Result<Vec<u8>, NetworkError> {
        let bytes = SinomoObjectCodec::default().encode(frame)?;
        if bytes.len() > self.max_frame_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: bytes.len(),
                max_bytes: self.max_frame_bytes,
            });
        }
        Ok(bytes)
    }

    pub fn decode(&self, bytes: &[u8]) -> Result<NetworkFrame, NetworkError> {
        if bytes.len() > self.max_frame_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: bytes.len(),
                max_bytes: self.max_frame_bytes,
            });
        }
        SinomoObjectCodec::default().decode(bytes)
    }
}

#[derive(Clone, Debug, Default)]
pub struct MdnsDiscoveryCache {
    records: BTreeMap<String, MdnsDiscoveryRecord>,
}

impl MdnsDiscoveryCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&mut self, record: MdnsDiscoveryRecord) {
        self.records.insert(record.record_id.clone(), record);
    }

    pub fn expire_after_tick(&mut self, tick: u64) -> usize {
        let before = self.records.len();
        self.records
            .retain(|_, record| record.expires_after_tick > tick);
        before.saturating_sub(self.records.len())
    }

    pub fn endpoint_candidates_for_node(&self, node_id: &NodeId) -> Vec<PeerEndpointCandidate> {
        self.records
            .values()
            .filter(|record| &record.endpoint.node_id == node_id)
            .map(|record| PeerEndpointCandidate::discovered(record.endpoint.clone()))
            .collect()
    }

    pub fn endpoints_for_cluster(&self, cluster_id: &ClusterId) -> Vec<ClusterEndpointRecord> {
        self.records
            .values()
            .filter(|record| &record.cluster_id == cluster_id)
            .map(|record| record.endpoint.clone())
            .collect()
    }
}

pub const SINOMO_LAN_DISCOVERY_PROTOCOL: &str = "sinomo.lan-discovery.v1";
pub const SINOMO_LAN_DISCOVERY_MULTICAST: Ipv4Addr = Ipv4Addr::new(239, 255, 77, 77);
pub const SINOMO_LAN_DISCOVERY_PORT: u16 = 21_359;
pub const SINOMO_LAN_DISCOVERY_TTL_TICKS: u64 = 10;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MdnsDiscoveryAdvertisement {
    pub protocol: String,
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<ClusterEndpointRecord>,
    pub advertised_tick: u64,
    pub ttl_ticks: u64,
}

impl MdnsDiscoveryAdvertisement {
    pub fn new(
        cluster_id: ClusterId,
        node_id: NodeId,
        endpoints: Vec<ClusterEndpointRecord>,
        advertised_tick: u64,
    ) -> Self {
        Self {
            protocol: SINOMO_LAN_DISCOVERY_PROTOCOL.to_string(),
            cluster_id,
            node_id,
            endpoints,
            advertised_tick,
            ttl_ticks: SINOMO_LAN_DISCOVERY_TTL_TICKS,
        }
    }

    pub fn records(&self) -> Vec<MdnsDiscoveryRecord> {
        self.endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.node_id == self.node_id
                    && matches!(
                        endpoint.transport,
                        ClusterTransportKind::LanTcp | ClusterTransportKind::LanQuic
                    )
                    && endpoint.privacy == ClusterEndpointPrivacy::Lan
            })
            .map(|endpoint| {
                let mut endpoint = endpoint.clone();
                endpoint.source = ClusterEndpointSource::Mdns;
                MdnsDiscoveryRecord {
                    record_id: format!(
                        "{}:{}:{}:{}",
                        self.cluster_id,
                        self.node_id,
                        cluster_transport_kind_label(&endpoint.transport),
                        endpoint.address
                    ),
                    cluster_id: self.cluster_id.clone(),
                    endpoint,
                    advertised_tick: self.advertised_tick,
                    expires_after_tick: self.advertised_tick.saturating_add(self.ttl_ticks),
                }
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct MdnsDiscoverySocket {
    socket: UdpSocket,
    multicast_address: SocketAddrV4,
}

impl MdnsDiscoverySocket {
    pub fn bind_default() -> Result<Self, NetworkError> {
        Self::bind_port(SINOMO_LAN_DISCOVERY_PORT)
    }

    pub fn bind_port(port: u16) -> Result<Self, NetworkError> {
        let socket =
            UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).map_err(|source| {
                NetworkError::Io {
                    operation: "bind lan discovery socket".to_string(),
                    detail: source.to_string(),
                }
            })?;
        socket
            .set_nonblocking(true)
            .map_err(|source| NetworkError::Io {
                operation: "set lan discovery nonblocking".to_string(),
                detail: source.to_string(),
            })?;
        socket
            .set_multicast_loop_v4(true)
            .map_err(|source| NetworkError::Io {
                operation: "enable lan discovery multicast loopback".to_string(),
                detail: source.to_string(),
            })?;
        socket
            .set_multicast_ttl_v4(16)
            .map_err(|source| NetworkError::Io {
                operation: "set lan discovery multicast ttl".to_string(),
                detail: source.to_string(),
            })?;
        socket
            .join_multicast_v4(&SINOMO_LAN_DISCOVERY_MULTICAST, &Ipv4Addr::UNSPECIFIED)
            .map_err(|source| NetworkError::Io {
                operation: "join lan discovery multicast group".to_string(),
                detail: source.to_string(),
            })?;

        Ok(Self {
            socket,
            multicast_address: SocketAddrV4::new(SINOMO_LAN_DISCOVERY_MULTICAST, port),
        })
    }

    pub fn announce(
        &self,
        advertisement: &MdnsDiscoveryAdvertisement,
    ) -> Result<usize, NetworkError> {
        let bytes = SinomoObjectCodec::default().encode(advertisement)?;
        self.socket
            .send_to(&bytes, self.multicast_address)
            .map_err(|source| NetworkError::Io {
                operation: "send lan discovery advertisement".to_string(),
                detail: source.to_string(),
            })
    }

    pub fn drain(&self) -> Result<Vec<MdnsDiscoveryAdvertisement>, NetworkError> {
        let mut advertisements = Vec::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            match self.socket.recv_from(&mut buffer) {
                Ok((received, _source)) => {
                    let advertisement: MdnsDiscoveryAdvertisement =
                        SinomoObjectCodec::default().decode(&buffer[..received])?;
                    if advertisement.protocol == SINOMO_LAN_DISCOVERY_PROTOCOL {
                        advertisements.push(advertisement);
                    }
                }
                Err(source) if source.kind() == io::ErrorKind::WouldBlock => break,
                Err(source) => {
                    return Err(NetworkError::Io {
                        operation: "receive lan discovery advertisement".to_string(),
                        detail: source.to_string(),
                    });
                }
            }
        }
        Ok(advertisements)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MdnsDiscoveryRecord {
    pub record_id: String,
    pub cluster_id: ClusterId,
    pub endpoint: ClusterEndpointRecord,
    pub advertised_tick: u64,
    pub expires_after_tick: u64,
}

fn cluster_transport_kind_label(transport: &ClusterTransportKind) -> &'static str {
    match transport {
        ClusterTransportKind::InProcess => "in-process",
        ClusterTransportKind::Loopback => "loopback",
        ClusterTransportKind::LanTcp => "lan-tcp",
        ClusterTransportKind::LanQuic => "lan-quic",
        ClusterTransportKind::Iroh => "iroh",
        ClusterTransportKind::WebRtc => "webrtc",
        ClusterTransportKind::TurnRelay => "turn-relay",
        ClusterTransportKind::HttpsRelay => "https-relay",
    }
}

impl MdnsDiscoveryRecord {
    pub fn lan_quic(
        cluster_id: ClusterId,
        node_id: NodeId,
        address: impl Into<String>,
        advertised_tick: u64,
        ttl_ticks: u64,
    ) -> Self {
        let address = address.into();
        let record_id = format!("{cluster_id}:{node_id}:lan-quic:{address}");
        Self {
            record_id,
            cluster_id,
            endpoint: ClusterEndpointRecord {
                endpoint_id: sinomo_core::ClusterEndpointId::generate(),
                node_id,
                transport: ClusterTransportKind::LanQuic,
                address,
                source: ClusterEndpointSource::Mdns,
                privacy: ClusterEndpointPrivacy::Lan,
                priority: 10,
                last_observed_latency_ms: None,
            },
            advertised_tick,
            expires_after_tick: advertised_tick.saturating_add(ttl_ticks),
        }
    }
}
