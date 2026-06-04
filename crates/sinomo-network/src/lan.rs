use crate::error::NetworkError;
use crate::object::SinomoObjectCodec;
use crate::peer::{NetworkFrame, PeerEndpointCandidate};
use crate::prelude::*;
use std::collections::BTreeMap;
use std::sync::{
    LazyLock,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Instant;

const DEFAULT_LAN_IP_DISCOVERY_CACHE_TTL: Duration = Duration::from_secs(2);
const PUBLIC_IP_DISCOVERY_CACHE_TTL: Duration = Duration::from_secs(300);
pub(crate) const PUBLIC_IP_DISCOVERY_EMPTY_CACHE_TTL: Duration = Duration::from_secs(30);
const MAX_PUBLIC_IP_DISCOVERY_CACHE_ENTRIES: usize = 8;
pub(crate) const MAX_PUBLIC_IP_DISCOVERY_RESPONSE_BYTES: u64 = 16 * 1024;
pub(crate) const MAX_PUBLIC_IP_DNS_WORKERS: usize = 2;
static DEFAULT_LAN_IP_DISCOVERY_CACHE: LazyLock<Mutex<Option<LocalIpDiscoveryCacheEntry>>> =
    LazyLock::new(|| Mutex::new(None));
static PUBLIC_IP_DISCOVERY_CACHE: LazyLock<
    Mutex<BTreeMap<Vec<IpAddr>, PublicIpDiscoveryCacheEntry>>,
> = LazyLock::new(|| Mutex::new(BTreeMap::new()));
static PUBLIC_IP_DNS_WORKERS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Debug)]
struct LocalIpDiscoveryCacheEntry {
    checked_at: Instant,
    candidates: Vec<IpAddr>,
}

#[derive(Clone, Debug)]
struct PublicIpDiscoveryCacheEntry {
    checked_at: Instant,
    candidates: Vec<IpAddr>,
}

impl PublicIpDiscoveryCacheEntry {
    fn ttl(&self) -> Duration {
        if self.candidates.is_empty() {
            PUBLIC_IP_DISCOVERY_EMPTY_CACHE_TTL
        } else {
            PUBLIC_IP_DISCOVERY_CACHE_TTL
        }
    }

    fn is_fresh_at(&self, now: Instant) -> bool {
        now.duration_since(self.checked_at) < self.ttl()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublicIpDiscoveryResult {
    Complete(Vec<IpAddr>),
    Deferred(Vec<IpAddr>),
}

#[derive(Debug)]
pub(crate) struct PublicIpDnsWorkerPermit;

impl Drop for PublicIpDnsWorkerPermit {
    fn drop(&mut self) {
        PUBLIC_IP_DNS_WORKERS.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) fn try_acquire_public_ip_dns_worker() -> Option<PublicIpDnsWorkerPermit> {
    let mut current = PUBLIC_IP_DNS_WORKERS.load(Ordering::Acquire);
    loop {
        if current >= MAX_PUBLIC_IP_DNS_WORKERS {
            return None;
        }
        match PUBLIC_IP_DNS_WORKERS.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Some(PublicIpDnsWorkerPermit),
            Err(observed) => current = observed,
        }
    }
}

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

pub fn discover_default_lan_ip_candidates() -> Vec<IpAddr> {
    cached_default_lan_ip_candidates_with(discover_default_lan_ip_candidates_uncached)
}

pub(crate) fn cached_default_lan_ip_candidates_with(
    discover: impl FnOnce() -> Vec<IpAddr>,
) -> Vec<IpAddr> {
    let now = Instant::now();
    if let Ok(cache) = DEFAULT_LAN_IP_DISCOVERY_CACHE.lock()
        && let Some(entry) = cache.as_ref()
        && now.duration_since(entry.checked_at) < DEFAULT_LAN_IP_DISCOVERY_CACHE_TTL
    {
        return entry.candidates.clone();
    }
    let candidates = discover();
    if let Ok(mut cache) = DEFAULT_LAN_IP_DISCOVERY_CACHE.lock() {
        *cache = Some(LocalIpDiscoveryCacheEntry {
            checked_at: now,
            candidates: candidates.clone(),
        });
    }
    candidates
}

fn discover_default_lan_ip_candidates_uncached() -> Vec<IpAddr> {
    let targets = ["8.8.8.8:80", "1.1.1.1:80", "192.0.2.1:80"]
        .into_iter()
        .filter_map(|target| target.parse::<SocketAddr>().ok());
    let mut candidates = discover_local_ip_candidates_for_targets(targets);
    for ip in discover_interface_ip_candidates() {
        if !candidates.contains(&ip) {
            candidates.push(ip);
        }
    }
    candidates
}

#[cfg(test)]
pub(crate) fn clear_default_lan_ip_discovery_cache() {
    if let Ok(mut cache) = DEFAULT_LAN_IP_DISCOVERY_CACHE.lock() {
        *cache = None;
    }
}

pub fn discover_interface_ip_candidates() -> Vec<IpAddr> {
    let Ok(interfaces) = local_ip_address::list_afinet_netifas() else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    for (_name, ip) in interfaces {
        if is_advertisable_interface_ip(ip) && !candidates.contains(&ip) {
            candidates.push(ip);
        }
    }
    candidates
}

/// Whether iroh transport is disabled for this process via the `SINOMO_DISABLE_IROH`
/// environment variable (`=1`). Lets a deployment run on direct LAN transports only --
/// e.g. the segmented network test cluster -- without starting or advertising iroh.
pub fn iroh_disabled_by_env() -> bool {
    std::env::var("SINOMO_DISABLE_IROH").ok().as_deref() == Some("1")
}

pub fn discover_default_public_ip_candidates() -> Vec<IpAddr> {
    if std::env::var("SINOMO_DISABLE_PUBLIC_IP_DISCOVERY")
        .ok()
        .as_deref()
        == Some("1")
    {
        return Vec::new();
    }
    cached_default_public_ip_candidates_for_key_with(Vec::new(), || {
        discover_public_ip_candidates_from_hosts_with_status(&[
            ("api.ipify.org", "/"),
            ("ifconfig.me", "/ip"),
        ])
    })
}

pub(crate) fn discover_default_public_ip_candidates_for_local_ips(
    local_ips: impl IntoIterator<Item = IpAddr>,
) -> Vec<IpAddr> {
    if std::env::var("SINOMO_DISABLE_PUBLIC_IP_DISCOVERY")
        .ok()
        .as_deref()
        == Some("1")
    {
        return Vec::new();
    }
    cached_default_public_ip_candidates_for_local_ips_with(local_ips, || {
        discover_public_ip_candidates_from_hosts_with_status(&[
            ("api.ipify.org", "/"),
            ("ifconfig.me", "/ip"),
        ])
    })
}

pub(crate) fn cached_default_public_ip_candidates_for_local_ips_with(
    local_ips: impl IntoIterator<Item = IpAddr>,
    discover: impl FnOnce() -> PublicIpDiscoveryResult,
) -> Vec<IpAddr> {
    cached_default_public_ip_candidates_for_key_with(local_network_cache_key(local_ips), discover)
}

fn cached_default_public_ip_candidates_for_key_with(
    cache_key: Vec<IpAddr>,
    discover: impl FnOnce() -> PublicIpDiscoveryResult,
) -> Vec<IpAddr> {
    let now = Instant::now();
    if let Ok(cache) = PUBLIC_IP_DISCOVERY_CACHE.lock()
        && let Some(entry) = cache.get(&cache_key)
        && entry.is_fresh_at(now)
    {
        return entry.candidates.clone();
    }
    let result = discover();
    let candidates = match result {
        PublicIpDiscoveryResult::Complete(candidates) => candidates,
        PublicIpDiscoveryResult::Deferred(candidates) => return candidates,
    };
    if let Ok(mut cache) = PUBLIC_IP_DISCOVERY_CACHE.lock() {
        if cache.len() >= MAX_PUBLIC_IP_DISCOVERY_CACHE_ENTRIES {
            cache.retain(|_, entry| entry.is_fresh_at(now));
        }
        if cache.len() >= MAX_PUBLIC_IP_DISCOVERY_CACHE_ENTRIES
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.checked_at)
                .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
        cache.insert(
            cache_key,
            PublicIpDiscoveryCacheEntry {
                checked_at: now,
                candidates: candidates.clone(),
            },
        );
    }
    candidates
}

pub(crate) fn local_network_cache_key(local_ips: impl IntoIterator<Item = IpAddr>) -> Vec<IpAddr> {
    let mut key = Vec::new();
    for ip in local_ips {
        if is_invitable_ip(ip) && !key.contains(&ip) {
            key.push(ip);
        }
    }
    key.sort();
    key
}

#[cfg(test)]
pub(crate) fn clear_public_ip_discovery_cache() {
    if let Ok(mut cache) = PUBLIC_IP_DISCOVERY_CACHE.lock() {
        cache.clear();
    }
}

#[cfg(test)]
pub(crate) fn age_public_ip_discovery_cache_for_test(age: Duration) {
    if let Ok(mut cache) = PUBLIC_IP_DISCOVERY_CACHE.lock() {
        for entry in cache.values_mut() {
            if let Some(checked_at) = entry.checked_at.checked_sub(age) {
                entry.checked_at = checked_at;
            }
        }
    }
}

pub fn discover_public_ip_candidates_from_hosts(hosts: &[(&str, &str)]) -> Vec<IpAddr> {
    match discover_public_ip_candidates_from_hosts_with_status(hosts) {
        PublicIpDiscoveryResult::Complete(candidates)
        | PublicIpDiscoveryResult::Deferred(candidates) => candidates,
    }
}

pub(crate) fn discover_public_ip_candidates_from_hosts_with_status(
    hosts: &[(&str, &str)],
) -> PublicIpDiscoveryResult {
    let mut candidates = Vec::new();
    if std::env::var("SINOMO_DISABLE_PUBLIC_IP_DISCOVERY")
        .ok()
        .as_deref()
        == Some("1")
    {
        return PublicIpDiscoveryResult::Complete(candidates);
    }
    for (host, path) in hosts {
        let address = match resolve_public_ip_service_address(host, Duration::from_millis(750)) {
            PublicIpServiceAddressResolution::Resolved(address) => address,
            PublicIpServiceAddressResolution::Unavailable => continue,
            PublicIpServiceAddressResolution::Deferred => {
                return PublicIpDiscoveryResult::Deferred(candidates);
            }
        };
        let Ok(mut stream) =
            std::net::TcpStream::connect_timeout(&address, Duration::from_millis(750))
        else {
            continue;
        };
        let _ = stream.set_read_timeout(Some(Duration::from_millis(750)));
        let _ = stream.set_write_timeout(Some(Duration::from_millis(750)));
        let request = format!(
            "GET {path} HTTP/1.0\r\nHost: {host}\r\nUser-Agent: sinomo-public-ip-discovery/0.1\r\nConnection: close\r\n\r\n"
        );
        if std::io::Write::write_all(&mut stream, request.as_bytes()).is_err() {
            continue;
        }
        let Ok(response) = read_public_ip_discovery_response(&mut stream) else {
            continue;
        };
        let body = response.rsplit("\r\n\r\n").next().unwrap_or(&response);
        for token in body
            .split(|ch: char| ch.is_whitespace() || matches!(ch, ',' | ';' | '"' | '\''))
            .map(str::trim)
            .filter(|token| !token.is_empty())
        {
            let Ok(ip) = token.parse::<IpAddr>() else {
                continue;
            };
            if is_invitable_ip(ip) && !is_private_or_link_local_ip(ip) && !candidates.contains(&ip)
            {
                candidates.push(ip);
            }
        }
    }
    PublicIpDiscoveryResult::Complete(candidates)
}

pub(crate) fn read_public_ip_discovery_response(
    reader: impl std::io::Read,
) -> std::io::Result<String> {
    let mut response = String::new();
    let mut limited = std::io::Read::take(reader, MAX_PUBLIC_IP_DISCOVERY_RESPONSE_BYTES);
    std::io::Read::read_to_string(&mut limited, &mut response)?;
    Ok(response)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicIpServiceAddressResolution {
    Resolved(SocketAddr),
    Unavailable,
    Deferred,
}

fn resolve_public_ip_service_address(
    host: &str,
    timeout: Duration,
) -> PublicIpServiceAddressResolution {
    let Some(permit) = try_acquire_public_ip_dns_worker() else {
        return PublicIpServiceAddressResolution::Deferred;
    };
    let host = host.to_string();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _permit = permit;
        let address = std::net::ToSocketAddrs::to_socket_addrs(&(host.as_str(), 80))
            .ok()
            .and_then(|mut addresses| addresses.next());
        let _ = sender.send(address);
    });
    match receiver.recv_timeout(timeout) {
        Ok(Some(address)) => PublicIpServiceAddressResolution::Resolved(address),
        Ok(None) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            PublicIpServiceAddressResolution::Unavailable
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            PublicIpServiceAddressResolution::Deferred
        }
    }
}

pub fn is_private_or_link_local_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private() || ip.is_link_local() || ip.is_broadcast() || ip.is_documentation()
        }
        IpAddr::V6(ip) => {
            ip.is_unique_local()
                || ip.is_unicast_link_local()
                || matches!(ip.segments(), [0x2001, 0x0db8, _, _, _, _, _, _])
        }
    }
}

pub fn discover_local_ip_candidates_for_targets<I>(targets: I) -> Vec<IpAddr>
where
    I: IntoIterator<Item = SocketAddr>,
{
    let mut candidates = Vec::new();
    for target in targets {
        let bind = if target.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let Ok(socket) = UdpSocket::bind(bind) else {
            continue;
        };
        if socket.connect(target).is_err() {
            continue;
        }
        let Ok(address) = socket.local_addr() else {
            continue;
        };
        let ip = address.ip();
        if is_invitable_ip(ip) && !candidates.contains(&ip) {
            candidates.push(ip);
        }
    }
    candidates
}

pub fn is_invitable_ip(ip: IpAddr) -> bool {
    !ip.is_loopback() && !ip.is_unspecified() && !ip.is_multicast()
}

pub(crate) fn is_advertisable_interface_ip(ip: IpAddr) -> bool {
    if !is_invitable_ip(ip) {
        return false;
    }
    match ip {
        IpAddr::V4(ip) => !ip.is_link_local() && !ip.is_broadcast() && !ip.is_documentation(),
        IpAddr::V6(ip) => {
            !ip.is_unicast_link_local()
                && !matches!(ip.segments(), [0x2001, 0x0db8, _, _, _, _, _, _])
        }
    }
}

pub fn format_ip_port(ip: IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(ip) => format!("{ip}:{port}"),
        IpAddr::V6(ip) => format!("[{ip}]:{port}"),
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
        self.prune_to_capacity();
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

    fn prune_to_capacity(&mut self) {
        while self.records.len() > MAX_MDNS_DISCOVERY_RECORDS {
            let Some(oldest) = self
                .records
                .iter()
                .min_by(|(left_id, left), (right_id, right)| {
                    (
                        left.expires_after_tick,
                        left.advertised_tick,
                        left_id.as_str(),
                    )
                        .cmp(&(
                            right.expires_after_tick,
                            right.advertised_tick,
                            right_id.as_str(),
                        ))
                })
                .map(|(record_id, _)| record_id.clone())
            else {
                return;
            };
            self.records.remove(&oldest);
        }
    }
}

pub const SINOMO_LAN_DISCOVERY_PROTOCOL: &str = "sinomo.lan-discovery.v1";
pub const SINOMO_LAN_DISCOVERY_MULTICAST: Ipv4Addr = Ipv4Addr::new(239, 255, 77, 77);
pub const SINOMO_LAN_DISCOVERY_PORT: u16 = 21_359;
pub const SINOMO_LAN_DISCOVERY_TTL_TICKS: u64 = 10;
pub(crate) const MAX_MDNS_DISCOVERY_RECORDS: usize = 4096;
pub(crate) const MAX_MDNS_DISCOVERY_DRAIN_DATAGRAMS: usize = 256;

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
        for _ in 0..MAX_MDNS_DISCOVERY_DRAIN_DATAGRAMS {
            match self.socket.recv_from(&mut buffer) {
                Ok((received, _source)) => {
                    if let Some(advertisement) = decode_lan_discovery_datagram(&buffer[..received])
                    {
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

pub(crate) fn decode_lan_discovery_datagram(bytes: &[u8]) -> Option<MdnsDiscoveryAdvertisement> {
    let advertisement: MdnsDiscoveryAdvertisement =
        SinomoObjectCodec::default().decode(bytes).ok()?;
    (advertisement.protocol == SINOMO_LAN_DISCOVERY_PROTOCOL).then_some(advertisement)
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
                endpoint_id: ClusterEndpointId::generate(),
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
