use crate::error::NetworkError;
use crate::object::SinomoObjectCodec;
#[cfg(test)]
use crate::peer::EventFrame;
use crate::peer::PeerEndpointCandidate;
use crate::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::LazyLock;
use std::time::Instant;

const DEFAULT_LAN_IP_DISCOVERY_CACHE_TTL: Duration = Duration::from_secs(2);
const PUBLIC_IP_DISCOVERY_CACHE_TTL: Duration = Duration::from_secs(300);
pub(crate) const PUBLIC_IP_DISCOVERY_EMPTY_CACHE_TTL: Duration = Duration::from_secs(30);
pub(crate) const PUBLIC_IP_DISCOVERY_DEFERRED_CACHE_TTL: Duration = Duration::from_secs(30);
pub(crate) const MAX_PUBLIC_IP_DISCOVERY_CACHE_ENTRIES: usize = 8;
pub(crate) const MAX_PUBLIC_IP_DISCOVERY_RESPONSE_BYTES: u64 = 16 * 1024;
const PUBLIC_IP_DISCOVERY_IO_TIMEOUT: Duration = Duration::from_millis(750);
static DEFAULT_LAN_IP_DISCOVERY_CACHE: LazyLock<Mutex<Option<LocalIpDiscoveryCacheEntry>>> =
    LazyLock::new(|| Mutex::new(None));
static PUBLIC_IP_DISCOVERY_CACHE: LazyLock<Mutex<PublicIpDiscoveryCache>> =
    LazyLock::new(|| Mutex::new(PublicIpDiscoveryCache::default()));

#[derive(Clone, Debug)]
struct LocalIpDiscoveryCacheEntry {
    checked_at: Instant,
    candidates: Vec<IpAddr>,
}

#[derive(Clone, Debug)]
struct PublicIpDiscoveryCacheEntry {
    checked_at: Instant,
    candidates: Vec<IpAddr>,
    complete: bool,
}

#[derive(Clone, Debug, Default)]
struct PublicIpDiscoveryCache {
    entries: BTreeMap<Vec<IpAddr>, PublicIpDiscoveryCacheEntry>,
    expiry_index: BTreeMap<Instant, BTreeSet<Vec<IpAddr>>>,
}

impl PublicIpDiscoveryCacheEntry {
    fn ttl(&self) -> Duration {
        if self.candidates.is_empty() {
            PUBLIC_IP_DISCOVERY_EMPTY_CACHE_TTL
        } else if !self.complete {
            PUBLIC_IP_DISCOVERY_DEFERRED_CACHE_TTL
        } else {
            PUBLIC_IP_DISCOVERY_CACHE_TTL
        }
    }

    fn is_fresh_at(&self, now: Instant) -> bool {
        now.duration_since(self.checked_at) < self.ttl()
    }

    fn expires_at(&self) -> Instant {
        self.checked_at
            .checked_add(self.ttl())
            .unwrap_or(self.checked_at)
    }
}

impl PublicIpDiscoveryCache {
    fn get_fresh(&self, key: &[IpAddr], now: Instant) -> Option<Vec<IpAddr>> {
        let entry = self.entries.get(key)?;
        entry.is_fresh_at(now).then(|| entry.candidates.clone())
    }

    fn insert(&mut self, key: Vec<IpAddr>, entry: PublicIpDiscoveryCacheEntry, now: Instant) {
        self.remove(&key);
        self.expire_stale_at(now);
        while self.entries.len() >= MAX_PUBLIC_IP_DISCOVERY_CACHE_ENTRIES {
            if !self.evict_next_expiring() {
                break;
            }
        }
        self.insert_expiry_index_entry(&key, &entry);
        self.entries.insert(key, entry);
    }

    #[cfg(test)]
    fn clear(&mut self) {
        self.entries.clear();
        self.expiry_index.clear();
    }

    #[cfg(test)]
    fn age_for_test(&mut self, age: Duration) {
        for entry in self.entries.values_mut() {
            if let Some(checked_at) = entry.checked_at.checked_sub(age) {
                entry.checked_at = checked_at;
            }
        }
        self.rebuild_expiry_index();
    }

    fn expire_stale_at(&mut self, now: Instant) {
        while let Some((expires_at, _)) = self.expiry_index.first_key_value() {
            if *expires_at > now {
                break;
            }
            let expires_at = *expires_at;
            let Some(keys) = self.expiry_index.remove(&expires_at) else {
                break;
            };
            for key in keys {
                if self
                    .entries
                    .get(&key)
                    .is_some_and(|entry| entry.expires_at() <= now)
                {
                    self.entries.remove(&key);
                }
            }
        }
    }

    fn evict_next_expiring(&mut self) -> bool {
        loop {
            let Some((expires_at, _)) = self.expiry_index.first_key_value() else {
                return false;
            };
            let expires_at = *expires_at;
            let Some(keys) = self.expiry_index.get_mut(&expires_at) else {
                return false;
            };
            let Some(key) = keys.pop_first() else {
                self.expiry_index.remove(&expires_at);
                continue;
            };
            if keys.is_empty() {
                self.expiry_index.remove(&expires_at);
            }
            if self.entries.remove(&key).is_some() {
                return true;
            }
        }
    }

    fn remove(&mut self, key: &[IpAddr]) {
        if let Some(entry) = self.entries.remove(key) {
            self.remove_expiry_index_entry(key, entry.expires_at());
        }
    }

    fn insert_expiry_index_entry(&mut self, key: &[IpAddr], entry: &PublicIpDiscoveryCacheEntry) {
        self.expiry_index
            .entry(entry.expires_at())
            .or_default()
            .insert(key.to_vec());
    }

    fn remove_expiry_index_entry(&mut self, key: &[IpAddr], at: Instant) {
        let should_remove_bucket = if let Some(keys) = self.expiry_index.get_mut(&at) {
            keys.remove(key);
            keys.is_empty()
        } else {
            false
        };
        if should_remove_bucket {
            self.expiry_index.remove(&at);
        }
    }

    #[cfg(test)]
    fn rebuild_expiry_index(&mut self) {
        self.expiry_index.clear();
        let entries = self
            .entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.expires_at()))
            .collect::<Vec<_>>();
        for (key, expires_at) in entries {
            self.expiry_index.entry(expires_at).or_default().insert(key);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublicIpDiscoveryResult {
    Complete(Vec<IpAddr>),
    Deferred(Vec<IpAddr>),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[cfg(test)]
pub(crate) struct LanQuicEndpointConfig {
    pub bind_address: String,
    pub alpn: String,
    pub max_frame_bytes: usize,
}

#[cfg(test)]
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
#[cfg(test)]
pub(crate) struct LanQuicEventFrameCodec {
    pub max_frame_bytes: usize,
}

#[cfg(test)]
impl Default for LanQuicEventFrameCodec {
    fn default() -> Self {
        Self {
            max_frame_bytes: LanQuicEndpointConfig::default().max_frame_bytes,
        }
    }
}

#[cfg(test)]
impl LanQuicEventFrameCodec {
    pub(crate) fn encode(&self, frame: &EventFrame) -> Result<Vec<u8>, NetworkError> {
        let bytes = SinomoObjectCodec::default().encode(frame)?;
        if bytes.len() > self.max_frame_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: bytes.len(),
                max_bytes: self.max_frame_bytes,
            });
        }
        Ok(bytes)
    }

    pub(crate) fn decode(&self, bytes: &[u8]) -> Result<EventFrame, NetworkError> {
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

pub async fn discover_default_lan_ip_candidates_async() -> Vec<IpAddr> {
    cached_default_lan_ip_candidates_with_async(discover_default_lan_ip_candidates_uncached_async)
        .await
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

pub(crate) async fn cached_default_lan_ip_candidates_with_async<F, Fut>(discover: F) -> Vec<IpAddr>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Vec<IpAddr>>,
{
    let now = Instant::now();
    if let Ok(cache) = DEFAULT_LAN_IP_DISCOVERY_CACHE.lock()
        && let Some(entry) = cache.as_ref()
        && now.duration_since(entry.checked_at) < DEFAULT_LAN_IP_DISCOVERY_CACHE_TTL
    {
        return entry.candidates.clone();
    }
    let candidates = discover().await;
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

async fn discover_default_lan_ip_candidates_uncached_async() -> Vec<IpAddr> {
    let targets = ["8.8.8.8:80", "1.1.1.1:80", "192.0.2.1:80"]
        .into_iter()
        .filter_map(|target| target.parse::<SocketAddr>().ok())
        .collect::<Vec<_>>();
    let route_candidates = discover_local_ip_candidates_for_targets_async(targets);
    let interface_candidates = tokio::task::spawn_blocking(discover_interface_ip_candidates);
    let (mut candidates, interface_candidates) =
        tokio::join!(route_candidates, interface_candidates);
    if let Ok(interface_candidates) = interface_candidates {
        for ip in interface_candidates {
            if !candidates.contains(&ip) {
                candidates.push(ip);
            }
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

pub(crate) async fn discover_default_public_ip_candidates_for_local_ips_async(
    local_ips: impl IntoIterator<Item = IpAddr>,
) -> Vec<IpAddr> {
    if std::env::var("SINOMO_DISABLE_PUBLIC_IP_DISCOVERY")
        .ok()
        .as_deref()
        == Some("1")
    {
        return Vec::new();
    }
    cached_default_public_ip_candidates_for_local_ips_with_async(local_ips, || async {
        discover_public_ip_candidates_from_hosts_with_status_async(&[
            ("api.ipify.org", "/"),
            ("ifconfig.me", "/ip"),
        ])
        .await
    })
    .await
}

#[cfg(test)]
pub(crate) fn cached_default_public_ip_candidates_for_local_ips_with(
    local_ips: impl IntoIterator<Item = IpAddr>,
    discover: impl FnOnce() -> PublicIpDiscoveryResult,
) -> Vec<IpAddr> {
    cached_default_public_ip_candidates_for_key_with(local_network_cache_key(local_ips), discover)
}

pub(crate) async fn cached_default_public_ip_candidates_for_local_ips_with_async<F, Fut>(
    local_ips: impl IntoIterator<Item = IpAddr>,
    discover: F,
) -> Vec<IpAddr>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = PublicIpDiscoveryResult>,
{
    cached_default_public_ip_candidates_for_key_with_async(
        local_network_cache_key(local_ips),
        discover,
    )
    .await
}

#[cfg(test)]
fn cached_default_public_ip_candidates_for_key_with(
    cache_key: Vec<IpAddr>,
    discover: impl FnOnce() -> PublicIpDiscoveryResult,
) -> Vec<IpAddr> {
    let now = Instant::now();
    if let Ok(cache) = PUBLIC_IP_DISCOVERY_CACHE.lock()
        && let Some(candidates) = cache.get_fresh(&cache_key, now)
    {
        return candidates;
    }
    let result = discover();
    let (candidates, complete) = match result {
        PublicIpDiscoveryResult::Complete(candidates) => (candidates, true),
        PublicIpDiscoveryResult::Deferred(candidates) if candidates.is_empty() => {
            return candidates;
        }
        PublicIpDiscoveryResult::Deferred(candidates) => (candidates, false),
    };
    if let Ok(mut cache) = PUBLIC_IP_DISCOVERY_CACHE.lock() {
        cache.insert(
            cache_key,
            PublicIpDiscoveryCacheEntry {
                checked_at: now,
                candidates: candidates.clone(),
                complete,
            },
            now,
        );
    }
    candidates
}

async fn cached_default_public_ip_candidates_for_key_with_async<F, Fut>(
    cache_key: Vec<IpAddr>,
    discover: F,
) -> Vec<IpAddr>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = PublicIpDiscoveryResult>,
{
    let now = Instant::now();
    if let Ok(cache) = PUBLIC_IP_DISCOVERY_CACHE.lock()
        && let Some(candidates) = cache.get_fresh(&cache_key, now)
    {
        return candidates;
    }
    let result = discover().await;
    let (candidates, complete) = match result {
        PublicIpDiscoveryResult::Complete(candidates) => (candidates, true),
        PublicIpDiscoveryResult::Deferred(candidates) if candidates.is_empty() => {
            return candidates;
        }
        PublicIpDiscoveryResult::Deferred(candidates) => (candidates, false),
    };
    if let Ok(mut cache) = PUBLIC_IP_DISCOVERY_CACHE.lock() {
        cache.insert(
            cache_key,
            PublicIpDiscoveryCacheEntry {
                checked_at: now,
                candidates: candidates.clone(),
                complete,
            },
            now,
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
        cache.age_for_test(age);
    }
}

pub(crate) async fn discover_public_ip_candidates_from_hosts_with_status_async(
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
        let address = match resolve_public_ip_service_address_async(host).await {
            PublicIpServiceAddressResolution::Resolved(address) => address,
            PublicIpServiceAddressResolution::Unavailable => continue,
            PublicIpServiceAddressResolution::Deferred => {
                return PublicIpDiscoveryResult::Deferred(candidates);
            }
        };
        let Ok(service_candidates) =
            fetch_public_ip_candidates_from_service_async(host, path, address).await
        else {
            continue;
        };
        for ip in service_candidates {
            if !candidates.contains(&ip) {
                candidates.push(ip);
            }
        }
    }
    PublicIpDiscoveryResult::Complete(candidates)
}

pub(crate) async fn fetch_public_ip_candidates_from_service_async(
    host: &str,
    path: &str,
    address: SocketAddr,
) -> Result<Vec<IpAddr>, NetworkError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::time::timeout(
        PUBLIC_IP_DISCOVERY_IO_TIMEOUT,
        tokio::net::TcpStream::connect(address),
    )
    .await
    .map_err(|_| NetworkError::Io {
        operation: format!("connect public ip service {host}"),
        detail: format!(
            "timed out after {}ms",
            PUBLIC_IP_DISCOVERY_IO_TIMEOUT.as_millis()
        ),
    })?
    .map_err(|source| NetworkError::Io {
        operation: format!("connect public ip service {host}"),
        detail: source.to_string(),
    })?;
    let request = public_ip_discovery_http_request(host, path);
    tokio::time::timeout(
        PUBLIC_IP_DISCOVERY_IO_TIMEOUT,
        stream.write_all(request.as_bytes()),
    )
    .await
    .map_err(|_| NetworkError::Io {
        operation: format!("write public ip service {host}"),
        detail: format!(
            "timed out after {}ms",
            PUBLIC_IP_DISCOVERY_IO_TIMEOUT.as_millis()
        ),
    })?
    .map_err(|source| NetworkError::Io {
        operation: format!("write public ip service {host}"),
        detail: source.to_string(),
    })?;

    let mut response = String::new();
    let mut limited = stream.take(MAX_PUBLIC_IP_DISCOVERY_RESPONSE_BYTES);
    tokio::time::timeout(
        PUBLIC_IP_DISCOVERY_IO_TIMEOUT,
        limited.read_to_string(&mut response),
    )
    .await
    .map_err(|_| NetworkError::Io {
        operation: format!("read public ip service {host}"),
        detail: format!(
            "timed out after {}ms",
            PUBLIC_IP_DISCOVERY_IO_TIMEOUT.as_millis()
        ),
    })?
    .map_err(|source| NetworkError::Io {
        operation: format!("read public ip service {host}"),
        detail: source.to_string(),
    })?;
    Ok(parse_public_ip_discovery_response(&response))
}

fn public_ip_discovery_http_request(host: &str, path: &str) -> String {
    format!(
        "GET {path} HTTP/1.0\r\nHost: {host}\r\nUser-Agent: sinomo-public-ip-discovery/0.1\r\nConnection: close\r\n\r\n"
    )
}

pub(crate) fn parse_public_ip_discovery_response(response: &str) -> Vec<IpAddr> {
    let mut candidates = Vec::new();
    let body = response.rsplit("\r\n\r\n").next().unwrap_or(response);
    for token in body
        .split(|ch: char| ch.is_whitespace() || matches!(ch, ',' | ';' | '"' | '\''))
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let Ok(ip) = token.parse::<IpAddr>() else {
            continue;
        };
        if is_invitable_ip(ip) && !is_private_or_link_local_ip(ip) && !candidates.contains(&ip) {
            candidates.push(ip);
        }
    }
    candidates
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicIpServiceAddressResolution {
    Resolved(SocketAddr),
    Unavailable,
    Deferred,
}

async fn resolve_public_ip_service_address_async(host: &str) -> PublicIpServiceAddressResolution {
    let lookup = tokio::net::lookup_host((host, 80));
    match tokio::time::timeout(PUBLIC_IP_DISCOVERY_IO_TIMEOUT, lookup).await {
        Ok(Ok(mut addresses)) => addresses
            .next()
            .map(PublicIpServiceAddressResolution::Resolved)
            .unwrap_or(PublicIpServiceAddressResolution::Unavailable),
        Ok(Err(_)) => PublicIpServiceAddressResolution::Unavailable,
        Err(_) => PublicIpServiceAddressResolution::Deferred,
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

pub async fn discover_local_ip_candidates_for_targets_async<I>(targets: I) -> Vec<IpAddr>
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
        let Ok(socket) = tokio::net::UdpSocket::bind(bind).await else {
            continue;
        };
        if socket.connect(target).await.is_err() {
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
    expiry_index: BTreeMap<u64, BTreeSet<(u64, String)>>,
    node_index: BTreeMap<NodeId, BTreeSet<String>>,
    cluster_index: BTreeMap<ClusterId, BTreeSet<String>>,
}

impl MdnsDiscoveryCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&mut self, record: MdnsDiscoveryRecord) {
        if let Some(previous) = self.records.get(&record.record_id) {
            remove_mdns_index_entries(
                &mut self.expiry_index,
                &mut self.node_index,
                &mut self.cluster_index,
                previous,
            );
        }
        insert_mdns_index_entries(
            &mut self.expiry_index,
            &mut self.node_index,
            &mut self.cluster_index,
            &record,
        );
        self.records.insert(record.record_id.clone(), record);
        self.prune_to_capacity();
    }

    pub fn expire_after_tick(&mut self, tick: u64) -> usize {
        let mut expired = 0_usize;
        while let Some((expires_after_tick, _)) = self.expiry_index.first_key_value() {
            if *expires_after_tick > tick {
                break;
            }
            let expires_after_tick = *expires_after_tick;
            let Some(indexed_records) = self.expiry_index.remove(&expires_after_tick) else {
                break;
            };
            for (_, record_id) in indexed_records {
                if let Some(record) = self.records.remove(&record_id) {
                    remove_mdns_node_index_entry(&mut self.node_index, &record);
                    remove_mdns_cluster_index_entry(&mut self.cluster_index, &record);
                    expired += 1;
                }
            }
        }
        expired
    }

    pub fn endpoint_candidates_for_node(&self, node_id: &NodeId) -> Vec<PeerEndpointCandidate> {
        self.node_index
            .get(node_id)
            .into_iter()
            .flat_map(|record_ids| record_ids.iter())
            .filter_map(|record_id| self.records.get(record_id))
            .map(|record| PeerEndpointCandidate::discovered(record.endpoint.clone()))
            .collect()
    }

    pub fn endpoints_for_cluster(&self, cluster_id: &ClusterId) -> Vec<ClusterEndpointRecord> {
        self.cluster_index
            .get(cluster_id)
            .into_iter()
            .flat_map(|record_ids| record_ids.iter())
            .filter_map(|record_id| self.records.get(record_id))
            .map(|record| record.endpoint.clone())
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn index_record_counts_for_test(&self) -> (usize, usize) {
        (
            self.node_index.values().map(BTreeSet::len).sum(),
            self.cluster_index.values().map(BTreeSet::len).sum(),
        )
    }

    fn prune_to_capacity(&mut self) {
        while self.records.len() > MAX_MDNS_DISCOVERY_RECORDS {
            let Some((expires_after_tick, _)) = self.expiry_index.first_key_value() else {
                return;
            };
            let expires_after_tick = *expires_after_tick;
            let Some(indexed_records) = self.expiry_index.get_mut(&expires_after_tick) else {
                return;
            };
            let Some((advertised_tick, record_id)) = indexed_records.pop_first() else {
                self.expiry_index.remove(&expires_after_tick);
                continue;
            };
            if indexed_records.is_empty() {
                self.expiry_index.remove(&expires_after_tick);
            }
            if let Some(record) = self.records.remove(&record_id) {
                debug_assert_eq!(record.expires_after_tick, expires_after_tick);
                debug_assert_eq!(record.advertised_tick, advertised_tick);
                remove_mdns_node_index_entry(&mut self.node_index, &record);
                remove_mdns_cluster_index_entry(&mut self.cluster_index, &record);
            }
        }
    }
}

fn insert_mdns_index_entries(
    expiry_index: &mut BTreeMap<u64, BTreeSet<(u64, String)>>,
    node_index: &mut BTreeMap<NodeId, BTreeSet<String>>,
    cluster_index: &mut BTreeMap<ClusterId, BTreeSet<String>>,
    record: &MdnsDiscoveryRecord,
) {
    expiry_index
        .entry(record.expires_after_tick)
        .or_default()
        .insert((record.advertised_tick, record.record_id.clone()));
    node_index
        .entry(record.endpoint.node_id.clone())
        .or_default()
        .insert(record.record_id.clone());
    cluster_index
        .entry(record.cluster_id.clone())
        .or_default()
        .insert(record.record_id.clone());
}

fn remove_mdns_index_entries(
    expiry_index: &mut BTreeMap<u64, BTreeSet<(u64, String)>>,
    node_index: &mut BTreeMap<NodeId, BTreeSet<String>>,
    cluster_index: &mut BTreeMap<ClusterId, BTreeSet<String>>,
    record: &MdnsDiscoveryRecord,
) {
    remove_mdns_expiry_index_entry(expiry_index, record);
    remove_mdns_node_index_entry(node_index, record);
    remove_mdns_cluster_index_entry(cluster_index, record);
}

fn remove_mdns_expiry_index_entry(
    expiry_index: &mut BTreeMap<u64, BTreeSet<(u64, String)>>,
    record: &MdnsDiscoveryRecord,
) {
    let should_remove_deadline =
        if let Some(records) = expiry_index.get_mut(&record.expires_after_tick) {
            records.remove(&(record.advertised_tick, record.record_id.clone()));
            records.is_empty()
        } else {
            false
        };
    if should_remove_deadline {
        expiry_index.remove(&record.expires_after_tick);
    }
}

fn remove_mdns_node_index_entry(
    node_index: &mut BTreeMap<NodeId, BTreeSet<String>>,
    record: &MdnsDiscoveryRecord,
) {
    let should_remove_node = if let Some(records) = node_index.get_mut(&record.endpoint.node_id) {
        records.remove(&record.record_id);
        records.is_empty()
    } else {
        false
    };
    if should_remove_node {
        node_index.remove(&record.endpoint.node_id);
    }
}

fn remove_mdns_cluster_index_entry(
    cluster_index: &mut BTreeMap<ClusterId, BTreeSet<String>>,
    record: &MdnsDiscoveryRecord,
) {
    let should_remove_cluster = if let Some(records) = cluster_index.get_mut(&record.cluster_id) {
        records.remove(&record.record_id);
        records.is_empty()
    } else {
        false
    };
    if should_remove_cluster {
        cluster_index.remove(&record.cluster_id);
    }
}

pub const SINOMO_LAN_DISCOVERY_PROTOCOL: &str = "sinomo.lan-discovery.v1";
pub const SINOMO_LAN_DISCOVERY_MULTICAST: Ipv4Addr = Ipv4Addr::new(239, 255, 77, 77);
pub const SINOMO_LAN_DISCOVERY_PORT: u16 = 21_359;
pub const SINOMO_LAN_DISCOVERY_TTL_TICKS: u64 = 10;
pub(crate) const MAX_SINOMO_LAN_DISCOVERY_TTL_TICKS: u64 = SINOMO_LAN_DISCOVERY_TTL_TICKS * 3;
pub(crate) const MAX_MDNS_DISCOVERY_RECORDS: usize = 4096;
pub(crate) const MAX_MDNS_DISCOVERY_DRAIN_DATAGRAMS: usize = 256;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MdnsDiscoveryAdvertisement {
    pub protocol: String,
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "crate::fallible_serde::deserialize_vec"
    )]
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
        let ttl_ticks = clamped_lan_discovery_ttl_ticks(self.ttl_ticks);
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
                    expires_after_tick: self.advertised_tick.saturating_add(ttl_ticks),
                }
            })
            .collect()
    }
}

pub fn lan_discovery_advertisement_for_state(
    state: &ClusterBootstrapState,
    advertised_tick: u64,
) -> Option<MdnsDiscoveryAdvertisement> {
    let member = state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)?;
    let endpoints = member
        .endpoints
        .iter()
        .filter(|endpoint| {
            endpoint.privacy == ClusterEndpointPrivacy::Lan
                && matches!(
                    endpoint.transport,
                    ClusterTransportKind::LanTcp | ClusterTransportKind::LanQuic
                )
        })
        .cloned()
        .collect::<Vec<_>>();
    (!endpoints.is_empty()).then(|| {
        MdnsDiscoveryAdvertisement::new(
            state.cluster_id.clone(),
            state.local_node_id.clone(),
            endpoints,
            advertised_tick,
        )
    })
}

pub fn apply_lan_discovery_advertisements(
    state: &mut ClusterBootstrapState,
    cache: &mut MdnsDiscoveryCache,
    advertisements: impl IntoIterator<Item = MdnsDiscoveryAdvertisement>,
) -> bool {
    let mut changed = false;
    for advertisement in advertisements {
        if advertisement.cluster_id != state.cluster_id
            || advertisement.node_id == state.local_node_id
        {
            continue;
        }
        for record in advertisement.records() {
            cache.upsert(record.clone());
            if let Some(member) = state
                .members
                .iter_mut()
                .find(|member| member.node_id == record.endpoint.node_id)
            {
                changed |= crate::upsert_member_endpoint(member, record.endpoint);
            }
        }
    }
    changed
}

fn clamped_lan_discovery_ttl_ticks(ttl_ticks: u64) -> u64 {
    ttl_ticks.min(MAX_SINOMO_LAN_DISCOVERY_TTL_TICKS)
}

#[derive(Debug)]
pub struct AsyncMdnsDiscoverySocket {
    socket: tokio::net::UdpSocket,
    multicast_address: SocketAddrV4,
}

impl AsyncMdnsDiscoverySocket {
    pub async fn bind_default() -> Result<Self, NetworkError> {
        Self::bind_port(SINOMO_LAN_DISCOVERY_PORT).await
    }

    pub async fn bind_port(port: u16) -> Result<Self, NetworkError> {
        let socket = tokio::net::UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port))
            .await
            .map_err(|source| NetworkError::Io {
                operation: "bind async lan discovery socket".to_string(),
                detail: source.to_string(),
            })?;
        socket
            .set_multicast_loop_v4(true)
            .map_err(|source| NetworkError::Io {
                operation: "enable async lan discovery multicast loopback".to_string(),
                detail: source.to_string(),
            })?;
        socket
            .set_multicast_ttl_v4(16)
            .map_err(|source| NetworkError::Io {
                operation: "set async lan discovery multicast ttl".to_string(),
                detail: source.to_string(),
            })?;
        socket
            .join_multicast_v4(SINOMO_LAN_DISCOVERY_MULTICAST, Ipv4Addr::UNSPECIFIED)
            .map_err(|source| NetworkError::Io {
                operation: "join async lan discovery multicast group".to_string(),
                detail: source.to_string(),
            })?;

        Ok(Self {
            socket,
            multicast_address: SocketAddrV4::new(SINOMO_LAN_DISCOVERY_MULTICAST, port),
        })
    }

    pub async fn announce(
        &self,
        advertisement: &MdnsDiscoveryAdvertisement,
    ) -> Result<usize, NetworkError> {
        let bytes = SinomoObjectCodec::default().encode(advertisement)?;
        self.socket
            .send_to(&bytes, self.multicast_address)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "send async lan discovery advertisement".to_string(),
                detail: source.to_string(),
            })
    }

    pub fn drain(&self) -> Result<Vec<MdnsDiscoveryAdvertisement>, NetworkError> {
        let mut advertisements = Vec::new();
        let mut buffer = [0_u8; 64 * 1024];
        for _ in 0..MAX_MDNS_DISCOVERY_DRAIN_DATAGRAMS {
            match self.socket.try_recv_from(&mut buffer) {
                Ok((received, _source)) => {
                    if let Some(advertisement) = decode_lan_discovery_datagram(&buffer[..received])
                    {
                        advertisements.push(advertisement);
                    }
                }
                Err(source) if source.kind() == io::ErrorKind::WouldBlock => break,
                Err(source) => {
                    return Err(NetworkError::Io {
                        operation: "receive async lan discovery advertisement".to_string(),
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
            expires_after_tick: advertised_tick
                .saturating_add(clamped_lan_discovery_ttl_ticks(ttl_ticks)),
        }
    }
}
