use crate::is_loopback_connect_address;
use crate::lan::{
    discover_default_public_ip_candidates_for_local_ips_async, format_ip_port, is_invitable_ip,
    is_private_or_link_local_ip, local_network_cache_key,
};
use sinomo_base::{
    ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterTransportKind,
};
use sinomo_node::{NodeId, daemon_lan_endpoint_id, daemon_loopback_endpoint_id};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

pub(crate) const PUBLIC_ENDPOINT_REACHABILITY_POSITIVE_CACHE_TTL: Duration =
    Duration::from_secs(60);
pub const PUBLIC_ENDPOINT_REACHABILITY_NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(10);
pub const MAX_PUBLIC_ENDPOINT_REACHABILITY_CACHE_ENTRIES: usize = 128;

/// How long to wait when probing this node's own public endpoint for reachability.
const PUBLIC_ENDPOINT_PROBE_TIMEOUT: Duration = Duration::from_millis(750);

static PUBLIC_ENDPOINT_REACHABILITY_CACHE: LazyLock<Mutex<PublicEndpointReachabilityCache>> =
    LazyLock::new(|| Mutex::new(PublicEndpointReachabilityCache::default()));

#[derive(Clone, Debug)]
struct PublicEndpointReachabilityCacheEntry {
    checked_at: Instant,
    reachable: bool,
}

#[derive(Clone, Debug, Default)]
struct PublicEndpointReachabilityCache {
    entries: HashMap<PublicEndpointReachabilityCacheKey, PublicEndpointReachabilityCacheEntry>,
    expiry_index: BTreeMap<Instant, BTreeSet<PublicEndpointReachabilityCacheKey>>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct PublicEndpointReachabilityCacheKey {
    address: SocketAddr,
    local_network: Vec<IpAddr>,
}

impl PublicEndpointReachabilityCacheEntry {
    fn ttl(&self) -> Duration {
        if self.reachable {
            PUBLIC_ENDPOINT_REACHABILITY_POSITIVE_CACHE_TTL
        } else {
            PUBLIC_ENDPOINT_REACHABILITY_NEGATIVE_CACHE_TTL
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

impl PublicEndpointReachabilityCache {
    fn get_fresh(&self, key: &PublicEndpointReachabilityCacheKey, now: Instant) -> Option<bool> {
        let entry = self.entries.get(key)?;
        entry.is_fresh_at(now).then_some(entry.reachable)
    }

    fn insert(
        &mut self,
        key: PublicEndpointReachabilityCacheKey,
        entry: PublicEndpointReachabilityCacheEntry,
        now: Instant,
    ) {
        self.remove(&key);
        self.expire_stale_at(now);
        while self.entries.len() >= MAX_PUBLIC_ENDPOINT_REACHABILITY_CACHE_ENTRIES {
            if !self.evict_next_expiring() {
                break;
            }
        }
        self.insert_expiry_index_entry(&key, &entry);
        self.entries.insert(key, entry);
    }

    #[cfg(any(test, feature = "test-support"))]
    fn clear(&mut self) {
        self.entries.clear();
        self.expiry_index.clear();
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

    fn remove(&mut self, key: &PublicEndpointReachabilityCacheKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.remove_expiry_index_entry(key, entry.expires_at());
        }
    }

    fn insert_expiry_index_entry(
        &mut self,
        key: &PublicEndpointReachabilityCacheKey,
        entry: &PublicEndpointReachabilityCacheEntry,
    ) {
        self.expiry_index
            .entry(entry.expires_at())
            .or_default()
            .insert(key.clone());
    }

    fn remove_expiry_index_entry(&mut self, key: &PublicEndpointReachabilityCacheKey, at: Instant) {
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
}

pub fn daemon_advertised_endpoints_from_static_addresses(
    node_id: &NodeId,
    loopback_address: String,
    host: &str,
    port: u16,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_endpoints: bool,
    advertise_host: Option<&str>,
) -> Vec<ClusterEndpointRecord> {
    daemon_advertised_endpoints_from_addresses_with_verified_public_ips(
        node_id,
        loopback_address,
        host,
        port,
        priority,
        lan_ips,
        route_ips,
        allow_public_endpoints,
        advertise_host,
        std::iter::empty(),
    )
}

/// Whether this node's own public endpoint (`ip:port`) is actually reachable -- i.e. an
/// external connection to it lands on this daemon, via a user-configured port-forward (or
/// NAT hairpin). Only a verified public endpoint should be advertised; an unverified one (a
/// NAT'd node with no forward) would publish an unreachable address that every peer then
/// wastes connection attempts on. Manual endpoint configuration is future work.
pub async fn public_endpoint_is_reachable_for_local_ips_async(
    ip: IpAddr,
    port: u16,
    local_ips: impl IntoIterator<Item = IpAddr>,
) -> bool {
    public_endpoint_is_reachable_for_local_ips_with_async_probe_at(
        ip,
        port,
        Instant::now(),
        local_ips,
        |address| async move {
            matches!(
                tokio::time::timeout(
                    PUBLIC_ENDPOINT_PROBE_TIMEOUT,
                    tokio::net::TcpStream::connect(address),
                )
                .await,
                Ok(Ok(_stream))
            )
        },
    )
    .await
}

#[cfg(any(test, feature = "test-support"))]
pub fn public_endpoint_is_reachable_with_probe(
    ip: IpAddr,
    port: u16,
    probe: impl FnOnce(SocketAddr) -> bool,
) -> bool {
    public_endpoint_is_reachable_with_probe_at(ip, port, Instant::now(), probe)
}

#[cfg(any(test, feature = "test-support"))]
pub fn public_endpoint_is_reachable_with_probe_at(
    ip: IpAddr,
    port: u16,
    now: Instant,
    probe: impl FnOnce(SocketAddr) -> bool,
) -> bool {
    public_endpoint_is_reachable_for_local_ips_with_probe_at(
        ip,
        port,
        now,
        std::iter::empty(),
        probe,
    )
}

pub(crate) async fn public_endpoint_is_reachable_for_local_ips_with_async_probe_at<F, Fut>(
    ip: IpAddr,
    port: u16,
    now: Instant,
    local_ips: impl IntoIterator<Item = IpAddr>,
    probe: F,
) -> bool
where
    F: FnOnce(SocketAddr) -> Fut,
    Fut: Future<Output = bool>,
{
    let address = SocketAddr::new(ip, port);
    let cache_key = PublicEndpointReachabilityCacheKey {
        address,
        local_network: local_network_cache_key(local_ips),
    };
    if let Ok(cache) = PUBLIC_ENDPOINT_REACHABILITY_CACHE.lock()
        && let Some(reachable) = cache.get_fresh(&cache_key, now)
    {
        return reachable;
    }
    let reachable = probe(address).await;
    if let Ok(mut cache) = PUBLIC_ENDPOINT_REACHABILITY_CACHE.lock() {
        cache.insert(
            cache_key,
            PublicEndpointReachabilityCacheEntry {
                checked_at: now,
                reachable,
            },
            now,
        );
    }
    reachable
}

#[cfg(any(test, feature = "test-support"))]
pub fn public_endpoint_is_reachable_for_local_ips_with_probe_at(
    ip: IpAddr,
    port: u16,
    now: Instant,
    local_ips: impl IntoIterator<Item = IpAddr>,
    probe: impl FnOnce(SocketAddr) -> bool,
) -> bool {
    let address = SocketAddr::new(ip, port);
    let cache_key = PublicEndpointReachabilityCacheKey {
        address,
        local_network: local_network_cache_key(local_ips),
    };
    if let Ok(cache) = PUBLIC_ENDPOINT_REACHABILITY_CACHE.lock()
        && let Some(reachable) = cache.get_fresh(&cache_key, now)
    {
        return reachable;
    }
    let reachable = probe(address);
    if let Ok(mut cache) = PUBLIC_ENDPOINT_REACHABILITY_CACHE.lock() {
        cache.insert(
            cache_key,
            PublicEndpointReachabilityCacheEntry {
                checked_at: now,
                reachable,
            },
            now,
        );
    }
    reachable
}

#[cfg(any(test, feature = "test-support"))]
pub fn clear_public_endpoint_reachability_cache() {
    if let Ok(mut cache) = PUBLIC_ENDPOINT_REACHABILITY_CACHE.lock() {
        cache.clear();
    }
}

pub async fn daemon_advertised_endpoints_from_addresses_with_public_discovery_and_advertise_host_async(
    node_id: &NodeId,
    loopback_address: String,
    host: &str,
    port: u16,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_ip_discovery: bool,
    advertise_host: Option<&str>,
) -> Vec<ClusterEndpointRecord> {
    let verified_public_ips = verified_public_endpoint_ips_async(
        host,
        port,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
    )
    .await;
    daemon_advertised_endpoints_from_addresses_with_verified_public_ips(
        node_id,
        loopback_address,
        host,
        port,
        priority,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
        advertise_host,
        verified_public_ips,
    )
}

fn daemon_advertised_endpoints_from_addresses_with_verified_public_ips(
    node_id: &NodeId,
    loopback_address: String,
    host: &str,
    port: u16,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_ip_discovery: bool,
    advertise_host: Option<&str>,
    verified_public_ips: impl IntoIterator<Item = IpAddr>,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = vec![daemon_loopback_endpoint(
        node_id,
        loopback_address,
        priority,
    )];
    let mut lan_addresses = Vec::new();
    if matches!(host, "0.0.0.0" | "::") {
        for ip in lan_ips.iter().chain(route_ips.iter()).copied() {
            if is_invitable_ip(ip) && (allow_public_ip_discovery || is_private_or_link_local_ip(ip))
            {
                push_unique_address(&mut lan_addresses, format_ip_port(ip, port));
            }
        }
        let has_public_candidate = lan_ips
            .iter()
            .chain(route_ips.iter())
            .copied()
            .any(|ip| is_invitable_ip(ip) && !is_private_or_link_local_ip(ip));
        if allow_public_ip_discovery && !has_public_candidate {
            for ip in verified_public_ips {
                // Only advertise a discovered public IP when it is actually reachable on the
                // daemon port -- i.e. the user has a port-forward (or NAT hairpin) back to
                // this daemon. An unverified public IP would otherwise pollute every peer's
                // connection-target list with an unreachable endpoint.
                if is_invitable_ip(ip) {
                    push_unique_address(&mut lan_addresses, format_ip_port(ip, port));
                }
            }
        }
    } else if host.parse::<IpAddr>().is_ok_and(is_invitable_ip)
        || !is_loopback_connect_address(&format!("{host}:{port}"))
    {
        push_unique_address(&mut lan_addresses, format!("{host}:{port}"));
    }

    for address in lan_addresses {
        endpoints.push(daemon_lan_endpoint(
            node_id,
            address,
            priority.saturating_add(20),
        ));
    }
    if let Some(address) = daemon_advertise_host_address(advertise_host, port)
        && endpoints.iter().all(|endpoint| endpoint.address != address)
    {
        endpoints.push(daemon_lan_endpoint(
            node_id,
            address,
            priority.saturating_add(5),
        ));
    }
    endpoints
}

fn should_discover_public_endpoint_candidates(
    host: &str,
    allow_public_ip_discovery: bool,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
) -> bool {
    if !matches!(host, "0.0.0.0" | "::") || !allow_public_ip_discovery {
        return false;
    }
    !lan_ips
        .iter()
        .chain(route_ips.iter())
        .copied()
        .any(|ip| is_invitable_ip(ip) && !is_private_or_link_local_ip(ip))
}

async fn verified_public_endpoint_ips_async(
    host: &str,
    port: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_ip_discovery: bool,
) -> Vec<IpAddr> {
    if !should_discover_public_endpoint_candidates(
        host,
        allow_public_ip_discovery,
        lan_ips,
        route_ips,
    ) {
        return Vec::new();
    }
    let mut verified = Vec::new();
    for ip in discover_default_public_ip_candidates_for_local_ips_async(
        lan_ips.iter().chain(route_ips.iter()).copied(),
    )
    .await
    {
        if is_invitable_ip(ip)
            && public_endpoint_is_reachable_for_local_ips_async(
                ip,
                port,
                lan_ips.iter().chain(route_ips.iter()).copied(),
            )
            .await
        {
            verified.push(ip);
        }
    }
    verified
}

fn daemon_advertise_host_address(host: Option<&str>, port: u16) -> Option<String> {
    let host = host?.trim();
    if host.is_empty()
        || matches!(host, "0.0.0.0" | "::")
        || host.contains("://")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_unspecified())
    {
        return None;
    }
    let address = if let Ok(socket) = host.parse::<SocketAddr>() {
        if socket.ip().is_unspecified() {
            return None;
        }
        host.to_string()
    } else if let Ok(ip) = host.parse::<IpAddr>() {
        format_ip_port(ip, port)
    } else if let Some((name, explicit_port)) = host.rsplit_once(':') {
        if !name.contains(':') && explicit_port.parse::<u16>().is_ok() {
            host.to_string()
        } else {
            format!("[{host}]:{port}")
        }
    } else {
        format!("{host}:{port}")
    };
    if is_loopback_connect_address(&address) {
        return None;
    }
    Some(address)
}

pub fn daemon_loopback_endpoint(
    node_id: &NodeId,
    address: String,
    priority: u16,
) -> ClusterEndpointRecord {
    ClusterEndpointRecord {
        endpoint_id: daemon_loopback_endpoint_id(node_id),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Loopback,
        address,
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::LocalOnly,
        priority,
        last_observed_latency_ms: None,
    }
}

pub fn daemon_lan_endpoint(
    node_id: &NodeId,
    address: String,
    priority: u16,
) -> ClusterEndpointRecord {
    let privacy = address
        .parse::<SocketAddr>()
        .ok()
        .map(|address| address.ip())
        .filter(|ip| !is_private_or_link_local_ip(*ip))
        .map(|_| ClusterEndpointPrivacy::InternetDirect)
        .unwrap_or(ClusterEndpointPrivacy::Lan);
    ClusterEndpointRecord {
        endpoint_id: daemon_lan_endpoint_id(node_id, &address),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address,
        source: ClusterEndpointSource::Observed,
        privacy,
        priority,
        last_observed_latency_ms: None,
    }
}

fn push_unique_address(addresses: &mut Vec<String>, address: String) {
    if !addresses.iter().any(|candidate| candidate == &address) {
        addresses.push(address);
    }
}
