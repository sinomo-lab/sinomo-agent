use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

/// Selects the replacement main listener without accidentally narrowing an
/// existing remotely reachable bind. An explicit operator override wins.
pub fn select_rollout_main_host(
    current_host: Option<&str>,
    supervised_main_host: Option<&str>,
    requested_host: &str,
    requested_host_override: Option<&str>,
) -> String {
    requested_host_override
        .or(current_host)
        .or(supervised_main_host)
        .unwrap_or(requested_host)
        .to_string()
}

/// Selects the replacement shadow listener, preserving its current bind when
/// possible and otherwise following the candidate main listener.
pub fn select_rollout_shadow_host(
    current_shadow_host: Option<&str>,
    candidate_main_host: &str,
    requested_host_override: Option<&str>,
) -> String {
    requested_host_override
        .or(current_shadow_host)
        .unwrap_or(candidate_main_host)
        .to_string()
}

/// Returns whether a rollout listener bind can serve one normalized manual
/// TCP endpoint. Wildcard binds require address-family proof for hostnames.
pub fn rollout_bind_covers_manual_tcp_endpoint(
    bind_host: &str,
    bind_port: u16,
    endpoint_address: &str,
) -> bool {
    rollout_bind_covers_manual_tcp_endpoint_with(
        bind_host,
        bind_port,
        endpoint_address,
        |host, port| {
            (host, port)
                .to_socket_addrs()
                .map(|addresses| addresses.collect())
                .unwrap_or_default()
        },
    )
}

fn rollout_bind_covers_manual_tcp_endpoint_with<R>(
    bind_host: &str,
    bind_port: u16,
    endpoint_address: &str,
    mut resolve_host: R,
) -> bool
where
    R: FnMut(&str, u16) -> Vec<SocketAddr>,
{
    let endpoint_port = endpoint_address
        .parse::<SocketAddr>()
        .ok()
        .map(|socket| socket.port())
        .or_else(|| {
            endpoint_address
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>().ok())
        });
    if endpoint_port != Some(bind_port) {
        return false;
    }

    let bind_host = bind_host
        .trim()
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or_else(|| bind_host.trim());
    if let Ok(bind_ip) = bind_host.parse::<IpAddr>() {
        if bind_ip.is_unspecified() {
            if let Ok(endpoint) = endpoint_address.parse::<SocketAddr>() {
                return bind_ip.is_ipv4() == endpoint.ip().is_ipv4();
            }
            let Some(endpoint_host) = manual_endpoint_host(endpoint_address) else {
                return false;
            };
            let resolved = resolve_host(endpoint_host, bind_port);
            return !resolved.is_empty()
                && resolved.into_iter().all(|endpoint| {
                    endpoint.port() == bind_port && bind_ip.is_ipv4() == endpoint.ip().is_ipv4()
                });
        }
        return endpoint_address
            .parse::<SocketAddr>()
            .is_ok_and(|endpoint| endpoint.ip() == bind_ip);
    }
    manual_endpoint_host(endpoint_address)
        .is_some_and(|endpoint_host| endpoint_host.eq_ignore_ascii_case(bind_host))
}

fn manual_endpoint_host(address: &str) -> Option<&str> {
    if let Some(rest) = address.strip_prefix('[') {
        return rest.split_once("]:").map(|(host, _)| host);
    }
    address.rsplit_once(':').map(|(host, _)| host)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_selection_preserves_existing_remote_listeners() {
        assert_eq!(
            select_rollout_main_host(Some("10.14.1.11"), None, "127.0.0.1", None),
            "10.14.1.11"
        );
        assert_eq!(
            select_rollout_shadow_host(Some("10.14.1.12"), "10.14.1.11", None),
            "10.14.1.12"
        );
    }

    #[test]
    fn explicit_host_override_wins_for_main_and_shadow() {
        assert_eq!(
            select_rollout_main_host(
                Some("10.14.1.11"),
                Some("10.14.1.12"),
                "127.0.0.1",
                Some("0.0.0.0"),
            ),
            "0.0.0.0"
        );
        assert_eq!(
            select_rollout_shadow_host(Some("10.14.1.12"), "10.14.1.11", Some("0.0.0.0")),
            "0.0.0.0"
        );
    }

    #[test]
    fn wildcard_ipv4_bind_requires_ipv4_hostname_resolution_proof() {
        let resolve = |host: &str, port: u16| match host {
            "v4.rollout.test" => vec![SocketAddr::from(([192, 0, 2, 41], port))],
            "v6.rollout.test" => vec![SocketAddr::from((
                "2001:db8::41".parse::<std::net::Ipv6Addr>().expect("IPv6"),
                port,
            ))],
            "dual.rollout.test" => vec![
                SocketAddr::from(([192, 0, 2, 43], port)),
                SocketAddr::from((
                    "2001:db8::43".parse::<std::net::Ipv6Addr>().expect("IPv6"),
                    port,
                )),
            ],
            _ => Vec::new(),
        };

        assert!(rollout_bind_covers_manual_tcp_endpoint_with(
            "0.0.0.0",
            21_353,
            "v4.rollout.test:21353",
            resolve,
        ));
        assert!(!rollout_bind_covers_manual_tcp_endpoint_with(
            "0.0.0.0",
            21_353,
            "v6.rollout.test:21353",
            resolve,
        ));
        assert!(!rollout_bind_covers_manual_tcp_endpoint_with(
            "0.0.0.0",
            21_353,
            "unresolved.rollout.test:21353",
            resolve,
        ));
        assert!(!rollout_bind_covers_manual_tcp_endpoint_with(
            "0.0.0.0",
            21_353,
            "dual.rollout.test:21353",
            resolve,
        ));
    }

    #[test]
    fn wildcard_ipv6_bind_requires_ipv6_hostname_resolution_proof() {
        let resolve = |host: &str, port: u16| match host {
            "v4.rollout.test" => vec![SocketAddr::from(([192, 0, 2, 42], port))],
            "v6.rollout.test" => vec![SocketAddr::from((
                "2001:db8::42".parse::<std::net::Ipv6Addr>().expect("IPv6"),
                port,
            ))],
            "dual.rollout.test" => vec![
                SocketAddr::from(([192, 0, 2, 44], port)),
                SocketAddr::from((
                    "2001:db8::44".parse::<std::net::Ipv6Addr>().expect("IPv6"),
                    port,
                )),
            ],
            _ => Vec::new(),
        };

        assert!(rollout_bind_covers_manual_tcp_endpoint_with(
            "::",
            21_353,
            "v6.rollout.test:21353",
            resolve,
        ));
        assert!(!rollout_bind_covers_manual_tcp_endpoint_with(
            "::",
            21_353,
            "v4.rollout.test:21353",
            resolve,
        ));
        assert!(!rollout_bind_covers_manual_tcp_endpoint_with(
            "::",
            21_353,
            "unresolved.rollout.test:21353",
            resolve,
        ));
        assert!(!rollout_bind_covers_manual_tcp_endpoint_with(
            "::",
            21_353,
            "dual.rollout.test:21353",
            resolve,
        ));
    }
}
