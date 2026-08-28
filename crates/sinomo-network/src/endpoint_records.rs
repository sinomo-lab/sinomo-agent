use crate::prelude::*;
use sinomo_node::{cluster_endpoint_stable_eq, cluster_has_remote_peers};

/// True for an endpoint the local node learned over the LAN bridge rather
/// than from a member advertisement: a TCP-ish observed endpoint whose
/// address is a host:port string that does not parse as a socket address.
pub(crate) fn locally_learned_bridge_route_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(
        endpoint.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
    ) && endpoint.address.parse::<SocketAddr>().is_err()
        && endpoint.address.contains(':')
        && matches!(endpoint.source, ClusterEndpointSource::Observed)
        && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
}

/// Whether the local node still needs to run join registration: it either has
/// no member record yet, or its record is an unauthorized learner.
pub fn cluster_state_needs_join_registration(state: &ClusterBootstrapState) -> bool {
    state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .is_none_or(|member| {
            matches!(member.role, ClusterMemberRole::Learner) && !member.state.is_authorized()
        })
}

/// A settled solo cluster: no remote peers and no pending join registration.
pub fn cluster_is_settled_solo(state: &ClusterBootstrapState) -> bool {
    !cluster_has_remote_peers(state) && !cluster_state_needs_join_registration(state)
}

pub fn apply_cluster_endpoint_advertisement(
    state: &mut ClusterBootstrapState,
    endpoint: ClusterEndpointRecord,
    persisted_endpoints: Option<&[ClusterEndpointRecord]>,
    timestamp: OffsetDateTime,
) -> Option<ClusterMemberRecord> {
    let member = state
        .members
        .iter_mut()
        .find(|member| member.node_id == endpoint.node_id)?;
    upsert_member_endpoint(member, endpoint);
    if persisted_endpoints == Some(member.endpoints.as_slice()) {
        return None;
    }
    state.updated_at = timestamp;
    Some(member.clone())
}

pub fn cleanup_cluster_endpoint_duplicates(state: &mut ClusterBootstrapState) -> bool {
    let mut changed = false;
    for member in &mut state.members {
        changed |= cleanup_member_endpoint_duplicates(member);
    }
    changed
}

pub fn cleanup_member_endpoint_duplicates(member: &mut ClusterMemberRecord) -> bool {
    let original = member.endpoints.clone();
    let mut endpoints: Vec<ClusterEndpointRecord> = Vec::new();
    for endpoint in original {
        if let Some(index) = endpoints
            .iter()
            .position(|candidate| cluster_endpoint_duplicate_identity_eq(candidate, &endpoint))
        {
            if cluster_endpoint_duplicate_replacement_is_preferred(&endpoints[index], &endpoint) {
                endpoints[index] = endpoint;
            }
            continue;
        }
        endpoints.push(endpoint);
    }
    if member.endpoints == endpoints {
        false
    } else {
        member.endpoints = endpoints;
        true
    }
}

pub fn upsert_member_endpoint(
    member: &mut ClusterMemberRecord,
    endpoint: ClusterEndpointRecord,
) -> bool {
    let changed = false;
    if let Some(existing) = member
        .endpoints
        .iter_mut()
        .find(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
    {
        if *existing == endpoint {
            changed
        } else if let Some(prefer_incoming) =
            cluster_endpoint_iroh_relay_replacement_preference(existing, &endpoint)
        {
            if prefer_incoming {
                *existing = endpoint;
                true
            } else {
                changed
            }
        } else if cluster_endpoint_stable_eq(existing, &endpoint) {
            changed
        } else if cluster_endpoint_route_differs_only_by_discovery_source(existing, &endpoint) {
            if cluster_endpoint_static_source_is_preferred(existing, &endpoint) {
                *existing = endpoint;
                true
            } else {
                changed
            }
        } else {
            *existing = endpoint;
            true
        }
    } else if let Some(existing) = member
        .endpoints
        .iter_mut()
        .find(|candidate| cluster_endpoint_duplicate_identity_eq(candidate, &endpoint))
    {
        if cluster_endpoint_duplicate_replacement_is_preferred(existing, &endpoint) {
            *existing = endpoint;
            true
        } else {
            changed
        }
    } else {
        member.endpoints.push(endpoint);
        true
    }
}

fn cluster_endpoint_route_differs_only_by_discovery_source(
    existing: &ClusterEndpointRecord,
    incoming: &ClusterEndpointRecord,
) -> bool {
    existing.endpoint_id == incoming.endpoint_id
        && existing.node_id == incoming.node_id
        && existing.transport == incoming.transport
        && existing.address == incoming.address
        && existing.privacy == incoming.privacy
        && existing.priority == incoming.priority
        && existing.last_observed_latency_ms == incoming.last_observed_latency_ms
        && matches!(
            (&existing.source, &incoming.source),
            (
                ClusterEndpointSource::Static,
                ClusterEndpointSource::Mdns | ClusterEndpointSource::Observed
            ) | (
                ClusterEndpointSource::Mdns | ClusterEndpointSource::Observed,
                ClusterEndpointSource::Static
            )
        )
}

fn cluster_endpoint_static_source_is_preferred(
    existing: &ClusterEndpointRecord,
    incoming: &ClusterEndpointRecord,
) -> bool {
    !matches!(existing.source, ClusterEndpointSource::Static)
        && matches!(incoming.source, ClusterEndpointSource::Static)
}

pub fn cluster_endpoint_duplicate_identity_eq(
    left: &ClusterEndpointRecord,
    right: &ClusterEndpointRecord,
) -> bool {
    if left.node_id != right.node_id {
        return false;
    }
    left.endpoint_id == right.endpoint_id
        || (left.transport == right.transport && left.address == right.address)
        || cluster_endpoint_module_url_identity(left)
            .zip(cluster_endpoint_module_url_identity(right))
            .is_some_and(|(left, right)| left == right)
}

fn cluster_endpoint_duplicate_replacement_is_preferred(
    existing: &ClusterEndpointRecord,
    incoming: &ClusterEndpointRecord,
) -> bool {
    if let Some(prefer_incoming) =
        cluster_endpoint_iroh_relay_replacement_preference(existing, incoming)
    {
        return prefer_incoming;
    }
    match (
        cluster_endpoint_module_url_identity(existing),
        cluster_endpoint_module_url_identity(incoming),
    ) {
        (Some(existing_identity), Some(incoming_identity))
            if existing_identity == incoming_identity =>
        {
            cluster_endpoint_module_url_rank(incoming) < cluster_endpoint_module_url_rank(existing)
        }
        _ => false,
    }
}

/// Resolves only the legacy Iroh key-only versus relay-bearing case. A live
/// relay route is usable outside the endpoint's private network, while the
/// key-only form is retained cluster state that the Iroh module no longer
/// advertises. Relay-to-relay changes deliberately have no preference here so
/// the ordinary same-id upsert path can install relay rotations.
fn cluster_endpoint_iroh_relay_replacement_preference(
    existing: &ClusterEndpointRecord,
    incoming: &ClusterEndpointRecord,
) -> Option<bool> {
    if existing.transport != ClusterTransportKind::Iroh
        || incoming.transport != ClusterTransportKind::Iroh
    {
        return None;
    }
    let existing_identity = cluster_endpoint_module_url_identity(existing)?;
    let incoming_identity = cluster_endpoint_module_url_identity(incoming)?;
    if existing_identity != incoming_identity || !existing_identity.starts_with("iroh://") {
        return None;
    }
    match (
        cluster_endpoint_iroh_has_relay(existing),
        cluster_endpoint_iroh_has_relay(incoming),
    ) {
        (true, false) => Some(false),
        (false, true) => Some(true),
        _ => None,
    }
}

fn cluster_endpoint_iroh_has_relay(endpoint: &ClusterEndpointRecord) -> bool {
    let Some((_, query)) = endpoint.address.split_once('?') else {
        return false;
    };
    let query = query
        .split_once('#')
        .map_or(query, |(query, _fragment)| query);
    query.split('&').any(|part| {
        part.strip_prefix("relay=")
            .is_some_and(|relay| !relay.trim().is_empty())
    })
}

pub fn cluster_endpoint_module_url_identity(endpoint: &ClusterEndpointRecord) -> Option<String> {
    let (scheme, rest) = endpoint.address.split_once("://")?;
    let authority = rest
        .split(['?', '#'])
        .next()
        .filter(|authority| !authority.is_empty())?;
    Some(format!(
        "{}://{}",
        scheme.trim().to_ascii_lowercase(),
        authority
    ))
}

fn cluster_endpoint_module_url_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    if endpoint.address.contains('?') || endpoint.address.contains('#') {
        1
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_member(node_id: &NodeId) -> ClusterMemberRecord {
        ClusterMemberRecord::local_voter(
            ClusterId::from_legacy_named("cluster_endpoint_relay_preference").expect("cluster"),
            node_id.clone(),
            "relay preference",
            "node-public-key",
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    fn test_iroh_endpoint(
        endpoint_id: ClusterEndpointId,
        node_id: &NodeId,
        address: &str,
    ) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id,
            node_id: node_id.clone(),
            transport: ClusterTransportKind::Iroh,
            address: address.to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: if address.contains("?relay=") {
                ClusterEndpointPrivacy::Relayed
            } else {
                ClusterEndpointPrivacy::InternetDirect
            },
            priority: 90,
            last_observed_latency_ms: None,
        }
    }

    fn test_non_iroh_module_endpoint(
        endpoint_id: &str,
        node_id: &NodeId,
        address: &str,
    ) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(endpoint_id).expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::HttpsRelay,
            address: address.to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 90,
            last_observed_latency_ms: None,
        }
    }

    #[test]
    fn same_id_iroh_relay_survives_incoming_key_only_record() {
        let node_id = NodeId::from_legacy_named("node_iroh_relay_preserve").expect("node");
        let endpoint_id =
            ClusterEndpointId::from_legacy_named("endpoint_iroh_relay_preserve").expect("endpoint");
        let relay = test_iroh_endpoint(
            endpoint_id.clone(),
            &node_id,
            "iroh://public-key?relay=https://relay-a.example/",
        );
        let key_only = test_iroh_endpoint(endpoint_id, &node_id, "iroh://public-key");
        let mut member = test_member(&node_id);
        member.endpoints.push(relay.clone());

        assert!(!upsert_member_endpoint(&mut member, key_only));
        assert_eq!(member.endpoints, vec![relay]);
    }

    #[test]
    fn same_id_iroh_relay_replaces_key_only_record() {
        let node_id = NodeId::from_legacy_named("node_iroh_relay_upgrade").expect("node");
        let endpoint_id =
            ClusterEndpointId::from_legacy_named("endpoint_iroh_relay_upgrade").expect("endpoint");
        let key_only = test_iroh_endpoint(endpoint_id.clone(), &node_id, "iroh://public-key");
        let relay = test_iroh_endpoint(
            endpoint_id,
            &node_id,
            "iroh://public-key?relay=https://relay-a.example/",
        );
        let mut member = test_member(&node_id);
        member.endpoints.push(key_only);

        assert!(upsert_member_endpoint(&mut member, relay.clone()));
        assert_eq!(member.endpoints, vec![relay]);
    }

    #[test]
    fn same_id_iroh_relay_rotation_uses_incoming_record() {
        let node_id = NodeId::from_legacy_named("node_iroh_relay_rotation").expect("node");
        let endpoint_id =
            ClusterEndpointId::from_legacy_named("endpoint_iroh_relay_rotation").expect("endpoint");
        let relay_a = test_iroh_endpoint(
            endpoint_id.clone(),
            &node_id,
            "iroh://public-key?relay=https://relay-a.example/",
        );
        let relay_b = test_iroh_endpoint(
            endpoint_id,
            &node_id,
            "iroh://public-key?relay=https://relay-b.example/",
        );
        let mut member = test_member(&node_id);
        member.endpoints.push(relay_a);

        assert!(upsert_member_endpoint(&mut member, relay_b.clone()));
        assert_eq!(member.endpoints, vec![relay_b]);
    }

    #[test]
    fn duplicate_iroh_ids_collapse_to_relay_in_either_order() {
        let node_id = NodeId::from_legacy_named("node_iroh_relay_cleanup").expect("node");
        let key_only = test_iroh_endpoint(
            ClusterEndpointId::from_legacy_named("endpoint_iroh_key_only").expect("endpoint"),
            &node_id,
            "iroh://public-key",
        );
        let relay = test_iroh_endpoint(
            ClusterEndpointId::from_legacy_named("endpoint_iroh_relay").expect("endpoint"),
            &node_id,
            "iroh://public-key?relay=https://relay-a.example/",
        );

        for endpoints in [
            vec![key_only.clone(), relay.clone()],
            vec![relay.clone(), key_only.clone()],
        ] {
            let mut member = test_member(&node_id);
            member.endpoints = endpoints;

            assert!(cleanup_member_endpoint_duplicates(&mut member));
            assert_eq!(member.endpoints, vec![relay.clone()]);
        }
    }

    #[test]
    fn iroh_relay_query_after_direct_address_and_before_fragment_is_detected() {
        let node_id = NodeId::from_legacy_named("node_iroh_relay_query_parser").expect("node");
        let endpoint = test_iroh_endpoint(
            ClusterEndpointId::from_legacy_named("endpoint_iroh_relay_query_parser")
                .expect("endpoint"),
            &node_id,
            "iroh://public-key?direct=10.14.5.15:21353&relay=https://relay.example/#metadata",
        );

        assert!(cluster_endpoint_iroh_has_relay(&endpoint));
    }

    #[test]
    fn non_iroh_module_urls_still_prefer_bare_record() {
        let node_id = NodeId::from_legacy_named("node_non_iroh_module_cleanup").expect("node");
        let qualified = test_non_iroh_module_endpoint(
            "endpoint_non_iroh_qualified",
            &node_id,
            "module://public-key?route=relay",
        );
        let bare = test_non_iroh_module_endpoint(
            "endpoint_non_iroh_bare",
            &node_id,
            "module://public-key",
        );

        for endpoints in [
            vec![qualified.clone(), bare.clone()],
            vec![bare.clone(), qualified.clone()],
        ] {
            let mut member = test_member(&node_id);
            member.endpoints = endpoints;

            assert!(cleanup_member_endpoint_duplicates(&mut member));
            assert_eq!(member.endpoints, vec![bare.clone()]);
        }
    }
}
