use crate::error::NetworkError;
use crate::iroh_endpoint_public_key;
use crate::prelude::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{ED25519, UnparsedPublicKey};
use sinomo_node::NodeIdentity;
use time::format_description::well_known::Rfc3339;

pub const CLUSTER_INVITE_AUTH_PROTOCOL: &str = "sinomo.cluster-invite.v1";
pub const SINOMO_JOIN_URI_PREFIX: &str = "sinomo-join://v1/";
pub const PENDING_INVITE_CLUSTER_ID: &str = "01hxy6qa9k0t9vr4g9n8d3q5c0";

pub fn cluster_join_command(uri: &str) -> String {
    format!("sinomo cluster join \"{uri}\"")
}

pub fn encode_cluster_join_invite(invite: &ClusterJoinInvite) -> Result<String, NetworkError> {
    let mut params = Vec::new();
    push_invite_param(&mut params, "public_key", &invite.cluster_public_key);
    push_invite_param(&mut params, "invite_id", invite.invite_id.as_str());
    push_invite_param(&mut params, "secret", &invite.invitation_secret);
    for channel in &invite.communication_channels {
        push_invite_param_raw(&mut params, "channel", &encode_invite_channel(channel)?);
    }

    Ok(format!("{SINOMO_JOIN_URI_PREFIX}?{}", params.join("&")))
}

pub fn decode_cluster_join_invite(raw: &str) -> Result<ClusterJoinInvite, NetworkError> {
    if let Some(payload) = raw.strip_prefix(SINOMO_JOIN_URI_PREFIX) {
        decode_cluster_join_invite_uri(payload)
    } else if raw.trim_start().starts_with('{') {
        serde_json::from_str(raw).map_err(|source| NetworkError::Json {
            path: "cluster invite".to_string(),
            detail: source.to_string(),
        })
    } else {
        Err(NetworkError::Serialization {
            detail:
                "cluster join requires a sinomo-join://v1 invite, invite JSON, or invite file path"
                    .to_string(),
        })
    }
}

pub fn extract_invite_from_join_command(raw: &str) -> &str {
    let first_line = raw
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(raw);
    let trimmed = first_line.trim();
    let Some(rest) = trimmed.strip_prefix("sinomo cluster join ") else {
        return raw.trim();
    };
    let rest = rest.trim();
    if let Some(after_quote) = rest.strip_prefix('"')
        && let Some(end) = after_quote.find('"')
    {
        return &after_quote[..end];
    }
    rest.split_whitespace().next().unwrap_or(rest)
}

pub fn cluster_invite_signing_payload(invite: &ClusterJoinInvite) -> Result<String, NetworkError> {
    let expires_at =
        invite
            .expires_at
            .format(&Rfc3339)
            .map_err(|source| NetworkError::Serialization {
                detail: format!("invalid invite expiry timestamp: {source}"),
            })?;
    let introducers =
        serde_json::to_string(&invite.introducers).map_err(|source| NetworkError::Json {
            path: "cluster invite introducers".to_string(),
            detail: source.to_string(),
        })?;
    let channels = serde_json::to_string(&invite.communication_channels).map_err(|source| {
        NetworkError::Json {
            path: "cluster invite communication channels".to_string(),
            detail: source.to_string(),
        }
    })?;
    let allowed_roles = serde_json::to_string(&invite.allowed_member_roles).map_err(|source| {
        NetworkError::Json {
            path: "cluster invite allowed roles".to_string(),
            detail: source.to_string(),
        }
    })?;
    Ok(format!(
        "{CLUSTER_INVITE_AUTH_PROTOCOL}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        invite.invite_id,
        invite.cluster_id,
        invite.cluster_name.clone().unwrap_or_default(),
        invite.cluster_public_key,
        invite.invitation_secret,
        channels,
        introducers,
        invite.token_id,
        expires_at,
        invite.trust_domain,
        allowed_roles,
        invite.issuer_node_id,
        invite.issuer_public_key
    ))
}

pub fn sign_cluster_join_invite(
    invite: &ClusterJoinInvite,
    identity: &NodeIdentity,
) -> Result<String, NetworkError> {
    let key_pair = identity
        .key_pair()
        .map_err(|source| NetworkError::Serialization {
            detail: source.to_string(),
        })?;
    let payload = cluster_invite_signing_payload(invite)?;
    Ok(URL_SAFE_NO_PAD.encode(key_pair.sign(payload.as_bytes()).as_ref()))
}

pub fn verify_cluster_join_invite_signature(
    invite: &ClusterJoinInvite,
) -> Result<(), NetworkError> {
    if invite.signature.trim().is_empty()
        || invite.issuer_public_key.trim().is_empty()
        || invite.issuer_node_id.as_str().trim().is_empty()
    {
        return Err(NetworkError::AuthDenied);
    }
    let public_key = URL_SAFE_NO_PAD
        .decode(invite.issuer_public_key.as_bytes())
        .map_err(|_| NetworkError::AuthDenied)?;
    let signature = URL_SAFE_NO_PAD
        .decode(invite.signature.as_bytes())
        .map_err(|_| NetworkError::AuthDenied)?;
    let payload = cluster_invite_signing_payload(invite)?;
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(payload.as_bytes(), &signature)
        .map_err(|_| NetworkError::AuthDenied)
}

pub fn random_url_secret(byte_len: usize) -> Result<String, NetworkError> {
    let rng = SystemRandom::new();
    let mut bytes = vec![0_u8; byte_len];
    rng.fill(&mut bytes)
        .map_err(|_| NetworkError::Serialization {
            detail: "failed to generate secure random URL secret".to_string(),
        })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub fn invite_has_embedded_cluster_identity(invite: &ClusterJoinInvite) -> bool {
    invite.cluster_id.as_str() != PENDING_INVITE_CLUSTER_ID
}

pub fn invite_has_embedded_signature(invite: &ClusterJoinInvite) -> bool {
    !invite.signature.trim().is_empty()
}

pub fn invite_communication_channels(
    state: &ClusterBootstrapState,
    endpoints: &[ClusterEndpointRecord],
) -> Vec<ClusterInvitationChannel> {
    endpoints
        .iter()
        .filter_map(|endpoint| {
            let member = state.members.iter().find(|member| {
                member.node_id == endpoint.node_id && member.state.is_authorized()
            })?;
            Some(ClusterInvitationChannel {
                endpoint: endpoint.clone(),
                node_public_key: member.node_public_key.clone(),
            })
        })
        .collect()
}

pub fn invite_channels(invite: &ClusterJoinInvite) -> Vec<ClusterInvitationChannel> {
    if !invite.communication_channels.is_empty() {
        return invite.communication_channels.clone();
    }
    invite
        .introducers
        .iter()
        .cloned()
        .map(|endpoint| ClusterInvitationChannel {
            endpoint,
            node_public_key: invite.issuer_public_key.clone(),
        })
        .collect()
}

pub fn invite_channels_for_join_registration(
    invite: &ClusterJoinInvite,
) -> Vec<ClusterInvitationChannel> {
    let channels = invite_channels(invite);
    if channels
        .iter()
        .any(|channel| is_remote_join_registration_endpoint(&channel.endpoint))
    {
        channels
            .into_iter()
            .filter(|channel| is_remote_join_registration_endpoint(&channel.endpoint))
            .collect()
    } else {
        channels
    }
}

pub fn is_remote_join_registration_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(endpoint.transport, ClusterTransportKind::Iroh)
        || is_remote_usable_invite_endpoint(endpoint)
}

pub fn push_unique_introducer(
    introducers: &mut Vec<ClusterEndpointRecord>,
    endpoint: ClusterEndpointRecord,
) {
    if let Some(public_key) = iroh_endpoint_public_key(&endpoint)
        && introducers
            .iter()
            .any(|candidate| iroh_endpoint_public_key(candidate).as_deref() == Some(&public_key))
    {
        return;
    }
    if !introducers
        .iter()
        .any(|candidate| candidate.address == endpoint.address)
    {
        introducers.push(endpoint);
    }
}

pub fn sort_default_invite_introducers(introducers: &mut [ClusterEndpointRecord]) {
    introducers.sort_by_key(|endpoint| {
        (
            !is_remote_usable_invite_endpoint(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
}

pub fn compact_default_invite_introducers(introducers: &mut Vec<ClusterEndpointRecord>) {
    let remote_direct = introducers
        .iter()
        .filter(|endpoint| {
            is_remote_usable_invite_endpoint(endpoint)
                && !matches!(endpoint.transport, ClusterTransportKind::Iroh)
        })
        .take(2)
        .cloned()
        .collect::<Vec<_>>();
    let remote_iroh = introducers
        .iter()
        .find(|endpoint| {
            is_remote_usable_invite_endpoint(endpoint)
                && matches!(endpoint.transport, ClusterTransportKind::Iroh)
        })
        .cloned();
    if !remote_direct.is_empty() {
        *introducers = remote_direct;
        if let Some(iroh) = remote_iroh {
            push_unique_introducer(introducers, iroh);
        }
        return;
    }

    if let Some(iroh) = remote_iroh {
        *introducers = vec![iroh];
        return;
    }

    introducers.truncate(1);
}

pub fn is_remote_usable_invite_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    !matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly)
        && !is_loopback_connect_address(&endpoint.address)
}

pub fn is_loopback_connect_address(address: &str) -> bool {
    address.starts_with("127.")
        || address.starts_with("localhost:")
        || address.starts_with("[::1]:")
        || address.starts_with("::1:")
}

pub fn invite_introducer_endpoint(node_id: &NodeId, address: String) -> ClusterEndpointRecord {
    let is_loopback = is_loopback_connect_address(&address);
    ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::generate(),
        node_id: node_id.clone(),
        transport: if is_loopback {
            ClusterTransportKind::Loopback
        } else {
            ClusterTransportKind::LanTcp
        },
        address,
        source: ClusterEndpointSource::Static,
        privacy: if is_loopback {
            ClusterEndpointPrivacy::LocalOnly
        } else {
            ClusterEndpointPrivacy::Lan
        },
        priority: if is_loopback { 100 } else { 20 },
        last_observed_latency_ms: None,
    }
}

pub fn invite_introducer_endpoint_from_state(
    state: &ClusterBootstrapState,
    address: String,
) -> ClusterEndpointRecord {
    if let Some(endpoint) = state
        .members
        .iter()
        .filter(|member| member.state.is_authorized())
        .flat_map(|member| member.endpoints.iter())
        .find(|endpoint| endpoint.address == address)
    {
        return endpoint.clone();
    }
    invite_introducer_endpoint(&state.local_node_id, address)
}

fn push_invite_param(params: &mut Vec<String>, key: &str, value: &str) {
    params.push(format!("{key}={}", percent_encode_invite_value(value)));
}

fn push_invite_param_raw(params: &mut Vec<String>, key: &str, value: &str) {
    params.push(format!("{key}={value}"));
}

fn decode_cluster_join_invite_uri(payload: &str) -> Result<ClusterJoinInvite, NetworkError> {
    let params = parse_invite_query(payload.strip_prefix('?').unwrap_or(payload))?;
    let invite_id = ClusterInviteId::deserialize_migrating_legacy(&required_invite_param(
        &params,
        "invite_id",
    )?)
    .map_err(|source| NetworkError::Serialization {
        detail: format!("invalid invite_id: {source}"),
    })?;
    let cluster_id = ClusterId::deserialize_migrating_legacy(
        &optional_invite_param(&params, "cluster_id")?
            .unwrap_or_else(|| PENDING_INVITE_CLUSTER_ID.to_string()),
    )
    .map_err(|source| NetworkError::Serialization {
        detail: format!("invalid cluster_id: {source}"),
    })?;
    let cluster_name =
        optional_invite_param(&params, "cluster_name")?.filter(|value| !value.is_empty());
    let expires_at = optional_invite_param(&params, "expires_at")?
        .map(|value| {
            OffsetDateTime::parse(&value, &Rfc3339).map_err(|source| NetworkError::Serialization {
                detail: format!("invalid invite expires_at: {source}"),
            })
        })
        .transpose()?
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    let trust_domain = TrustDomain::new(
        &optional_invite_param(&params, "trust_domain")?
            .unwrap_or_else(|| "trust.local".to_string()),
    )
    .map_err(|source| NetworkError::Serialization {
        detail: format!("invalid trust_domain: {source}"),
    })?;
    let allowed_member_roles = params
        .iter()
        .filter(|(key, _)| key == "role")
        .map(|(_, value)| {
            percent_decode_invite_value(value)
                .and_then(|value| parse_query_enum::<ClusterMemberRole>(&value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let issuer_node_id = if let Some(value) = optional_invite_param(&params, "issuer_node_id")? {
        NodeId::deserialize_migrating_legacy(&value).map_err(|source| {
            NetworkError::Serialization {
                detail: format!("invalid issuer_node_id: {source}"),
            }
        })?
    } else {
        pending_invite_node_id()
    };
    let communication_channels = params
        .iter()
        .filter(|(key, _)| key == "channel")
        .map(|(_, value)| decode_invite_channel(value))
        .collect::<Result<Vec<_>, _>>()?;
    let introducers = params
        .iter()
        .filter(|(key, _)| key == "introducer")
        .map(|(_, value)| decode_invite_endpoint(value))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ClusterJoinInvite {
        invite_id,
        cluster_id,
        cluster_name,
        cluster_public_key: required_invite_param_any(
            &params,
            &["public_key", "cluster_public_key"],
        )?,
        invitation_secret: required_invite_param_any(&params, &["secret", "invitation_secret"])?,
        communication_channels,
        introducers,
        token_id: optional_invite_param(&params, "token_id")?.unwrap_or_default(),
        expires_at,
        trust_domain,
        allowed_member_roles,
        issuer_node_id,
        issuer_public_key: optional_invite_param(&params, "issuer_public_key")?.unwrap_or_default(),
        signature: optional_invite_param(&params, "signature")?.unwrap_or_default(),
    })
}

fn required_invite_param(params: &[(String, String)], key: &str) -> Result<String, NetworkError> {
    params
        .iter()
        .find(|(candidate, _)| candidate == key)
        .map(|(_, value)| percent_decode_invite_value(value))
        .transpose()?
        .ok_or_else(|| NetworkError::Serialization {
            detail: format!("cluster invite is missing `{key}`"),
        })
}

fn required_invite_param_any(
    params: &[(String, String)],
    keys: &[&str],
) -> Result<String, NetworkError> {
    for key in keys {
        if let Some(value) = optional_invite_param(params, key)? {
            return Ok(value);
        }
    }
    Err(NetworkError::Serialization {
        detail: format!("cluster invite is missing `{}`", keys.join("` or `")),
    })
}

fn pending_invite_node_id() -> NodeId {
    NodeId::from_legacy_named("node_pending_invite").expect("static pending invite node id")
}

fn optional_invite_param(
    params: &[(String, String)],
    key: &str,
) -> Result<Option<String>, NetworkError> {
    params
        .iter()
        .find(|(candidate, _)| candidate == key)
        .map(|(_, value)| percent_decode_invite_value(value))
        .transpose()
}

fn parse_invite_query(payload: &str) -> Result<Vec<(String, String)>, NetworkError> {
    if payload.trim().is_empty() {
        return Err(NetworkError::Serialization {
            detail: "cluster invite URI has an empty query".to_string(),
        });
    }
    payload
        .split('&')
        .map(|part| {
            let (key, value) = part
                .split_once('=')
                .ok_or_else(|| NetworkError::Serialization {
                    detail: format!("cluster invite query parameter `{part}` has no value"),
                })?;
            Ok((percent_decode_invite_value(key)?, value.to_string()))
        })
        .collect()
}

fn encode_invite_channel(channel: &ClusterInvitationChannel) -> Result<String, NetworkError> {
    Ok(join_invite_parts(&[
        query_enum(&channel.endpoint.transport)?,
        channel.endpoint.address.clone(),
        channel.node_public_key.clone(),
    ]))
}

fn decode_invite_channel(value: &str) -> Result<ClusterInvitationChannel, NetworkError> {
    let parts = split_invite_parts(value)?;
    if parts.len() == 3 {
        return Ok(ClusterInvitationChannel {
            endpoint: ClusterEndpointRecord {
                endpoint_id: ClusterEndpointId::generate(),
                node_id: NodeId::from_legacy_named("node_invite_channel")
                    .expect("static invite channel node id"),
                transport: parse_query_enum(&parts[0])?,
                address: parts[1].clone(),
                source: ClusterEndpointSource::Static,
                privacy: ClusterEndpointPrivacy::InternetDirect,
                priority: 0,
                last_observed_latency_ms: None,
            },
            node_public_key: parts[2].clone(),
        });
    }
    if parts.len() == 4 {
        let node_id = NodeId::new(&parts[0]).map_err(|source| NetworkError::Serialization {
            detail: format!("invalid invite channel node_id `{}`: {source}", parts[0]),
        })?;
        return Ok(ClusterInvitationChannel {
            endpoint: ClusterEndpointRecord {
                endpoint_id: ClusterEndpointId::generate(),
                node_id,
                transport: parse_query_enum(&parts[1])?,
                address: parts[2].clone(),
                source: ClusterEndpointSource::Static,
                privacy: ClusterEndpointPrivacy::InternetDirect,
                priority: 0,
                last_observed_latency_ms: None,
            },
            node_public_key: parts[3].clone(),
        });
    }
    if parts.len() != 9 {
        return Err(NetworkError::Serialization {
            detail: format!(
                "cluster invite channel expected 3 fields, found {}",
                parts.len()
            ),
        });
    }
    Ok(ClusterInvitationChannel {
        endpoint: decode_invite_endpoint_parts(&parts[..8])?,
        node_public_key: parts[8].clone(),
    })
}

fn decode_invite_endpoint(value: &str) -> Result<ClusterEndpointRecord, NetworkError> {
    let parts = split_invite_parts(value)?;
    if parts.len() != 8 {
        return Err(NetworkError::Serialization {
            detail: format!(
                "cluster invite endpoint expected 8 fields, found {}",
                parts.len()
            ),
        });
    }
    decode_invite_endpoint_parts(&parts)
}

fn decode_invite_endpoint_parts(parts: &[String]) -> Result<ClusterEndpointRecord, NetworkError> {
    let priority = parts[6]
        .parse::<u16>()
        .map_err(|source| NetworkError::Serialization {
            detail: format!("invalid invite endpoint priority `{}`: {source}", parts[6]),
        })?;
    let last_observed_latency_ms = if parts[7].is_empty() {
        None
    } else {
        Some(
            parts[7]
                .parse::<u32>()
                .map_err(|source| NetworkError::Serialization {
                    detail: format!("invalid invite endpoint latency `{}`: {source}", parts[7]),
                })?,
        )
    };
    Ok(ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::new(&parts[0]).map_err(|source| {
            NetworkError::Serialization {
                detail: format!("invalid invite endpoint_id `{}`: {source}", parts[0]),
            }
        })?,
        node_id: NodeId::new(&parts[1]).map_err(|source| NetworkError::Serialization {
            detail: format!("invalid invite endpoint node_id `{}`: {source}", parts[1]),
        })?,
        transport: parse_query_enum(&parts[2])?,
        address: parts[3].clone(),
        source: parse_query_enum(&parts[4])?,
        privacy: parse_query_enum(&parts[5])?,
        priority,
        last_observed_latency_ms,
    })
}

fn query_enum<T: Serialize>(value: &T) -> Result<String, NetworkError> {
    serde_json::to_string(value)
        .map(|value| value.trim_matches('"').to_string())
        .map_err(|source| NetworkError::Json {
            path: "cluster invite enum".to_string(),
            detail: source.to_string(),
        })
}

fn parse_query_enum<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, NetworkError> {
    serde_json::from_str(&format!("\"{value}\"")).map_err(|source| NetworkError::Serialization {
        detail: format!("invalid cluster invite enum `{value}`: {source}"),
    })
}

fn join_invite_parts(parts: &[String]) -> String {
    parts
        .iter()
        .map(|part| percent_encode_invite_value(part))
        .collect::<Vec<_>>()
        .join(",")
}

fn split_invite_parts(value: &str) -> Result<Vec<String>, NetworkError> {
    value
        .split(',')
        .map(percent_decode_invite_value)
        .collect::<Result<Vec<_>, _>>()
}

fn percent_encode_invite_value(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b':' | b'@') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn percent_decode_invite_value(value: &str) -> Result<String, NetworkError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(NetworkError::Serialization {
                    detail: format!("invalid percent escape in cluster invite value `{value}`"),
                });
            }
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).map_err(|source| {
                NetworkError::Serialization {
                    detail: format!("invalid percent escape in cluster invite value: {source}"),
                }
            })?;
            decoded.push(u8::from_str_radix(hex, 16).map_err(|source| {
                NetworkError::Serialization {
                    detail: format!("invalid percent escape `%{hex}`: {source}"),
                }
            })?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|source| NetworkError::Serialization {
        detail: format!("cluster invite value is not valid UTF-8: {source}"),
    })
}
