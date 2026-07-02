use crate::invite_publish::read_cluster_invitation_from_kv;
use crate::kv_local_store::kv_err;
use crate::prelude::*;
use crate::read_cluster_state;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{ED25519, UnparsedPublicKey};
use sinomo_network::NetworkError;
#[allow(unused_imports)]
pub use sinomo_network::is_loopback_connect_address;
use sinomo_network::{ClusterPaths, iroh_endpoint_public_key, stable_iroh_endpoint_id};
use sinomo_node::NodeIdentity;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const CLUSTER_INVITE_AUTH_PROTOCOL: &str = "sinomo.cluster-invite.v1";
pub const CLUSTER_INVITE_CHALLENGE_PROTOCOL: &str = "sinomo.cluster-invite-challenge.v1";
pub const CLUSTER_INVITE_CHALLENGE_MAX_CLOCK_SKEW_SECONDS: i64 = 300;
pub const SINOMO_JOIN_URI_PREFIX: &str = "sinomo-join://v1/";
pub const PENDING_INVITE_CLUSTER_ID: &str = "01hxy6qa9k0t9vr4g9n8d3q5c0";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterInviteChallengeRequest {
    pub protocol: String,
    pub invite_id: ClusterInviteId,
    pub challenge: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterInviteChallengeProof {
    pub protocol: String,
    pub invite_id: ClusterInviteId,
    pub cluster_id: ClusterId,
    pub responder_node_id: String,
    pub responder_public_key: String,
    pub challenge: String,
    pub timestamp: String,
    pub signature: String,
}

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

pub fn cluster_invite_challenge_signing_payload(
    invite_id: &ClusterInviteId,
    cluster_id: &ClusterId,
    responder_node_id: &str,
    responder_public_key: &str,
    challenge: &str,
    timestamp: &str,
) -> String {
    format!(
        "{CLUSTER_INVITE_CHALLENGE_PROTOCOL}\n{invite_id}\n{cluster_id}\n{responder_node_id}\n{responder_public_key}\n{challenge}\n{timestamp}"
    )
}

pub fn verify_cluster_invite_challenge_proof(
    invite: &ClusterJoinInvite,
    expected_challenge: &str,
    proof: &ClusterInviteChallengeProof,
) -> Result<(), NetworkError> {
    if proof.protocol != CLUSTER_INVITE_CHALLENGE_PROTOCOL
        || proof.invite_id != invite.invite_id
        || (proof.cluster_id != invite.cluster_id
            && invite.cluster_id.as_str() != PENDING_INVITE_CLUSTER_ID
            && invite.cluster_id.as_str() != "cluster_pending_join")
        || proof.challenge != expected_challenge
    {
        return Err(NetworkError::AuthDenied);
    }
    let signed_at =
        OffsetDateTime::parse(&proof.timestamp, &Rfc3339).map_err(|_| NetworkError::AuthDenied)?;
    let skew = (OffsetDateTime::now_utc() - signed_at)
        .whole_seconds()
        .abs();
    if skew > CLUSTER_INVITE_CHALLENGE_MAX_CLOCK_SKEW_SECONDS {
        return Err(NetworkError::AuthExpired);
    }
    let responder_is_issuer = proof.responder_public_key == invite.issuer_public_key;
    let responder_is_channel = invite
        .communication_channels
        .iter()
        .any(|channel| channel.node_public_key == proof.responder_public_key);
    if !responder_is_issuer && !responder_is_channel {
        return Err(NetworkError::AuthDenied);
    }
    let public_key = URL_SAFE_NO_PAD
        .decode(proof.responder_public_key.as_bytes())
        .map_err(|_| NetworkError::AuthDenied)?;
    let signature = URL_SAFE_NO_PAD
        .decode(proof.signature.as_bytes())
        .map_err(|_| NetworkError::AuthDenied)?;
    let signing_payload = cluster_invite_challenge_signing_payload(
        &proof.invite_id,
        &proof.cluster_id,
        &proof.responder_node_id,
        &proof.responder_public_key,
        &proof.challenge,
        &proof.timestamp,
    );
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(signing_payload.as_bytes(), &signature)
        .map_err(|_| NetworkError::AuthDenied)
}

pub fn create_cluster_invite_challenge_proof(
    paths: &ClusterPaths,
    identity: &NodeIdentity,
    request: &ClusterInviteChallengeRequest,
    now: OffsetDateTime,
) -> Result<ClusterInviteChallengeProof, NetworkError> {
    if request.protocol != CLUSTER_INVITE_CHALLENGE_PROTOCOL || request.challenge.trim().is_empty()
    {
        return Err(NetworkError::AuthDenied);
    }
    let invite = read_cluster_invitation_from_kv(paths, &request.invite_id)?;
    if invite.expires_at <= now {
        return Err(NetworkError::AuthExpired);
    }
    let state = read_cluster_state(paths)?;
    if invite.cluster_id != state.cluster_id {
        return Err(NetworkError::AuthDenied);
    }
    let local_member = state
        .members
        .iter()
        .find(|member| {
            member.node_id == state.local_node_id
                && member.node_public_key == identity.public_key
                && member.state.is_authorized()
        })
        .ok_or(NetworkError::AuthDenied)?;
    let is_issuer = invite.issuer_node_id == local_member.node_id
        && invite.issuer_public_key == identity.public_key;
    let is_channel = invite.communication_channels.iter().any(|channel| {
        channel.endpoint.node_id == local_member.node_id
            && channel.node_public_key == identity.public_key
    });
    if !is_issuer && !is_channel {
        return Err(NetworkError::AuthDenied);
    }
    let timestamp = now
        .format(&Rfc3339)
        .map_err(|source| NetworkError::Serialization {
            detail: format!("invalid challenge proof timestamp: {source}"),
        })?;
    let signing_payload = cluster_invite_challenge_signing_payload(
        &request.invite_id,
        &invite.cluster_id,
        local_member.node_id.as_str(),
        &identity.public_key,
        &request.challenge,
        &timestamp,
    );
    let signature = identity
        .key_pair()
        .map_err(|source| NetworkError::Serialization {
            detail: source.to_string(),
        })?
        .sign(signing_payload.as_bytes());
    Ok(ClusterInviteChallengeProof {
        protocol: CLUSTER_INVITE_CHALLENGE_PROTOCOL.to_string(),
        invite_id: request.invite_id.clone(),
        cluster_id: invite.cluster_id,
        responder_node_id: local_member.node_id.to_string(),
        responder_public_key: identity.public_key.clone(),
        challenge: request.challenge.clone(),
        timestamp,
        signature: URL_SAFE_NO_PAD.encode(signature.as_ref()),
    })
}

pub fn build_cluster_join_invite(
    state: &ClusterBootstrapState,
    issuer_identity: &NodeIdentity,
    introducers: Vec<ClusterEndpointRecord>,
    ttl_seconds: i64,
    now: OffsetDateTime,
) -> Result<ClusterJoinInvite, NetworkError> {
    let invite_id = ClusterInviteId::generate();
    let invitation_secret = random_url_secret(32)?;
    let communication_channels = invite_communication_channels(state, &introducers);
    let mut invite = ClusterJoinInvite {
        invite_id: invite_id.clone(),
        cluster_id: state.cluster_id.clone(),
        cluster_name: Some(state.cluster_name.clone()),
        cluster_public_key: issuer_identity.public_key.clone(),
        invitation_secret,
        communication_channels,
        introducers,
        token_id: format!("token_{}", invite_id.as_str().trim_start_matches("invite_")),
        expires_at: now + time::Duration::seconds(ttl_seconds),
        trust_domain: TrustDomain::new("trust.local").expect("static trust domain"),
        allowed_member_roles: vec![ClusterMemberRole::Learner, ClusterMemberRole::NonVoting],
        issuer_node_id: state.local_node_id.clone(),
        issuer_public_key: issuer_identity.public_key.clone(),
        signature: String::new(),
    };
    invite.signature = sign_cluster_join_invite(&invite, issuer_identity)?;
    Ok(invite)
}

pub fn random_url_secret(byte_len: usize) -> Result<String, NetworkError> {
    let rng = SystemRandom::new();
    let mut bytes = sinomo_network::try_zeroed_vec(byte_len, "generate cluster invite secret")?;
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
    let mut channels = invite_channels(invite);
    if channels
        .iter()
        .any(|channel| is_remote_join_registration_endpoint(&channel.endpoint))
    {
        channels = channels
            .into_iter()
            .filter(|channel| is_remote_join_registration_endpoint(&channel.endpoint))
            .collect();
    }
    channels.sort_by_key(|channel| {
        (
            invite_endpoint_fallback_rank(&channel.endpoint),
            channel.endpoint.priority,
            channel.endpoint.address.clone(),
        )
    });
    channels
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
            invite_endpoint_fallback_rank(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
}

fn invite_endpoint_fallback_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    match endpoint.transport {
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            invite_direct_endpoint_fallback_rank(endpoint)
        }
        ClusterTransportKind::Iroh => 2,
        _ => 3,
    }
}

fn invite_direct_endpoint_fallback_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    let Ok(address) = endpoint.address.parse::<std::net::SocketAddr>() else {
        return if matches!(endpoint.privacy, ClusterEndpointPrivacy::Lan) {
            0
        } else {
            1
        };
    };
    match address.ip() {
        std::net::IpAddr::V4(ip) if ip.is_private() || ip.is_link_local() => 0,
        std::net::IpAddr::V6(ip) if ip.is_unique_local() || ip.is_unicast_link_local() => 0,
        ip if ip.is_loopback() || ip.is_unspecified() => 3,
        _ => 1,
    }
}

pub fn compact_default_invite_introducers(introducers: &mut Vec<ClusterEndpointRecord>) {
    let remote_lan = introducers
        .iter()
        .filter(|endpoint| {
            is_remote_usable_invite_endpoint(endpoint)
                && !matches!(endpoint.transport, ClusterTransportKind::Iroh)
                && invite_direct_endpoint_fallback_rank(endpoint) == 0
        })
        .next()
        .cloned();
    let remote_public = introducers
        .iter()
        .filter(|endpoint| {
            is_remote_usable_invite_endpoint(endpoint)
                && !matches!(endpoint.transport, ClusterTransportKind::Iroh)
                && invite_direct_endpoint_fallback_rank(endpoint) == 1
        })
        .next()
        .cloned()
        .or_else(|| {
            introducers
                .iter()
                .filter(|endpoint| {
                    is_remote_usable_invite_endpoint(endpoint)
                        && !matches!(endpoint.transport, ClusterTransportKind::Iroh)
                })
                .find(|endpoint| {
                    remote_lan
                        .as_ref()
                        .is_none_or(|lan| lan.endpoint_id != endpoint.endpoint_id)
                })
                .cloned()
        });
    let remote_iroh = introducers
        .iter()
        .find(|endpoint| {
            is_remote_usable_invite_endpoint(endpoint)
                && matches!(endpoint.transport, ClusterTransportKind::Iroh)
        })
        .cloned();
    let mut remote_direct = Vec::new();
    if let Some(lan) = remote_lan {
        push_unique_introducer(&mut remote_direct, lan);
    }
    if let Some(public) = remote_public {
        push_unique_introducer(&mut remote_direct, public);
    }
    for endpoint in introducers.iter().filter(|endpoint| {
        is_remote_usable_invite_endpoint(endpoint)
            && !matches!(endpoint.transport, ClusterTransportKind::Iroh)
    }) {
        if remote_direct.len() >= 2 {
            break;
        }
        push_unique_introducer(&mut remote_direct, endpoint.clone());
    }

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

fn iroh_address_has_direct_query(address: &str) -> bool {
    let Some((_, query)) = address.split_once('?') else {
        return false;
    };
    let query = query.split('#').next().unwrap_or(query);
    query.split('&').any(|part| part.starts_with("direct="))
}

pub fn invite_introducer_endpoint(node_id: &NodeId, address: String) -> ClusterEndpointRecord {
    if let Some(rest) = address.strip_prefix("iroh://") {
        let public_key = rest.split(['?', '#']).next().unwrap_or_default().trim();
        if !public_key.is_empty() {
            let has_direct_address = iroh_address_has_direct_query(&address);
            return ClusterEndpointRecord {
                endpoint_id: stable_iroh_endpoint_id(node_id, public_key),
                node_id: node_id.clone(),
                transport: ClusterTransportKind::Iroh,
                address,
                source: ClusterEndpointSource::Rendezvous,
                privacy: if has_direct_address {
                    ClusterEndpointPrivacy::InternetDirect
                } else {
                    ClusterEndpointPrivacy::Relayed
                },
                priority: if has_direct_address { 40 } else { 90 },
                last_observed_latency_ms: None,
            };
        }
    }
    let is_loopback = is_loopback_connect_address(&address);
    let privacy = if is_loopback {
        ClusterEndpointPrivacy::LocalOnly
    } else {
        address
            .parse::<std::net::SocketAddr>()
            .ok()
            .map(|address| address.ip())
            .filter(|ip| match ip {
                std::net::IpAddr::V4(ip) => !(ip.is_private() || ip.is_link_local()),
                std::net::IpAddr::V6(ip) => !(ip.is_unique_local() || ip.is_unicast_link_local()),
            })
            .map(|_| ClusterEndpointPrivacy::InternetDirect)
            .unwrap_or(ClusterEndpointPrivacy::Lan)
    };
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
        privacy,
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
    let parts = vec![
        query_enum(&channel.endpoint.transport)?,
        channel.endpoint.address.clone(),
        channel.node_public_key.clone(),
    ];
    Ok(join_invite_parts(&parts))
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
    let mut decoded = Vec::new();
    for part in value.split(',') {
        sinomo_network::try_reserve_vec(&mut decoded, 1, "decode cluster invite parts")?;
        decoded.push(percent_decode_invite_value(part)?);
    }
    Ok(decoded)
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
    let mut decoded = Vec::new();
    sinomo_network::try_reserve_vec(
        &mut decoded,
        bytes.len(),
        "decode cluster invite percent value",
    )?;
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
