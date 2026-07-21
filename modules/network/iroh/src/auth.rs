//! Iroh-owned proof that a byte stream belongs to the authenticated QUIC link.
//!
//! The generic network layer carries these bytes opaquely. Only this module
//! knows that they are Ed25519 signatures over both endpoint principals, the
//! QUIC exporter, and the bidirectional stream index.

use sinomo_core::network::{AuthenticatedTransportMetadata, NetworkError};
use tokio::io::AsyncWriteExt;

const IROH_LINK_PROOF_DOMAIN: &[u8] = b"sinomo.iroh-authenticated-link-proof.v1\0";
const IROH_LINK_OPEN_PREFIX: [u8; 4] = *b"IAO1";
const IROH_LINK_ACCEPT_PREFIX: [u8; 4] = *b"IAA1";
const IROH_LINK_OPEN_ROLE: u8 = 1;
const IROH_LINK_ACCEPT_ROLE: u8 = 2;
const IROH_LINK_PROOF_BYTES: usize = 1 + 8 + iroh::Signature::LENGTH;
const IROH_LINK_WIRE_BYTES: usize = 4 + 8 + iroh::Signature::LENGTH;

fn proof_transcript(
    role: u8,
    initiator: &iroh::PublicKey,
    responder: &iroh::PublicKey,
    channel_binding: &[u8; 32],
    stream_index: u64,
) -> Vec<u8> {
    let mut transcript = Vec::with_capacity(IROH_LINK_PROOF_DOMAIN.len() + 1 + 32 + 32 + 32 + 8);
    transcript.extend_from_slice(IROH_LINK_PROOF_DOMAIN);
    transcript.push(role);
    transcript.extend_from_slice(initiator.as_bytes());
    transcript.extend_from_slice(responder.as_bytes());
    transcript.extend_from_slice(channel_binding);
    transcript.extend_from_slice(&stream_index.to_be_bytes());
    transcript
}

fn sign_proof(
    role: u8,
    signer: &iroh::SecretKey,
    initiator: &iroh::PublicKey,
    responder: &iroh::PublicKey,
    channel_binding: &[u8; 32],
    stream_index: u64,
) -> Vec<u8> {
    let signature = signer.sign(&proof_transcript(
        role,
        initiator,
        responder,
        channel_binding,
        stream_index,
    ));
    let mut proof = Vec::with_capacity(IROH_LINK_PROOF_BYTES);
    proof.push(role);
    proof.extend_from_slice(&stream_index.to_be_bytes());
    proof.extend_from_slice(&signature.to_bytes());
    proof
}

fn parse_proof(proof: &[u8]) -> Option<(u8, u64, iroh::Signature)> {
    if proof.len() != IROH_LINK_PROOF_BYTES {
        return None;
    }
    let stream_index = u64::from_be_bytes(proof[1..9].try_into().ok()?);
    let signature = iroh::Signature::from_bytes(proof[9..].try_into().ok()?);
    Some((proof[0], stream_index, signature))
}

fn verify_proof(
    proof: &[u8],
    expected_role: u8,
    signer: &iroh::PublicKey,
    initiator: &iroh::PublicKey,
    responder: &iroh::PublicKey,
    channel_binding: &[u8; 32],
    expected_stream_index: Option<u64>,
) -> bool {
    let Some((role, stream_index, signature)) = parse_proof(proof) else {
        return false;
    };
    if role != expected_role
        || expected_stream_index.is_some_and(|expected| expected != stream_index)
    {
        return false;
    }
    signer
        .verify(
            &proof_transcript(role, initiator, responder, channel_binding, stream_index),
            &signature,
        )
        .is_ok()
}

fn link_auth_io(operation: &str, error: impl std::fmt::Display) -> NetworkError {
    NetworkError::Io {
        operation: operation.to_string(),
        detail: error.to_string(),
    }
}

async fn write_wire_proof(
    send: &mut iroh::endpoint::SendStream,
    prefix: [u8; 4],
    proof: &[u8],
) -> Result<(), NetworkError> {
    let Some((_, stream_index, signature)) = parse_proof(proof) else {
        return Err(NetworkError::AuthDenied);
    };
    let mut wire = [0_u8; IROH_LINK_WIRE_BYTES];
    wire[..4].copy_from_slice(&prefix);
    wire[4..12].copy_from_slice(&stream_index.to_be_bytes());
    wire[12..].copy_from_slice(&signature.to_bytes());
    send.write_all(&wire)
        .await
        .map_err(|error| link_auth_io("write iroh link proof", error))?;
    send.flush()
        .await
        .map_err(|error| link_auth_io("flush iroh link proof", error))
}

async fn read_wire_proof(
    recv: &mut iroh::endpoint::RecvStream,
    prefix: [u8; 4],
    role: u8,
) -> Result<Vec<u8>, NetworkError> {
    let mut wire = [0_u8; IROH_LINK_WIRE_BYTES];
    recv.read_exact(&mut wire)
        .await
        .map_err(|error| link_auth_io("read iroh link proof", error))?;
    if wire[..4] != prefix {
        return Err(NetworkError::AuthDenied);
    }
    let mut proof = Vec::with_capacity(IROH_LINK_PROOF_BYTES);
    proof.push(role);
    proof.extend_from_slice(&wire[4..]);
    Ok(proof)
}

/// Proves the initiating endpoint and verifies the accepting endpoint before
/// any routed-session bytes are exposed to the module ABI.
pub(crate) async fn authenticate_outbound_stream(
    local_endpoint: &iroh::Endpoint,
    remote: &iroh::PublicKey,
    channel_binding: &[u8; 32],
    send: &mut iroh::endpoint::SendStream,
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<Vec<u8>, NetworkError> {
    let stream_index = send.id().index();
    if recv.id().index() != stream_index {
        return Err(NetworkError::AuthDenied);
    }
    let local = local_endpoint.id();
    let open_proof = sign_proof(
        IROH_LINK_OPEN_ROLE,
        local_endpoint.secret_key(),
        &local,
        remote,
        channel_binding,
        stream_index,
    );
    write_wire_proof(send, IROH_LINK_OPEN_PREFIX, &open_proof).await?;
    let accept_proof =
        read_wire_proof(recv, IROH_LINK_ACCEPT_PREFIX, IROH_LINK_ACCEPT_ROLE).await?;
    if !verify_proof(
        &accept_proof,
        IROH_LINK_ACCEPT_ROLE,
        remote,
        &local,
        remote,
        channel_binding,
        Some(stream_index),
    ) {
        return Err(NetworkError::AuthDenied);
    }
    Ok(accept_proof)
}

/// Verifies the initiating endpoint and proves the accepting endpoint before
/// forwarding any application bytes to the local daemon.
pub(crate) async fn authenticate_inbound_stream(
    local_secret: &iroh::SecretKey,
    remote: &iroh::PublicKey,
    channel_binding: &[u8; 32],
    send: &mut iroh::endpoint::SendStream,
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<Vec<u8>, NetworkError> {
    let stream_index = recv.id().index();
    if send.id().index() != stream_index {
        return Err(NetworkError::AuthDenied);
    }
    let local = local_secret.public();
    let open_proof = read_wire_proof(recv, IROH_LINK_OPEN_PREFIX, IROH_LINK_OPEN_ROLE).await?;
    if !verify_proof(
        &open_proof,
        IROH_LINK_OPEN_ROLE,
        remote,
        remote,
        &local,
        channel_binding,
        Some(stream_index),
    ) {
        return Err(NetworkError::AuthDenied);
    }
    let accept_proof = sign_proof(
        IROH_LINK_ACCEPT_ROLE,
        local_secret,
        remote,
        &local,
        channel_binding,
        stream_index,
    );
    write_wire_proof(send, IROH_LINK_ACCEPT_PREFIX, &accept_proof).await?;
    Ok(open_proof)
}

/// Verifies module-owned proof bytes after they cross the generic link ABI.
/// Open proofs authenticate inbound peers; accept proofs authenticate an
/// outbound endpoint selected from the directory.
pub(crate) fn verify_transport_metadata(
    local_transport_identity: &str,
    metadata: &AuthenticatedTransportMetadata,
) -> bool {
    let Ok(local) = local_transport_identity.parse::<iroh::PublicKey>() else {
        return false;
    };
    let Ok(peer) = metadata.peer_transport_identity.parse::<iroh::PublicKey>() else {
        return false;
    };
    let Some((role, _, _)) = parse_proof(&metadata.transport_proof) else {
        return false;
    };
    match role {
        IROH_LINK_OPEN_ROLE => verify_proof(
            &metadata.transport_proof,
            role,
            &peer,
            &peer,
            &local,
            &metadata.channel_binding,
            None,
        ),
        IROH_LINK_ACCEPT_ROLE => verify_proof(
            &metadata.transport_proof,
            role,
            &peer,
            &local,
            &peer,
            &metadata.channel_binding,
            None,
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::ClusterTransportKind;
    use sinomo_core::network::DirectStreamCapabilities;

    fn metadata(
        local: &iroh::SecretKey,
        peer: &iroh::SecretKey,
        role: u8,
    ) -> AuthenticatedTransportMetadata {
        let binding = [7_u8; 32];
        let proof = match role {
            IROH_LINK_OPEN_ROLE => {
                sign_proof(role, peer, &peer.public(), &local.public(), &binding, 17)
            }
            IROH_LINK_ACCEPT_ROLE => {
                sign_proof(role, peer, &local.public(), &peer.public(), &binding, 17)
            }
            _ => unreachable!(),
        };
        AuthenticatedTransportMetadata::try_new(
            ClusterTransportKind::Iroh,
            peer.public().to_string(),
            binding,
            proof,
            DirectStreamCapabilities::multiplexed_full_duplex(1),
        )
        .expect("metadata")
    }

    #[test]
    fn transport_metadata_verifies_both_roles_and_rejects_tampering() {
        let local = iroh::SecretKey::generate();
        let peer = iroh::SecretKey::generate();
        for role in [IROH_LINK_OPEN_ROLE, IROH_LINK_ACCEPT_ROLE] {
            let mut metadata = metadata(&local, &peer, role);
            assert!(verify_transport_metadata(
                &local.public().to_string(),
                &metadata
            ));
            metadata.channel_binding[0] ^= 1;
            assert!(!verify_transport_metadata(
                &local.public().to_string(),
                &metadata
            ));
        }
    }
}
