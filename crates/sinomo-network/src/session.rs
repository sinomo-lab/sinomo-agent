//! Canonical body-aware object-RPC session primitives.
//!
//! Public callers describe whole request or response bodies. Transfer frames,
//! flow-control windows, replay offsets, and synchronous-reader buffering stay
//! private to `sinomo-network`.

use crate::daemon_auth::{
    BodyFinishProofBinding, BodyFinishProofInput, ResponseHeadProofInput, SessionFrameDirection,
    SessionFrameProofInput, SessionFrameSigner, SignedBodyFinishProof, SignedDaemonCommand,
    SignedResponseHeadProof, SignedSessionFrameProof, verify_body_finish_proof,
    verify_response_head_proof, verify_session_frame_proof,
};
pub(crate) use crate::transfer::NetworkCallPolicy;
use crate::transfer::{
    MAX_TRANSFER_CONTROL_FRAME_BYTES, TransferControl, TransferDataFrame, TransferDataHeader,
    TransferDigest, TransferEncodedWireFrameHeader, TransferFlowWindow, TransferId, TransferOpen,
    TransferReceiverState, TransferSenderState, TransferVerifiedData,
    read_transfer_data_payload_to_vec, read_transfer_encoded_frame_header,
    write_transfer_data_frame, write_transfer_encoded_control_frame,
};
use crate::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sinomo_local::SinomoDataDir;
use std::any::Any;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

const BODY_READER_BUFFER_BYTES: usize = 64 * 1024;
const BODY_READER_BUFFER_DEPTH: usize = 2;
const OBJECT_RPC_SESSION_PROTOCOL: &str = "sinomo.object-rpc-session.v1";
const OBJECT_RPC_SESSION_METADATA_KEY: &str = "sinomo.rpc-session";
const OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY: &str = "sinomo.rpc-response-head-proof";
const OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY: &str =
    "sinomo.rpc-session.buffered-response-body";
const OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_VALUE: &str = "v1";
const SESSION_ZERO_DIGEST: TransferDigest = TransferDigest::new([0_u8; 32]);

/// Returns whether a non-consuming transport peek identifies the canonical
/// session wire. Unary compatibility frames use an object-frame length prefix
/// and therefore cannot share this magic.
pub fn object_rpc_session_wire_prefix_matches(prefix: &[u8]) -> bool {
    prefix.len() >= 4 && prefix[..4] == *b"SXFR"
}

/// A transport-owned bidirectional byte stream. The canonical RPC session
/// engine owns every byte written after the transport opens this stream.
pub trait NetworkDuplexStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> NetworkDuplexStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ObjectRpcSessionDescriptor {
    protocol: String,
    session_id: TransferId,
    request_body: Option<TransferOpen>,
}

impl ObjectRpcSessionDescriptor {
    fn for_call(call: &ObjectRpcSessionCall) -> Self {
        let session_id = TransferId::generate();
        let request_body = (!call.body.is_empty()).then(|| TransferOpen {
            transfer_id: TransferId::generate(),
            route: call.request.route.clone(),
            operation_id: call.request.operation_id().map(ToOwned::to_owned),
            total_bytes: call.body.total_bytes(),
            expected_digest: call.body.known_digest(),
            replayable: call.body.is_replayable(),
            requested_window: TransferFlowWindow::default(),
        });
        Self {
            protocol: OBJECT_RPC_SESSION_PROTOCOL.to_string(),
            session_id,
            request_body,
        }
    }

    fn encode_metadata(&self) -> Result<String, NetworkError> {
        let encoded = crate::SinomoObjectCodec::network_frame().encode(self)?;
        Ok(URL_SAFE_NO_PAD.encode(encoded))
    }

    fn decode_metadata(value: &str) -> Result<Self, NetworkError> {
        let bytes =
            URL_SAFE_NO_PAD
                .decode(value.as_bytes())
                .map_err(|_| NetworkError::Serialization {
                    detail: "object RPC session descriptor is not valid base64".to_string(),
                })?;
        let descriptor: Self = crate::SinomoObjectCodec::network_frame().decode(&bytes)?;
        if descriptor.protocol != OBJECT_RPC_SESSION_PROTOCOL {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "unsupported object RPC session protocol {}",
                    descriptor.protocol
                ),
            });
        }
        Ok(descriptor)
    }
}

fn request_body_finish_binding(
    request: &crate::ObjectRpcRequest,
    descriptor: &ObjectRpcSessionDescriptor,
) -> Result<BodyFinishProofBinding, NetworkError> {
    let mut normalized = request.clone();
    normalized.auth = None;
    let encoded = crate::SinomoObjectCodec::network_frame().encode(&normalized)?;
    Ok(BodyFinishProofBinding {
        session_id: descriptor.session_id.clone(),
        direction: SessionFrameDirection::InitiatorToResponder,
        head_digest: TransferDigest::blake3(&encoded),
    })
}

fn response_body_finish_binding(
    session_id: &TransferId,
    response: &crate::ObjectRpcResponse,
) -> Result<BodyFinishProofBinding, NetworkError> {
    let encoded = crate::SinomoObjectCodec::network_frame().encode(response)?;
    Ok(BodyFinishProofBinding {
        session_id: session_id.clone(),
        direction: SessionFrameDirection::ResponderToInitiator,
        head_digest: TransferDigest::blake3(&encoded),
    })
}

fn response_head_digest(
    response: &crate::ObjectRpcResponse,
) -> Result<TransferDigest, NetworkError> {
    let mut normalized = response.clone();
    normalized
        .metadata
        .remove(OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY);
    let encoded = crate::SinomoObjectCodec::network_frame().encode(&normalized)?;
    Ok(TransferDigest::blake3(&encoded))
}

fn encode_response_head_proof(proof: &SignedResponseHeadProof) -> Result<String, NetworkError> {
    let encoded = crate::SinomoObjectCodec::network_frame().encode(proof)?;
    Ok(URL_SAFE_NO_PAD.encode(encoded))
}

fn decode_response_head_proof(
    response: &crate::ObjectRpcResponse,
) -> Result<SignedResponseHeadProof, NetworkError> {
    let encoded = response
        .metadata
        .get(OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY)
        .ok_or(NetworkError::AuthDenied)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded.as_bytes())
        .map_err(|_| NetworkError::AuthDenied)?;
    crate::SinomoObjectCodec::network_frame().decode(&bytes)
}

#[cfg(test)]
async fn ensure_response_head_proof<W>(
    writer: &Arc<tokio::sync::Mutex<SessionFrameWriter<W>>>,
    session_id: &TransferId,
    response: &mut crate::ObjectRpcResponse,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    let writer = writer.lock().await;
    ensure_response_head_proof_with_writer(&writer, session_id, response)
}

fn ensure_response_head_proof_with_writer<W>(
    writer: &SessionFrameWriter<W>,
    session_id: &TransferId,
    response: &mut crate::ObjectRpcResponse,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    if response
        .metadata
        .contains_key(OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY)
    {
        return Ok(());
    }
    let proof = writer.sign_response_head(session_id.clone(), response_head_digest(response)?);
    response.metadata.insert(
        OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY.to_string(),
        encode_response_head_proof(&proof)?,
    );
    Ok(())
}

fn response_head_origin_is_verified(
    response: &crate::ObjectRpcResponse,
    expected_public_key: &[u8],
    expected_session_id: &TransferId,
) -> bool {
    decode_response_head_proof(response).is_ok_and(|proof| {
        response_head_digest(response).is_ok_and(|digest| {
            verify_response_head_proof(&proof, expected_public_key, expected_session_id, digest)
                .is_ok()
        })
    })
}

fn signed_request_body_finish_expectation(
    request: &crate::ObjectRpcRequest,
) -> Result<Option<BodyFinishProofExpectation>, NetworkError> {
    let origin_request = if request.route == crate::CLUSTER_RELAY_ROUTE {
        crate::RelayEnvelope::from_request(request)?.inner().clone()
    } else {
        request.clone()
    };
    let descriptor = session_descriptor_from_request(&origin_request)?;
    if descriptor.request_body.is_none() {
        return Ok(None);
    }
    let auth = origin_request
        .auth
        .as_ref()
        .ok_or(NetworkError::AuthDenied)?;
    let signer: SignedDaemonCommand = crate::SinomoObjectCodec::default().decode(auth)?;
    if signer.protocol != crate::DAEMON_AUTH_PROTOCOL || signer.public_key.is_empty() {
        return Err(NetworkError::AuthDenied);
    }
    Ok(Some(BodyFinishProofExpectation {
        binding: request_body_finish_binding(&origin_request, &descriptor)?,
        signer_node_id: Some(signer.node_id),
        public_key: signer.public_key,
    }))
}

fn response_proof_session_id(
    request: &crate::ObjectRpcRequest,
    transport_descriptor: &ObjectRpcSessionDescriptor,
) -> Result<TransferId, NetworkError> {
    if request.route != crate::CLUSTER_RELAY_ROUTE {
        return Ok(transport_descriptor.session_id.clone());
    }
    let envelope = crate::RelayEnvelope::from_request(request)?;
    Ok(session_descriptor_from_request(envelope.inner())?.session_id)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum SessionWirePayload {
    RequestHead {
        request: crate::ObjectRpcRequest,
        descriptor: ObjectRpcSessionDescriptor,
    },
    ResponseHead {
        response: crate::ObjectRpcResponse,
        body: Option<TransferOpen>,
    },
    Transfer(TransferControl),
    FinishAttestation {
        finish: crate::transfer::TransferFinish,
        proof: SignedBodyFinishProof,
    },
    Data(TransferDataHeader),
    HealthPing {
        nonce: u64,
    },
    HealthAck {
        nonce: u64,
    },
    Cancel {
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SignedSessionWireControl {
    payload: SessionWirePayload,
    proof: SignedSessionFrameProof,
}

#[derive(Debug)]
enum SessionInboundFrame {
    Payload(SessionWirePayload),
    Data {
        header: TransferDataHeader,
        payload: Vec<u8>,
    },
}

#[derive(Clone, Copy, Debug)]
struct SessionDeadline {
    overall_timeout: Option<std::time::Duration>,
    expires_at: Option<tokio::time::Instant>,
}

impl SessionDeadline {
    fn from_policy(policy: NetworkCallPolicy) -> Self {
        Self {
            overall_timeout: policy.overall_timeout,
            expires_at: policy
                .overall_timeout
                .and_then(|timeout| tokio::time::Instant::now().checked_add(timeout)),
        }
    }

    async fn run<T, F>(&self, operation: &'static str, future: F) -> Result<T, NetworkError>
    where
        F: std::future::Future<Output = Result<T, NetworkError>>,
    {
        let Some(expires_at) = self.expires_at else {
            return future.await;
        };
        match tokio::time::timeout_at(expires_at, future).await {
            Ok(result) => result,
            Err(_) => Err(NetworkError::timeout(
                operation,
                NetworkTimeoutPhase::Overall,
                self.overall_timeout.unwrap_or_default(),
                NetworkDeliveryState::PossiblyApplied,
            )),
        }
    }
}

struct SessionFrameWriter<W> {
    writer: W,
    data_dir: SinomoDataDir,
    signer: SessionFrameSigner,
    session_id: TransferId,
    direction: SessionFrameDirection,
    sequence: u64,
    previous_digest: TransferDigest,
    policy: NetworkCallPolicy,
    deadline: SessionDeadline,
}

impl<W> SessionFrameWriter<W>
where
    W: AsyncWrite + Unpin,
{
    fn new(
        writer: W,
        data_dir: SinomoDataDir,
        session_id: TransferId,
        direction: SessionFrameDirection,
        policy: NetworkCallPolicy,
    ) -> Result<Self, NetworkError> {
        let signer = SessionFrameSigner::from_data_dir(&data_dir)?;
        Ok(Self {
            writer,
            data_dir,
            signer,
            session_id,
            direction,
            sequence: 0,
            previous_digest: SESSION_ZERO_DIGEST,
            policy,
            deadline: SessionDeadline::from_policy(policy),
        })
    }

    fn encode_control(
        &self,
        payload: SessionWirePayload,
    ) -> Result<(Vec<u8>, TransferDigest), NetworkError> {
        let encoded_payload = crate::SinomoObjectCodec::network_frame().encode(&payload)?;
        let payload_digest = match &payload {
            SessionWirePayload::Data(header) => Some(header.payload_digest),
            _ => None,
        };
        let input = SessionFrameProofInput {
            session_id: self.session_id.clone(),
            direction: self.direction,
            sequence: self.sequence,
            previous_digest: self.previous_digest,
            frame_header_digest: TransferDigest::blake3(&encoded_payload),
            payload_digest,
        };
        let proof = self.signer.sign(input);
        let next_digest = proof.transcript_digest;
        let encoded = crate::SinomoObjectCodec::network_frame()
            .encode(&SignedSessionWireControl { payload, proof })?;
        Ok((encoded, next_digest))
    }

    fn encoded_control_len(&self, payload: SessionWirePayload) -> Result<usize, NetworkError> {
        self.encode_control(payload)
            .map(|(encoded, _)| encoded.len())
    }

    async fn send(&mut self, payload: SessionWirePayload) -> Result<(), NetworkError> {
        let (encoded, next_digest) = self.encode_control(payload)?;
        self.deadline
            .run(
                "write object RPC session control",
                write_transfer_encoded_control_frame(
                    &mut self.writer,
                    &encoded,
                    self.policy.health,
                ),
            )
            .await?;
        self.sequence =
            self.sequence
                .checked_add(1)
                .ok_or_else(|| NetworkError::Serialization {
                    detail: "object RPC session frame sequence overflowed".to_string(),
                })?;
        self.previous_digest = next_digest;
        Ok(())
    }

    async fn send_data(&mut self, frame: TransferDataFrame<'_>) -> Result<(), NetworkError> {
        let header = frame.header.clone();
        self.send(SessionWirePayload::Data(header)).await?;
        self.deadline
            .run(
                "write object RPC session data",
                write_transfer_data_frame(&mut self.writer, &frame, self.policy.health),
            )
            .await
    }

    fn sign_body_finish(
        &self,
        binding: BodyFinishProofBinding,
        total_bytes: u64,
        digest: TransferDigest,
    ) -> SignedBodyFinishProof {
        self.signer.sign_body_finish(BodyFinishProofInput {
            binding,
            total_bytes,
            digest,
        })
    }

    fn sign_response_head(
        &self,
        session_id: TransferId,
        head_digest: TransferDigest,
    ) -> SignedResponseHeadProof {
        self.signer.sign_response_head(ResponseHeadProofInput {
            session_id,
            head_digest,
        })
    }
}

struct SessionFrameReader<R> {
    reader: R,
    session_id: TransferId,
    direction: SessionFrameDirection,
    expected_public_key: Vec<u8>,
    sequence: u64,
    previous_digest: TransferDigest,
    header_buffer: Vec<u8>,
    policy: NetworkCallPolicy,
    deadline: SessionDeadline,
}

impl<R> SessionFrameReader<R>
where
    R: AsyncRead + Unpin,
{
    fn new(
        reader: R,
        session_id: TransferId,
        direction: SessionFrameDirection,
        expected_public_key: Vec<u8>,
        policy: NetworkCallPolicy,
    ) -> Self {
        Self {
            reader,
            session_id,
            direction,
            expected_public_key,
            sequence: 0,
            previous_digest: SESSION_ZERO_DIGEST,
            header_buffer: Vec::new(),
            policy,
            deadline: SessionDeadline::from_policy(policy),
        }
    }

    fn after_verified_bootstrap(
        reader: R,
        session_id: TransferId,
        direction: SessionFrameDirection,
        expected_public_key: Vec<u8>,
        policy: NetworkCallPolicy,
        sequence: u64,
        previous_digest: TransferDigest,
        header_buffer: Vec<u8>,
    ) -> Self {
        Self {
            reader,
            session_id,
            direction,
            expected_public_key,
            sequence,
            previous_digest,
            header_buffer,
            policy,
            deadline: SessionDeadline::from_policy(policy),
        }
    }

    async fn receive(&mut self) -> Result<SessionInboundFrame, NetworkError> {
        let header = self
            .deadline
            .run(
                "read object RPC session control",
                read_transfer_encoded_frame_header(
                    &mut self.reader,
                    &mut self.header_buffer,
                    self.policy.health,
                ),
            )
            .await?;
        let TransferEncodedWireFrameHeader::Control(encoded) = header else {
            return Err(NetworkError::AuthDenied);
        };
        let signed: SignedSessionWireControl =
            crate::SinomoObjectCodec::network_frame().decode(&encoded)?;
        let encoded_payload = crate::SinomoObjectCodec::network_frame().encode(&signed.payload)?;
        let payload_digest = match &signed.payload {
            SessionWirePayload::Data(header) => Some(header.payload_digest),
            _ => None,
        };
        let expected_input = SessionFrameProofInput {
            session_id: self.session_id.clone(),
            direction: self.direction,
            sequence: self.sequence,
            previous_digest: self.previous_digest,
            frame_header_digest: TransferDigest::blake3(&encoded_payload),
            payload_digest,
        };
        let next_digest =
            verify_session_frame_proof(&signed.proof, &self.expected_public_key, &expected_input)?;
        self.sequence =
            self.sequence
                .checked_add(1)
                .ok_or_else(|| NetworkError::Serialization {
                    detail: "object RPC session frame sequence overflowed".to_string(),
                })?;
        self.previous_digest = next_digest;

        let SessionWirePayload::Data(expected_header) = signed.payload else {
            return Ok(SessionInboundFrame::Payload(signed.payload));
        };
        let data_header = self
            .deadline
            .run(
                "read object RPC session data header",
                read_transfer_encoded_frame_header(
                    &mut self.reader,
                    &mut self.header_buffer,
                    self.policy.health,
                ),
            )
            .await?;
        let TransferEncodedWireFrameHeader::Data(header) = data_header else {
            return Err(NetworkError::AuthDenied);
        };
        if header != expected_header {
            return Err(NetworkError::AuthDenied);
        }
        let payload = self
            .deadline
            .run(
                "read object RPC session data payload",
                read_transfer_data_payload_to_vec(&mut self.reader, &header, self.policy.health),
            )
            .await?;
        Ok(SessionInboundFrame::Data { header, payload })
    }
}

fn session_transfer_error(error: impl fmt::Display) -> NetworkError {
    NetworkError::Serialization {
        detail: error.to_string(),
    }
}

#[derive(Debug)]
struct SessionSpoolCleanup {
    path: PathBuf,
}

impl Drop for SessionSpoolCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug)]
struct IncomingSpoolSetup {
    file: tokio::fs::File,
    base_offset: u64,
    _cleanup: Arc<SessionSpoolCleanup>,
}

#[derive(Clone, Debug, Default)]
struct IncomingSpoolProgress {
    available_bytes: u64,
    complete: bool,
    error: Option<String>,
}

struct SessionTransferStagingFile {
    file: Option<File>,
    _cleanup: Arc<SessionSpoolCleanup>,
}

impl SessionTransferStagingFile {
    async fn create(
        root: PathBuf,
        transfer_id: TransferId,
        base_offset: u64,
    ) -> Result<(Self, IncomingSpoolSetup), NetworkError> {
        tokio::task::spawn_blocking(move || {
            let directory = root.join("network-transfers");
            std::fs::create_dir_all(&directory).map_err(|source| NetworkError::Io {
                operation: format!(
                    "create session transfer staging directory {}",
                    directory.display()
                ),
                detail: source.to_string(),
            })?;
            // A replay keeps its signed transfer id. A unique suffix prevents
            // an old transport that is still unwinding from truncating the
            // new attempt's spool.
            let path = directory.join(format!(
                "{}-{}.part",
                transfer_id.as_str(),
                TransferId::generate().as_str()
            ));
            let file = std::fs::OpenOptions::new()
                .create(true)
                .create_new(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|source| NetworkError::Io {
                    operation: format!("open session transfer staging file {}", path.display()),
                    detail: source.to_string(),
                })?;
            let reader = File::open(&path).map_err(|source| NetworkError::Io {
                operation: format!("open session transfer spool reader {}", path.display()),
                detail: source.to_string(),
            })?;
            let cleanup = Arc::new(SessionSpoolCleanup { path });
            Ok((
                Self {
                    file: Some(file),
                    _cleanup: Arc::clone(&cleanup),
                },
                IncomingSpoolSetup {
                    file: tokio::fs::File::from_std(reader),
                    base_offset,
                    _cleanup: cleanup,
                },
            ))
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "join session transfer staging creation".to_string(),
            detail: source.to_string(),
        })?
    }

    async fn append(&mut self, bytes: Vec<u8>) -> Result<(), NetworkError> {
        let mut file = self.file.take().ok_or_else(|| NetworkError::Io {
            operation: "append session transfer staging file".to_string(),
            detail: "staging file is closed".to_string(),
        })?;
        let file = tokio::task::spawn_blocking(move || {
            use std::io::Write as _;
            file.write_all(&bytes).map_err(|source| NetworkError::Io {
                operation: "write session transfer staging file".to_string(),
                detail: source.to_string(),
            })?;
            Ok::<_, NetworkError>(file)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "join session transfer staging write".to_string(),
            detail: source.to_string(),
        })??;
        self.file = Some(file);
        Ok(())
    }
}

impl Drop for SessionTransferStagingFile {
    fn drop(&mut self) {
        self.file.take();
    }
}

async fn send_session_payload<W>(
    writer: &Arc<tokio::sync::Mutex<SessionFrameWriter<W>>>,
    payload: SessionWirePayload,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    writer.lock().await.send(payload).await
}

fn spawn_session_keepalive<W>(
    writer: Arc<tokio::sync::Mutex<SessionFrameWriter<W>>>,
    policy: NetworkCallPolicy,
) -> tokio::task::JoinHandle<()>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let interval = policy
        .health
        .no_progress_timeout
        .checked_div(2)
        .unwrap_or(policy.health.no_progress_timeout)
        .max(std::time::Duration::from_millis(1));
    tokio::spawn(async move {
        let mut nonce = 1_u64;
        loop {
            tokio::time::sleep(interval).await;
            if send_session_payload(&writer, SessionWirePayload::HealthPing { nonce })
                .await
                .is_err()
            {
                return;
            }
            nonce = nonce.wrapping_add(1).max(1);
        }
    })
}

async fn receive_sender_feedback(
    receiver: &mut tokio::sync::mpsc::Receiver<TransferControl>,
    policy: NetworkCallPolicy,
    deadline: SessionDeadline,
) -> Result<TransferControl, NetworkError> {
    deadline
        .run("wait for object RPC transfer acknowledgement", async {
            match tokio::time::timeout(policy.health.no_progress_timeout, receiver.recv()).await {
                Ok(Some(feedback)) => Ok(feedback),
                Ok(None) => Err(NetworkError::Io {
                    operation: "wait for object RPC transfer acknowledgement".to_string(),
                    detail: "session reader stopped before transfer acknowledgement".to_string(),
                }),
                Err(_) => Err(NetworkError::timeout(
                    "wait for object RPC transfer acknowledgement",
                    NetworkTimeoutPhase::Idle,
                    policy.health.no_progress_timeout,
                    NetworkDeliveryState::PossiblyApplied,
                )),
            }
        })
        .await
}

async fn pump_outgoing_body<W>(
    writer: Arc<tokio::sync::Mutex<SessionFrameWriter<W>>>,
    source: ObjectRpcBodySource,
    open: TransferOpen,
    mut feedback: tokio::sync::mpsc::Receiver<TransferControl>,
    policy: NetworkCallPolicy,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    let deadline = SessionDeadline::from_policy(policy);
    let mut state = TransferSenderState::new(
        open.transfer_id.clone(),
        open.total_bytes,
        open.requested_window,
    )
    .map_err(session_transfer_error)?;
    state.mark_opening().map_err(session_transfer_error)?;
    send_session_payload(
        &writer,
        SessionWirePayload::Transfer(TransferControl::Open(open.clone())),
    )
    .await?;
    let open_ack = loop {
        match receive_sender_feedback(&mut feedback, policy, deadline).await? {
            TransferControl::OpenAck(ack) if ack.transfer_id == open.transfer_id => break ack,
            TransferControl::Cancel {
                transfer_id,
                reason,
            }
            | TransferControl::Reset {
                transfer_id,
                reason,
            } if transfer_id == open.transfer_id => {
                return Err(NetworkError::delivery_failed(
                    "send object RPC transfer",
                    NetworkDeliveryState::PossiblyApplied,
                    reason,
                ));
            }
            _ => continue,
        }
    };
    state
        .accept_open_ack(&open_ack)
        .map_err(session_transfer_error)?;
    let mut producer = source.producer(open_ack.receive_window.max_frame_bytes as usize)?;
    let mut hasher = blake3::Hasher::new();
    let mut pending = None;
    let mut source_finished = false;
    while !source_finished || pending.is_some() {
        while let Ok(control) = feedback.try_recv() {
            match control {
                TransferControl::Ack(ack) if ack.transfer_id == open.transfer_id => {
                    state.accept_ack(&ack).map_err(session_transfer_error)?;
                }
                TransferControl::Cancel {
                    transfer_id,
                    reason,
                }
                | TransferControl::Reset {
                    transfer_id,
                    reason,
                } if transfer_id == open.transfer_id => {
                    return Err(NetworkError::delivery_failed(
                        "send object RPC transfer",
                        NetworkDeliveryState::PossiblyApplied,
                        reason,
                    ));
                }
                _ => {}
            }
        }
        if pending.is_none() && !source_finished {
            pending = deadline
                .run("produce object RPC request/response body", producer.next())
                .await?;
            source_finished = pending.is_none();
        }
        let Some(bytes) = pending.take() else {
            continue;
        };
        let frame = match state.reserve_data(&bytes) {
            Ok(frame) => frame,
            Err(crate::transfer::TransferError::FlowControlBlocked { .. }) => {
                pending = Some(bytes);
                match receive_sender_feedback(&mut feedback, policy, deadline).await? {
                    TransferControl::Ack(ack) if ack.transfer_id == open.transfer_id => {
                        state.accept_ack(&ack).map_err(session_transfer_error)?;
                    }
                    TransferControl::Cancel {
                        transfer_id,
                        reason,
                    }
                    | TransferControl::Reset {
                        transfer_id,
                        reason,
                    } if transfer_id == open.transfer_id => {
                        return Err(NetworkError::delivery_failed(
                            "send flow-controlled object RPC transfer",
                            NetworkDeliveryState::PossiblyApplied,
                            reason,
                        ));
                    }
                    _ => {}
                }
                continue;
            }
            Err(error) => return Err(session_transfer_error(error)),
        };
        hasher.update(&bytes);
        writer.lock().await.send_data(frame).await?;
    }
    producer.finish().await?;
    let digest = TransferDigest::new(*hasher.finalize().as_bytes());
    if open
        .expected_digest
        .is_some_and(|expected| expected != digest)
    {
        return Err(NetworkError::AuthDenied);
    }
    let finish = state.finish(digest).map_err(session_transfer_error)?;
    let proof = source
        .finish_proof(&writer, finish.total_bytes, finish.digest)
        .await?;
    send_session_payload(
        &writer,
        SessionWirePayload::FinishAttestation { finish, proof },
    )
    .await?;
    state.mark_finish_sent().map_err(session_transfer_error)?;
    loop {
        match receive_sender_feedback(&mut feedback, policy, deadline).await? {
            TransferControl::Complete(receipt) if receipt.transfer_id == open.transfer_id => {
                state
                    .accept_receipt(&receipt)
                    .map_err(session_transfer_error)?;
                return Ok(());
            }
            TransferControl::Cancel {
                transfer_id,
                reason,
            }
            | TransferControl::Reset {
                transfer_id,
                reason,
            } if transfer_id == open.transfer_id => {
                return Err(NetworkError::delivery_failed(
                    "finish object RPC transfer",
                    NetworkDeliveryState::PossiblyApplied,
                    reason,
                ));
            }
            _ => continue,
        }
    }
}

#[derive(Debug)]
struct SessionReaderOutcome;

#[derive(Debug, Default)]
struct SharedOutgoingTransfer {
    initialized: bool,
    transfer_id: Option<TransferId>,
    complete: bool,
}

async fn pump_incoming_session<R, W>(
    mut reader: SessionFrameReader<R>,
    writer: Arc<tokio::sync::Mutex<SessionFrameWriter<W>>>,
    expected_incoming_open: Option<TransferOpen>,
    outgoing: Arc<Mutex<SharedOutgoingTransfer>>,
    feedback: tokio::sync::mpsc::Sender<TransferControl>,
    incoming: Option<IncomingBodySender>,
    head_sender: tokio::sync::oneshot::Sender<SessionWirePayload>,
    initial_head: Option<SessionWirePayload>,
) -> Result<SessionReaderOutcome, NetworkError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut expected_incoming_open = expected_incoming_open;
    let mut receiver_state = None;
    let mut incoming_sender = incoming;
    let mut incoming_hasher = blake3::Hasher::new();
    let mut incoming_opened = false;
    let mut incoming_complete = false;
    let mut head_sender = Some(head_sender);
    let mut staging: Option<SessionTransferStagingFile> = None;

    if let Some(payload) = initial_head {
        let declared_open = match &payload {
            SessionWirePayload::RequestHead { descriptor, .. } => descriptor.request_body.clone(),
            SessionWirePayload::ResponseHead { body, .. } => body.clone(),
            _ => {
                return Err(NetworkError::Serialization {
                    detail: "object RPC session bootstrap is not a request/response head"
                        .to_string(),
                });
            }
        };
        if let Some(expected) = expected_incoming_open.as_ref()
            && declared_open.as_ref() != Some(expected)
        {
            return Err(NetworkError::AuthDenied);
        }
        expected_incoming_open = declared_open;
        receiver_state = expected_incoming_open
            .as_ref()
            .map(|open| TransferReceiverState::from_open(open, TransferFlowWindow::default()))
            .transpose()
            .map_err(session_transfer_error)?;
        if expected_incoming_open.is_none() {
            incoming_opened = true;
            incoming_complete = true;
            incoming_sender.take();
        }
        head_sender
            .take()
            .expect("initial session head sender")
            .send(payload)
            .map_err(|_| NetworkError::Io {
                operation: "deliver object RPC session bootstrap head".to_string(),
                detail: "session caller stopped before receiving the bootstrap head".to_string(),
            })?;
    }

    loop {
        let frame = reader.receive().await?;
        match frame {
            SessionInboundFrame::Payload(payload @ SessionWirePayload::RequestHead { .. })
            | SessionInboundFrame::Payload(payload @ SessionWirePayload::ResponseHead { .. }) => {
                let Some(sender) = head_sender.take() else {
                    return Err(NetworkError::Serialization {
                        detail: "peer sent more than one object RPC session head".to_string(),
                    });
                };
                let declared_open = match &payload {
                    SessionWirePayload::RequestHead { descriptor, .. } => {
                        descriptor.request_body.clone()
                    }
                    SessionWirePayload::ResponseHead { body, .. } => body.clone(),
                    _ => unreachable!(),
                };
                if let Some(expected) = expected_incoming_open.as_ref()
                    && declared_open.as_ref() != Some(expected)
                {
                    return Err(NetworkError::AuthDenied);
                }
                expected_incoming_open = declared_open;
                receiver_state = expected_incoming_open
                    .as_ref()
                    .map(|open| {
                        TransferReceiverState::from_open(open, TransferFlowWindow::default())
                    })
                    .transpose()
                    .map_err(session_transfer_error)?;
                if expected_incoming_open.is_none() {
                    incoming_opened = true;
                    incoming_complete = true;
                    incoming_sender.take();
                }
                sender.send(payload).map_err(|_| NetworkError::Io {
                    operation: "deliver object RPC session head".to_string(),
                    detail: "session caller stopped before receiving the peer head".to_string(),
                })?;
            }
            SessionInboundFrame::Payload(SessionWirePayload::Transfer(control)) => {
                let transfer_id = transfer_control_id(&control);
                let outgoing_transfer_id = outgoing
                    .lock()
                    .map_err(|_| NetworkError::Io {
                        operation: "read object RPC outgoing transfer state".to_string(),
                        detail: "outgoing transfer state lock poisoned".to_string(),
                    })?
                    .transfer_id
                    .clone();
                if outgoing_transfer_id
                    .as_ref()
                    .is_some_and(|outgoing| transfer_id == Some(outgoing))
                    && matches!(
                        control,
                        TransferControl::OpenAck(_)
                            | TransferControl::Ack(_)
                            | TransferControl::Complete(_)
                            | TransferControl::Cancel { .. }
                            | TransferControl::Reset { .. }
                    )
                {
                    if matches!(control, TransferControl::Complete(_)) {
                        outgoing
                            .lock()
                            .map_err(|_| NetworkError::Io {
                                operation: "complete object RPC outgoing transfer".to_string(),
                                detail: "outgoing transfer state lock poisoned".to_string(),
                            })?
                            .complete = true;
                    }
                    feedback.send(control).await.map_err(|_| NetworkError::Io {
                        operation: "route object RPC transfer acknowledgement".to_string(),
                        detail: "outgoing transfer pump stopped".to_string(),
                    })?;
                } else {
                    match control {
                        TransferControl::Open(open) => {
                            let expected = expected_incoming_open.as_ref().ok_or_else(|| {
                                NetworkError::Serialization {
                                    detail: "peer opened an unexpected object RPC body".to_string(),
                                }
                            })?;
                            if &open != expected || incoming_opened {
                                return Err(NetworkError::AuthDenied);
                            }
                            incoming_opened = true;
                            // Partial reconnect state is deliberately not
                            // claimed yet. A new transport safely restarts at
                            // zero; bytes/files can replay, while one-pass
                            // sources fail explicitly rather than silently
                            // losing data.
                            let ack = receiver_state
                                .as_ref()
                                .expect("state exists for expected open")
                                .open_ack();
                            send_session_payload(
                                &writer,
                                SessionWirePayload::Transfer(TransferControl::OpenAck(ack)),
                            )
                            .await?;
                        }
                        TransferControl::Cancel { reason, .. }
                        | TransferControl::Reset { reason, .. } => {
                            if let Some(sender) = incoming_sender.take() {
                                sender.fail(reason.clone());
                            }
                            return Err(NetworkError::delivery_failed(
                                "receive object RPC transfer",
                                NetworkDeliveryState::PossiblyApplied,
                                reason,
                            ));
                        }
                        _ => {
                            return Err(NetworkError::Serialization {
                                detail: "unexpected object RPC transfer control direction"
                                    .to_string(),
                            });
                        }
                    }
                }
            }
            SessionInboundFrame::Payload(SessionWirePayload::FinishAttestation {
                finish,
                proof,
            }) => {
                let state = receiver_state
                    .as_mut()
                    .ok_or_else(|| NetworkError::Serialization {
                        detail: "peer finished an unexpected object RPC body".to_string(),
                    })?;
                state
                    .accept_finish(&finish)
                    .map_err(session_transfer_error)?;
                let digest = TransferDigest::new(*incoming_hasher.finalize().as_bytes());
                let receipt = state.complete(digest).map_err(session_transfer_error)?;
                if let Some(sender) = incoming_sender.as_ref() {
                    sender.complete(
                        receipt.completed_bytes,
                        receipt.digest,
                        staging.is_some(),
                        Some(proof),
                    )?;
                }
                send_session_payload(
                    &writer,
                    SessionWirePayload::Transfer(TransferControl::Complete(receipt.clone())),
                )
                .await?;
                incoming_sender.take();
                drop(staging.take());
                incoming_complete = true;
            }
            SessionInboundFrame::Data { header, payload } => {
                if !incoming_opened {
                    return Err(NetworkError::Serialization {
                        detail: "object RPC body data arrived before Open".to_string(),
                    });
                }
                let state = receiver_state
                    .as_mut()
                    .ok_or_else(|| NetworkError::Serialization {
                        detail: "peer sent an unexpected object RPC body".to_string(),
                    })?;
                let pending = state.begin_data(&header).map_err(session_transfer_error)?;
                let verified = TransferVerifiedData::from_payload(pending, &payload)
                    .map_err(session_transfer_error)?;
                incoming_hasher.update(&payload);
                if let Some(staging) = staging.as_mut() {
                    staging.append(payload).await?;
                } else if let Some(sender) = incoming_sender.as_ref() {
                    match sender.try_deliver(payload) {
                        IncomingDirectDelivery::Delivered
                        | IncomingDirectDelivery::ConsumerClosed => {}
                        IncomingDirectDelivery::Spill(payload) => {
                            let staging_root = writer.lock().await.data_dir.root();
                            let (mut new_staging, spool_setup) =
                                SessionTransferStagingFile::create(
                                    staging_root,
                                    header.transfer_id.clone(),
                                    header.offset,
                                )
                                .await?;
                            sender.start_spool(spool_setup)?;
                            new_staging.append(payload).await?;
                            staging = Some(new_staging);
                        }
                    }
                }
                state
                    .commit_data(verified)
                    .map_err(session_transfer_error)?;
                let available_offset = state.received_offset();
                let ack = state
                    .acknowledge_received(available_offset)
                    .map_err(session_transfer_error)?;
                if staging.is_some()
                    && let Some(sender) = incoming_sender.as_ref()
                {
                    sender.advance_available(available_offset)?;
                }
                send_session_payload(
                    &writer,
                    SessionWirePayload::Transfer(TransferControl::Ack(ack)),
                )
                .await?;
            }
            SessionInboundFrame::Payload(SessionWirePayload::HealthPing { nonce }) => {
                send_session_payload(&writer, SessionWirePayload::HealthAck { nonce }).await?;
            }
            SessionInboundFrame::Payload(SessionWirePayload::HealthAck { .. }) => {}
            SessionInboundFrame::Payload(SessionWirePayload::Cancel { reason }) => {
                if let Some(sender) = incoming_sender.take() {
                    sender.fail(reason.clone());
                }
                return Err(NetworkError::delivery_failed(
                    "object RPC session cancelled",
                    NetworkDeliveryState::PossiblyApplied,
                    reason,
                ));
            }
            SessionInboundFrame::Payload(SessionWirePayload::Data(_)) => {
                unreachable!("data controls are converted to data frames by the reader")
            }
        }

        let outgoing_complete = {
            let outgoing = outgoing.lock().map_err(|_| NetworkError::Io {
                operation: "inspect object RPC outgoing transfer completion".to_string(),
                detail: "outgoing transfer state lock poisoned".to_string(),
            })?;
            outgoing.initialized && (outgoing.transfer_id.is_none() || outgoing.complete)
        };
        if head_sender.is_none() && incoming_complete && outgoing_complete {
            return Ok(SessionReaderOutcome);
        }
    }
}

fn transfer_control_id(control: &TransferControl) -> Option<&TransferId> {
    match control {
        TransferControl::Open(open) => Some(&open.transfer_id),
        TransferControl::OpenAck(ack) => Some(&ack.transfer_id),
        TransferControl::Ack(ack) => Some(&ack.transfer_id),
        TransferControl::Complete(receipt) => Some(&receipt.transfer_id),
        TransferControl::Cancel { transfer_id, .. }
        | TransferControl::Reset { transfer_id, .. } => Some(transfer_id),
    }
}

type BoxedSessionStream = Box<dyn NetworkDuplexStream>;
type BoxedSessionReadHalf = tokio::io::ReadHalf<BoxedSessionStream>;
type BoxedSessionWriteHalf = tokio::io::WriteHalf<BoxedSessionStream>;
type BoxedSessionWriter = SessionFrameWriter<BoxedSessionWriteHalf>;

async fn receive_server_session_bootstrap<F>(
    mut reader: BoxedSessionReadHalf,
    policy: NetworkCallPolicy,
    authenticate: F,
) -> Result<
    (
        SessionWirePayload,
        ObjectRpcSessionDescriptor,
        SessionFrameReader<BoxedSessionReadHalf>,
    ),
    NetworkError,
>
where
    F: FnOnce(&crate::ObjectRpcRequest, &str, &[u8]) -> Result<(), NetworkError>,
{
    let deadline = SessionDeadline::from_policy(policy);
    let mut header_buffer = Vec::new();
    let frame = deadline
        .run(
            "read object RPC session bootstrap",
            read_transfer_encoded_frame_header(&mut reader, &mut header_buffer, policy.health),
        )
        .await?;
    let TransferEncodedWireFrameHeader::Control(encoded) = frame else {
        return Err(NetworkError::AuthDenied);
    };
    let signed: SignedSessionWireControl =
        crate::SinomoObjectCodec::network_frame().decode(&encoded)?;
    let SessionWirePayload::RequestHead {
        request,
        descriptor,
    } = &signed.payload
    else {
        return Err(NetworkError::AuthDenied);
    };
    let signed_descriptor = session_descriptor_from_request(request)?;
    if &signed_descriptor != descriptor {
        return Err(NetworkError::AuthDenied);
    }
    let expected_public_key = signed.proof.public_key.clone();
    let encoded_payload = crate::SinomoObjectCodec::network_frame().encode(&signed.payload)?;
    let expected_input = SessionFrameProofInput {
        session_id: descriptor.session_id.clone(),
        direction: SessionFrameDirection::InitiatorToResponder,
        sequence: 0,
        previous_digest: SESSION_ZERO_DIGEST,
        frame_header_digest: TransferDigest::blake3(&encoded_payload),
        payload_digest: None,
    };
    let previous_digest =
        verify_session_frame_proof(&signed.proof, &expected_public_key, &expected_input)?;
    // Membership, freshness, nonce replay, route authority, and the signature
    // over the descriptor-bearing request remain the daemon authenticator's
    // responsibility. Verify the claimed link proof first so an invalid frame
    // cannot consume a valid request nonce. Body bytes are not read before
    // both checks succeed. The request may remain origin-signed across relays
    // while this link's frames are signed by the immediate authorized relay.
    authenticate(request, &signed.proof.signer_node_id, &expected_public_key)?;
    let descriptor = descriptor.clone();
    let frame_reader = SessionFrameReader::after_verified_bootstrap(
        reader,
        descriptor.session_id.clone(),
        SessionFrameDirection::InitiatorToResponder,
        expected_public_key,
        policy,
        1,
        previous_digest,
        header_buffer,
    );
    Ok((signed.payload, descriptor, frame_reader))
}

/// Starts one client-side canonical RPC session on an already-open transport.
/// The request descriptor must be prepared before `request_auth` is produced.
/// The returned body owns the background read/write pumps and surfaces their
/// failures through `AsyncRead`.
pub(crate) async fn start_object_rpc_client_session(
    stream: BoxedSessionStream,
    mut prepared: PreparedSessionCall,
    request_auth: Vec<u8>,
    local_data_dir: SinomoDataDir,
    expected_responder_public_key: Vec<u8>,
    expected_response_origin_public_key: Vec<u8>,
    policy: NetworkCallPolicy,
) -> Result<ObjectRpcSessionResult, NetworkError> {
    if request_auth.is_empty() || prepared.request.auth.is_some() {
        return Err(NetworkError::Serialization {
            detail: "object RPC session requires one freshly produced request signature"
                .to_string(),
        });
    }
    prepared.request.auth = Some(request_auth);
    let request_uses_relay = prepared.request.route == crate::CLUSTER_RELAY_ROUTE;
    let session_id = prepared.descriptor.session_id.clone();
    let response_proof_session_id = prepared.response_proof_session_id.clone();
    let response_proof_public_key = expected_response_origin_public_key;
    let request_open = prepared.descriptor.request_body.clone();
    let (read_half, write_half) = tokio::io::split(stream);
    let writer = Arc::new(tokio::sync::Mutex::new(SessionFrameWriter::new(
        write_half,
        local_data_dir,
        session_id.clone(),
        SessionFrameDirection::InitiatorToResponder,
        policy,
    )?));
    let reader = SessionFrameReader::new(
        read_half,
        session_id,
        SessionFrameDirection::ResponderToInitiator,
        expected_responder_public_key,
        policy,
    );
    let outgoing = Arc::new(Mutex::new(SharedOutgoingTransfer {
        initialized: true,
        transfer_id: request_open.as_ref().map(|open| open.transfer_id.clone()),
        complete: request_open.is_none(),
    }));
    let (feedback_tx, feedback_rx) = tokio::sync::mpsc::channel(16);
    let (body_tx, mut incoming_body) = ObjectRpcIncomingBody::channel(None);
    let (head_tx, head_rx) = tokio::sync::oneshot::channel();
    let read_body_tx = body_tx.clone();
    let reader_writer = Arc::clone(&writer);
    let reader_outgoing = Arc::clone(&outgoing);
    let reader_task = tokio::spawn(async move {
        if let Err(error) = pump_incoming_session(
            reader,
            reader_writer,
            None,
            reader_outgoing,
            feedback_tx,
            Some(read_body_tx.clone()),
            head_tx,
            None,
        )
        .await
        {
            read_body_tx.fail(error.to_string());
        }
    });
    let keepalive_task = spawn_session_keepalive(Arc::clone(&writer), policy);

    send_session_payload(
        &writer,
        SessionWirePayload::RequestHead {
            request: prepared.request,
            descriptor: prepared.descriptor,
        },
    )
    .await?;

    let mut outgoing_task = if let Some(open) = request_open {
        let outgoing_writer = Arc::clone(&writer);
        Some(tokio::spawn(async move {
            pump_outgoing_body(outgoing_writer, prepared.body, open, feedback_rx, policy).await
        }))
    } else {
        drop(feedback_rx);
        None
    };

    let mut head_rx = head_rx;
    enum ClientSessionWait {
        Head(Result<SessionWirePayload, tokio::sync::oneshot::error::RecvError>),
        Outgoing(Result<Result<(), NetworkError>, tokio::task::JoinError>),
    }
    let first = if let Some(task) = outgoing_task.as_mut() {
        tokio::select! {
            head = &mut head_rx => ClientSessionWait::Head(head),
            outgoing = task => ClientSessionWait::Outgoing(outgoing),
        }
    } else {
        ClientSessionWait::Head((&mut head_rx).await)
    };
    let head = match first {
        ClientSessionWait::Head(head) => head.map_err(|_| NetworkError::Io {
            operation: "receive object RPC response session head".to_string(),
            detail: "session reader stopped before the response head".to_string(),
        })?,
        ClientSessionWait::Outgoing(outgoing) => {
            outgoing_task.take();
            match outgoing {
                Ok(Ok(())) => head_rx.await.map_err(|_| NetworkError::Io {
                    operation: "receive object RPC response session head".to_string(),
                    detail: "session reader stopped after the request body completed".to_string(),
                })?,
                Ok(Err(error)) => {
                    reader_task.abort();
                    keepalive_task.abort();
                    body_tx.fail(error.to_string());
                    return Err(error);
                }
                Err(source) => {
                    reader_task.abort();
                    keepalive_task.abort();
                    let error = NetworkError::Io {
                        operation: "send object RPC request body".to_string(),
                        detail: format!("request body task failed: {source}"),
                    };
                    body_tx.fail(error.to_string());
                    return Err(error);
                }
            }
        }
    };
    let SessionWirePayload::ResponseHead { response, body } = head else {
        return Err(NetworkError::Serialization {
            detail: "object RPC client received a non-response session head".to_string(),
        });
    };
    let response_origin_verified = response_head_origin_is_verified(
        &response,
        &response_proof_public_key,
        &response_proof_session_id,
    );
    if !response_origin_verified && !request_uses_relay {
        return Err(NetworkError::AuthDenied);
    }
    if body.is_none() {
        if let Some(task) = outgoing_task.take() {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    reader_task.abort();
                    keepalive_task.abort();
                    body_tx.fail(error.to_string());
                    return Err(error);
                }
                Err(source) => {
                    reader_task.abort();
                    keepalive_task.abort();
                    let error = NetworkError::Io {
                        operation: "finish object RPC request body".to_string(),
                        detail: format!("request body task failed: {source}"),
                    };
                    body_tx.fail(error.to_string());
                    return Err(error);
                }
            }
        }
        drop(incoming_body);
        incoming_body = ObjectRpcIncomingBody::empty();
    } else {
        incoming_body.total_bytes = body.as_ref().and_then(|open| open.total_bytes);
        incoming_body.require_finish_proof(BodyFinishProofExpectation {
            binding: response_body_finish_binding(&response_proof_session_id, &response)?,
            signer_node_id: None,
            public_key: response_proof_public_key,
        })?;
    }
    incoming_body.hold_completion_task(reader_task);
    incoming_body.hold_completion_task(keepalive_task);
    if let Some(task) = outgoing_task {
        let outgoing_body_tx = body_tx;
        incoming_body.hold_completion_task(tokio::spawn(async move {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => outgoing_body_tx.fail(error.to_string()),
                Err(source) => outgoing_body_tx.fail(format!(
                    "object RPC request body task failed after response head: {source}"
                )),
            }
        }));
    }
    Ok(ObjectRpcSessionResult::with_origin_verification(
        response,
        incoming_body,
        response_origin_verified,
    ))
}

/// Handle returned with an authenticated server request. Sending the response
/// starts its body producer while the background reader continues consuming
/// and acknowledging the request body.
pub struct ObjectRpcSessionResponder {
    writer: Arc<tokio::sync::Mutex<BoxedSessionWriter>>,
    outgoing: Arc<Mutex<SharedOutgoingTransfer>>,
    feedback: Option<tokio::sync::mpsc::Receiver<TransferControl>>,
    reader_task: Option<tokio::task::JoinHandle<()>>,
    session_id: TransferId,
    operation_id: Option<String>,
    policy: NetworkCallPolicy,
    keepalive_task: Option<tokio::task::JoinHandle<()>>,
}

impl fmt::Debug for ObjectRpcSessionResponder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectRpcSessionResponder")
            .field("session_id", &self.session_id)
            .field("operation_id", &self.operation_id)
            .finish_non_exhaustive()
    }
}

impl ObjectRpcSessionResponder {
    pub async fn respond(
        mut self,
        mut response: ObjectRpcSessionResponse,
    ) -> Result<(), NetworkError> {
        let response_had_origin_proof = response
            .response
            .metadata
            .contains_key(OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY);
        let existing_buffered_marker = response
            .response
            .metadata
            .get(OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY)
            .cloned();
        if let Some(marker) = existing_buffered_marker.as_deref()
            && !(marker == OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_VALUE
                && response_had_origin_proof
                && response.response.body.is_empty()
                && !response.body.is_empty())
        {
            return Err(NetworkError::Serialization {
                detail: "reserved buffered object RPC response marker is invalid on a local or malformed response"
                    .to_string(),
            });
        }
        let buffered_body_bytes = response.response.body.len();
        let can_spill_buffered_body = existing_buffered_marker.is_none()
            && !response_had_origin_proof
            && response.body.is_empty()
            && buffered_body_bytes != 0;
        if can_spill_buffered_body
            && buffered_body_bytes > crate::OBJECT_RPC_UNARY_ADAPTER_MAX_BODY_BYTES
        {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: buffered_body_bytes,
                max_bytes: crate::OBJECT_RPC_UNARY_ADAPTER_MAX_BODY_BYTES,
            });
        }
        let body_open = {
            // Keep the writer guard across exact signed-head sizing and the
            // actual write. A keepalive must not advance the transcript between
            // those two operations.
            let mut writer = self.writer.lock().await;
            let mut spilled_buffered_body = false;
            let mut inline_head_bytes = None;
            if can_spill_buffered_body && buffered_body_bytes >= MAX_TRANSFER_CONTROL_FRAME_BYTES {
                response.spill_buffered_response_body()?;
                spilled_buffered_body = true;
            } else {
                ensure_response_head_proof_with_writer(
                    &writer,
                    &self.session_id,
                    &mut response.response,
                )?;
                let inline_payload = SessionWirePayload::ResponseHead {
                    response: response.response.clone(),
                    body: None,
                };
                let measured = writer.encoded_control_len(inline_payload)?;
                inline_head_bytes = Some(measured);
                if measured > MAX_TRANSFER_CONTROL_FRAME_BYTES && can_spill_buffered_body {
                    response
                        .response
                        .metadata
                        .remove(OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY);
                    response.spill_buffered_response_body()?;
                    spilled_buffered_body = true;
                }
            }
            if spilled_buffered_body {
                ensure_response_head_proof_with_writer(
                    &writer,
                    &self.session_id,
                    &mut response.response,
                )?;
                tracing::debug!(
                    target: "sinomo.network.object_rpc",
                    route = %response.response.route,
                    inline_head_bytes = inline_head_bytes.unwrap_or(buffered_body_bytes),
                    frame_limit_bytes = MAX_TRANSFER_CONTROL_FRAME_BYTES,
                    transferred_body_bytes = response.body.total_bytes().unwrap_or_default(),
                    "spilling oversized buffered object RPC response into transfer body"
                );
            }
            let body_open = (!response.body.is_empty()).then(|| TransferOpen {
                transfer_id: TransferId::generate(),
                route: response.response.route.clone(),
                operation_id: self.operation_id.clone(),
                total_bytes: response.body.total_bytes(),
                expected_digest: response.body.known_digest(),
                replayable: response.body.is_replayable(),
                requested_window: TransferFlowWindow::default(),
            });
            if body_open.is_some() {
                response
                    .body
                    .ensure_local_finish_binding(response_body_finish_binding(
                        &self.session_id,
                        &response.response,
                    )?)?;
            }
            {
                let mut outgoing = self.outgoing.lock().map_err(|_| NetworkError::Io {
                    operation: "initialize object RPC response transfer".to_string(),
                    detail: "outgoing transfer state lock poisoned".to_string(),
                })?;
                outgoing.initialized = true;
                outgoing.transfer_id = body_open.as_ref().map(|open| open.transfer_id.clone());
                outgoing.complete = body_open.is_none();
            }
            let payload = SessionWirePayload::ResponseHead {
                response: response.response,
                body: body_open.clone(),
            };
            let head_bytes = writer.encoded_control_len(payload.clone())?;
            if head_bytes > MAX_TRANSFER_CONTROL_FRAME_BYTES {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: head_bytes,
                    max_bytes: MAX_TRANSFER_CONTROL_FRAME_BYTES,
                });
            }
            writer.send(payload).await?;
            body_open
        };
        if let Some(open) = body_open {
            let feedback = self.feedback.take().ok_or_else(|| NetworkError::Io {
                operation: "start object RPC response transfer".to_string(),
                detail: "session response feedback receiver is unavailable".to_string(),
            })?;
            pump_outgoing_body(
                Arc::clone(&self.writer),
                response.body,
                open,
                feedback,
                self.policy,
            )
            .await?;
        }
        // Dropping a JoinHandle detaches the request reader. It normally exits
        // after both transfers complete; the daemon connection owns the final
        // transport close.
        self.reader_task.take();
        if let Some(task) = self.keepalive_task.take() {
            task.abort();
        }
        Ok(())
    }
}

impl Drop for ObjectRpcSessionResponder {
    fn drop(&mut self) {
        if let Some(task) = self.reader_task.take() {
            task.abort();
        }
        if let Some(task) = self.keepalive_task.take() {
            task.abort();
        }
    }
}

/// Accepts and authenticates a server-side canonical session. The callback
/// must perform normal daemon request authentication and independently
/// authorize the claimed immediate-link node/key pair. No attachment bytes
/// are consumed before that callback and frame-zero proof both succeed.
pub async fn accept_object_rpc_server_session<F>(
    stream: BoxedSessionStream,
    local_data_dir: SinomoDataDir,
    policy: NetworkCallPolicy,
    authenticate: F,
) -> Result<(ObjectRpcSessionRequest, ObjectRpcSessionResponder), NetworkError>
where
    F: FnOnce(&crate::ObjectRpcRequest, &str, &[u8]) -> Result<(), NetworkError>,
{
    let (read_half, write_half) = tokio::io::split(stream);
    let (initial_head, descriptor, reader) =
        receive_server_session_bootstrap(read_half, policy, authenticate).await?;
    let SessionWirePayload::RequestHead { .. } = &initial_head else {
        unreachable!("bootstrap validates request head");
    };
    let writer = Arc::new(tokio::sync::Mutex::new(SessionFrameWriter::new(
        write_half,
        local_data_dir,
        descriptor.session_id.clone(),
        SessionFrameDirection::ResponderToInitiator,
        policy,
    )?));
    let outgoing = Arc::new(Mutex::new(SharedOutgoingTransfer::default()));
    let (feedback_tx, feedback_rx) = tokio::sync::mpsc::channel(16);
    let (body_tx, mut incoming_body) = if descriptor.request_body.is_some() {
        let (sender, body) = ObjectRpcIncomingBody::channel(
            descriptor
                .request_body
                .as_ref()
                .and_then(|open| open.total_bytes),
        );
        (Some(sender), body)
    } else {
        (None, ObjectRpcIncomingBody::empty())
    };
    if descriptor.request_body.is_some() {
        let SessionWirePayload::RequestHead { request, .. } = &initial_head else {
            unreachable!("bootstrap validates request head");
        };
        let expectation =
            signed_request_body_finish_expectation(request)?.ok_or(NetworkError::AuthDenied)?;
        incoming_body.require_finish_proof(expectation)?;
    }
    let (head_tx, head_rx) = tokio::sync::oneshot::channel();
    let reader_writer = Arc::clone(&writer);
    let reader_outgoing = Arc::clone(&outgoing);
    let failure_body_tx = body_tx.clone();
    let expected_open = descriptor.request_body.clone();
    let reader_task = tokio::spawn(async move {
        if let Err(error) = pump_incoming_session(
            reader,
            reader_writer,
            expected_open,
            reader_outgoing,
            feedback_tx,
            body_tx,
            head_tx,
            Some(initial_head),
        )
        .await
            && let Some(sender) = failure_body_tx
        {
            sender.fail(error.to_string());
        }
    });
    let keepalive_task = spawn_session_keepalive(Arc::clone(&writer), policy);
    let delivered_head = head_rx.await.map_err(|_| NetworkError::Io {
        operation: "accept object RPC request session head".to_string(),
        detail: "session reader stopped during request bootstrap".to_string(),
    })?;
    let SessionWirePayload::RequestHead { request, .. } = delivered_head else {
        unreachable!("server bootstrap delivers request head");
    };
    let responder = ObjectRpcSessionResponder {
        writer,
        outgoing,
        feedback: Some(feedback_rx),
        reader_task: Some(reader_task),
        session_id: descriptor.session_id,
        operation_id: request.operation_id().map(ToOwned::to_owned),
        policy,
        keepalive_task: Some(keepalive_task),
    };
    Ok((
        ObjectRpcSessionRequest::new(request, incoming_body),
        responder,
    ))
}

#[derive(Clone, Debug)]
struct ReplayableBytes {
    bytes: Arc<[u8]>,
}

#[derive(Clone, Debug)]
struct ReplayableFile {
    path: PathBuf,
    total_bytes: u64,
}

struct OnePassReader {
    reader: Option<Box<dyn Read + Send>>,
    total_bytes: Option<u64>,
}

struct OnePassAsyncReader {
    reader: Option<Pin<Box<dyn AsyncRead + Send>>>,
    total_bytes: Option<u64>,
}

impl fmt::Debug for OnePassAsyncReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OnePassAsyncReader")
            .field("available", &self.reader.is_some())
            .field("total_bytes", &self.total_bytes)
            .finish()
    }
}

impl fmt::Debug for OnePassReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OnePassReader")
            .field("available", &self.reader.is_some())
            .field("total_bytes", &self.total_bytes)
            .finish()
    }
}

#[derive(Debug)]
enum ObjectRpcBodySourceKind {
    Empty,
    Bytes(ReplayableBytes),
    File(ReplayableFile),
    Reader(Mutex<OnePassReader>),
    AsyncReader(Mutex<OnePassAsyncReader>),
}

#[derive(Debug)]
enum BodyFinishProofSource {
    Local(Option<BodyFinishProofBinding>),
    Forwarded(Arc<Mutex<IncomingBodyIntegrity>>),
}

/// A whole RPC attachment source.
///
/// `Bytes` and `File` sources can be replayed after a reconnect. This session
/// implementation restarts a new transport attempt at offset zero until a
/// persisted receiver checkpoint with bounded garbage collection exists. A
/// one-pass reader is consumed exactly once and reports an explicit replay
/// error instead of silently buffering the complete input.
pub struct ObjectRpcBodySource {
    kind: ObjectRpcBodySourceKind,
    lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
    finish_proof: BodyFinishProofSource,
}

impl fmt::Debug for ObjectRpcBodySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectRpcBodySource")
            .field("kind", &self.kind)
            .field(
                "lifetime_guard_count",
                &self
                    .lifetime_guards
                    .lock()
                    .map(|guards| guards.len())
                    .unwrap_or_default(),
            )
            .finish()
    }
}

impl Default for ObjectRpcBodySource {
    fn default() -> Self {
        Self::empty()
    }
}

impl ObjectRpcBodySource {
    pub fn empty() -> Self {
        Self {
            kind: ObjectRpcBodySourceKind::Empty,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
            finish_proof: BodyFinishProofSource::Local(None),
        }
    }

    pub fn bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: ObjectRpcBodySourceKind::Bytes(ReplayableBytes {
                bytes: Arc::from(bytes.into()),
            }),
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
            finish_proof: BodyFinishProofSource::Local(None),
        }
    }

    fn replace_empty_with_bytes(&mut self, bytes: Vec<u8>) -> Result<(), NetworkError> {
        if !self.is_empty() || !matches!(self.finish_proof, BodyFinishProofSource::Local(_)) {
            return Err(NetworkError::Serialization {
                detail: "cannot replace a non-empty or forwarded object RPC body source"
                    .to_string(),
            });
        }
        self.kind = ObjectRpcBodySourceKind::Bytes(ReplayableBytes {
            bytes: Arc::from(bytes),
        });
        self.finish_proof = BodyFinishProofSource::Local(None);
        Ok(())
    }

    pub fn file(path: impl Into<PathBuf>) -> Result<Self, NetworkError> {
        let path = path.into();
        let metadata = std::fs::metadata(&path).map_err(|source| NetworkError::Io {
            operation: format!("inspect object RPC body file {}", path.display()),
            detail: source.to_string(),
        })?;
        if !metadata.is_file() {
            return Err(NetworkError::Io {
                operation: format!("inspect object RPC body file {}", path.display()),
                detail: "body source is not a regular file".to_string(),
            });
        }
        Ok(Self {
            kind: ObjectRpcBodySourceKind::File(ReplayableFile {
                path,
                total_bytes: metadata.len(),
            }),
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
            finish_proof: BodyFinishProofSource::Local(None),
        })
    }

    pub fn reader(reader: Box<dyn Read + Send>, total_bytes: Option<u64>) -> Self {
        Self {
            kind: ObjectRpcBodySourceKind::Reader(Mutex::new(OnePassReader {
                reader: Some(reader),
                total_bytes,
            })),
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
            finish_proof: BodyFinishProofSource::Local(None),
        }
    }

    pub fn from_reader<R>(reader: R, total_bytes: Option<u64>) -> Self
    where
        R: Read + Send + 'static,
    {
        Self::reader(Box::new(reader), total_bytes)
    }

    pub fn async_reader<R>(reader: R, total_bytes: Option<u64>) -> Self
    where
        R: AsyncRead + Send + 'static,
    {
        Self {
            kind: ObjectRpcBodySourceKind::AsyncReader(Mutex::new(OnePassAsyncReader {
                reader: Some(Box::pin(reader)),
                total_bytes,
            })),
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
            finish_proof: BodyFinishProofSource::Local(None),
        }
    }

    pub fn from_incoming_body(body: ObjectRpcIncomingBody) -> Self {
        let total_bytes = body.total_bytes();
        Self::from_incoming_body_with_total(body, total_bytes)
    }

    pub fn from_incoming_body_with_total(
        body: ObjectRpcIncomingBody,
        total_bytes: Option<u64>,
    ) -> Self {
        let finish_proof = body
            .integrity
            .as_ref()
            .map(|integrity| BodyFinishProofSource::Forwarded(Arc::clone(integrity)))
            .unwrap_or(BodyFinishProofSource::Local(None));
        Self {
            kind: ObjectRpcBodySourceKind::AsyncReader(Mutex::new(OnePassAsyncReader {
                reader: Some(Box::pin(body)),
                total_bytes,
            })),
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
            finish_proof,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.kind, ObjectRpcBodySourceKind::Empty)
            || matches!(&self.kind, ObjectRpcBodySourceKind::Bytes(source) if source.bytes.is_empty())
            || matches!(&self.kind, ObjectRpcBodySourceKind::File(source) if source.total_bytes == 0)
            || matches!(&self.kind, ObjectRpcBodySourceKind::Reader(source)
                if source.lock().ok().and_then(|source| source.total_bytes) == Some(0))
            || matches!(&self.kind, ObjectRpcBodySourceKind::AsyncReader(source)
                if source.lock().ok().and_then(|source| source.total_bytes) == Some(0))
    }

    pub fn total_bytes(&self) -> Option<u64> {
        match &self.kind {
            ObjectRpcBodySourceKind::Empty => Some(0),
            ObjectRpcBodySourceKind::Bytes(source) => u64::try_from(source.bytes.len()).ok(),
            ObjectRpcBodySourceKind::File(source) => Some(source.total_bytes),
            ObjectRpcBodySourceKind::Reader(source) => {
                source.lock().ok().and_then(|source| source.total_bytes)
            }
            ObjectRpcBodySourceKind::AsyncReader(source) => {
                source.lock().ok().and_then(|source| source.total_bytes)
            }
        }
    }

    /// Keeps an opaque route/concurrency/resource guard alive until this body
    /// source and every producer opened from it have finished or been dropped.
    pub fn hold_lifetime_guard<T>(&mut self, guard: T) -> Result<(), NetworkError>
    where
        T: Any + Send,
    {
        self.lifetime_guards
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "attach object RPC body lifetime guard".to_string(),
                detail: "body lifetime guard lock poisoned".to_string(),
            })?
            .push(Box::new(guard));
        Ok(())
    }

    pub fn with_lifetime_guard<T>(mut self, guard: T) -> Result<Self, NetworkError>
    where
        T: Any + Send,
    {
        self.hold_lifetime_guard(guard)?;
        Ok(self)
    }

    pub(crate) fn is_replayable(&self) -> bool {
        !matches!(
            self.kind,
            ObjectRpcBodySourceKind::Reader(_) | ObjectRpcBodySourceKind::AsyncReader(_)
        )
    }

    pub(crate) fn known_digest(&self) -> Option<TransferDigest> {
        match &self.kind {
            ObjectRpcBodySourceKind::Empty => Some(TransferDigest::blake3(&[])),
            ObjectRpcBodySourceKind::Bytes(source) => {
                Some(TransferDigest::blake3(source.bytes.as_ref()))
            }
            ObjectRpcBodySourceKind::File(_)
            | ObjectRpcBodySourceKind::Reader(_)
            | ObjectRpcBodySourceKind::AsyncReader(_) => None,
        }
    }

    fn ensure_local_finish_binding(
        &mut self,
        binding: BodyFinishProofBinding,
    ) -> Result<(), NetworkError> {
        match &mut self.finish_proof {
            BodyFinishProofSource::Local(slot) => {
                if slot.is_none() {
                    *slot = Some(binding);
                }
                Ok(())
            }
            BodyFinishProofSource::Forwarded(_) => Ok(()),
        }
    }

    async fn finish_proof<W>(
        &self,
        writer: &Arc<tokio::sync::Mutex<SessionFrameWriter<W>>>,
        total_bytes: u64,
        digest: TransferDigest,
    ) -> Result<SignedBodyFinishProof, NetworkError>
    where
        W: AsyncWrite + Unpin,
    {
        match &self.finish_proof {
            BodyFinishProofSource::Local(Some(binding)) => Ok(writer
                .lock()
                .await
                .sign_body_finish(binding.clone(), total_bytes, digest)),
            BodyFinishProofSource::Local(None) => Err(NetworkError::Serialization {
                detail: "object RPC body is missing its authenticated finish binding".to_string(),
            }),
            BodyFinishProofSource::Forwarded(integrity) => integrity
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "forward object RPC body finish proof".to_string(),
                    detail: "incoming body integrity lock poisoned".to_string(),
                })?
                .finish_proof
                .clone()
                .ok_or_else(|| NetworkError::AuthDenied),
        }
    }

    fn open(&self) -> Result<Box<dyn Read + Send>, NetworkError> {
        match &self.kind {
            ObjectRpcBodySourceKind::Empty => Ok(Box::new(std::io::empty())),
            ObjectRpcBodySourceKind::Bytes(source) => {
                Ok(Box::new(ArcSliceReader::new(Arc::clone(&source.bytes))))
            }
            ObjectRpcBodySourceKind::File(source) => {
                let file = File::open(&source.path).map_err(|error| NetworkError::Io {
                    operation: format!("open object RPC body file {}", source.path.display()),
                    detail: error.to_string(),
                })?;
                Ok(Box::new(file))
            }
            ObjectRpcBodySourceKind::Reader(source) => source
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open one-pass object RPC body reader".to_string(),
                    detail: "reader lock poisoned".to_string(),
                })?
                .reader
                .take()
                .ok_or_else(|| body_replay_error("one-pass reader was already consumed")),
            ObjectRpcBodySourceKind::AsyncReader(_) => Err(body_replay_error(
                "asynchronous reader requires the async producer path",
            )),
        }
    }

    /// Collects this source for a bounded in-process or unary adapter.
    pub async fn collect_to_vec(self, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
        if let ObjectRpcBodySourceKind::AsyncReader(source) = &self.kind {
            let mut reader = source
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open asynchronous object RPC body reader".to_string(),
                    detail: "reader lock poisoned".to_string(),
                })?
                .reader
                .take()
                .ok_or_else(|| body_replay_error("asynchronous reader was already consumed"))?;
            return read_async_body_bounded(&mut reader, max_bytes).await;
        }
        tokio::task::spawn_blocking(move || {
            let mut reader = self.open()?;
            read_sync_body_bounded(&mut reader, max_bytes)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "join object RPC body collector".to_string(),
            detail: source.to_string(),
        })?
    }

    fn producer(&self, max_chunk_bytes: usize) -> Result<BodyProducer, NetworkError> {
        if let ObjectRpcBodySourceKind::AsyncReader(source) = &self.kind {
            let reader = source
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open asynchronous object RPC body reader".to_string(),
                    detail: "reader lock poisoned".to_string(),
                })?
                .reader
                .take()
                .ok_or_else(|| body_replay_error("asynchronous reader was already consumed"))?;
            return Ok(BodyProducer::Async(AsyncBodyProducer {
                reader,
                chunk_bytes: max_chunk_bytes.max(1),
                _lifetime_guards: Arc::clone(&self.lifetime_guards),
            }));
        }
        let reader = self.open()?;
        SyncBodyProducer::spawn(reader, max_chunk_bytes, Arc::clone(&self.lifetime_guards))
            .map(BodyProducer::Sync)
    }

    pub(crate) fn try_clone_replayable(&self) -> Option<Self> {
        let kind = match &self.kind {
            ObjectRpcBodySourceKind::Empty => ObjectRpcBodySourceKind::Empty,
            ObjectRpcBodySourceKind::Bytes(source) => {
                ObjectRpcBodySourceKind::Bytes(source.clone())
            }
            ObjectRpcBodySourceKind::File(source) => ObjectRpcBodySourceKind::File(source.clone()),
            ObjectRpcBodySourceKind::Reader(_) | ObjectRpcBodySourceKind::AsyncReader(_) => {
                return None;
            }
        };
        let finish_proof = match &self.finish_proof {
            BodyFinishProofSource::Local(binding) => BodyFinishProofSource::Local(binding.clone()),
            BodyFinishProofSource::Forwarded(integrity) => {
                BodyFinishProofSource::Forwarded(Arc::clone(integrity))
            }
        };
        Some(Self {
            kind,
            lifetime_guards: Arc::clone(&self.lifetime_guards),
            finish_proof,
        })
    }
}

fn body_replay_error(detail: &str) -> NetworkError {
    NetworkError::delivery_failed(
        "replay object RPC body source",
        NetworkDeliveryState::DefinitelyNotSent,
        detail,
    )
}

#[derive(Debug)]
struct ArcSliceReader {
    bytes: Arc<[u8]>,
    offset: usize,
}

impl ArcSliceReader {
    fn new(bytes: Arc<[u8]>) -> Self {
        Self { bytes, offset: 0 }
    }
}

impl Read for ArcSliceReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let remaining = &self.bytes[self.offset..];
        let read = remaining.len().min(output.len());
        output[..read].copy_from_slice(&remaining[..read]);
        self.offset += read;
        Ok(read)
    }
}

#[derive(Debug)]
enum IncomingBodyItem {
    Bytes(Vec<u8>),
    Error(std::io::Error),
}

#[derive(Debug)]
enum ObjectRpcIncomingBodyInner {
    Empty,
    Buffered(std::io::Cursor<Vec<u8>>),
    Channel {
        receiver: tokio::sync::mpsc::Receiver<IncomingBodyItem>,
        current: Option<std::io::Cursor<Vec<u8>>>,
    },
}

#[derive(Debug, Default)]
struct IncomingBodyIntegrity {
    expected_total_bytes: Option<u64>,
    expected_digest: Option<TransferDigest>,
    actual: Option<(u64, TransferDigest)>,
    expected_finish_proof: Option<BodyFinishProofExpectation>,
    finish_proof: Option<SignedBodyFinishProof>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BodyFinishProofExpectation {
    binding: BodyFinishProofBinding,
    signer_node_id: Option<String>,
    public_key: Vec<u8>,
}

fn validate_incoming_body_integrity(
    integrity: &IncomingBodyIntegrity,
    actual_total_bytes: u64,
    actual_digest: TransferDigest,
) -> Result<(), NetworkError> {
    if integrity
        .expected_total_bytes
        .is_some_and(|expected| expected != actual_total_bytes)
    {
        return Err(NetworkError::Serialization {
            detail: format!(
                "signed object RPC body length {} does not match received length {actual_total_bytes}",
                integrity.expected_total_bytes.unwrap_or_default()
            ),
        });
    }
    if integrity
        .expected_digest
        .is_some_and(|expected| expected != actual_digest)
    {
        return Err(NetworkError::AuthDenied);
    }
    Ok(())
}

fn validate_incoming_body_finish_proof(
    integrity: &IncomingBodyIntegrity,
    actual_total_bytes: u64,
    actual_digest: TransferDigest,
) -> Result<(), NetworkError> {
    let Some(expected) = integrity.expected_finish_proof.as_ref() else {
        return Ok(());
    };
    let proof = integrity
        .finish_proof
        .as_ref()
        .ok_or(NetworkError::AuthDenied)?;
    verify_body_finish_proof(
        proof,
        expected.signer_node_id.as_deref(),
        &expected.public_key,
        &expected.binding,
        actual_total_bytes,
        actual_digest,
    )
}

/// A received RPC attachment. It exposes ordinary asynchronous byte reads;
/// transport frames and chunk sizes remain private. Verified frames move
/// through a bounded in-memory channel first. If the application falls behind,
/// the wire reader lazily spills the remaining suffix to a temporary file so
/// application backpressure cannot block full-duplex control traffic.
#[derive(Debug)]
pub struct ObjectRpcIncomingBody {
    inner: ObjectRpcIncomingBodyInner,
    completion_tasks: Vec<tokio::task::JoinHandle<()>>,
    total_bytes: Option<u64>,
    integrity: Option<Arc<Mutex<IncomingBodyIntegrity>>>,
}

impl Default for ObjectRpcIncomingBody {
    fn default() -> Self {
        Self::empty()
    }
}

impl ObjectRpcIncomingBody {
    pub const fn empty() -> Self {
        Self {
            inner: ObjectRpcIncomingBodyInner::Empty,
            completion_tasks: Vec::new(),
            total_bytes: Some(0),
            integrity: None,
        }
    }

    pub fn bytes(bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            return Self::empty();
        }
        let total_bytes = u64::try_from(bytes.len()).ok();
        Self {
            inner: ObjectRpcIncomingBodyInner::Buffered(std::io::Cursor::new(bytes)),
            completion_tasks: Vec::new(),
            total_bytes,
            integrity: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.inner, ObjectRpcIncomingBodyInner::Empty) || self.total_bytes == Some(0)
    }

    pub const fn total_bytes(&self) -> Option<u64> {
        self.total_bytes
    }

    fn channel(total_bytes: Option<u64>) -> (IncomingBodySender, Self) {
        let (body_sender, receiver) = tokio::sync::mpsc::channel(BODY_READER_BUFFER_DEPTH);
        let (setup_sender, setup_receiver) = tokio::sync::oneshot::channel();
        let (progress_sender, progress_receiver) =
            tokio::sync::watch::channel(IncomingSpoolProgress::default());
        let integrity = Arc::new(Mutex::new(IncomingBodyIntegrity::default()));
        // The spool tailer is deliberately detached from the application
        // reader's abort-on-drop session tasks. If the application ignores a
        // request body, closing the channel makes this task discard its tail
        // while the transport reader continues hashing and acknowledging it.
        drop(tokio::spawn(deliver_incoming_spool(
            setup_receiver,
            progress_receiver,
            body_sender.clone(),
        )));
        (
            IncomingBodySender {
                setup: Arc::new(Mutex::new(Some(setup_sender))),
                progress: progress_sender,
                direct: Arc::new(Mutex::new(Some(body_sender))),
                integrity: Arc::clone(&integrity),
            },
            Self {
                inner: ObjectRpcIncomingBodyInner::Channel {
                    receiver,
                    current: None,
                },
                completion_tasks: Vec::new(),
                total_bytes,
                integrity: Some(integrity),
            },
        )
    }

    fn hold_completion_task(&mut self, task: tokio::task::JoinHandle<()>) {
        self.completion_tasks.push(task);
    }

    fn require_signed_contract(
        &mut self,
        expected_total_bytes: Option<u64>,
        expected_digest: Option<TransferDigest>,
    ) -> Result<(), NetworkError> {
        if let Some(integrity) = self.integrity.as_ref() {
            let mut integrity = integrity.lock().map_err(|_| NetworkError::Io {
                operation: "bind signed object RPC body contract".to_string(),
                detail: "incoming body integrity lock poisoned".to_string(),
            })?;
            if integrity
                .expected_total_bytes
                .is_some_and(|existing| Some(existing) != expected_total_bytes)
                || integrity
                    .expected_digest
                    .is_some_and(|existing| Some(existing) != expected_digest)
            {
                return Err(NetworkError::AuthDenied);
            }
            integrity.expected_total_bytes = expected_total_bytes;
            integrity.expected_digest = expected_digest;
            if let Some((actual_total_bytes, actual_digest)) = integrity.actual {
                validate_incoming_body_integrity(&integrity, actual_total_bytes, actual_digest)?;
            }
            return Ok(());
        }

        let bytes: &[u8] = match &self.inner {
            ObjectRpcIncomingBodyInner::Empty => &[],
            ObjectRpcIncomingBodyInner::Buffered(bytes) => bytes.get_ref(),
            ObjectRpcIncomingBodyInner::Channel { .. } => {
                return Err(NetworkError::Io {
                    operation: "bind signed object RPC body contract".to_string(),
                    detail: "channel body is missing its integrity state".to_string(),
                });
            }
        };
        let integrity = IncomingBodyIntegrity {
            expected_total_bytes,
            expected_digest,
            actual: None,
            ..IncomingBodyIntegrity::default()
        };
        validate_incoming_body_integrity(
            &integrity,
            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            TransferDigest::blake3(bytes),
        )
    }

    fn require_finish_proof(
        &mut self,
        expectation: BodyFinishProofExpectation,
    ) -> Result<(), NetworkError> {
        let Some(integrity) = self.integrity.as_ref() else {
            return if self.is_empty() {
                Ok(())
            } else {
                Err(NetworkError::AuthDenied)
            };
        };
        let mut integrity = integrity.lock().map_err(|_| NetworkError::Io {
            operation: "bind object RPC body finish proof".to_string(),
            detail: "incoming body integrity lock poisoned".to_string(),
        })?;
        if integrity
            .expected_finish_proof
            .as_ref()
            .is_some_and(|existing| existing != &expectation)
        {
            return Err(NetworkError::AuthDenied);
        }
        integrity.expected_finish_proof = Some(expectation);
        if let Some((actual_total_bytes, actual_digest)) = integrity.actual {
            validate_incoming_body_finish_proof(&integrity, actual_total_bytes, actual_digest)?;
        }
        Ok(())
    }

    pub async fn collect_to_vec(mut self, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
        let mut output = Vec::new();
        let mut buffer = vec![0_u8; BODY_READER_BUFFER_BYTES];
        loop {
            let read = self
                .read(&mut buffer)
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "read object RPC incoming body".to_string(),
                    detail: source.to_string(),
                })?;
            if read == 0 {
                return Ok(output);
            }
            let next = output
                .len()
                .checked_add(read)
                .ok_or(NetworkError::FrameTooLarge {
                    actual_bytes: usize::MAX,
                    max_bytes,
                })?;
            if next > max_bytes {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: next,
                    max_bytes,
                });
            }
            output
                .try_reserve(read)
                .map_err(|_| NetworkError::ResourceExhausted {
                    operation: "collect object RPC incoming body".to_string(),
                    requested_bytes: next,
                })?;
            output.extend_from_slice(&buffer[..read]);
        }
    }
}

impl Drop for ObjectRpcIncomingBody {
    fn drop(&mut self) {
        for task in &self.completion_tasks {
            task.abort();
        }
    }
}

impl AsyncRead for ObjectRpcIncomingBody {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        loop {
            match &mut this.inner {
                ObjectRpcIncomingBodyInner::Empty => return Poll::Ready(Ok(())),
                ObjectRpcIncomingBodyInner::Buffered(bytes) => {
                    let position = usize::try_from(bytes.position()).unwrap_or(usize::MAX);
                    if position >= bytes.get_ref().len() {
                        return Poll::Ready(Ok(()));
                    }
                    let read = output.remaining().min(bytes.get_ref().len() - position);
                    output.put_slice(&bytes.get_ref()[position..position + read]);
                    bytes.set_position((position + read) as u64);
                    return Poll::Ready(Ok(()));
                }
                ObjectRpcIncomingBodyInner::Channel { receiver, current } => {
                    if let Some(chunk) = current {
                        let position = usize::try_from(chunk.position()).unwrap_or(usize::MAX);
                        let bytes = chunk.get_ref();
                        if position < bytes.len() {
                            let read = output.remaining().min(bytes.len() - position);
                            output.put_slice(&bytes[position..position + read]);
                            chunk.set_position((position + read) as u64);
                            return Poll::Ready(Ok(()));
                        }
                        *current = None;
                    }
                    match receiver.poll_recv(context) {
                        Poll::Ready(Some(IncomingBodyItem::Bytes(bytes))) if bytes.is_empty() => {
                            continue;
                        }
                        Poll::Ready(Some(IncomingBodyItem::Bytes(bytes))) => {
                            *current = Some(std::io::Cursor::new(bytes));
                        }
                        Poll::Ready(Some(IncomingBodyItem::Error(error))) => {
                            return Poll::Ready(Err(error));
                        }
                        Poll::Ready(None) => return Poll::Ready(Ok(())),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct IncomingBodySender {
    setup: Arc<
        Mutex<Option<tokio::sync::oneshot::Sender<Result<Option<IncomingSpoolSetup>, String>>>>,
    >,
    progress: tokio::sync::watch::Sender<IncomingSpoolProgress>,
    direct: Arc<Mutex<Option<tokio::sync::mpsc::Sender<IncomingBodyItem>>>>,
    integrity: Arc<Mutex<IncomingBodyIntegrity>>,
}

#[derive(Debug)]
enum IncomingDirectDelivery {
    Delivered,
    Spill(Vec<u8>),
    ConsumerClosed,
}

impl IncomingBodySender {
    fn try_deliver(&self, bytes: Vec<u8>) -> IncomingDirectDelivery {
        let direct = match self.direct.lock() {
            Ok(direct) => direct.clone(),
            Err(_) => return IncomingDirectDelivery::ConsumerClosed,
        };
        let Some(direct) = direct else {
            return IncomingDirectDelivery::ConsumerClosed;
        };
        match direct.try_send(IncomingBodyItem::Bytes(bytes)) {
            Ok(()) => IncomingDirectDelivery::Delivered,
            Err(tokio::sync::mpsc::error::TrySendError::Full(IncomingBodyItem::Bytes(bytes))) => {
                IncomingDirectDelivery::Spill(bytes)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                IncomingDirectDelivery::ConsumerClosed
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(IncomingBodyItem::Error(_))) => {
                unreachable!("direct body delivery only sends byte items")
            }
        }
    }

    fn start_spool(&self, setup: IncomingSpoolSetup) -> Result<(), NetworkError> {
        let sender = self
            .setup
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "start object RPC incoming body spool".to_string(),
                detail: "incoming body spool setup lock poisoned".to_string(),
            })?
            .take()
            .ok_or_else(|| NetworkError::Serialization {
                detail: "object RPC incoming body spool started more than once".to_string(),
            })?;
        sender.send(Ok(Some(setup))).map_err(|_| NetworkError::Io {
            operation: "start object RPC incoming body spool".to_string(),
            detail: "incoming body spool tailer stopped before spill".to_string(),
        })
    }

    fn advance_available(&self, available_bytes: u64) -> Result<(), NetworkError> {
        let previous = self.progress.borrow().available_bytes;
        if available_bytes < previous {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "object RPC body available offset moved backwards from {previous} to {available_bytes}"
                ),
            });
        }
        self.progress.send_modify(|progress| {
            progress.available_bytes = available_bytes;
        });
        Ok(())
    }

    fn complete(
        &self,
        available_bytes: u64,
        actual_digest: TransferDigest,
        has_spool: bool,
        finish_proof: Option<SignedBodyFinishProof>,
    ) -> Result<(), NetworkError> {
        {
            let mut integrity = self.integrity.lock().map_err(|_| NetworkError::Io {
                operation: "verify signed object RPC body contract".to_string(),
                detail: "incoming body integrity lock poisoned".to_string(),
            })?;
            integrity.actual = Some((available_bytes, actual_digest));
            integrity.finish_proof = finish_proof;
            if let Err(error) =
                validate_incoming_body_integrity(&integrity, available_bytes, actual_digest)
                    .and_then(|()| {
                        validate_incoming_body_finish_proof(
                            &integrity,
                            available_bytes,
                            actual_digest,
                        )
                    })
            {
                drop(integrity);
                self.fail(error.to_string());
                return Err(error);
            }
        }
        if has_spool {
            self.advance_available(available_bytes)?;
        } else {
            let sender = self
                .setup
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "finish direct object RPC body delivery".to_string(),
                    detail: "incoming body spool setup lock poisoned".to_string(),
                })?
                .take()
                .ok_or_else(|| NetworkError::Serialization {
                    detail: "object RPC body completed without a direct or spill delivery path"
                        .to_string(),
                })?;
            sender.send(Ok(None)).map_err(|_| NetworkError::Io {
                operation: "finish direct object RPC body delivery".to_string(),
                detail: "incoming body delivery task stopped before completion".to_string(),
            })?;
        }
        self.progress.send_modify(|progress| {
            progress.complete = true;
        });
        if let Ok(mut direct) = self.direct.lock() {
            direct.take();
        }
        Ok(())
    }

    fn fail(&self, error: impl Into<String>) {
        let error = error.into();
        if let Ok(mut setup) = self.setup.lock()
            && let Some(sender) = setup.take()
        {
            let _ = sender.send(Err(error.clone()));
        }
        self.progress.send_modify(|progress| {
            progress.error = Some(error);
        });
        if let Ok(mut direct) = self.direct.lock() {
            direct.take();
        }
    }
}

async fn deliver_incoming_spool(
    setup: tokio::sync::oneshot::Receiver<Result<Option<IncomingSpoolSetup>, String>>,
    mut progress: tokio::sync::watch::Receiver<IncomingSpoolProgress>,
    sender: tokio::sync::mpsc::Sender<IncomingBodyItem>,
) {
    let mut spool = match setup.await {
        Ok(Ok(Some(setup))) => setup,
        Ok(Ok(None)) => return,
        Ok(Err(error)) => {
            let _ = sender
                .send(IncomingBodyItem::Error(std::io::Error::other(error)))
                .await;
            return;
        }
        Err(_) => return,
    };
    let mut delivered_bytes = spool.base_offset;
    loop {
        let current = progress.borrow().clone();
        if let Some(error) = current.error {
            let _ = sender
                .send(IncomingBodyItem::Error(std::io::Error::other(error)))
                .await;
            return;
        }
        while delivered_bytes < current.available_bytes {
            let remaining = current.available_bytes - delivered_bytes;
            let chunk_bytes = usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(BODY_READER_BUFFER_BYTES);
            let mut bytes = match try_zeroed_body_buffer(chunk_bytes) {
                Ok(bytes) => bytes,
                Err(error) => {
                    let _ = sender
                        .send(IncomingBodyItem::Error(std::io::Error::other(
                            error.to_string(),
                        )))
                        .await;
                    return;
                }
            };
            if let Err(source) = spool.file.read_exact(&mut bytes).await {
                let _ = sender
                    .send(IncomingBodyItem::Error(std::io::Error::new(
                        source.kind(),
                        format!("read available object RPC body spool: {source}"),
                    )))
                    .await;
                return;
            }
            delivered_bytes += u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if sender.send(IncomingBodyItem::Bytes(bytes)).await.is_err() {
                return;
            }
        }
        if current.complete {
            return;
        }
        if progress.changed().await.is_err() {
            return;
        }
    }
}

/// Client-side outgoing request head and optional body.
#[derive(Debug)]
pub struct ObjectRpcSessionCall {
    pub request: crate::ObjectRpcRequest,
    pub body: ObjectRpcBodySource,
}

impl ObjectRpcSessionCall {
    pub fn new(request: crate::ObjectRpcRequest, body: ObjectRpcBodySource) -> Self {
        Self { request, body }
    }

    pub fn buffered(request: crate::ObjectRpcRequest) -> Self {
        Self::new(request, ObjectRpcBodySource::empty())
    }

    pub(crate) fn try_clone_replayable(&self) -> Option<Self> {
        Some(Self {
            request: self.request.clone(),
            body: self.body.try_clone_replayable()?,
        })
    }

    /// Inserts the session descriptor into the request before daemon-auth
    /// signing. Route clients must call this before producing `request.auth`;
    /// mutating metadata after signing is deliberately unsupported.
    pub(crate) fn prepare_for_signing(mut self) -> Result<PreparedSessionCall, NetworkError> {
        if self.request.auth.is_some() {
            return Err(NetworkError::Serialization {
                detail: "object RPC session call must be prepared before request signing"
                    .to_string(),
            });
        }
        let descriptor = ObjectRpcSessionDescriptor::for_call(&self);
        self.request.metadata.insert(
            OBJECT_RPC_SESSION_METADATA_KEY.to_string(),
            descriptor.encode_metadata()?,
        );
        if descriptor.request_body.is_some() {
            self.body
                .ensure_local_finish_binding(request_body_finish_binding(
                    &self.request,
                    &descriptor,
                )?)?;
        }
        let response_proof_session_id = response_proof_session_id(&self.request, &descriptor)?;
        Ok(PreparedSessionCall {
            request: self.request,
            body: self.body,
            descriptor,
            response_proof_session_id,
        })
    }

    /// Reconstructs a prepared call around an already origin-signed request,
    /// preserving the exact signed descriptor for relay forwarding. The auth
    /// bytes are returned separately because the transport driver restores
    /// them without changing any signed request field.
    pub(crate) fn prepare_from_signed_request(
        mut self,
    ) -> Result<(PreparedSessionCall, Vec<u8>), NetworkError> {
        let auth = self
            .request
            .auth
            .take()
            .ok_or_else(|| NetworkError::Serialization {
                detail: "relayed object RPC session request is not signed".to_string(),
            })?;
        let descriptor = session_descriptor_from_request(&self.request)?;
        let open = descriptor.request_body.as_ref();
        if self.body.is_empty() != open.is_none() {
            return Err(NetworkError::Serialization {
                detail: "relayed object RPC session body does not match its signed descriptor"
                    .to_string(),
            });
        }
        if let Some(open) = open {
            if open.route != self.request.route
                || open.operation_id.as_deref() != self.request.operation_id()
            {
                return Err(NetworkError::AuthDenied);
            }
            if let (Some(declared), Some(actual)) = (open.total_bytes, self.body.total_bytes())
                && declared != actual
            {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "relayed object RPC body length {actual} does not match signed length {declared}"
                    ),
                });
            }
            if let (Some(declared), Some(actual)) = (open.expected_digest, self.body.known_digest())
                && declared != actual
            {
                return Err(NetworkError::AuthDenied);
            }
        }
        let response_proof_session_id = response_proof_session_id(&self.request, &descriptor)?;
        Ok((
            PreparedSessionCall {
                request: self.request,
                body: self.body,
                descriptor,
                response_proof_session_id,
            },
            auth,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct PreparedSessionCall {
    pub(crate) request: crate::ObjectRpcRequest,
    pub(crate) body: ObjectRpcBodySource,
    descriptor: ObjectRpcSessionDescriptor,
    response_proof_session_id: TransferId,
}

fn session_descriptor_from_request(
    request: &crate::ObjectRpcRequest,
) -> Result<ObjectRpcSessionDescriptor, NetworkError> {
    let encoded = request
        .metadata
        .get(OBJECT_RPC_SESSION_METADATA_KEY)
        .ok_or_else(|| NetworkError::Serialization {
            detail: "object RPC request is missing its signed session descriptor".to_string(),
        })?;
    ObjectRpcSessionDescriptor::decode_metadata(encoded)
}

/// Server-side authenticated request head and received body.
#[derive(Debug)]
pub struct ObjectRpcSessionRequest {
    pub request: crate::ObjectRpcRequest,
    pub body: ObjectRpcIncomingBody,
}

impl ObjectRpcSessionRequest {
    pub fn new(request: crate::ObjectRpcRequest, body: ObjectRpcIncomingBody) -> Self {
        Self { request, body }
    }

    pub fn buffered(request: crate::ObjectRpcRequest) -> Self {
        Self::new(request, ObjectRpcIncomingBody::empty())
    }

    /// Rebinds a received relay body to an already-signed inner request without
    /// consuming or copying it. The signed descriptor must agree with body
    /// presence, route, operation, declared length, and digest. Length/digest
    /// checks that require the final byte are enforced by the body reader at
    /// EOF before it reports successful completion.
    pub fn bind_signed_inner_request(
        mut self,
        request: crate::ObjectRpcRequest,
    ) -> Result<Self, NetworkError> {
        if request.auth.is_none() {
            return Err(NetworkError::AuthDenied);
        }
        let descriptor = session_descriptor_from_request(&request)?;
        let open = descriptor.request_body.as_ref();
        if self.body.is_empty() != open.is_none() {
            return Err(NetworkError::Serialization {
                detail: "relayed object RPC body does not match its signed inner descriptor"
                    .to_string(),
            });
        }
        if let Some(open) = open {
            if open.route != request.route || open.operation_id.as_deref() != request.operation_id()
            {
                return Err(NetworkError::AuthDenied);
            }
            if let (Some(declared), Some(outer_declared)) =
                (open.total_bytes, self.body.total_bytes())
                && declared != outer_declared
            {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "relayed object RPC body length {outer_declared} does not match signed inner length {declared}"
                    ),
                });
            }
            self.body
                .require_signed_contract(open.total_bytes, open.expected_digest)?;
            let expectation = signed_request_body_finish_expectation(&request)?
                .ok_or(NetworkError::AuthDenied)?;
            self.body.require_finish_proof(expectation)?;
        }
        self.request = request;
        Ok(self)
    }
}

/// Server-side response head and optional outgoing body.
#[derive(Debug)]
pub struct ObjectRpcSessionResponse {
    pub response: crate::ObjectRpcResponse,
    pub body: ObjectRpcBodySource,
}

impl ObjectRpcSessionResponse {
    pub fn new(response: crate::ObjectRpcResponse, body: ObjectRpcBodySource) -> Self {
        Self { response, body }
    }

    pub fn buffered(response: crate::ObjectRpcResponse) -> Self {
        Self::new(response, ObjectRpcBodySource::empty())
    }

    fn spill_buffered_response_body(&mut self) -> Result<(), NetworkError> {
        if !self.body.is_empty() || self.response.body.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.response.body);
        self.response.metadata.insert(
            OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY.to_string(),
            OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_VALUE.to_string(),
        );
        self.body.replace_empty_with_bytes(bytes)?;
        Ok(())
    }
}

/// Client-side received response head and body.
#[derive(Debug)]
pub struct ObjectRpcSessionResult {
    pub response: crate::ObjectRpcResponse,
    pub body: ObjectRpcIncomingBody,
    response_origin_verified: bool,
}

impl ObjectRpcSessionResult {
    pub fn new(response: crate::ObjectRpcResponse, body: ObjectRpcIncomingBody) -> Self {
        Self {
            response,
            body,
            response_origin_verified: true,
        }
    }

    pub fn buffered(response: crate::ObjectRpcResponse) -> Self {
        Self::new(response, ObjectRpcIncomingBody::empty())
    }

    /// Splits the application-visible response head and body while keeping
    /// transport verification state private to the routing layer.
    pub fn into_parts(self) -> (crate::ObjectRpcResponse, ObjectRpcIncomingBody) {
        (self.response, self.body)
    }

    fn with_origin_verification(
        response: crate::ObjectRpcResponse,
        body: ObjectRpcIncomingBody,
        response_origin_verified: bool,
    ) -> Self {
        Self {
            response,
            body,
            response_origin_verified,
        }
    }

    pub(crate) const fn response_origin_verified(&self) -> bool {
        self.response_origin_verified
    }

    pub(crate) async fn restore_large_buffered_response_body(
        mut self,
    ) -> Result<Self, NetworkError> {
        let Some(version) = self
            .response
            .metadata
            .get(OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY)
        else {
            return Ok(self);
        };
        if version != OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_VALUE
            || !self.response.body.is_empty()
            || self.body.is_empty()
        {
            return Err(NetworkError::Serialization {
                detail: "invalid buffered object RPC response transfer marker".to_string(),
            });
        }
        let body = std::mem::replace(&mut self.body, ObjectRpcIncomingBody::empty());
        self.response.body = body
            .collect_to_vec(crate::OBJECT_RPC_UNARY_ADAPTER_MAX_BODY_BYTES)
            .await?;
        self.response
            .metadata
            .remove(OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY);
        self.response
            .metadata
            .remove(OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY);
        Ok(self)
    }
}

enum BodyProducer {
    Sync(SyncBodyProducer),
    Async(AsyncBodyProducer),
}

impl BodyProducer {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        match self {
            Self::Sync(producer) => producer.next().await,
            Self::Async(producer) => producer.next().await,
        }
    }

    async fn finish(self) -> Result<(), NetworkError> {
        match self {
            Self::Sync(producer) => producer.finish().await,
            Self::Async(_) => Ok(()),
        }
    }
}

struct AsyncBodyProducer {
    reader: Pin<Box<dyn AsyncRead + Send>>,
    chunk_bytes: usize,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl AsyncBodyProducer {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        let mut bytes = try_zeroed_body_buffer(self.chunk_bytes)?;
        let read = self
            .reader
            .read(&mut bytes)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "read asynchronous object RPC body source".to_string(),
                detail: source.to_string(),
            })?;
        if read == 0 {
            return Ok(None);
        }
        bytes.truncate(read);
        Ok(Some(bytes))
    }
}

struct SyncBodyProducer {
    receiver: tokio::sync::mpsc::Receiver<Result<Vec<u8>, NetworkError>>,
    task: tokio::task::JoinHandle<()>,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl SyncBodyProducer {
    fn spawn(
        mut reader: Box<dyn Read + Send>,
        max_chunk_bytes: usize,
        lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
    ) -> Result<Self, NetworkError> {
        let chunk_bytes = max_chunk_bytes.max(1);
        let (sender, receiver) = tokio::sync::mpsc::channel(BODY_READER_BUFFER_DEPTH);
        let task = tokio::task::spawn_blocking(move || {
            loop {
                let mut bytes = match try_zeroed_body_buffer(chunk_bytes) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        let _ = sender.blocking_send(Err(error));
                        return;
                    }
                };
                let read = match reader.read(&mut bytes) {
                    Ok(read) => read,
                    Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(source) => {
                        let _ = sender.blocking_send(Err(NetworkError::Io {
                            operation: "read synchronous object RPC body source".to_string(),
                            detail: source.to_string(),
                        }));
                        return;
                    }
                };
                bytes.truncate(read);
                if sender.blocking_send(Ok(bytes)).is_err() || read == 0 {
                    return;
                }
            }
        });
        Ok(Self {
            receiver,
            task,
            _lifetime_guards: lifetime_guards,
        })
    }

    async fn next(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        match self.receiver.recv().await {
            Some(Ok(bytes)) if bytes.is_empty() => Ok(None),
            Some(Ok(bytes)) => Ok(Some(bytes)),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }

    async fn finish(self) -> Result<(), NetworkError> {
        self.task.await.map_err(|source| NetworkError::Io {
            operation: "join object RPC body producer".to_string(),
            detail: source.to_string(),
        })
    }
}

fn try_zeroed_body_buffer(len: usize) -> Result<Vec<u8>, NetworkError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| NetworkError::ResourceExhausted {
            operation: "allocate object RPC body producer buffer".to_string(),
            requested_bytes: len,
        })?;
    bytes.resize(len, 0);
    Ok(bytes)
}

fn read_sync_body_bounded(
    reader: &mut Box<dyn Read + Send>,
    max_bytes: usize,
) -> Result<Vec<u8>, NetworkError> {
    let mut output = Vec::new();
    let mut buffer = [0_u8; BODY_READER_BUFFER_BYTES];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| NetworkError::Io {
                operation: "collect synchronous object RPC body source".to_string(),
                detail: source.to_string(),
            })?;
        if read == 0 {
            return Ok(output);
        }
        let next = output
            .len()
            .checked_add(read)
            .ok_or(NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes,
            })?;
        if next > max_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: next,
                max_bytes,
            });
        }
        output
            .try_reserve(read)
            .map_err(|_| NetworkError::ResourceExhausted {
                operation: "collect synchronous object RPC body source".to_string(),
                requested_bytes: next,
            })?;
        output.extend_from_slice(&buffer[..read]);
    }
}

async fn read_async_body_bounded(
    reader: &mut Pin<Box<dyn AsyncRead + Send>>,
    max_bytes: usize,
) -> Result<Vec<u8>, NetworkError> {
    let mut output = Vec::new();
    let mut buffer = vec![0_u8; BODY_READER_BUFFER_BYTES];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "collect asynchronous object RPC body source".to_string(),
                detail: source.to_string(),
            })?;
        if read == 0 {
            return Ok(output);
        }
        let next = output
            .len()
            .checked_add(read)
            .ok_or(NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes,
            })?;
        if next > max_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: next,
                max_bytes,
            });
        }
        output
            .try_reserve(read)
            .map_err(|_| NetworkError::ResourceExhausted {
                operation: "collect asynchronous object RPC body source".to_string(),
                requested_bytes: next,
            })?;
        output.extend_from_slice(&buffer[..read]);
    }
}

#[cfg(test)]
mod body_tests {
    use super::*;
    use crate::daemon_auth::{
        SignedDaemonCommand, daemon_rpc_body_sha256, sign_daemon_rpc_request_from_data_dir,
        sign_session_frame_proof_from_data_dir,
    };
    use crate::transfer::TransferReceipt;
    use std::io::Cursor;
    use tokio::io::AsyncWriteExt as _;

    fn proof_public_key(data_dir: &SinomoDataDir) -> Vec<u8> {
        sign_session_frame_proof_from_data_dir(
            data_dir,
            SessionFrameProofInput {
                session_id: TransferId::generate(),
                direction: SessionFrameDirection::InitiatorToResponder,
                sequence: 0,
                previous_digest: SESSION_ZERO_DIGEST,
                frame_header_digest: TransferDigest::blake3(b"public-key-probe"),
                payload_digest: None,
            },
        )
        .expect("session proof identity")
        .public_key
    }

    fn authenticate_test_request(
        request: &crate::ObjectRpcRequest,
        claimed_link_node_id: &str,
        claimed_link_public_key: &[u8],
    ) -> Result<(), NetworkError> {
        let auth = request.auth.as_ref().ok_or(NetworkError::AuthDenied)?;
        let envelope: SignedDaemonCommand =
            crate::SinomoObjectCodec::network_frame().decode(auth)?;
        if envelope.rpc_body_sha256.as_deref() != Some(daemon_rpc_body_sha256(request).as_slice()) {
            return Err(NetworkError::AuthDenied);
        }
        if envelope.node_id != claimed_link_node_id
            || envelope.public_key != claimed_link_public_key
        {
            return Err(NetworkError::AuthDenied);
        }
        Ok(())
    }

    #[tokio::test]
    async fn empty_sources_and_bodies_are_defaults() {
        assert!(ObjectRpcBodySource::default().is_empty());
        assert!(ObjectRpcIncomingBody::default().is_empty());
        assert_eq!(
            ObjectRpcBodySource::default()
                .collect_to_vec(1)
                .await
                .expect("empty source"),
            Vec::<u8>::new()
        );
    }

    #[tokio::test]
    async fn bounded_source_collection_rejects_oversize_input() {
        let source = ObjectRpcBodySource::from_reader(Cursor::new(vec![7_u8; 1024]), Some(1024));
        let error = source
            .collect_to_vec(100)
            .await
            .expect_err("body must exceed compatibility cap");
        assert!(matches!(
            error,
            NetworkError::FrameTooLarge { max_bytes: 100, .. }
        ));
    }

    #[test]
    fn replayable_byte_sources_reopen_from_the_start() {
        let source = ObjectRpcBodySource::bytes(b"abcdefgh".to_vec());
        let mut replayed = source.open().expect("open replayable bytes");
        let mut bytes = Vec::new();
        replayed.read_to_end(&mut bytes).expect("read bytes");
        assert_eq!(bytes, b"abcdefgh");
    }

    #[test]
    fn one_pass_sources_fail_explicitly_on_a_second_attempt() {
        let source = ObjectRpcBodySource::from_reader(Cursor::new(b"one pass".to_vec()), None);
        source.open().expect("first one-pass attempt");
        let error = source.open().err().expect("one-pass replay must fail");
        assert!(error.to_string().contains("already consumed"));
    }

    #[tokio::test]
    async fn incoming_body_delivers_directly_without_creating_a_spool() {
        let root = sinomo_testdir::TestDir::new("sinomo-session-body-direct");
        let bytes = b"hello world".to_vec();
        let digest = TransferDigest::blake3(&bytes);
        let (sender, body) = ObjectRpcIncomingBody::channel(Some(bytes.len() as u64));
        let ObjectRpcIncomingBodyInner::Channel { receiver, .. } = &body.inner else {
            panic!("incoming body must use the bounded delivery channel");
        };
        assert_eq!(receiver.max_capacity(), BODY_READER_BUFFER_DEPTH);
        assert!(matches!(
            sender.try_deliver(bytes.clone()),
            IncomingDirectDelivery::Delivered
        ));
        sender
            .complete(bytes.len() as u64, digest, false, None)
            .expect("complete direct body");
        drop(sender);
        assert_eq!(
            body.collect_to_vec(32)
                .await
                .expect("collect incoming body"),
            bytes
        );
        assert!(
            !root.path().join("network-transfers").exists(),
            "a body consumed within the bounded channel must not touch disk"
        );
    }

    #[tokio::test]
    async fn incoming_body_lazily_spills_after_the_bounded_channel_fills() {
        let root = sinomo_testdir::TestDir::new("sinomo-session-body-spill");
        let bytes = b"abcdef".to_vec();
        let digest = TransferDigest::blake3(&bytes);
        let (sender, body) = ObjectRpcIncomingBody::channel(Some(bytes.len() as u64));
        assert!(matches!(
            sender.try_deliver(b"a".to_vec()),
            IncomingDirectDelivery::Delivered
        ));
        assert!(matches!(
            sender.try_deliver(b"b".to_vec()),
            IncomingDirectDelivery::Delivered
        ));
        let IncomingDirectDelivery::Spill(spill) = sender.try_deliver(b"cdef".to_vec()) else {
            panic!("the third queued frame must spill");
        };
        let (mut staging, setup) = SessionTransferStagingFile::create(
            root.path().to_path_buf(),
            TransferId::generate(),
            2,
        )
        .await
        .expect("create body spool");
        sender.start_spool(setup).expect("start body spool");
        staging.append(spill).await.expect("append spilled suffix");
        sender.advance_available(6).expect("publish spilled suffix");
        sender
            .complete(bytes.len() as u64, digest, true, None)
            .expect("complete spilled body");
        drop(sender);
        drop(staging);
        assert_eq!(
            body.collect_to_vec(32)
                .await
                .expect("collect spilled incoming body"),
            bytes
        );
        let spool_dir = root.path().join("network-transfers");
        assert_eq!(
            std::fs::read_dir(spool_dir)
                .expect("read spool directory")
                .count(),
            0,
            "the lazy spill file must be removed after both handles close"
        );
    }

    #[tokio::test]
    async fn sync_reader_bridge_uses_a_bounded_channel() {
        let source = ObjectRpcBodySource::from_reader(
            Cursor::new(vec![9_u8; BODY_READER_BUFFER_BYTES * 6]),
            None,
        );
        let mut producer = source.producer(BODY_READER_BUFFER_BYTES).expect("producer");
        let BodyProducer::Sync(sync) = &producer else {
            panic!("synchronous source must use synchronous producer");
        };
        assert_eq!(sync.receiver.max_capacity(), BODY_READER_BUFFER_DEPTH);
        let mut chunks = 0;
        while producer.next().await.expect("producer chunk").is_some() {
            chunks += 1;
        }
        producer.finish().await.expect("producer join");
        assert_eq!(chunks, 6);
    }

    #[test]
    fn session_descriptor_is_signed_and_preserved_for_replay() {
        let root = sinomo_testdir::TestDir::new("sinomo-session-signed-descriptor");
        let data_dir = SinomoDataDir::new(root.path());
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/signed-descriptor",
            "test",
            &"head",
        )
        .expect("request");
        let unsigned_digest = daemon_rpc_body_sha256(&request);
        let prepared =
            ObjectRpcSessionCall::new(request, ObjectRpcBodySource::bytes(b"signed body".to_vec()))
                .prepare_for_signing()
                .expect("prepare call");
        assert_ne!(unsigned_digest, daemon_rpc_body_sha256(&prepared.request));
        let auth = sign_daemon_rpc_request_from_data_dir(&data_dir, &prepared.request)
            .expect("sign descriptor-bearing request");
        let envelope: SignedDaemonCommand = crate::SinomoObjectCodec::network_frame()
            .decode(&auth)
            .expect("auth envelope");
        let mut signed_request = prepared.request.clone();
        signed_request.auth = Some(auth.clone());
        authenticate_test_request(&signed_request, &envelope.node_id, &envelope.public_key)
            .expect("descriptor-bearing signature");
        assert!(
            authenticate_test_request(&signed_request, "forged-link-node", &envelope.public_key)
                .is_err(),
            "an authorized key must not be able to forge the audited link node id"
        );

        let replay_body = prepared
            .body
            .try_clone_replayable()
            .expect("byte body is replayable");
        let (reconstructed, returned_auth) =
            ObjectRpcSessionCall::new(signed_request.clone(), replay_body)
                .prepare_from_signed_request()
                .expect("reconstruct signed relay call");
        assert_eq!(returned_auth, auth);
        assert_eq!(reconstructed.descriptor, prepared.descriptor);

        let mismatched_operation = signed_request
            .clone()
            .with_operation_id("different-operation");
        let replay_body = prepared
            .body
            .try_clone_replayable()
            .expect("byte body is replayable");
        assert!(matches!(
            ObjectRpcSessionCall::new(mismatched_operation, replay_body)
                .prepare_from_signed_request(),
            Err(NetworkError::AuthDenied)
        ));

        let mut tampered = signed_request;
        let replacement_descriptor =
            ObjectRpcSessionDescriptor::for_call(&ObjectRpcSessionCall::buffered(tampered.clone()))
                .encode_metadata()
                .expect("replacement descriptor");
        tampered.metadata.insert(
            OBJECT_RPC_SESSION_METADATA_KEY.to_string(),
            replacement_descriptor,
        );
        assert!(
            authenticate_test_request(&tampered, &envelope.node_id, &envelope.public_key).is_err()
        );
    }

    #[tokio::test]
    async fn signed_inner_body_contract_is_checked_without_prebuffering() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-inner-contract-client");
        let spool_root = sinomo_testdir::TestDir::new("sinomo-session-inner-contract-spool");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let inner =
            crate::ObjectRpcRequest::encode_body("/test/session/signed-inner", "test", &"head")
                .expect("inner request");
        let prepared =
            ObjectRpcSessionCall::new(inner, ObjectRpcBodySource::bytes(b"good".to_vec()))
                .prepare_for_signing()
                .expect("prepare inner request");
        let mut signed_inner = prepared.request.clone();
        signed_inner.auth = Some(
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("sign inner request"),
        );
        let outer =
            crate::ObjectRpcRequest::encode_body(crate::CLUSTER_RELAY_ROUTE, "test", &"outer")
                .expect("outer request");
        let (sender, body) = ObjectRpcIncomingBody::channel(Some(4));
        let bound = ObjectRpcSessionRequest::new(outer, body)
            .bind_signed_inner_request(signed_inner)
            .expect("bind signed inner contract before consuming body");
        let (mut staging, setup) = SessionTransferStagingFile::create(
            spool_root.path().to_path_buf(),
            TransferId::generate(),
            0,
        )
        .await
        .expect("create body spool");
        sender.start_spool(setup).expect("start body spool");
        staging
            .append(b"evil".to_vec())
            .await
            .expect("append tampered bytes");
        sender.advance_available(4).expect("publish bytes");
        sender
            .complete(4, TransferDigest::blake3(b"evil"), true, None)
            .expect_err("signed digest must reject tampered bytes");
        drop(sender);
        drop(staging);

        let error = bound
            .body
            .collect_to_vec(16)
            .await
            .expect_err("signed inner digest must reject tampered relay bytes at EOF");
        assert!(error.to_string().contains("authorization"));
    }

    #[tokio::test]
    async fn relay_finish_proof_rejects_same_length_body_substitution() {
        let origin_root = sinomo_testdir::TestDir::new("sinomo-session-relay-proof-origin");
        let origin_data_dir = SinomoDataDir::new(origin_root.path());
        let inner =
            crate::ObjectRpcRequest::encode_body("/test/session/relay-proof", "test", &"head")
                .expect("inner request");
        let prepared = ObjectRpcSessionCall::new(
            inner,
            ObjectRpcBodySource::from_reader(Cursor::new(b"good".to_vec()), Some(4)),
        )
        .prepare_for_signing()
        .expect("prepare origin request");
        let binding = request_body_finish_binding(&prepared.request, &prepared.descriptor)
            .expect("origin body binding");
        let good_digest = TransferDigest::blake3(b"good");
        let proof = SessionFrameSigner::from_data_dir(&origin_data_dir)
            .expect("origin proof signer")
            .sign_body_finish(BodyFinishProofInput {
                binding,
                total_bytes: 4,
                digest: good_digest,
            });
        let mut signed_inner = prepared.request;
        signed_inner.auth = Some(
            sign_daemon_rpc_request_from_data_dir(&origin_data_dir, &signed_inner)
                .expect("sign inner request"),
        );
        let outer =
            crate::ObjectRpcRequest::encode_body(crate::CLUSTER_RELAY_ROUTE, "test", &"outer")
                .expect("outer request");
        let (sender, body) = ObjectRpcIncomingBody::channel(Some(4));
        let bound = ObjectRpcSessionRequest::new(outer, body)
            .bind_signed_inner_request(signed_inner)
            .expect("bind origin proof contract");
        assert!(matches!(
            sender.try_deliver(b"evil".to_vec()),
            IncomingDirectDelivery::Delivered
        ));
        let error = sender
            .complete(4, TransferDigest::blake3(b"evil"), false, Some(proof))
            .expect_err("same-length relay substitution must fail origin proof");
        assert!(matches!(error, NetworkError::AuthDenied));
        drop(sender);
        assert!(
            bound.body.collect_to_vec(16).await.is_err(),
            "the application reader must observe the rejected finish proof"
        );
    }

    #[tokio::test]
    async fn relay_body_source_preserves_the_origin_finish_proof() {
        let origin_root = sinomo_testdir::TestDir::new("sinomo-session-forward-proof-origin");
        let relay_root = sinomo_testdir::TestDir::new("sinomo-session-forward-proof-relay");
        let origin_data_dir = SinomoDataDir::new(origin_root.path());
        let relay_data_dir = SinomoDataDir::new(relay_root.path());
        let inner =
            crate::ObjectRpcRequest::encode_body("/test/session/forward-proof", "test", &"head")
                .expect("inner request");
        let prepared = ObjectRpcSessionCall::new(
            inner,
            ObjectRpcBodySource::from_reader(Cursor::new(b"body".to_vec()), Some(4)),
        )
        .prepare_for_signing()
        .expect("prepare origin request");
        let binding = request_body_finish_binding(&prepared.request, &prepared.descriptor)
            .expect("origin body binding");
        let digest = TransferDigest::blake3(b"body");
        let origin_proof = SessionFrameSigner::from_data_dir(&origin_data_dir)
            .expect("origin proof signer")
            .sign_body_finish(BodyFinishProofInput {
                binding,
                total_bytes: 4,
                digest,
            });
        let mut signed_inner = prepared.request;
        signed_inner.auth = Some(
            sign_daemon_rpc_request_from_data_dir(&origin_data_dir, &signed_inner)
                .expect("sign inner request"),
        );
        let outer =
            crate::ObjectRpcRequest::encode_body(crate::CLUSTER_RELAY_ROUTE, "test", &"outer")
                .expect("outer request");
        let (sender, body) = ObjectRpcIncomingBody::channel(Some(4));
        let bound = ObjectRpcSessionRequest::new(outer, body)
            .bind_signed_inner_request(signed_inner)
            .expect("bind origin proof contract");
        assert!(matches!(
            sender.try_deliver(b"body".to_vec()),
            IncomingDirectDelivery::Delivered
        ));
        sender
            .complete(4, digest, false, Some(origin_proof.clone()))
            .expect("complete authenticated origin body");
        drop(sender);

        let source = ObjectRpcBodySource::from_incoming_body(bound.body);
        let (stream, _peer) = tokio::io::duplex(4096);
        let relay_writer = Arc::new(tokio::sync::Mutex::new(
            SessionFrameWriter::new(
                stream,
                relay_data_dir,
                TransferId::generate(),
                SessionFrameDirection::InitiatorToResponder,
                NetworkCallPolicy::default(),
            )
            .expect("relay writer"),
        ));
        let forwarded = source
            .finish_proof(&relay_writer, 4, digest)
            .await
            .expect("forward origin proof");
        assert_eq!(forwarded, origin_proof);
    }

    #[tokio::test]
    async fn response_finish_proof_rejects_wrong_target_and_head_binding() {
        let target_root = sinomo_testdir::TestDir::new("sinomo-session-response-proof-target");
        let attacker_root = sinomo_testdir::TestDir::new("sinomo-session-response-proof-attacker");
        let target_data_dir = SinomoDataDir::new(target_root.path());
        let attacker_data_dir = SinomoDataDir::new(attacker_root.path());
        let session_id = TransferId::generate();
        let response =
            crate::ObjectRpcResponse::encode_body("/test/session/response-proof", &"response-head")
                .expect("response head");
        let binding =
            response_body_finish_binding(&session_id, &response).expect("response binding");
        let digest = TransferDigest::blake3(b"body");
        let target_proof = SessionFrameSigner::from_data_dir(&target_data_dir)
            .expect("target proof signer")
            .sign_body_finish(BodyFinishProofInput {
                binding: binding.clone(),
                total_bytes: 4,
                digest,
            });
        let attacker_proof = SessionFrameSigner::from_data_dir(&attacker_data_dir)
            .expect("attacker proof signer")
            .sign_body_finish(BodyFinishProofInput {
                binding: binding.clone(),
                total_bytes: 4,
                digest,
            });

        let (wrong_signer_sender, mut wrong_signer_body) = ObjectRpcIncomingBody::channel(Some(4));
        wrong_signer_body
            .require_finish_proof(BodyFinishProofExpectation {
                binding: binding.clone(),
                signer_node_id: None,
                public_key: proof_public_key(&target_data_dir),
            })
            .expect("target response expectation");
        assert!(matches!(
            wrong_signer_sender.try_deliver(b"body".to_vec()),
            IncomingDirectDelivery::Delivered
        ));
        assert!(matches!(
            wrong_signer_sender.complete(4, digest, false, Some(attacker_proof)),
            Err(NetworkError::AuthDenied)
        ));
        drop(wrong_signer_sender);
        drop(wrong_signer_body);

        let changed_response =
            crate::ObjectRpcResponse::encode_body("/test/session/response-proof", &"changed-head")
                .expect("changed response head");
        let wrong_binding = response_body_finish_binding(&session_id, &changed_response)
            .expect("changed response binding");
        let (wrong_head_sender, mut wrong_head_body) = ObjectRpcIncomingBody::channel(Some(4));
        wrong_head_body
            .require_finish_proof(BodyFinishProofExpectation {
                binding: wrong_binding,
                signer_node_id: None,
                public_key: proof_public_key(&target_data_dir),
            })
            .expect("changed response expectation");
        assert!(matches!(
            wrong_head_sender.try_deliver(b"body".to_vec()),
            IncomingDirectDelivery::Delivered
        ));
        assert!(matches!(
            wrong_head_sender.complete(4, digest, false, Some(target_proof)),
            Err(NetworkError::AuthDenied)
        ));
    }

    #[tokio::test]
    async fn empty_response_head_proof_survives_relay_and_rejects_tamper() {
        let target_root = sinomo_testdir::TestDir::new("sinomo-session-head-proof-target");
        let relay_root = sinomo_testdir::TestDir::new("sinomo-session-head-proof-relay");
        let target_data_dir = SinomoDataDir::new(target_root.path());
        let relay_data_dir = SinomoDataDir::new(relay_root.path());
        let target_public_key = proof_public_key(&target_data_dir);
        let relay_public_key = proof_public_key(&relay_data_dir);
        let logical_session_id = TransferId::generate();
        let mut response = crate::ObjectRpcResponse::from_encoded_body(
            "/test/session/empty-head-proof",
            b"same-size-head".to_vec(),
        );
        let (target_stream, _target_peer) = tokio::io::duplex(4096);
        let target_writer = Arc::new(tokio::sync::Mutex::new(
            SessionFrameWriter::new(
                target_stream,
                target_data_dir,
                logical_session_id.clone(),
                SessionFrameDirection::ResponderToInitiator,
                NetworkCallPolicy::default(),
            )
            .expect("target writer"),
        ));
        ensure_response_head_proof(&target_writer, &logical_session_id, &mut response)
            .await
            .expect("target response proof");
        assert!(response_head_origin_is_verified(
            &response,
            &target_public_key,
            &logical_session_id,
        ));
        let original_attestation = response
            .metadata
            .get(OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY)
            .cloned()
            .expect("response attestation");

        let (relay_stream, _relay_peer) = tokio::io::duplex(4096);
        let relay_writer = Arc::new(tokio::sync::Mutex::new(
            SessionFrameWriter::new(
                relay_stream,
                relay_data_dir,
                TransferId::generate(),
                SessionFrameDirection::ResponderToInitiator,
                NetworkCallPolicy::default(),
            )
            .expect("relay writer"),
        ));
        ensure_response_head_proof(&relay_writer, &TransferId::generate(), &mut response)
            .await
            .expect("relay preserves target proof");
        assert_eq!(
            response
                .metadata
                .get(OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY),
            Some(&original_attestation),
            "a relay must not replace the target attestation with its hop proof"
        );

        let mut tampered = response.clone();
        tampered.body = b"other-sizehead".to_vec();
        assert_eq!(tampered.body.len(), response.body.len());
        assert!(!response_head_origin_is_verified(
            &tampered,
            &target_public_key,
            &logical_session_id,
        ));
        assert!(!response_head_origin_is_verified(
            &response,
            &relay_public_key,
            &logical_session_id,
        ));
        assert!(!response_head_origin_is_verified(
            &response,
            &target_public_key,
            &TransferId::generate(),
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn full_duplex_response_does_not_wait_for_handler_to_drain_request() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-spool-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-spool-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let request_bytes = vec![0x51_u8; 3 * 1024 * 1024 + 17];
        let response_bytes = vec![0x92_u8; 12 * 1024 * 1024 + 29];
        let expected_request = request_bytes.clone();
        let expected_response = response_bytes.clone();
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/spool-full-duplex",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared =
            ObjectRpcSessionCall::new(request, ObjectRpcBodySource::bytes(request_bytes))
                .prepare_for_signing()
                .expect("prepare session call");
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(128 * 1024);
        let server = tokio::spawn(async move {
            let (request, responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                authenticate_test_request,
            )
            .await
            .expect("accept session");
            let response = crate::ObjectRpcResponse::encode_body(
                "/test/session/spool-full-duplex",
                &"response-head",
            )
            .expect("response head");
            // Deliberately finish a response larger than the flow-control
            // window before reading a byte of the request. The network reader
            // must continue spooling request data and routing response ACKs.
            responder
                .respond(ObjectRpcSessionResponse::new(
                    response,
                    ObjectRpcBodySource::bytes(response_bytes),
                ))
                .await
                .expect("send response before draining request");
            request
                .body
                .collect_to_vec(8 * 1024 * 1024)
                .await
                .expect("request body after response")
        });
        let result = start_object_rpc_client_session(
            Box::new(client_stream),
            prepared,
            request_auth,
            client_data_dir,
            server_public_key.clone(),
            server_public_key,
            NetworkCallPolicy::default(),
        )
        .await
        .expect("start client session");
        let response_body = result
            .body
            .collect_to_vec(16 * 1024 * 1024)
            .await
            .expect("response body");
        let request_body = tokio::time::timeout(std::time::Duration::from_secs(30), server)
            .await
            .expect("full-duplex session must not deadlock")
            .expect("server task");
        assert_eq!(request_body, expected_request);
        assert_eq!(response_body, expected_response);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn large_buffered_response_uses_transfer_body_and_restores_payload() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-buffered-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-buffered-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let response_bytes = vec![0x92_u8; 237_451];
        let expected_response = response_bytes.clone();
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/large-buffered-response",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared = ObjectRpcSessionCall::buffered(request)
            .prepare_for_signing()
            .expect("prepare session call");
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(128 * 1024);
        let server = tokio::spawn(async move {
            let (_request, responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                authenticate_test_request,
            )
            .await
            .expect("accept session");
            responder
                .respond(ObjectRpcSessionResponse::buffered(
                    crate::ObjectRpcResponse::from_encoded_body(
                        "/test/session/large-buffered-response",
                        response_bytes,
                    ),
                ))
                .await
                .expect("send large buffered response");
        });
        let result = start_object_rpc_client_session(
            Box::new(client_stream),
            prepared,
            request_auth,
            client_data_dir,
            server_public_key.clone(),
            server_public_key,
            NetworkCallPolicy::default(),
        )
        .await
        .expect("receive transferred buffered response");
        assert!(result.response.body.is_empty());
        assert!(!result.body.is_empty());
        let result = result
            .restore_large_buffered_response_body()
            .await
            .expect("restore buffered response payload");

        assert_eq!(result.response.body, expected_response);
        assert!(result.body.is_empty());
        assert!(
            !result
                .response
                .metadata
                .contains_key(OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY)
        );
        assert!(
            !result
                .response
                .metadata
                .contains_key(OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY)
        );
        server.await.expect("server task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn oversized_metadata_only_response_remains_a_control_frame_error() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-metadata-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-metadata-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/oversized-metadata",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared = ObjectRpcSessionCall::buffered(request)
            .prepare_for_signing()
            .expect("prepare session call");
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(128 * 1024);
        let server = tokio::spawn(async move {
            let (_request, responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                authenticate_test_request,
            )
            .await
            .expect("accept session");
            let mut response = crate::ObjectRpcResponse::from_encoded_body(
                "/test/session/oversized-metadata",
                Vec::new(),
            );
            response.metadata.insert(
                "test-oversized-metadata".to_string(),
                "x".repeat(128 * 1024),
            );
            responder
                .respond(ObjectRpcSessionResponse::buffered(response))
                .await
        });
        let client_error = start_object_rpc_client_session(
            Box::new(client_stream),
            prepared,
            request_auth,
            client_data_dir,
            server_public_key.clone(),
            server_public_key,
            NetworkCallPolicy::default(),
        )
        .await
        .expect_err("oversized metadata-only response must close before a head arrives");
        assert!(matches!(client_error, NetworkError::Io { .. }));
        let server_error = server
            .await
            .expect("server task")
            .expect_err("metadata-only head cannot spill");
        assert!(matches!(
            server_error,
            NetworkError::FrameTooLarge {
                max_bytes: MAX_TRANSFER_CONTROL_FRAME_BYTES,
                ..
            }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_response_cannot_claim_the_reserved_buffered_transfer_marker() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-marker-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-marker-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/reserved-marker",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared = ObjectRpcSessionCall::buffered(request)
            .prepare_for_signing()
            .expect("prepare session call");
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(128 * 1024);
        let server = tokio::spawn(async move {
            let (_request, responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                authenticate_test_request,
            )
            .await
            .expect("accept session");
            let mut response = crate::ObjectRpcResponse::from_encoded_body(
                "/test/session/reserved-marker",
                Vec::new(),
            );
            response.metadata.insert(
                OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY.to_string(),
                OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_VALUE.to_string(),
            );
            responder
                .respond(ObjectRpcSessionResponse::new(
                    response,
                    ObjectRpcBodySource::bytes(b"semantic-stream".to_vec()),
                ))
                .await
        });
        let client_error = start_object_rpc_client_session(
            Box::new(client_stream),
            prepared,
            request_auth,
            client_data_dir,
            server_public_key.clone(),
            server_public_key,
            NetworkCallPolicy::default(),
        )
        .await
        .expect_err("reserved local marker must prevent a response head");
        assert!(matches!(client_error, NetworkError::Io { .. }));
        let server_error = server
            .await
            .expect("server task")
            .expect_err("local response marker must be rejected");
        assert!(matches!(server_error, NetworkError::Serialization { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ignored_request_body_is_drained_without_spooling_or_losing_the_response() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-ignore-body-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-ignore-body-server");
        let server_path = server_root.path().to_path_buf();
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(&server_path);
        let server_public_key = proof_public_key(&server_data_dir);
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/ignore-body",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared = ObjectRpcSessionCall::new(
            request,
            ObjectRpcBodySource::bytes(vec![0x61_u8; 3 * 1024 * 1024 + 17]),
        )
        .prepare_for_signing()
        .expect("prepare session call");
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(128 * 1024);
        let server = tokio::spawn(async move {
            let (request, responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                authenticate_test_request,
            )
            .await
            .expect("accept session");
            drop(request.body);
            let response =
                crate::ObjectRpcResponse::encode_body("/test/session/ignore-body", &"accepted")
                    .expect("response head");
            responder
                .respond(ObjectRpcSessionResponse::buffered(response))
                .await
                .expect("send response while discarding request body");
        });
        let result = start_object_rpc_client_session(
            Box::new(client_stream),
            prepared,
            request_auth,
            client_data_dir,
            server_public_key.clone(),
            server_public_key,
            NetworkCallPolicy::default(),
        )
        .await
        .expect("ignored request body must still complete its transport");
        assert!(result.body.is_empty());
        assert!(
            !result
                .response
                .metadata
                .contains_key(OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY)
        );
        assert_eq!(
            result
                .response
                .decode_body::<String>()
                .expect("decode response"),
            "accepted"
        );
        server.await.expect("server task");
        assert!(
            !server_path.join("network-transfers").exists(),
            "discarding an ignored request body must not create a spill file"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn canonical_session_streams_request_and_response_bodies_full_duplex() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-full-duplex-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-full-duplex-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let request_bytes = vec![0x31_u8; 3 * 1024 * 1024 + 17];
        let response_bytes = vec![0x72_u8; 4 * 1024 * 1024 + 29];
        let expected_request = request_bytes.clone();
        let expected_response = response_bytes.clone();
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/full-duplex",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared =
            ObjectRpcSessionCall::new(request, ObjectRpcBodySource::bytes(request_bytes))
                .prepare_for_signing()
                .expect("prepare session call");
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(128 * 1024);
        let server = tokio::spawn(async move {
            let (request, responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                authenticate_test_request,
            )
            .await
            .expect("accept session");
            let response = crate::ObjectRpcResponse::encode_body(
                "/test/session/full-duplex",
                &"response-head",
            )
            .expect("response head");
            let receive_request = request.body.collect_to_vec(8 * 1024 * 1024);
            let send_response = responder.respond(ObjectRpcSessionResponse::new(
                response,
                ObjectRpcBodySource::bytes(response_bytes),
            ));
            let (request_body, ()) = tokio::join!(receive_request, async {
                send_response.await.expect("send response")
            });
            request_body.expect("request body")
        });
        let result = start_object_rpc_client_session(
            Box::new(client_stream),
            prepared,
            request_auth,
            client_data_dir,
            server_public_key.clone(),
            server_public_key,
            NetworkCallPolicy::default(),
        )
        .await
        .expect("start client session");
        let response_body = result
            .body
            .collect_to_vec(8 * 1024 * 1024)
            .await
            .expect("response body");
        let request_body = server.await.expect("server task");
        assert_eq!(request_body, expected_request);
        assert_eq!(response_body, expected_response);
    }

    #[tokio::test(start_paused = true)]
    async fn healthy_keepalives_allow_handlers_to_run_longer_than_five_seconds() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-long-handler-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-long-handler-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/long-handler",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared = ObjectRpcSessionCall::buffered(request)
            .prepare_for_signing()
            .expect("prepare session call");
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let (_request, responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                authenticate_test_request,
            )
            .await
            .expect("accept session");
            tokio::time::sleep(std::time::Duration::from_secs(12)).await;
            let response = crate::ObjectRpcResponse::encode_body(
                "/test/session/long-handler",
                &"response-head",
            )
            .expect("response head");
            responder
                .respond(ObjectRpcSessionResponse::buffered(response))
                .await
                .expect("respond after long handler");
        });
        let result = start_object_rpc_client_session(
            Box::new(client_stream),
            prepared,
            request_auth,
            client_data_dir,
            server_public_key.clone(),
            server_public_key,
            NetworkCallPolicy::default(),
        )
        .await
        .expect("keepalives preserve healthy call");
        assert!(result.body.is_empty());
        server.await.expect("server task");
    }

    #[tokio::test(start_paused = true)]
    async fn slow_async_body_production_is_not_a_network_idle_timeout() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-slow-source-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-slow-source-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let (body_reader, mut body_writer) = tokio::io::duplex(64);
        let body_producer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(12)).await;
            body_writer.write_all(b"slow").await.expect("produce body");
            body_writer.shutdown().await.expect("finish body source");
        });
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/slow-source",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared = ObjectRpcSessionCall::new(
            request,
            ObjectRpcBodySource::async_reader(body_reader, Some(4)),
        )
        .prepare_for_signing()
        .expect("prepare session call");
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let (request, responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                authenticate_test_request,
            )
            .await
            .expect("accept session");
            let body = request
                .body
                .collect_to_vec(16)
                .await
                .expect("slow request body");
            let response = crate::ObjectRpcResponse::encode_body(
                "/test/session/slow-source",
                &"response-head",
            )
            .expect("response head");
            responder
                .respond(ObjectRpcSessionResponse::buffered(response))
                .await
                .expect("response");
            body
        });
        let result = start_object_rpc_client_session(
            Box::new(client_stream),
            prepared,
            request_auth,
            client_data_dir,
            server_public_key.clone(),
            server_public_key,
            NetworkCallPolicy::default(),
        )
        .await
        .expect("slow local producer must not be a network timeout");
        assert!(result.body.is_empty());
        assert_eq!(server.await.expect("server task"), b"slow");
        body_producer.await.expect("body producer task");
    }

    #[tokio::test]
    async fn outgoing_body_pipelines_multiple_frames_before_the_first_ack() {
        let root = sinomo_testdir::TestDir::new("sinomo-session-pipelined-window");
        let data_dir = SinomoDataDir::new(root.path());
        let public_key = proof_public_key(&data_dir);
        let session_id = TransferId::generate();
        let transfer_id = TransferId::generate();
        let frame_bytes = u64::from(crate::transfer::DEFAULT_TRANSFER_FRAME_BYTES);
        let body = vec![0x4a_u8; usize::try_from(frame_bytes * 3).expect("test body size")];
        let digest = TransferDigest::blake3(&body);
        let open = TransferOpen {
            transfer_id: transfer_id.clone(),
            route: "/test/session/pipelined-window".to_string(),
            operation_id: Some("pipeline-test".to_string()),
            total_bytes: Some(frame_bytes * 3),
            expected_digest: Some(digest),
            replayable: true,
            requested_window: TransferFlowWindow::default(),
        };
        let (wire_writer, wire_reader) = tokio::io::duplex(4 * 1024 * 1024);
        let writer = Arc::new(tokio::sync::Mutex::new(
            SessionFrameWriter::new(
                wire_writer,
                data_dir,
                session_id.clone(),
                SessionFrameDirection::InitiatorToResponder,
                NetworkCallPolicy::default(),
            )
            .expect("session writer"),
        ));
        let (feedback_tx, feedback_rx) = tokio::sync::mpsc::channel(16);
        feedback_tx
            .send(TransferControl::OpenAck(crate::transfer::TransferOpenAck {
                transfer_id: transfer_id.clone(),
                receive_window: TransferFlowWindow::default(),
            }))
            .await
            .expect("open acknowledgement");
        let mut source = ObjectRpcBodySource::bytes(body);
        source
            .ensure_local_finish_binding(BodyFinishProofBinding {
                session_id: session_id.clone(),
                direction: SessionFrameDirection::InitiatorToResponder,
                head_digest: TransferDigest::blake3(b"pipeline-test-head"),
            })
            .expect("finish proof binding");
        let send_task = tokio::spawn(pump_outgoing_body(
            Arc::clone(&writer),
            source,
            open.clone(),
            feedback_rx,
            NetworkCallPolicy::default(),
        ));
        let mut reader = SessionFrameReader::new(
            wire_reader,
            session_id,
            SessionFrameDirection::InitiatorToResponder,
            public_key,
            NetworkCallPolicy::default(),
        );
        assert!(matches!(
            reader.receive().await.expect("Open"),
            SessionInboundFrame::Payload(SessionWirePayload::Transfer(TransferControl::Open(
                received
            ))) if received == open
        ));
        for expected_offset in [0, frame_bytes] {
            let frame = tokio::time::timeout(std::time::Duration::from_secs(1), reader.receive())
                .await
                .expect("sender must pipeline without waiting for an ACK")
                .expect("pipelined data frame");
            assert!(matches!(
                frame,
                SessionInboundFrame::Data { header, .. }
                    if header.offset == expected_offset
            ));
        }
        feedback_tx
            .send(TransferControl::Ack(crate::transfer::TransferAck {
                transfer_id: transfer_id.clone(),
                received_contiguous_offset: frame_bytes * 2,
                receive_window: TransferFlowWindow::default(),
            }))
            .await
            .expect("cumulative acknowledgement");
        assert!(matches!(
            reader.receive().await.expect("third data frame"),
            SessionInboundFrame::Data { header, .. } if header.offset == frame_bytes * 2
        ));
        assert!(matches!(
            reader.receive().await.expect("Finish"),
            SessionInboundFrame::Payload(SessionWirePayload::FinishAttestation { finish, .. })
                if finish.total_bytes == frame_bytes * 3 && finish.digest == digest
        ));
        feedback_tx
            .send(TransferControl::Complete(TransferReceipt {
                transfer_id,
                completed_bytes: frame_bytes * 3,
                digest,
            }))
            .await
            .expect("completion receipt");
        send_task
            .await
            .expect("sender task")
            .expect("pipelined transfer");
    }

    #[tokio::test]
    async fn authenticated_data_frame_rejects_payload_tampering() {
        let root = sinomo_testdir::TestDir::new("sinomo-session-tampered-payload");
        let data_dir = SinomoDataDir::new(root.path());
        let public_key = proof_public_key(&data_dir);
        let session_id = TransferId::generate();
        let transfer_id = TransferId::generate();
        let valid = TransferDataFrame::new(transfer_id, 0, b"good", TransferFlowWindow::default())
            .expect("valid frame");
        let corrupt = TransferDataFrame {
            header: valid.header,
            payload: b"evil",
        };
        let (sender, receiver) = tokio::io::duplex(4096);
        let mut writer = SessionFrameWriter::new(
            sender,
            data_dir,
            session_id.clone(),
            SessionFrameDirection::InitiatorToResponder,
            NetworkCallPolicy::default(),
        )
        .expect("session writer");
        let send = tokio::spawn(async move { writer.send_data(corrupt).await });
        let mut reader = SessionFrameReader::new(
            receiver,
            session_id,
            SessionFrameDirection::InitiatorToResponder,
            public_key,
            NetworkCallPolicy::default(),
        );
        let error = reader
            .receive()
            .await
            .expect_err("tampered payload must fail");
        assert!(error.to_string().contains("digest mismatch"));
        send.await
            .expect("sender task")
            .expect("send corrupt bytes");
    }

    #[tokio::test(start_paused = true)]
    async fn session_reader_no_progress_is_a_typed_idle_timeout() {
        let (_sender, receiver) = tokio::io::duplex(64);
        let mut reader = SessionFrameReader::new(
            receiver,
            TransferId::generate(),
            SessionFrameDirection::InitiatorToResponder,
            vec![0_u8; 32],
            NetworkCallPolicy::default(),
        );
        let error = reader
            .receive()
            .await
            .expect_err("silent peer must time out");
        assert!(matches!(
            error,
            NetworkError::Timeout {
                phase: NetworkTimeoutPhase::Idle,
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn explicit_session_deadline_preempts_the_health_timeout() {
        let (_sender, receiver) = tokio::io::duplex(64);
        let policy = NetworkCallPolicy::default()
            .with_overall_timeout(std::time::Duration::from_millis(100));
        let mut reader = SessionFrameReader::new(
            receiver,
            TransferId::generate(),
            SessionFrameDirection::InitiatorToResponder,
            vec![0_u8; 32],
            policy,
        );
        let error = reader
            .receive()
            .await
            .expect_err("overall deadline must expire");
        assert!(matches!(
            error,
            NetworkError::Timeout {
                phase: NetworkTimeoutPhase::Overall,
                ..
            }
        ));
    }
}
