//! Canonical body-aware object-RPC session primitives.
//!
//! Public callers describe whole request or response bodies. Transfer frames,
//! flow-control windows, replay offsets, and synchronous-reader buffering stay
//! private to `sinomo-network`.

use crate::daemon_auth::{
    ResponseIdentityProofInput, SessionIdentitySigner, SignedDaemonCommand,
    SignedResponseIdentityProof, verify_response_identity_proof,
};
use crate::path_metrics::{
    NetworkPathEstimator, NetworkPathKey, NetworkPeerReceiveCreditLease,
    reserve_shared_network_peer_receive_credit, shared_network_path_estimator,
};
pub(crate) use crate::transfer::NetworkCallPolicy;
use crate::transfer::{
    DEFAULT_TRANSFER_FRAME_BYTES, MAX_TRANSFER_FRAME_BYTES, MAX_TRANSFER_WINDOW_BYTES,
    MIN_TRANSFER_FRAME_BYTES, MIN_TRANSFER_WINDOW_BYTES, TransferAck, TransferControl,
    TransferDataFrame, TransferDataHeader, TransferEncodedWireFrameHeader, TransferFlowWindow,
    TransferId, TransferOpen, TransferReceiverState, TransferSenderState,
    read_transfer_data_payload, read_transfer_encoded_frame_header, shared_transfer_buffer_pool,
    write_transfer_data_frame, write_transfer_encoded_control_frame,
};
use crate::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use sinomo_local::SinomoDataDir;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

const BODY_READER_BUFFER_BYTES: usize = DEFAULT_TRANSFER_FRAME_BYTES as usize;
const BODY_READER_BUFFER_DEPTH: usize = 2;
const SESSION_WRITER_COMMAND_DEPTH: usize = 8;
/// Keep latency-sensitive typed values in the already signed request/response
/// head. Larger values retain the streaming transfer path and its bounded
/// flow-control window.
const INLINE_VALUE_MAX_BYTES: usize = 64 * 1024;
/// Give an active body consumer enough time to absorb a transient scheduler or
/// allocation stall before falling back to the disk spool. The healthy
/// `try_send` path does not wait. Five milliseconds is well below the session
/// no-progress timeout while spanning several ordinary scheduler quanta.
const BODY_READER_DIRECT_DELIVERY_GRACE: std::time::Duration = std::time::Duration::from_millis(5);
const DEFAULT_BDP_ROUND_TRIP_TIME: std::time::Duration = std::time::Duration::from_millis(1);
const MIN_TRANSFER_CREDIT_UPDATE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_TRANSFER_CREDIT_UPDATE_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_TRANSFER_CREDIT_UPDATE_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(2);
const MIN_TRANSFER_CREDIT_UPDATE_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(1);
const MAX_TRANSFER_CREDIT_UPDATE_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(10);
static BODY_READER_DIRECT_WAIT_TOTAL: AtomicU64 = AtomicU64::new(0);
static BODY_READER_DIRECT_WAIT_DELIVERED_TOTAL: AtomicU64 = AtomicU64::new(0);
static BODY_READER_DIRECT_WAIT_SPILL_TOTAL: AtomicU64 = AtomicU64::new(0);
static BODY_READER_DIRECT_WAIT_CLOSED_TOTAL: AtomicU64 = AtomicU64::new(0);
const OBJECT_RPC_SESSION_PROTOCOL: &str = "sinomo.object-rpc-session.v1";
const OBJECT_RPC_SESSION_METADATA_KEY: &str = "sinomo.rpc-session";
pub(crate) const OBJECT_RPC_RESPONSE_IDENTITY_PROOF_METADATA_KEY: &str =
    "sinomo.rpc-response-identity-proof";

/// Returns whether a non-consuming transport peek identifies the canonical
/// session wire.
pub fn object_rpc_session_wire_prefix_matches(prefix: &[u8]) -> bool {
    !prefix.is_empty() && prefix[..prefix.len().min(4)] == b"SXF2"[..prefix.len().min(4)]
}

/// A transport-owned bidirectional byte stream. The canonical RPC session
/// engine owns every byte written after the transport opens this stream.
pub trait NetworkDuplexStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> NetworkDuplexStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ObjectRpcSessionDescriptor {
    protocol: String,
    session_id: TransferId,
    request_body_role: ObjectRpcSessionBodyRole,
    request_body: Option<TransferOpen>,
}

/// Logical meaning of the single canonical transfer in one RPC direction.
///
/// A value is the ordinary encoded `ObjectRpcRequest::body` or
/// `ObjectRpcResponse::body` moved out of the signed control head. An
/// attachment is the explicitly streamed semantic body supplied alongside a
/// typed head. The role is carried in the signed session head, so receivers do
/// not infer protocol behavior from the number of bytes on the wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ObjectRpcSessionBodyRole {
    None,
    Value,
    Attachment,
}

impl ObjectRpcSessionBodyRole {
    fn validate(self, body: Option<&TransferOpen>) -> Result<(), NetworkError> {
        match (self, body) {
            (Self::None, None) | (Self::Value | Self::Attachment, Some(_)) => Ok(()),
            (Self::None, Some(_)) => Err(NetworkError::Serialization {
                detail: "object RPC session declares a body with the none role".to_string(),
            }),
            (Self::Value | Self::Attachment, None) => Err(NetworkError::Serialization {
                detail: "object RPC session body role is missing its transfer descriptor"
                    .to_string(),
            }),
        }
    }
}

impl ObjectRpcSessionDescriptor {
    fn for_call(call: &ObjectRpcSessionCall) -> Self {
        let session_id = TransferId::generate();
        let request_body_role = call.body.role();
        let request_body = (!call.body.is_empty()).then(|| TransferOpen {
            transfer_id: TransferId::generate(),
            route: call.request.route.clone(),
            operation_id: call.request.operation_id().map(ToOwned::to_owned),
            total_bytes: call.body.total_bytes(),
            replayable: call.body.is_replayable(),
            requested_window: TransferFlowWindow::maximum_supported(),
        });
        Self {
            protocol: OBJECT_RPC_SESSION_PROTOCOL.to_string(),
            session_id,
            request_body_role,
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
        descriptor
            .request_body_role
            .validate(descriptor.request_body.as_ref())?;
        Ok(descriptor)
    }
}

fn encode_response_identity_proof(
    proof: &SignedResponseIdentityProof,
) -> Result<String, NetworkError> {
    let encoded = crate::SinomoObjectCodec::network_frame().encode(proof)?;
    Ok(URL_SAFE_NO_PAD.encode(encoded))
}

fn decode_response_identity_proof(
    response: &crate::ObjectRpcResponse,
) -> Result<SignedResponseIdentityProof, NetworkError> {
    let encoded = response
        .metadata
        .get(OBJECT_RPC_RESPONSE_IDENTITY_PROOF_METADATA_KEY)
        .ok_or(NetworkError::AuthDenied)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded.as_bytes())
        .map_err(|_| NetworkError::AuthDenied)?;
    crate::SinomoObjectCodec::network_frame().decode(&bytes)
}

#[cfg(test)]
async fn ensure_response_identity_proof<W>(
    writer: &Arc<tokio::sync::Mutex<SessionFrameWriter<W>>>,
    session_id: &TransferId,
    response: &mut crate::ObjectRpcResponse,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    let writer = writer.lock().await;
    ensure_response_identity_proof_with_writer(&writer, session_id, response)
}

fn ensure_response_identity_proof_with_writer<W>(
    writer: &SessionFrameWriter<W>,
    session_id: &TransferId,
    response: &mut crate::ObjectRpcResponse,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    if response
        .metadata
        .contains_key(OBJECT_RPC_RESPONSE_IDENTITY_PROOF_METADATA_KEY)
    {
        return Ok(());
    }
    let proof = writer.sign_response_identity(session_id.clone());
    response.metadata.insert(
        OBJECT_RPC_RESPONSE_IDENTITY_PROOF_METADATA_KEY.to_string(),
        encode_response_identity_proof(&proof)?,
    );
    Ok(())
}

fn response_origin_is_verified(
    response: &crate::ObjectRpcResponse,
    expected_public_key: &[u8],
    expected_session_id: &TransferId,
) -> bool {
    decode_response_identity_proof(response).is_ok_and(|proof| {
        verify_response_identity_proof(&proof, expected_public_key, expected_session_id).is_ok()
    })
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
        body_role: ObjectRpcSessionBodyRole,
        body: Option<TransferOpen>,
    },
    Transfer(TransferControl),
    Finish(crate::transfer::TransferFinish),
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

#[derive(Debug)]
enum SessionInboundFrame {
    Payload(SessionWirePayload),
    Data {
        header: TransferDataHeader,
        payload: Bytes,
    },
}

#[derive(Clone, Copy, Debug)]
struct TransferPathTuning {
    flow_window: TransferFlowWindow,
    credit_update_bytes: u64,
    credit_update_interval: std::time::Duration,
}

fn transfer_path_tuning(estimator: &NetworkPathEstimator) -> TransferPathTuning {
    let estimate = estimator.snapshot();
    let max_in_flight_bytes = estimate.recommended_window_bytes_with_default_rtt(
        2,
        MIN_TRANSFER_WINDOW_BYTES,
        MAX_TRANSFER_WINDOW_BYTES,
        Some(DEFAULT_BDP_ROUND_TRIP_TIME),
    );
    let max_frame_bytes = if max_in_flight_bytes >= 48 * 1024 * 1024 {
        MAX_TRANSFER_FRAME_BYTES
    } else if max_in_flight_bytes >= 24 * 1024 * 1024 {
        2 * 1024 * 1024
    } else {
        MIN_TRANSFER_FRAME_BYTES
    };
    let half_window = max_in_flight_bytes / 2;
    let credit_update_bytes = (max_in_flight_bytes / 4)
        .clamp(
            MIN_TRANSFER_CREDIT_UPDATE_BYTES,
            MAX_TRANSFER_CREDIT_UPDATE_BYTES,
        )
        .max(2 * u64::from(max_frame_bytes))
        .min(half_window);
    let credit_update_interval = estimate
        .round_trip_time
        .and_then(|rtt| rtt.checked_div(4))
        .unwrap_or(DEFAULT_TRANSFER_CREDIT_UPDATE_INTERVAL)
        .clamp(
            MIN_TRANSFER_CREDIT_UPDATE_INTERVAL,
            MAX_TRANSFER_CREDIT_UPDATE_INTERVAL,
        );
    TransferPathTuning {
        flow_window: TransferFlowWindow {
            max_in_flight_bytes,
            max_frame_bytes,
        },
        credit_update_bytes,
        credit_update_interval,
    }
}

#[derive(Debug)]
struct OutgoingTransferMeasurement {
    estimator: NetworkPathEstimator,
    delivery_sample: Option<(u64, std::time::Instant)>,
    sent_offsets: VecDeque<(u64, std::time::Instant)>,
}

impl OutgoingTransferMeasurement {
    fn new(estimator: NetworkPathEstimator) -> Self {
        Self {
            estimator,
            delivery_sample: None,
            sent_offsets: VecDeque::new(),
        }
    }

    fn record_open_round_trip(&self, elapsed: std::time::Duration) {
        self.estimator.record_round_trip_time(elapsed);
    }

    fn record_sent(&mut self, end_offset: u64) {
        let now = std::time::Instant::now();
        self.delivery_sample.get_or_insert((0, now));
        self.sent_offsets.push_back((end_offset, now));
    }

    fn record_acknowledgement(&mut self, acknowledged_offset: u64) {
        let now = std::time::Instant::now();
        if let Some((previous_offset, previous_at)) = self.delivery_sample
            && acknowledged_offset > previous_offset
        {
            self.estimator.record_delivery(
                acknowledged_offset - previous_offset,
                now.saturating_duration_since(previous_at),
            );
            self.delivery_sample = Some((acknowledged_offset, now));
        }
        let mut latest_sent_at = None;
        while self
            .sent_offsets
            .front()
            .is_some_and(|(offset, _)| *offset <= acknowledged_offset)
        {
            latest_sent_at = self.sent_offsets.pop_front().map(|(_, sent_at)| sent_at);
        }
        if let Some(sent_at) = latest_sent_at {
            self.estimator
                .record_round_trip_time(now.saturating_duration_since(sent_at));
        }
    }
}

#[derive(Debug)]
struct IncomingTransferMeasurement {
    sample_offset: u64,
    sample_started: std::time::Instant,
}

impl IncomingTransferMeasurement {
    fn new() -> Self {
        Self {
            sample_offset: 0,
            sample_started: std::time::Instant::now(),
        }
    }

    fn record_until(&mut self, estimator: &NetworkPathEstimator, offset: u64) {
        let now = std::time::Instant::now();
        if offset > self.sample_offset {
            estimator.record_delivery(
                offset - self.sample_offset,
                now.saturating_duration_since(self.sample_started),
            );
            self.sample_offset = offset;
            self.sample_started = now;
        }
    }
}

#[derive(Debug)]
struct TransferCreditUpdateSchedule {
    acknowledged_offset: u64,
    acknowledged_at: std::time::Instant,
    update_bytes: u64,
    update_interval: std::time::Duration,
}

impl TransferCreditUpdateSchedule {
    fn new(tuning: TransferPathTuning) -> Self {
        Self {
            acknowledged_offset: 0,
            acknowledged_at: std::time::Instant::now(),
            update_bytes: tuning.credit_update_bytes,
            update_interval: tuning.credit_update_interval,
        }
    }

    fn update_tuning(&mut self, tuning: TransferPathTuning) {
        self.update_bytes = tuning.credit_update_bytes;
        self.update_interval = tuning.credit_update_interval;
    }

    fn due(&self, received_offset: u64) -> bool {
        let unacknowledged = received_offset.saturating_sub(self.acknowledged_offset);
        unacknowledged >= self.update_bytes
            || (unacknowledged > 0 && self.acknowledged_at.elapsed() >= self.update_interval)
    }

    fn mark(&mut self, acknowledged_offset: u64) {
        self.acknowledged_offset = acknowledged_offset;
        self.acknowledged_at = std::time::Instant::now();
    }
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
    signer: SessionIdentitySigner,
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
        policy: NetworkCallPolicy,
    ) -> Result<Self, NetworkError> {
        let signer = SessionIdentitySigner::from_data_dir(&data_dir)?;
        Ok(Self {
            writer,
            data_dir,
            signer,
            policy,
            deadline: SessionDeadline::from_policy(policy),
        })
    }

    fn encode_control(&self, payload: SessionWirePayload) -> Result<Vec<u8>, NetworkError> {
        crate::SinomoObjectCodec::network_frame().encode(&payload)
    }

    async fn send(&mut self, payload: SessionWirePayload) -> Result<(), NetworkError> {
        let encoded = self.encode_control(payload)?;
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

    fn sign_response_identity(&self, session_id: TransferId) -> SignedResponseIdentityProof {
        self.signer
            .sign_response_identity(ResponseIdentityProofInput { session_id })
    }
}

enum SessionWriteCommand {
    Control {
        payload: SessionWirePayload,
        completion: Option<tokio::sync::oneshot::Sender<Result<(), NetworkError>>>,
    },
    Data {
        header: TransferDataHeader,
        payload: Bytes,
        completion: tokio::sync::oneshot::Sender<Result<(), NetworkError>>,
    },
}

enum SessionWriterWork {
    Command(SessionWriteCommand),
    Acknowledgement(TransferAck),
}

/// Ordered, bounded write-side scheduler for one canonical session.
///
/// The wire reader must keep draining the peer while a large local frame is in
/// flight. Reader-generated acknowledgements therefore update one coalescing
/// slot instead of waiting for the socket writer. All actual writes remain
/// serialized here so stream order stays exact.
#[derive(Clone)]
struct SessionWriterHandle {
    commands: tokio::sync::mpsc::Sender<SessionWriteCommand>,
    acknowledgements: tokio::sync::watch::Sender<Option<TransferAck>>,
    data_dir: SinomoDataDir,
    signer: SessionIdentitySigner,
}

impl SessionWriterHandle {
    fn spawn<W>(mut writer: SessionFrameWriter<W>) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let data_dir = writer.data_dir.clone();
        let signer = writer.signer.clone();
        let (commands, mut command_receiver) =
            tokio::sync::mpsc::channel(SESSION_WRITER_COMMAND_DEPTH);
        let (acknowledgements, mut acknowledgement_receiver) =
            tokio::sync::watch::channel(None::<TransferAck>);
        tokio::spawn(async move {
            let mut acknowledgements_open = true;
            loop {
                let work = if acknowledgements_open {
                    tokio::select! {
                        changed = acknowledgement_receiver.changed() => {
                            match changed {
                                Ok(()) => SessionWriterWork::Acknowledgement(
                                    acknowledgement_receiver
                                        .borrow_and_update()
                                        .clone()
                                        .expect("acknowledgement updates are never empty"),
                                ),
                                Err(_) => {
                                    acknowledgements_open = false;
                                    continue;
                                }
                            }
                        }
                        command = command_receiver.recv() => {
                            let Some(command) = command else {
                                break;
                            };
                            SessionWriterWork::Command(command)
                        }
                    }
                } else {
                    let Some(command) = command_receiver.recv().await else {
                        break;
                    };
                    SessionWriterWork::Command(command)
                };

                match work {
                    SessionWriterWork::Acknowledgement(acknowledgement) => {
                        let result = writer
                            .send(SessionWirePayload::Transfer(TransferControl::Ack(
                                acknowledgement,
                            )))
                            .await;
                        if result.is_err() {
                            break;
                        }
                    }
                    SessionWriterWork::Command(command) => {
                        // A cumulative ACK logically precedes any control the
                        // reader queued after consuming those bytes (most
                        // importantly Complete). Flush the latest version at
                        // this command boundary so a delayed ACK can never
                        // arrive after its completion receipt.
                        if acknowledgements_open
                            && acknowledgement_receiver.has_changed().unwrap_or(false)
                        {
                            let acknowledgement = acknowledgement_receiver
                                .borrow_and_update()
                                .clone()
                                .expect("acknowledgement updates are never empty");
                            let result = writer
                                .send(SessionWirePayload::Transfer(TransferControl::Ack(
                                    acknowledgement,
                                )))
                                .await;
                            if result.is_err() {
                                break;
                            }
                        }
                        match command {
                            SessionWriteCommand::Control {
                                payload,
                                completion,
                            } => {
                                let result = writer.send(payload).await;
                                let failed = result.is_err();
                                if let Some(completion) = completion {
                                    let _ = completion.send(result);
                                }
                                if failed {
                                    break;
                                }
                            }
                            SessionWriteCommand::Data {
                                header,
                                payload,
                                completion,
                            } => {
                                let result = writer
                                    .send_data(TransferDataFrame {
                                        header,
                                        payload: &payload,
                                    })
                                    .await;
                                let failed = result.is_err();
                                let _ = completion.send(result);
                                if failed {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
        Self {
            commands,
            acknowledgements,
            data_dir,
            signer,
        }
    }

    async fn send(&self, payload: SessionWirePayload) -> Result<(), NetworkError> {
        let (completion, result) = tokio::sync::oneshot::channel();
        self.commands
            .send(SessionWriteCommand::Control {
                payload,
                completion: Some(completion),
            })
            .await
            .map_err(|_| session_writer_stopped("queue object RPC session control"))?;
        result
            .await
            .map_err(|_| session_writer_stopped("write object RPC session control"))?
    }

    async fn send_from_reader(&self, payload: SessionWirePayload) -> Result<(), NetworkError> {
        self.commands
            .send(SessionWriteCommand::Control {
                payload,
                completion: None,
            })
            .await
            .map_err(|_| session_writer_stopped("queue object RPC reader control"))
    }

    async fn send_data(
        &self,
        header: TransferDataHeader,
        payload: Bytes,
    ) -> Result<(), NetworkError> {
        let (completion, result) = tokio::sync::oneshot::channel();
        self.commands
            .send(SessionWriteCommand::Data {
                header,
                payload,
                completion,
            })
            .await
            .map_err(|_| session_writer_stopped("queue object RPC session data"))?;
        result
            .await
            .map_err(|_| session_writer_stopped("write object RPC session data"))?
    }

    fn acknowledge(&self, acknowledgement: TransferAck) -> Result<(), NetworkError> {
        self.acknowledgements
            .send(Some(acknowledgement))
            .map_err(|_| session_writer_stopped("queue object RPC transfer acknowledgement"))
    }

    fn staging_root(&self) -> PathBuf {
        self.data_dir.root()
    }

    fn sign_response_identity(&self, session_id: TransferId) -> SignedResponseIdentityProof {
        self.signer
            .sign_response_identity(ResponseIdentityProofInput { session_id })
    }

    fn ensure_response_identity_proof(
        &self,
        session_id: &TransferId,
        response: &mut crate::ObjectRpcResponse,
    ) -> Result<(), NetworkError> {
        if response
            .metadata
            .contains_key(OBJECT_RPC_RESPONSE_IDENTITY_PROOF_METADATA_KEY)
        {
            return Ok(());
        }
        let proof = self.sign_response_identity(session_id.clone());
        response.metadata.insert(
            OBJECT_RPC_RESPONSE_IDENTITY_PROOF_METADATA_KEY.to_string(),
            encode_response_identity_proof(&proof)?,
        );
        Ok(())
    }
}

fn session_writer_stopped(operation: &'static str) -> NetworkError {
    NetworkError::Io {
        operation: operation.to_string(),
        detail: "canonical session writer stopped".to_string(),
    }
}

struct SessionFrameReader<R> {
    reader: R,
    header_buffer: Vec<u8>,
    policy: NetworkCallPolicy,
    deadline: SessionDeadline,
}

impl<R> SessionFrameReader<R>
where
    R: AsyncRead + Unpin,
{
    fn new(reader: R, policy: NetworkCallPolicy) -> Self {
        Self {
            reader,
            header_buffer: Vec::new(),
            policy,
            deadline: SessionDeadline::from_policy(policy),
        }
    }

    fn after_bootstrap(reader: R, policy: NetworkCallPolicy, header_buffer: Vec<u8>) -> Self {
        Self {
            reader,
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
        let control: SessionWirePayload =
            crate::SinomoObjectCodec::network_frame().decode(&encoded)?;
        let SessionWirePayload::Data(expected_header) = control else {
            return Ok(SessionInboundFrame::Payload(control));
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
                read_transfer_data_payload(&mut self.reader, &header, self.policy.health),
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

#[derive(Debug)]
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

    async fn append(&mut self, bytes: impl Into<Bytes>) -> Result<(), NetworkError> {
        let bytes = bytes.into();
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

async fn send_session_payload(
    writer: &SessionWriterHandle,
    payload: SessionWirePayload,
) -> Result<(), NetworkError> {
    writer.send(payload).await
}

fn spawn_session_keepalive(
    writer: SessionWriterHandle,
    policy: NetworkCallPolicy,
) -> tokio::task::JoinHandle<()> {
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
                Ok(None) => Err(session_reader_stopped_before_transfer_acknowledgement()),
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

fn session_reader_stopped_before_transfer_acknowledgement() -> NetworkError {
    NetworkError::Io {
        operation: "wait for object RPC transfer acknowledgement".to_string(),
        detail: "session reader stopped before transfer acknowledgement".to_string(),
    }
}

fn apply_measured_transfer_ack(
    state: &mut TransferSenderState,
    measurement: &mut OutgoingTransferMeasurement,
    acknowledgement: &TransferAck,
) -> Result<(), NetworkError> {
    measurement.record_acknowledgement(acknowledgement.received_contiguous_offset);
    state
        .accept_ack(acknowledgement)
        .map_err(session_transfer_error)?;
    state
        .set_active_window_bytes(
            transfer_path_tuning(&measurement.estimator)
                .flow_window
                .max_in_flight_bytes,
        )
        .map_err(session_transfer_error)
}

fn apply_outgoing_transfer_feedback(
    control: TransferControl,
    transfer_id: &TransferId,
    state: &mut TransferSenderState,
    measurement: &mut OutgoingTransferMeasurement,
    operation: &'static str,
) -> Result<(), NetworkError> {
    match control {
        TransferControl::Ack(ack) if ack.transfer_id == *transfer_id => {
            apply_measured_transfer_ack(state, measurement, &ack)
        }
        TransferControl::Cancel {
            transfer_id: cancelled,
            reason,
        }
        | TransferControl::Reset {
            transfer_id: cancelled,
            reason,
        } if cancelled == *transfer_id => Err(NetworkError::delivery_failed(
            operation,
            NetworkDeliveryState::PossiblyApplied,
            reason,
        )),
        _ => Ok(()),
    }
}

async fn pump_outgoing_body(
    writer: SessionWriterHandle,
    source: ObjectRpcBodySource,
    open: TransferOpen,
    mut feedback: tokio::sync::mpsc::Receiver<TransferControl>,
    policy: NetworkCallPolicy,
    path_estimator: NetworkPathEstimator,
) -> Result<(), NetworkError> {
    let deadline = SessionDeadline::from_policy(policy);
    let mut state = TransferSenderState::new(
        open.transfer_id.clone(),
        open.total_bytes,
        open.requested_window,
    )
    .map_err(session_transfer_error)?;
    state.mark_opening().map_err(session_transfer_error)?;
    let open_started = std::time::Instant::now();
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
    let mut measurement = OutgoingTransferMeasurement::new(path_estimator);
    measurement.record_open_round_trip(open_started.elapsed());
    state
        .set_active_window_bytes(
            transfer_path_tuning(&measurement.estimator)
                .flow_window
                .max_in_flight_bytes,
        )
        .map_err(session_transfer_error)?;
    let mut producer = source.producer(open_ack.receive_window.max_frame_bytes as usize)?;
    let mut pending = None;
    let mut source_finished = false;
    while !source_finished || pending.is_some() {
        loop {
            match feedback.try_recv() {
                Ok(control) => apply_outgoing_transfer_feedback(
                    control,
                    &open.transfer_id,
                    &mut state,
                    &mut measurement,
                    "send object RPC transfer",
                )?,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    return Err(session_reader_stopped_before_transfer_acknowledgement());
                }
            }
        }
        if pending.is_none() && !source_finished {
            let produced = producer.next();
            tokio::pin!(produced);
            pending = deadline
                .run("produce object RPC request/response body", async {
                    loop {
                        tokio::select! {
                            biased;
                            control = feedback.recv() => {
                                let control = control.ok_or_else(
                                    session_reader_stopped_before_transfer_acknowledgement,
                                )?;
                                apply_outgoing_transfer_feedback(
                                    control,
                                    &open.transfer_id,
                                    &mut state,
                                    &mut measurement,
                                    "send object RPC transfer",
                                )?;
                            }
                            produced = &mut produced => return produced,
                        }
                    }
                })
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
                let control = receive_sender_feedback(&mut feedback, policy, deadline).await?;
                apply_outgoing_transfer_feedback(
                    control,
                    &open.transfer_id,
                    &mut state,
                    &mut measurement,
                    "send flow-controlled object RPC transfer",
                )?;
                continue;
            }
            Err(error) => return Err(session_transfer_error(error)),
        };
        let header = frame.header.clone();
        let end_offset = header
            .offset
            .saturating_add(u64::from(header.payload_bytes));
        drop(frame);
        writer.send_data(header, bytes).await?;
        measurement.record_sent(end_offset);
    }
    producer.finish().await?;
    let finish = state.finish().map_err(session_transfer_error)?;
    send_session_payload(&writer, SessionWirePayload::Finish(finish)).await?;
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

async fn acknowledge_incoming_completion(
    writer: SessionWriterHandle,
    incoming_sender: Option<IncomingBodySender>,
    receipt: crate::transfer::TransferReceipt,
    has_spool: bool,
) -> Result<(), NetworkError> {
    writer
        .send(SessionWirePayload::Transfer(TransferControl::Complete(
            receipt.clone(),
        )))
        .await?;
    if let Some(sender) = incoming_sender.as_ref() {
        sender.publish_completion(receipt.completed_bytes, has_spool)?;
    }
    Ok(())
}

#[derive(Debug)]
struct SessionReaderOutcome;

#[derive(Debug, Default)]
struct SharedOutgoingTransfer {
    initialized: bool,
    transfer_id: Option<TransferId>,
    complete: bool,
}

async fn pump_incoming_session<R>(
    mut reader: SessionFrameReader<R>,
    writer: SessionWriterHandle,
    expected_incoming_open: Option<TransferOpen>,
    outgoing: Arc<Mutex<SharedOutgoingTransfer>>,
    feedback: tokio::sync::mpsc::Sender<TransferControl>,
    incoming: Option<IncomingBodySender>,
    head_sender: tokio::sync::oneshot::Sender<SessionWirePayload>,
    initial_head: Option<SessionWirePayload>,
    path_key: NetworkPathKey,
    path_estimator: NetworkPathEstimator,
) -> Result<SessionReaderOutcome, NetworkError>
where
    R: AsyncRead + Unpin,
{
    let mut expected_incoming_open = expected_incoming_open;
    let mut receiver_state = None;
    let mut incoming_sender = incoming;
    let mut incoming_opened = false;
    let mut incoming_complete = false;
    let mut head_sender = Some(head_sender);
    let mut staging: Option<SessionTransferStagingFile> = None;
    let mut receive_credit: Option<NetworkPeerReceiveCreditLease> = None;
    let mut incoming_measurement: Option<IncomingTransferMeasurement> = None;
    let mut credit_schedule: Option<TransferCreditUpdateSchedule> = None;

    if let Some(payload) = initial_head {
        let declared_open = match &payload {
            SessionWirePayload::RequestHead { descriptor, .. } => descriptor.request_body.clone(),
            SessionWirePayload::ResponseHead {
                body_role, body, ..
            } => {
                body_role.validate(body.as_ref())?;
                body.clone()
            }
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
                    SessionWirePayload::ResponseHead {
                        body_role, body, ..
                    } => {
                        body_role.validate(body.as_ref())?;
                        body.clone()
                    }
                    _ => unreachable!(),
                };
                if let Some(expected) = expected_incoming_open.as_ref()
                    && declared_open.as_ref() != Some(expected)
                {
                    return Err(NetworkError::AuthDenied);
                }
                expected_incoming_open = declared_open;
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
                            let tuning = transfer_path_tuning(&path_estimator);
                            let state = TransferReceiverState::from_open(&open, tuning.flow_window)
                                .map_err(session_transfer_error)?;
                            let lease = reserve_shared_network_peer_receive_credit(
                                &path_key,
                                state.receive_window().max_in_flight_bytes,
                            )
                            .await?;
                            let ack = state.open_ack();
                            receiver_state = Some(state);
                            receive_credit = Some(lease);
                            incoming_measurement = Some(IncomingTransferMeasurement::new());
                            credit_schedule = Some(TransferCreditUpdateSchedule::new(tuning));
                            writer
                                .send_from_reader(SessionWirePayload::Transfer(
                                    TransferControl::OpenAck(ack),
                                ))
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
            SessionInboundFrame::Payload(SessionWirePayload::Finish(finish)) => {
                let state = receiver_state
                    .as_mut()
                    .ok_or_else(|| NetworkError::Serialization {
                        detail: "peer finished an unexpected object RPC body".to_string(),
                    })?;
                state
                    .accept_finish(&finish)
                    .map_err(session_transfer_error)?;
                if let Some(measurement) = incoming_measurement.as_mut() {
                    measurement.record_until(&path_estimator, state.received_offset());
                }
                let receipt = state.complete().map_err(session_transfer_error)?;
                acknowledge_incoming_completion(
                    writer.clone(),
                    incoming_sender.clone(),
                    receipt,
                    staging.is_some(),
                )
                .await?;
                incoming_sender.take();
                drop(staging.take());
                drop(receive_credit.take());
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
                if let Some(staging) = staging.as_mut() {
                    staging.append(payload).await?;
                } else if let Some(sender) = incoming_sender.as_ref() {
                    match sender.deliver_with_grace(payload).await {
                        IncomingDirectDelivery::Delivered
                        | IncomingDirectDelivery::ConsumerClosed => {}
                        IncomingDirectDelivery::Spill(payload) => {
                            let staging_root = writer.staging_root();
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
                state.commit_data(pending).map_err(session_transfer_error)?;
                let available_offset = state.received_offset();
                if staging.is_some()
                    && let Some(sender) = incoming_sender.as_ref()
                {
                    sender.advance_available(available_offset)?;
                }
                let schedule = credit_schedule
                    .as_mut()
                    .expect("credit schedule exists after transfer Open");
                if schedule.due(available_offset) {
                    if let Some(measurement) = incoming_measurement.as_mut() {
                        measurement.record_until(&path_estimator, available_offset);
                    }
                    let tuning = transfer_path_tuning(&path_estimator);
                    let reserved_bytes = receive_credit
                        .as_mut()
                        .expect("receive credit exists after transfer Open")
                        .try_grow_to(tuning.flow_window.max_in_flight_bytes);
                    let desired_bytes = state
                        .receive_window()
                        .max_in_flight_bytes
                        .max(tuning.flow_window.max_in_flight_bytes)
                        .min(reserved_bytes);
                    state
                        .set_receive_window_bytes(desired_bytes)
                        .map_err(session_transfer_error)?;
                    let ack = state
                        .acknowledge_received(available_offset)
                        .map_err(session_transfer_error)?;
                    schedule.update_tuning(tuning);
                    schedule.mark(available_offset);
                    writer.acknowledge(ack)?;
                }
            }
            SessionInboundFrame::Payload(SessionWirePayload::HealthPing { nonce }) => {
                writer
                    .send_from_reader(SessionWirePayload::HealthAck { nonce })
                    .await?;
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

async fn receive_server_session_bootstrap<F>(
    mut reader: BoxedSessionReadHalf,
    policy: NetworkCallPolicy,
    authenticate: F,
) -> Result<
    (
        SessionWirePayload,
        ObjectRpcSessionDescriptor,
        SessionFrameReader<BoxedSessionReadHalf>,
        Result<(), NetworkError>,
        NetworkPathKey,
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
    let payload: SessionWirePayload = crate::SinomoObjectCodec::network_frame().decode(&encoded)?;
    let SessionWirePayload::RequestHead {
        request,
        descriptor,
    } = &payload
    else {
        return Err(NetworkError::AuthDenied);
    };
    let signed_descriptor = session_descriptor_from_request(request)?;
    if &signed_descriptor != descriptor {
        return Err(NetworkError::AuthDenied);
    }
    let auth = request.auth.as_ref().ok_or(NetworkError::AuthDenied)?;
    let claimed: SignedDaemonCommand = crate::SinomoObjectCodec::default().decode(auth)?;
    let path_key = NetworkPathKey::for_peer_public_key(&claimed.public_key);
    // The daemon request signature establishes the caller identity. The
    // transport is trusted, so session frames do not carry a second proof or
    // content digest. Body bytes are still not read before authentication.
    let authentication = authenticate(request, &claimed.node_id, &claimed.public_key);
    let descriptor = descriptor.clone();
    let frame_reader = SessionFrameReader::after_bootstrap(reader, policy, header_buffer);
    Ok((payload, descriptor, frame_reader, authentication, path_key))
}

fn outgoing_body_failure_can_race_response_head(error: &NetworkError) -> bool {
    match error {
        NetworkError::Io { operation, .. } | NetworkError::Timeout { operation, .. } => {
            operation.starts_with("write transfer")
                || operation == "wait for object RPC transfer acknowledgement"
        }
        _ => false,
    }
}

/// Owns every background task and writer handle created while a canonical
/// client session is still being established.
///
/// Route deadlines cancel the session future by dropping it. Tokio join
/// handles detach on drop, so keeping the raw handles as locals would leave a
/// cancelled reader/keepalive pair holding the socket open until the peer's
/// no-progress timeout. This guard closes that cancellation hole. Successful
/// calls transfer the tasks to the returned body, whose existing drop behavior
/// then owns application-side cancellation.
struct ClientSessionTaskGuard {
    writer: Option<SessionWriterHandle>,
    reader: Option<tokio::task::JoinHandle<()>>,
    keepalive: Option<tokio::task::JoinHandle<()>>,
    outgoing: Option<tokio::task::JoinHandle<Result<(), NetworkError>>>,
}

impl ClientSessionTaskGuard {
    fn new(
        writer: SessionWriterHandle,
        reader: tokio::task::JoinHandle<()>,
        keepalive: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            writer: Some(writer),
            reader: Some(reader),
            keepalive: Some(keepalive),
            outgoing: None,
        }
    }

    fn writer(&self) -> &SessionWriterHandle {
        self.writer
            .as_ref()
            .expect("client session writer remains owned until task transfer")
    }

    fn transfer_to_body(
        mut self,
        mut body: ObjectRpcIncomingBody,
        outgoing_body_tx: IncomingBodySender,
    ) -> ObjectRpcIncomingBody {
        if let Some(task) = self.reader.take() {
            body.hold_completion_task(task);
        }
        if let Some(task) = self.keepalive.take() {
            body.hold_completion_task(task);
        }
        if let Some(task) = self.outgoing.take() {
            body.hold_completion_task(tokio::spawn(async move {
                match task.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => outgoing_body_tx.fail(error.to_string()),
                    Err(source) => outgoing_body_tx.fail(format!(
                        "object RPC request body task failed after response head: {source}"
                    )),
                }
            }));
        }
        // The transferred tasks retain any writer handles they still need.
        // Dropping this establishment handle lets the scheduler and socket
        // close as soon as those tasks complete or the returned body aborts.
        self.writer.take();
        body
    }
}

impl Drop for ClientSessionTaskGuard {
    fn drop(&mut self) {
        if let Some(task) = self.reader.take() {
            task.abort();
        }
        if let Some(task) = self.keepalive.take() {
            task.abort();
        }
        if let Some(task) = self.outgoing.take() {
            task.abort();
        }
        self.writer.take();
    }
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
    let perf_enabled = crate::route::network_perf_trace_enabled();
    let perf_route = perf_enabled.then(|| prepared.request.route.clone());
    let request_uses_relay = prepared.request.route == crate::CLUSTER_RELAY_ROUTE;
    let session_id = prepared.descriptor.session_id.clone();
    let perf_session_id = perf_enabled.then(|| session_id.clone());
    let response_proof_session_id = prepared.response_proof_session_id.clone();
    let response_proof_public_key = expected_response_origin_public_key;
    let response_link_session_id = session_id.clone();
    let response_link_public_key = expected_responder_public_key.clone();
    let request_open = prepared.descriptor.request_body.clone();
    let path_key = NetworkPathKey::for_peer_public_key(&expected_responder_public_key);
    let path_estimator = shared_network_path_estimator(&path_key);
    let (read_half, write_half) = tokio::io::split(stream);
    let writer =
        SessionWriterHandle::spawn(SessionFrameWriter::new(write_half, local_data_dir, policy)?);
    let reader = SessionFrameReader::new(read_half, policy);
    let outgoing = Arc::new(Mutex::new(SharedOutgoingTransfer {
        initialized: true,
        transfer_id: request_open.as_ref().map(|open| open.transfer_id.clone()),
        complete: request_open.is_none(),
    }));
    let (feedback_tx, feedback_rx) = tokio::sync::mpsc::channel(16);
    let (body_tx, mut incoming_body) =
        ObjectRpcIncomingBody::channel(None, ObjectRpcSessionBodyRole::None);
    let (head_tx, head_rx) = tokio::sync::oneshot::channel();
    let read_body_tx = body_tx.clone();
    let reader_writer = writer.clone();
    let reader_outgoing = Arc::clone(&outgoing);
    let reader_path_key = path_key.clone();
    let reader_path_estimator = path_estimator.clone();
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
            reader_path_key,
            reader_path_estimator,
        )
        .await
        {
            read_body_tx.fail(error.to_string());
        }
    });
    let keepalive_task = spawn_session_keepalive(writer.clone(), policy);
    let mut tasks = ClientSessionTaskGuard::new(writer, reader_task, keepalive_task);

    let send_head_started = perf_enabled.then(std::time::Instant::now);
    let send_head_result = send_session_payload(
        tasks.writer(),
        SessionWirePayload::RequestHead {
            request: prepared.request,
            descriptor: prepared.descriptor,
        },
    )
    .await;
    if let (Some(started), Some(route), Some(session_id)) = (
        send_head_started,
        perf_route.as_deref(),
        perf_session_id.as_ref(),
    ) {
        tracing::info!(
            target: "sinomo.network.perf",
            stage = "client_send_head",
            route,
            session_id = %session_id,
            relay = request_uses_relay,
            ok = send_head_result.is_ok(),
            elapsed_us = crate::route::network_perf_trace_elapsed_us(started),
        );
    }
    send_head_result?;

    let wait_response_started = perf_enabled.then(std::time::Instant::now);
    tasks.outgoing = if let Some(open) = request_open {
        let outgoing_writer = tasks.writer().clone();
        let outgoing_path_estimator = path_estimator.clone();
        Some(tokio::spawn(async move {
            pump_outgoing_body(
                outgoing_writer,
                prepared.body,
                open,
                feedback_rx,
                policy,
                outgoing_path_estimator,
            )
            .await
        }))
    } else {
        drop(feedback_rx);
        None
    };

    let mut head_rx = head_rx;
    let mut outgoing_failure = None;
    enum ClientSessionWait {
        Head(Result<SessionWirePayload, tokio::sync::oneshot::error::RecvError>),
        Outgoing(Result<Result<(), NetworkError>, tokio::task::JoinError>),
    }
    let first = if let Some(task) = tasks.outgoing.as_mut() {
        tokio::select! {
            head = &mut head_rx => ClientSessionWait::Head(head),
            outgoing = task => ClientSessionWait::Outgoing(outgoing),
        }
    } else {
        ClientSessionWait::Head((&mut head_rx).await)
    };
    let head = match first {
        ClientSessionWait::Head(Ok(head)) => head,
        ClientSessionWait::Head(Err(_)) => {
            // The session reader forwards transfer feedback before dropping
            // its response-head sender. If the peer terminates immediately
            // after a matching Cancel/Reset, both the closed head channel and
            // the outgoing task become ready together. Join the outgoing task
            // so its exact terminal transfer reason wins over the secondary
            // reader-EOF symptom, independent of select scheduling order.
            if let Some(task) = tasks.outgoing.take() {
                match task.await {
                    Ok(Err(error)) => {
                        body_tx.fail(error.to_string());
                        return Err(error);
                    }
                    Err(source) => {
                        let error = NetworkError::Io {
                            operation: "send object RPC request body".to_string(),
                            detail: format!("request body task failed: {source}"),
                        };
                        body_tx.fail(error.to_string());
                        return Err(error);
                    }
                    Ok(Ok(())) => {}
                }
            }
            return Err(NetworkError::Io {
                operation: "receive object RPC response session head".to_string(),
                detail: "session reader stopped before the response head".to_string(),
            });
        }
        ClientSessionWait::Outgoing(outgoing) => {
            tasks.outgoing.take();
            match outgoing {
                Ok(Ok(())) => head_rx.await.map_err(|_| NetworkError::Io {
                    operation: "receive object RPC response session head".to_string(),
                    detail: "session reader stopped after the request body completed".to_string(),
                })?,
                Ok(Err(error)) => {
                    if !outgoing_body_failure_can_race_response_head(&error) {
                        body_tx.fail(error.to_string());
                        return Err(error);
                    }
                    // A server can reject an authenticated session before it
                    // consumes the request body. In that case the response
                    // head is written before the transport closes, while the
                    // outgoing body task can observe the close first. Keep
                    // reading for at most one no-progress interval to prefer
                    // that signed, structured rejection over a secondary
                    // request-body transport failure. Local body-source
                    // failures still return immediately above.
                    outgoing_failure = Some(error);
                    match tokio::time::timeout(policy.health.no_progress_timeout, &mut head_rx)
                        .await
                    {
                        Ok(Ok(head)) => head,
                        Ok(Err(_)) | Err(_) => {
                            let error = outgoing_failure
                                .take()
                                .expect("outgoing failure was recorded");
                            body_tx.fail(error.to_string());
                            return Err(error);
                        }
                    }
                }
                Err(source) => {
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
    if let (Some(started), Some(route), Some(session_id)) = (
        wait_response_started,
        perf_route.as_deref(),
        perf_session_id.as_ref(),
    ) {
        tracing::info!(
            target: "sinomo.network.perf",
            stage = "client_wait_response_head",
            route,
            session_id = %session_id,
            relay = request_uses_relay,
            ok = true,
            elapsed_us = crate::route::network_perf_trace_elapsed_us(started),
        );
    }
    let SessionWirePayload::ResponseHead {
        mut response,
        body_role,
        body,
    } = head
    else {
        return Err(NetworkError::Serialization {
            detail: "object RPC client received a non-response session head".to_string(),
        });
    };
    body_role.validate(body.as_ref())?;
    if body_role == ObjectRpcSessionBodyRole::Value && !response.body.is_empty() {
        return Err(NetworkError::Serialization {
            detail: "object RPC response value is present in both the signed head and transfer"
                .to_string(),
        });
    }
    let response_origin_verified = response_origin_is_verified(
        &response,
        &response_proof_public_key,
        &response_proof_session_id,
    );
    // A relay can receive the final response over an authenticated pooled
    // channel. Such a response carries the exact target node id but no
    // canonical target proof for the upstream session, so the relay signs the
    // canonical hop as itself. Keep that independently verified hop identity
    // for the routing layer; it is never sufficient without an exact target
    // attestation in the response metadata.
    let response_link_verified = response_origin_verified
        || response_origin_is_verified(
            &response,
            &response_link_public_key,
            &response_link_session_id,
        );
    if !response_origin_verified && !request_uses_relay {
        return Err(NetworkError::AuthDenied);
    }
    if body.is_none() && response.error_detail().is_err() {
        // Bootstrap authorization failures deliberately carry no response
        // body: the signed metadata is the complete error contract. Do not
        // wait for an outbound request value or attachment that the rejecting
        // server is required not to consume.
        drop(incoming_body);
        return Ok(ObjectRpcSessionResult::with_response_verification(
            response,
            ObjectRpcIncomingBody::empty(),
            response_origin_verified,
            response_link_verified,
        ));
    }
    if let Some(error) = outgoing_failure {
        body_tx.fail(error.to_string());
        return Err(error);
    }
    if body.is_none() {
        if let Some(task) = tasks.outgoing.take() {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    body_tx.fail(error.to_string());
                    return Err(error);
                }
                Err(source) => {
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
        incoming_body.set_role(body_role);
    }
    if body_role == ObjectRpcSessionBodyRole::Value
        && let Some(task) = tasks.outgoing.take()
    {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                body_tx.fail(error.to_string());
                return Err(error);
            }
            Err(source) => {
                let error = NetworkError::Io {
                    operation: "finish object RPC request body".to_string(),
                    detail: format!("request body task failed: {source}"),
                };
                body_tx.fail(error.to_string());
                return Err(error);
            }
        }
    }
    if body_role == ObjectRpcSessionBodyRole::Value {
        let (value, forward) = incoming_body
            .collect_value_to_vec_for_forward("materialize object RPC response value")
            .await?;
        response.body = value;
        incoming_body = forward;
    }
    let incoming_body = tasks.transfer_to_body(incoming_body, body_tx);
    Ok(ObjectRpcSessionResult::with_response_verification(
        response,
        incoming_body,
        response_origin_verified,
        response_link_verified,
    ))
}

/// Handle returned with an authenticated server request. Sending the response
/// starts its body producer while the background reader continues consuming
/// and acknowledging the request body.
pub struct ObjectRpcSessionResponder {
    writer: SessionWriterHandle,
    outgoing: Arc<Mutex<SharedOutgoingTransfer>>,
    feedback: Option<tokio::sync::mpsc::Receiver<TransferControl>>,
    reader_task: Option<tokio::task::JoinHandle<()>>,
    session_id: TransferId,
    operation_id: Option<String>,
    policy: NetworkCallPolicy,
    keepalive_task: Option<tokio::task::JoinHandle<()>>,
    path_estimator: NetworkPathEstimator,
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
        if response.body.has_value_intent() && response.body.is_empty() {
            response.body = if response.response.body.len() <= INLINE_VALUE_MAX_BYTES {
                ObjectRpcBodySource::empty()
            } else {
                let value = std::mem::take(&mut response.response.body);
                ObjectRpcBodySource::bytes(value).with_role(ObjectRpcSessionBodyRole::Value)
            };
        }
        let body_open = {
            self.writer
                .ensure_response_identity_proof(&self.session_id, &mut response.response)?;
            let body_role = response.body.role();
            let body_open = (!response.body.is_empty()).then(|| TransferOpen {
                transfer_id: TransferId::generate(),
                route: response.response.route.clone(),
                operation_id: self.operation_id.clone(),
                total_bytes: response.body.total_bytes(),
                replayable: response.body.is_replayable(),
                requested_window: TransferFlowWindow::maximum_supported(),
            });
            body_role.validate(body_open.as_ref())?;
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
                body_role,
                body: body_open.clone(),
            };
            self.writer.send(payload).await?;
            body_open
        };
        if let Some(open) = body_open {
            let feedback = self.feedback.take().ok_or_else(|| NetworkError::Io {
                operation: "start object RPC response transfer".to_string(),
                detail: "session response feedback receiver is unavailable".to_string(),
            })?;
            pump_outgoing_body(
                self.writer.clone(),
                response.body,
                open,
                feedback,
                self.policy,
                self.path_estimator.clone(),
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
/// are consumed before that callback succeeds.
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
    let (initial_head, descriptor, reader, authentication, path_key) =
        receive_server_session_bootstrap(read_half, policy, authenticate).await?;
    let SessionWirePayload::RequestHead { .. } = &initial_head else {
        unreachable!("bootstrap validates request head");
    };
    if let Err(error) = authentication {
        let SessionWirePayload::RequestHead { request, .. } = &initial_head else {
            unreachable!("bootstrap validates request head");
        };
        let mut response =
            crate::ObjectRpcResponse::encode_error(request.route.clone(), error.to_string())?;
        // Error metadata is sufficient for ObjectRpcResponse::error_detail;
        // keeping this head body-free lets the client stop its request body
        // without starting a response transfer.
        response.body.clear();
        let mut writer = SessionFrameWriter::new(write_half, local_data_dir, policy)?;
        ensure_response_identity_proof_with_writer(&writer, &descriptor.session_id, &mut response)?;
        writer
            .send(SessionWirePayload::ResponseHead {
                response,
                body_role: ObjectRpcSessionBodyRole::None,
                body: None,
            })
            .await?;
        return Err(error);
    }
    let writer =
        SessionWriterHandle::spawn(SessionFrameWriter::new(write_half, local_data_dir, policy)?);
    let outgoing = Arc::new(Mutex::new(SharedOutgoingTransfer::default()));
    let (feedback_tx, feedback_rx) = tokio::sync::mpsc::channel(16);
    let (body_tx, mut incoming_body) = if descriptor.request_body.is_some() {
        let (sender, body) = ObjectRpcIncomingBody::channel(
            descriptor
                .request_body
                .as_ref()
                .and_then(|open| open.total_bytes),
            descriptor.request_body_role,
        );
        (Some(sender), body)
    } else {
        (None, ObjectRpcIncomingBody::empty())
    };
    let (head_tx, head_rx) = tokio::sync::oneshot::channel();
    let reader_writer = writer.clone();
    let reader_outgoing = Arc::clone(&outgoing);
    let failure_body_tx = body_tx.clone();
    let expected_open = descriptor.request_body.clone();
    let path_estimator = shared_network_path_estimator(&path_key);
    let reader_path_estimator = path_estimator.clone();
    let reader_path_key = path_key;
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
            reader_path_key,
            reader_path_estimator,
        )
        .await
            && let Some(sender) = failure_body_tx
        {
            sender.fail(error.to_string());
        }
    });
    let keepalive_task = spawn_session_keepalive(writer.clone(), policy);
    let delivered_head = head_rx.await.map_err(|_| NetworkError::Io {
        operation: "accept object RPC request session head".to_string(),
        detail: "session reader stopped during request bootstrap".to_string(),
    })?;
    let SessionWirePayload::RequestHead { mut request, .. } = delivered_head else {
        unreachable!("server bootstrap delivers request head");
    };
    if descriptor.request_body_role == ObjectRpcSessionBodyRole::Value {
        if !request.body.is_empty() {
            return Err(NetworkError::Serialization {
                detail: "object RPC value is present in both the signed head and transfer"
                    .to_string(),
            });
        }
        request.body = incoming_body
            .collect_value_to_vec("materialize object RPC request value")
            .await?;
        incoming_body = ObjectRpcIncomingBody::empty();
    }
    let responder = ObjectRpcSessionResponder {
        writer,
        outgoing,
        feedback: Some(feedback_rx),
        reader_task: Some(reader_task),
        session_id: descriptor.session_id,
        operation_id: request.operation_id().map(ToOwned::to_owned),
        policy,
        keepalive_task: Some(keepalive_task),
        path_estimator,
    };
    Ok((
        ObjectRpcSessionRequest::new(request, incoming_body),
        responder,
    ))
}

#[derive(Clone, Debug)]
struct ReplayableBytes {
    bytes: Bytes,
}

#[derive(Clone, Debug)]
struct ReplayableRepeatedBytes {
    pattern: Bytes,
    total_bytes: u64,
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

#[derive(Debug)]
struct OnePassIncomingChunkStream {
    stream: Option<ObjectRpcIncomingBodyChunkStream>,
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
    RepeatedBytes(ReplayableRepeatedBytes),
    File(ReplayableFile),
    Reader(Mutex<OnePassReader>),
    AsyncReader(Mutex<OnePassAsyncReader>),
    IncomingChunkStream(Mutex<OnePassIncomingChunkStream>),
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
    role: ObjectRpcSessionBodyRole,
    materialized_value_forward: bool,
    lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl fmt::Debug for ObjectRpcBodySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectRpcBodySource")
            .field("kind", &self.kind)
            .field("role", &self.role)
            .field(
                "materialized_value_forward",
                &self.materialized_value_forward,
            )
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
            role: ObjectRpcSessionBodyRole::None,
            materialized_value_forward: false,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn bytes(bytes: impl Into<Vec<u8>>) -> Self {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Self::empty();
        }
        Self {
            kind: ObjectRpcBodySourceKind::Bytes(ReplayableBytes {
                bytes: Bytes::from(bytes),
            }),
            role: ObjectRpcSessionBodyRole::Attachment,
            materialized_value_forward: false,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Builds a replayable, bounded-memory zero-filled attachment source.
    ///
    /// Every producer shares one immutable transfer-sized backing buffer and
    /// emits zero-copy `Bytes` slices from it until `total_bytes` is reached.
    /// This is useful for generated payloads and benchmarks that must measure
    /// transport work rather than an `AsyncRead` fill loop.
    pub fn zeroes(total_bytes: u64) -> Self {
        if total_bytes == 0 {
            return Self::empty();
        }
        static ZERO_PATTERN: OnceLock<Bytes> = OnceLock::new();
        let pattern = ZERO_PATTERN
            .get_or_init(|| Bytes::from(vec![0_u8; DEFAULT_TRANSFER_FRAME_BYTES as usize]))
            .clone();
        Self {
            kind: ObjectRpcBodySourceKind::RepeatedBytes(ReplayableRepeatedBytes {
                pattern,
                total_bytes,
            }),
            role: ObjectRpcSessionBodyRole::Attachment,
            materialized_value_forward: false,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
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
            role: if metadata.len() == 0 {
                ObjectRpcSessionBodyRole::None
            } else {
                ObjectRpcSessionBodyRole::Attachment
            },
            materialized_value_forward: false,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub fn reader(reader: Box<dyn Read + Send>, total_bytes: Option<u64>) -> Self {
        Self {
            kind: ObjectRpcBodySourceKind::Reader(Mutex::new(OnePassReader {
                reader: Some(reader),
                total_bytes,
            })),
            role: if total_bytes == Some(0) {
                ObjectRpcSessionBodyRole::None
            } else {
                ObjectRpcSessionBodyRole::Attachment
            },
            materialized_value_forward: false,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
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
            role: if total_bytes == Some(0) {
                ObjectRpcSessionBodyRole::None
            } else {
                ObjectRpcSessionBodyRole::Attachment
            },
            materialized_value_forward: false,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
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
        let role = body.role;
        let materialized_value_forward = body.is_materialized_value_forward();
        Self {
            kind: ObjectRpcBodySourceKind::AsyncReader(Mutex::new(OnePassAsyncReader {
                reader: Some(Box::pin(body)),
                total_bytes,
            })),
            role,
            materialized_value_forward,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Opens the private ownership-moving adapter used by relay forwarding.
    ///
    /// Unlike [`Self::from_incoming_body`], this path does not expose the
    /// incoming body through `AsyncRead`: received `Bytes` chunks move into the
    /// outgoing producer and are only split into zero-copy slices when the two
    /// carrier frame sizes differ.
    pub(crate) fn from_incoming_chunk_stream(body: ObjectRpcIncomingBody) -> Self {
        let total_bytes = body.total_bytes();
        let role = body.role;
        let materialized_value_forward = body.is_materialized_value_forward();
        Self {
            kind: ObjectRpcBodySourceKind::IncomingChunkStream(Mutex::new(
                OnePassIncomingChunkStream {
                    stream: Some(body.into_chunk_stream()),
                    total_bytes,
                },
            )),
            role,
            materialized_value_forward,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn role(&self) -> ObjectRpcSessionBodyRole {
        if self.is_empty() {
            ObjectRpcSessionBodyRole::None
        } else {
            self.role
        }
    }

    fn with_role(mut self, role: ObjectRpcSessionBodyRole) -> Self {
        self.role = if self.is_empty() {
            ObjectRpcSessionBodyRole::None
        } else {
            role
        };
        self
    }

    fn value_intent() -> Self {
        let mut source = Self::empty();
        source.role = ObjectRpcSessionBodyRole::Value;
        source
    }

    pub(crate) fn has_value_intent(&self) -> bool {
        self.role == ObjectRpcSessionBodyRole::Value
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.kind, ObjectRpcBodySourceKind::Empty)
            || matches!(&self.kind, ObjectRpcBodySourceKind::Bytes(source) if source.bytes.is_empty())
            || matches!(&self.kind, ObjectRpcBodySourceKind::RepeatedBytes(source)
                if source.total_bytes == 0)
            || matches!(&self.kind, ObjectRpcBodySourceKind::File(source) if source.total_bytes == 0)
            || matches!(&self.kind, ObjectRpcBodySourceKind::Reader(source)
                if source.lock().ok().and_then(|source| source.total_bytes) == Some(0))
            || matches!(&self.kind, ObjectRpcBodySourceKind::AsyncReader(source)
                if source.lock().ok().and_then(|source| source.total_bytes) == Some(0))
            || matches!(&self.kind, ObjectRpcBodySourceKind::IncomingChunkStream(source)
                if source.lock().ok().and_then(|source| source.total_bytes) == Some(0))
    }

    pub fn total_bytes(&self) -> Option<u64> {
        match &self.kind {
            ObjectRpcBodySourceKind::Empty => Some(0),
            ObjectRpcBodySourceKind::Bytes(source) => u64::try_from(source.bytes.len()).ok(),
            ObjectRpcBodySourceKind::RepeatedBytes(source) => Some(source.total_bytes),
            ObjectRpcBodySourceKind::File(source) => Some(source.total_bytes),
            ObjectRpcBodySourceKind::Reader(source) => {
                source.lock().ok().and_then(|source| source.total_bytes)
            }
            ObjectRpcBodySourceKind::AsyncReader(source) => {
                source.lock().ok().and_then(|source| source.total_bytes)
            }
            ObjectRpcBodySourceKind::IncomingChunkStream(source) => {
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
            ObjectRpcBodySourceKind::Reader(_)
                | ObjectRpcBodySourceKind::AsyncReader(_)
                | ObjectRpcBodySourceKind::IncomingChunkStream(_)
        )
    }

    fn open(&self) -> Result<Box<dyn Read + Send>, NetworkError> {
        match &self.kind {
            ObjectRpcBodySourceKind::Empty => Ok(Box::new(std::io::empty())),
            ObjectRpcBodySourceKind::Bytes(source) => {
                Ok(Box::new(BytesReader::new(source.bytes.clone())))
            }
            ObjectRpcBodySourceKind::RepeatedBytes(source) => Ok(Box::new(
                RepeatedBytesReader::new(source.pattern.clone(), source.total_bytes),
            )),
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
            ObjectRpcBodySourceKind::IncomingChunkStream(_) => Err(body_replay_error(
                "incoming chunk stream requires the ownership-moving producer path",
            )),
        }
    }

    /// Materializes this source under an explicit caller-owned resource bound.
    /// This does not change the network protocol or select a transfer path.
    pub async fn collect_to_vec(self, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
        if let ObjectRpcBodySourceKind::IncomingChunkStream(source) = &self.kind {
            let mut stream = source
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open incoming object RPC chunk stream".to_string(),
                    detail: "incoming chunk stream lock poisoned".to_string(),
                })?
                .stream
                .take()
                .ok_or_else(|| body_replay_error("incoming chunk stream was already consumed"))?;
            return collect_incoming_chunks_bounded(&mut stream, max_bytes).await;
        }
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

    pub(crate) fn producer(&self, max_chunk_bytes: usize) -> Result<BodyProducer, NetworkError> {
        if matches!(self.kind, ObjectRpcBodySourceKind::Empty) {
            return Ok(BodyProducer::Bytes(BytesBodyProducer {
                bytes: Bytes::new(),
                offset: 0,
                chunk_bytes: max_chunk_bytes.max(1),
                _lifetime_guards: Arc::clone(&self.lifetime_guards),
            }));
        }
        if let ObjectRpcBodySourceKind::Bytes(source) = &self.kind {
            return Ok(BodyProducer::Bytes(BytesBodyProducer {
                bytes: source.bytes.clone(),
                offset: 0,
                chunk_bytes: max_chunk_bytes.max(1),
                _lifetime_guards: Arc::clone(&self.lifetime_guards),
            }));
        }
        if let ObjectRpcBodySourceKind::RepeatedBytes(source) = &self.kind {
            return Ok(BodyProducer::RepeatedBytes(RepeatedBytesBodyProducer {
                pattern: source.pattern.clone(),
                pattern_offset: 0,
                remaining_bytes: source.total_bytes,
                chunk_bytes: max_chunk_bytes.max(1),
                _lifetime_guards: Arc::clone(&self.lifetime_guards),
            }));
        }
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
        if let ObjectRpcBodySourceKind::IncomingChunkStream(source) = &self.kind {
            let stream = source
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open incoming object RPC chunk stream".to_string(),
                    detail: "incoming chunk stream lock poisoned".to_string(),
                })?
                .stream
                .take()
                .ok_or_else(|| body_replay_error("incoming chunk stream was already consumed"))?;
            return Ok(BodyProducer::Incoming(IncomingBodyProducer {
                stream,
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
            ObjectRpcBodySourceKind::RepeatedBytes(source) => {
                ObjectRpcBodySourceKind::RepeatedBytes(source.clone())
            }
            ObjectRpcBodySourceKind::File(source) => ObjectRpcBodySourceKind::File(source.clone()),
            ObjectRpcBodySourceKind::Reader(_)
            | ObjectRpcBodySourceKind::AsyncReader(_)
            | ObjectRpcBodySourceKind::IncomingChunkStream(_) => {
                return None;
            }
        };
        Some(Self {
            kind,
            role: self.role,
            materialized_value_forward: self.materialized_value_forward,
            lifetime_guards: Arc::clone(&self.lifetime_guards),
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
struct BytesReader {
    bytes: Bytes,
    offset: usize,
}

#[derive(Debug)]
struct RepeatedBytesReader {
    pattern: Bytes,
    pattern_offset: usize,
    remaining_bytes: u64,
}

impl RepeatedBytesReader {
    fn new(pattern: Bytes, total_bytes: u64) -> Self {
        debug_assert!(!pattern.is_empty());
        Self {
            pattern,
            pattern_offset: 0,
            remaining_bytes: total_bytes,
        }
    }
}

impl Read for RepeatedBytesReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() || self.remaining_bytes == 0 {
            return Ok(0);
        }
        let wanted = output
            .len()
            .min(usize::try_from(self.remaining_bytes).unwrap_or(usize::MAX));
        let mut written = 0;
        while written < wanted {
            let available = self.pattern.len() - self.pattern_offset;
            let copied = available.min(wanted - written);
            output[written..written + copied]
                .copy_from_slice(&self.pattern[self.pattern_offset..self.pattern_offset + copied]);
            written += copied;
            self.pattern_offset += copied;
            if self.pattern_offset == self.pattern.len() {
                self.pattern_offset = 0;
            }
        }
        self.remaining_bytes -= written as u64;
        Ok(written)
    }
}

impl BytesReader {
    fn new(bytes: Bytes) -> Self {
        Self { bytes, offset: 0 }
    }
}

impl Read for BytesReader {
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
    Bytes(Bytes),
    Error(std::io::Error),
}

#[derive(Debug)]
enum MultiplexedBodyTerminal {
    Pending,
    Complete,
    Failed(String),
}

/// Narrow adapter used by the multiplexed channel. Its one-slot application
/// queue is fed by a per-call delivery task, never by the physical channel
/// reader, so a stalled consumer cannot block unrelated calls.
#[derive(Debug)]
pub(crate) struct ObjectRpcChannelBodySender {
    sender: IncomingBodySender,
    terminal: Arc<Mutex<MultiplexedBodyTerminal>>,
    staging_root: Result<PathBuf, String>,
    transfer_id: TransferId,
    staging: Option<SessionTransferStagingFile>,
    received_bytes: u64,
    consumer_closed: bool,
}

impl ObjectRpcChannelBodySender {
    /// Moves one ordered channel quantum to the application fast path. A
    /// consumer that remains full beyond the short direct-delivery grace gets
    /// an on-disk suffix instead, releasing carrier-owned pooled bytes before
    /// the physical reader advances to later calls or controls.
    ///
    /// `Ok(false)` means the public body was abandoned. Request ingress may
    /// keep draining and crediting such a body; response ingress resets the
    /// call because its only consumer disappeared.
    pub(crate) async fn send(&mut self, bytes: impl Into<Bytes>) -> Result<bool, NetworkError> {
        let bytes = bytes.into();
        let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let next_offset = self.received_bytes.checked_add(bytes_len).ok_or_else(|| {
            NetworkError::Serialization {
                detail: "multiplexed object RPC body offset overflow".to_string(),
            }
        })?;

        if self.consumer_closed || self.sender.consumer_closed() {
            self.consumer_closed = true;
            self.received_bytes = next_offset;
            return Ok(false);
        }

        if let Some(staging) = self.staging.as_mut() {
            staging.append(bytes).await?;
            self.received_bytes = next_offset;
            self.sender.advance_available(next_offset)?;
            return Ok(true);
        }

        match self.sender.deliver_with_grace(bytes).await {
            IncomingDirectDelivery::Delivered => {
                self.received_bytes = next_offset;
                Ok(true)
            }
            IncomingDirectDelivery::ConsumerClosed => {
                self.consumer_closed = true;
                self.received_bytes = next_offset;
                Ok(false)
            }
            IncomingDirectDelivery::Spill(bytes) => {
                let staging_root =
                    self.staging_root
                        .as_ref()
                        .map_err(|detail| NetworkError::Io {
                            operation: "resolve multiplexed object RPC body staging root"
                                .to_string(),
                            detail: detail.clone(),
                        })?;
                let (mut staging, setup) = SessionTransferStagingFile::create(
                    staging_root.clone(),
                    self.transfer_id.clone(),
                    self.received_bytes,
                )
                .await?;
                self.sender.start_spool(setup)?;
                staging.append(bytes).await?;
                self.received_bytes = next_offset;
                self.sender.advance_available(next_offset)?;
                self.staging = Some(staging);
                Ok(true)
            }
        }
    }

    pub(crate) fn fail(&mut self, detail: impl Into<String>) {
        let detail = detail.into();
        if let Ok(mut terminal) = self.terminal.lock()
            && matches!(&*terminal, MultiplexedBodyTerminal::Pending)
        {
            *terminal = MultiplexedBodyTerminal::Failed(detail.clone());
        }
        self.sender.fail(detail);
        self.staging.take();
    }

    /// Marks the ordered body complete after its framing and declared length
    /// have been validated by the channel state machine.
    pub(crate) async fn finish(&mut self, total_bytes: u64) -> Result<(), NetworkError> {
        if total_bytes != self.received_bytes {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "multiplexed object RPC body completed at {total_bytes} after delivering {} bytes",
                    self.received_bytes
                ),
            });
        }
        let completion = self
            .terminal
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "complete multiplexed object RPC body".to_string(),
                detail: "multiplexed body terminal state lock poisoned".to_string(),
            })
            .and_then(|mut terminal| match &*terminal {
                MultiplexedBodyTerminal::Pending => {
                    *terminal = MultiplexedBodyTerminal::Complete;
                    Ok(())
                }
                MultiplexedBodyTerminal::Complete => Ok(()),
                MultiplexedBodyTerminal::Failed(detail) => Err(NetworkError::Io {
                    operation: "complete multiplexed object RPC body".to_string(),
                    detail: detail.clone(),
                }),
            });
        completion?;
        self.sender
            .publish_completion(total_bytes, self.staging.is_some())?;
        self.staging.take();
        Ok(())
    }
}

#[derive(Debug)]
enum ObjectRpcIncomingBodyInner {
    Empty,
    Bytes(std::io::Cursor<Bytes>),
    Channel {
        receiver: tokio::sync::mpsc::Receiver<IncomingBodyItem>,
        current: Option<std::io::Cursor<Bytes>>,
    },
}

/// A received RPC attachment. It exposes ordinary asynchronous byte reads;
/// transport frames and chunk sizes remain private. Ordered frames move
/// through a bounded in-memory channel first. If the application falls behind,
/// the wire reader lazily spills the remaining suffix to a temporary file so
/// application backpressure cannot block full-duplex control traffic.
#[derive(Debug)]
pub struct ObjectRpcIncomingBody {
    inner: ObjectRpcIncomingBodyInner,
    // Natural EOF clears these handles, detaching still-needed session work;
    // an early application drop leaves them here so Drop cancels the session.
    completion_tasks: Vec<tokio::task::JoinHandle<()>>,
    total_bytes: Option<u64>,
    role: ObjectRpcSessionBodyRole,
    multiplexed_terminal: Option<Arc<Mutex<MultiplexedBodyTerminal>>>,
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
            role: ObjectRpcSessionBodyRole::None,
            multiplexed_terminal: None,
        }
    }

    pub fn bytes(bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            return Self::empty();
        }
        let total_bytes = u64::try_from(bytes.len()).ok();
        Self {
            inner: ObjectRpcIncomingBodyInner::Bytes(std::io::Cursor::new(Bytes::from(bytes))),
            completion_tasks: Vec::new(),
            total_bytes,
            role: ObjectRpcSessionBodyRole::Attachment,
            multiplexed_terminal: None,
        }
    }

    pub(crate) fn multiplexed_channel(
        total_bytes: Option<u64>,
    ) -> (ObjectRpcChannelBodySender, Self) {
        let staging_root = SinomoDataDir::from_env_or_home()
            .map(|data_dir| data_dir.root())
            .map_err(|error| error.to_string());
        Self::multiplexed_channel_with_staging_root(total_bytes, staging_root)
    }

    fn multiplexed_channel_with_staging_root(
        total_bytes: Option<u64>,
        staging_root: Result<PathBuf, String>,
    ) -> (ObjectRpcChannelBodySender, Self) {
        // One queued chunk plus one chunk held by the isolated delivery task
        // bounds a stalled call to two channel quanta.
        let terminal = Arc::new(Mutex::new(MultiplexedBodyTerminal::Pending));
        let (sender, body) = Self::channel_with_depth(
            total_bytes,
            ObjectRpcSessionBodyRole::Attachment,
            1,
            Some(Arc::clone(&terminal)),
        );
        (
            ObjectRpcChannelBodySender {
                sender,
                terminal: Arc::clone(&terminal),
                staging_root,
                transfer_id: TransferId::generate(),
                staging: None,
                received_bytes: 0,
                consumer_closed: false,
            },
            body,
        )
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.inner, ObjectRpcIncomingBodyInner::Empty) || self.total_bytes == Some(0)
    }

    pub const fn total_bytes(&self) -> Option<u64> {
        self.total_bytes
    }

    fn is_materialized_value_forward(&self) -> bool {
        // A materialized channel value keeps its forwarding role even though
        // the byte stream has already been collected into the response head.
        self.role == ObjectRpcSessionBodyRole::Value && self.is_empty()
    }

    fn materialized_value_forward(total_bytes: Option<u64>) -> Self {
        Self {
            inner: ObjectRpcIncomingBodyInner::Empty,
            completion_tasks: Vec::new(),
            total_bytes,
            role: ObjectRpcSessionBodyRole::Value,
            multiplexed_terminal: None,
        }
    }

    fn channel(
        total_bytes: Option<u64>,
        role: ObjectRpcSessionBodyRole,
    ) -> (IncomingBodySender, Self) {
        Self::channel_with_depth(total_bytes, role, BODY_READER_BUFFER_DEPTH, None)
    }

    fn channel_with_depth(
        total_bytes: Option<u64>,
        role: ObjectRpcSessionBodyRole,
        depth: usize,
        multiplexed_terminal: Option<Arc<Mutex<MultiplexedBodyTerminal>>>,
    ) -> (IncomingBodySender, Self) {
        let (body_sender, receiver) = tokio::sync::mpsc::channel(depth);
        let (setup_sender, setup_receiver) = tokio::sync::oneshot::channel();
        let (progress_sender, progress_receiver) =
            tokio::sync::watch::channel(IncomingSpoolProgress::default());
        // The spool tailer is deliberately detached from the application
        // reader's abort-on-drop session tasks. If the application ignores a
        // request body, closing the channel makes this task discard its tail
        // while the transport reader continues receiving it under bounded
        // peer credit.
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
            },
            Self {
                inner: ObjectRpcIncomingBodyInner::Channel {
                    receiver,
                    current: None,
                },
                completion_tasks: Vec::new(),
                total_bytes,
                role,
                multiplexed_terminal,
            },
        )
    }

    fn set_role(&mut self, role: ObjectRpcSessionBodyRole) {
        self.role = if self.is_empty() {
            ObjectRpcSessionBodyRole::None
        } else {
            role
        };
    }

    fn hold_completion_task(&mut self, task: tokio::task::JoinHandle<()>) {
        self.completion_tasks.push(task);
    }

    pub async fn collect_to_vec(mut self, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
        let mut output = Vec::new();
        if let Some(total_bytes) = self.total_bytes {
            let declared_bytes = usize::try_from(total_bytes).unwrap_or(usize::MAX);
            if declared_bytes > max_bytes {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: declared_bytes,
                    max_bytes,
                });
            }
            output.try_reserve_exact(declared_bytes).map_err(|_| {
                NetworkError::ResourceExhausted {
                    operation: "reserve object RPC incoming body".to_string(),
                    requested_bytes: declared_bytes,
                }
            })?;
        }
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

    /// Consumes and counts an incoming body without copying its chunks through
    /// the public `AsyncRead` adapter.
    ///
    /// This is an internal relay/control-plane escape hatch. Natural EOF still
    /// performs the same terminal validation and completion-task detachment as
    /// the application reader, while a transport failure remains an error.
    #[doc(hidden)]
    pub async fn discard_owned_chunks(self) -> Result<u64, NetworkError> {
        const OPERATION: &str = "discard object RPC incoming body";

        let mut stream = self.into_chunk_stream();
        let mut discarded_bytes = 0_u64;
        loop {
            let Some(bytes) =
                stream
                    .next_chunk(usize::MAX)
                    .await
                    .map_err(|source| NetworkError::Io {
                        operation: OPERATION.to_string(),
                        detail: source.to_string(),
                    })?
            else {
                return Ok(discarded_bytes);
            };
            let chunk_bytes =
                u64::try_from(bytes.len()).map_err(|_| NetworkError::Serialization {
                    detail: "object RPC incoming body byte count overflow while discarding"
                        .to_string(),
                })?;
            discarded_bytes = discarded_bytes.checked_add(chunk_bytes).ok_or_else(|| {
                NetworkError::Serialization {
                    detail: "object RPC incoming body byte count overflow while discarding"
                        .to_string(),
                }
            })?;
        }
    }

    /// Materializes a protocol-native value body without imposing a transport
    /// size policy. The value has already crossed the authenticated streaming
    /// path; failure here is limited to the host being unable to represent or
    /// allocate the resulting in-process `Vec` plus ordinary stream I/O.
    async fn collect_value_to_vec(
        mut self,
        operation: &'static str,
    ) -> Result<Vec<u8>, NetworkError> {
        let mut output = Vec::new();
        if let Some(total_bytes) = self.total_bytes {
            let declared_bytes =
                usize::try_from(total_bytes).map_err(|_| NetworkError::ResourceExhausted {
                    operation: operation.to_string(),
                    requested_bytes: usize::MAX,
                })?;
            output.try_reserve_exact(declared_bytes).map_err(|_| {
                NetworkError::ResourceExhausted {
                    operation: operation.to_string(),
                    requested_bytes: declared_bytes,
                }
            })?;
        }
        let mut buffer = vec![0_u8; BODY_READER_BUFFER_BYTES];
        loop {
            let read = self
                .read(&mut buffer)
                .await
                .map_err(|source| NetworkError::Io {
                    operation: operation.to_string(),
                    detail: source.to_string(),
                })?;
            if read == 0 {
                return Ok(output);
            }
            let next =
                output
                    .len()
                    .checked_add(read)
                    .ok_or_else(|| NetworkError::ResourceExhausted {
                        operation: operation.to_string(),
                        requested_bytes: usize::MAX,
                    })?;
            output
                .try_reserve(read)
                .map_err(|_| NetworkError::ResourceExhausted {
                    operation: operation.to_string(),
                    requested_bytes: next,
                })?;
            output.extend_from_slice(&buffer[..read]);
        }
    }

    async fn collect_value_to_vec_for_forward(
        self,
        operation: &'static str,
    ) -> Result<(Vec<u8>, Self), NetworkError> {
        let total_bytes = self.total_bytes;
        let value = self.collect_value_to_vec(operation).await?;
        Ok((value, Self::materialized_value_forward(total_bytes)))
    }

    /// Consumes the application adapter and exposes the received transport
    /// chunks to crate-internal relays without copying them through
    /// `AsyncRead` buffers.
    pub(crate) fn into_chunk_stream(self) -> ObjectRpcIncomingBodyChunkStream {
        ObjectRpcIncomingBodyChunkStream { body: self }
    }
}

/// Crate-private ownership-moving view of an incoming body.
///
/// This deliberately does not implement a public stream trait: transport
/// chunk boundaries stay private, while relay producers can forward the
/// underlying `Bytes` allocation directly.
#[derive(Debug)]
pub(crate) struct ObjectRpcIncomingBodyChunkStream {
    body: ObjectRpcIncomingBody,
}

impl ObjectRpcIncomingBodyChunkStream {
    async fn next_chunk(&mut self, max_chunk_bytes: usize) -> std::io::Result<Option<Bytes>> {
        let max_chunk_bytes = max_chunk_bytes.max(1);
        loop {
            match &mut self.body.inner {
                ObjectRpcIncomingBodyInner::Empty => {
                    self.body.completion_tasks.clear();
                    return Ok(None);
                }
                ObjectRpcIncomingBodyInner::Bytes(bytes) => {
                    let chunk = take_owned_cursor_chunk(bytes, max_chunk_bytes);
                    if chunk.is_none() {
                        self.body.completion_tasks.clear();
                    }
                    return Ok(chunk);
                }
                ObjectRpcIncomingBodyInner::Channel { receiver, current } => {
                    if let Some(bytes) = take_owned_current_chunk(current, max_chunk_bytes) {
                        return Ok(Some(bytes));
                    }
                    match receiver.recv().await {
                        Some(IncomingBodyItem::Bytes(bytes)) if bytes.is_empty() => continue,
                        Some(IncomingBodyItem::Bytes(bytes)) if bytes.len() <= max_chunk_bytes => {
                            return Ok(Some(bytes));
                        }
                        Some(IncomingBodyItem::Bytes(bytes)) => {
                            *current = Some(std::io::Cursor::new(bytes));
                        }
                        Some(IncomingBodyItem::Error(error)) => {
                            mark_multiplexed_body_terminal_error_delivered(
                                self.body.multiplexed_terminal.as_ref(),
                            );
                            return Err(error);
                        }
                        None => {
                            validate_multiplexed_body_eof(self.body.multiplexed_terminal.as_ref())?;
                            self.body.completion_tasks.clear();
                            return Ok(None);
                        }
                    }
                }
            }
        }
    }
}

fn take_owned_current_chunk(
    current: &mut Option<std::io::Cursor<Bytes>>,
    max_chunk_bytes: usize,
) -> Option<Bytes> {
    let mut cursor = current.take()?;
    let chunk = take_owned_cursor_chunk(&mut cursor, max_chunk_bytes);
    if !cursor.get_ref().is_empty() {
        *current = Some(cursor);
    }
    chunk
}

fn take_owned_cursor_chunk(
    cursor: &mut std::io::Cursor<Bytes>,
    max_chunk_bytes: usize,
) -> Option<Bytes> {
    let owned = std::mem::replace(cursor, std::io::Cursor::new(Bytes::new()));
    let position = usize::try_from(owned.position()).unwrap_or(usize::MAX);
    let mut remaining = owned.into_inner();
    let consumed = position.min(remaining.len());
    if consumed != 0 {
        drop(remaining.split_to(consumed));
    }
    if remaining.is_empty() {
        return None;
    }
    let take = remaining.len().min(max_chunk_bytes.max(1));
    let chunk = remaining.split_to(take);
    *cursor = std::io::Cursor::new(remaining);
    Some(chunk)
}

fn validate_multiplexed_body_eof(
    terminal: Option<&Arc<Mutex<MultiplexedBodyTerminal>>>,
) -> std::io::Result<()> {
    let Some(terminal) = terminal else {
        return Ok(());
    };
    match terminal.lock() {
        Ok(mut terminal) => {
            match std::mem::replace(&mut *terminal, MultiplexedBodyTerminal::Complete) {
                MultiplexedBodyTerminal::Pending => Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "multiplexed body ended before declared completion",
                )),
                MultiplexedBodyTerminal::Complete => Ok(()),
                MultiplexedBodyTerminal::Failed(detail) => Err(std::io::Error::other(detail)),
            }
        }
        Err(_) => Err(std::io::Error::other(
            "multiplexed body terminal state lock poisoned",
        )),
    }
}

fn mark_multiplexed_body_terminal_error_delivered(
    terminal: Option<&Arc<Mutex<MultiplexedBodyTerminal>>>,
) {
    let Some(terminal) = terminal else {
        return;
    };
    if let Ok(mut terminal) = terminal.lock()
        && matches!(&*terminal, MultiplexedBodyTerminal::Failed(_))
    {
        // The queued error is the single terminal-failure delivery. Mark it
        // consumed so later reads observe ordinary closed-stream EOF instead
        // of replaying the same failure indefinitely.
        *terminal = MultiplexedBodyTerminal::Complete;
    }
}

async fn collect_incoming_chunks_bounded(
    stream: &mut ObjectRpcIncomingBodyChunkStream,
    max_bytes: usize,
) -> Result<Vec<u8>, NetworkError> {
    let mut output = Vec::new();
    loop {
        let Some(bytes) = stream
            .next_chunk(BODY_READER_BUFFER_BYTES)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "read incoming object RPC chunk stream".to_string(),
                detail: source.to_string(),
            })?
        else {
            return Ok(output);
        };
        let next = output
            .len()
            .checked_add(bytes.len())
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
            .try_reserve(bytes.len())
            .map_err(|_| NetworkError::ResourceExhausted {
                operation: "collect incoming object RPC chunk stream".to_string(),
                requested_bytes: next,
            })?;
        output.extend_from_slice(&bytes);
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
                ObjectRpcIncomingBodyInner::Empty => {
                    this.completion_tasks.clear();
                    return Poll::Ready(Ok(()));
                }
                ObjectRpcIncomingBodyInner::Bytes(bytes) => {
                    let position = usize::try_from(bytes.position()).unwrap_or(usize::MAX);
                    if position >= bytes.get_ref().len() {
                        this.completion_tasks.clear();
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
                            mark_multiplexed_body_terminal_error_delivered(
                                this.multiplexed_terminal.as_ref(),
                            );
                            return Poll::Ready(Err(error));
                        }
                        Poll::Ready(None) => {
                            if let Err(error) =
                                validate_multiplexed_body_eof(this.multiplexed_terminal.as_ref())
                            {
                                return Poll::Ready(Err(error));
                            }
                            this.completion_tasks.clear();
                            return Poll::Ready(Ok(()));
                        }
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
}

#[derive(Debug)]
enum IncomingDirectDelivery {
    Delivered,
    Spill(Bytes),
    ConsumerClosed,
}

impl IncomingBodySender {
    fn consumer_closed(&self) -> bool {
        self.direct
            .lock()
            .map(|direct| {
                direct
                    .as_ref()
                    .is_none_or(tokio::sync::mpsc::Sender::is_closed)
            })
            .unwrap_or(true)
    }

    fn try_deliver(&self, bytes: impl Into<Bytes>) -> IncomingDirectDelivery {
        let bytes = bytes.into();
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

    /// Preserve the allocation-free `try_send` fast path, but do not turn a
    /// momentary full queue into a whole-body disk spool. The timeout keeps
    /// the wait finite: ignored consumers and simultaneous full-duplex bodies
    /// retain the existing spill escape hatch instead of deadlocking the wire
    /// reader behind application backpressure.
    async fn deliver_with_grace(&self, bytes: impl Into<Bytes>) -> IncomingDirectDelivery {
        self.deliver_with_grace_for(bytes, BODY_READER_DIRECT_DELIVERY_GRACE)
            .await
    }

    async fn deliver_with_grace_for(
        &self,
        bytes: impl Into<Bytes>,
        grace: std::time::Duration,
    ) -> IncomingDirectDelivery {
        let bytes = bytes.into();
        let bytes = match self.try_deliver(bytes) {
            IncomingDirectDelivery::Delivered => return IncomingDirectDelivery::Delivered,
            IncomingDirectDelivery::ConsumerClosed => {
                return IncomingDirectDelivery::ConsumerClosed;
            }
            IncomingDirectDelivery::Spill(bytes) => bytes,
        };
        let direct = match self.direct.lock() {
            Ok(direct) => direct.clone(),
            Err(_) => return IncomingDirectDelivery::ConsumerClosed,
        };
        let Some(direct) = direct else {
            return IncomingDirectDelivery::ConsumerClosed;
        };

        let wait_total = BODY_READER_DIRECT_WAIT_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
        let wait_started = std::time::Instant::now();
        let frame_bytes = bytes.len();
        match tokio::time::timeout(grace, direct.reserve()).await {
            Ok(Ok(permit)) => {
                permit.send(IncomingBodyItem::Bytes(bytes));
                let delivered_total =
                    BODY_READER_DIRECT_WAIT_DELIVERED_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
                tracing::debug!(
                    target: "sinomo.network.object_rpc",
                    frame_bytes,
                    wait_us = wait_started.elapsed().as_micros() as u64,
                    wait_total,
                    delivered_after_wait_total = delivered_total,
                    spill_after_wait_total = BODY_READER_DIRECT_WAIT_SPILL_TOTAL
                        .load(Ordering::Relaxed),
                    "object RPC body direct delivery resumed after bounded queue wait"
                );
                IncomingDirectDelivery::Delivered
            }
            Ok(Err(_)) => {
                let closed_total =
                    BODY_READER_DIRECT_WAIT_CLOSED_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
                tracing::debug!(
                    target: "sinomo.network.object_rpc",
                    frame_bytes,
                    wait_us = wait_started.elapsed().as_micros() as u64,
                    wait_total,
                    closed_after_wait_total = closed_total,
                    "object RPC body consumer closed during bounded queue wait"
                );
                IncomingDirectDelivery::ConsumerClosed
            }
            Err(_) => {
                let spill_total =
                    BODY_READER_DIRECT_WAIT_SPILL_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
                tracing::debug!(
                    target: "sinomo.network.object_rpc",
                    frame_bytes,
                    wait_us = wait_started.elapsed().as_micros() as u64,
                    grace_us = grace.as_micros() as u64,
                    wait_total,
                    spill_after_wait_total = spill_total,
                    delivered_after_wait_total = BODY_READER_DIRECT_WAIT_DELIVERED_TOTAL
                        .load(Ordering::Relaxed),
                    "object RPC body direct-delivery grace expired; falling back to spool"
                );
                IncomingDirectDelivery::Spill(bytes)
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

    /// Publishes completion to the application body.
    /// Callers must first write the transfer receipt to the peer.
    fn publish_completion(
        &self,
        available_bytes: u64,
        has_spool: bool,
    ) -> Result<(), NetworkError> {
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

    #[cfg(test)]
    fn complete(&self, available_bytes: u64, has_spool: bool) -> Result<(), NetworkError> {
        self.publish_completion(available_bytes, has_spool)
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
            let mut bytes = pooled_body_buffer(chunk_bytes);
            if let Err(source) = spool.file.read_exact(&mut bytes).await {
                drop(pooled_body_bytes(bytes));
                let _ = sender
                    .send(IncomingBodyItem::Error(std::io::Error::new(
                        source.kind(),
                        format!("read available object RPC body spool: {source}"),
                    )))
                    .await;
                return;
            }
            delivered_bytes += u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if sender
                .send(IncomingBodyItem::Bytes(pooled_body_bytes(bytes)))
                .await
                .is_err()
            {
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
        Self {
            request,
            body: body.with_role(ObjectRpcSessionBodyRole::Attachment),
        }
    }

    pub fn value(request: crate::ObjectRpcRequest) -> Self {
        Self {
            request,
            body: ObjectRpcBodySource::value_intent(),
        }
    }

    /// Rebinds a received request body for a trusted internal proxy hop.
    ///
    /// Non-empty attachment chunks retain ownership as they enter the outgoing
    /// producer. Empty bodies retain ordinary value semantics so large typed
    /// request values can still be moved out of the head during preparation.
    /// The prior hop's signed transfer descriptor is not reused: normal call
    /// preparation creates and signs a fresh descriptor for the next hop.
    #[doc(hidden)]
    pub fn from_relay_body(request: crate::ObjectRpcRequest, body: ObjectRpcIncomingBody) -> Self {
        if body.is_empty() {
            Self::value(request)
        } else {
            Self {
                request,
                body: ObjectRpcBodySource::from_incoming_chunk_stream(body),
            }
        }
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
        if self.body.has_value_intent() && self.body.is_empty() {
            self.body = if self.request.body.len() <= INLINE_VALUE_MAX_BYTES {
                ObjectRpcBodySource::empty()
            } else {
                let value = std::mem::take(&mut self.request.body);
                ObjectRpcBodySource::bytes(value).with_role(ObjectRpcSessionBodyRole::Value)
            };
        }
        let descriptor = ObjectRpcSessionDescriptor::for_call(&self);
        self.request.metadata.insert(
            OBJECT_RPC_SESSION_METADATA_KEY.to_string(),
            descriptor.encode_metadata()?,
        );
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
        self.body = self.body.with_role(descriptor.request_body_role);
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

impl PreparedSessionCall {
    /// Returns the still-unopened body after a prepared canonical dial fails.
    ///
    /// Route selection may prepare and sign a one-pass body before dialing so
    /// a stalled signer cannot leave an idle peer connection behind. A failed
    /// dial has not consumed the producer; the route layer combines this body
    /// with the logical request head it retained before route-specific wrapping.
    pub(crate) fn into_body_source(self) -> ObjectRpcBodySource {
        self.body
    }
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

/// Returns the request-head bytes covered by daemon authentication.
///
/// Ordinary RPC values are moved out of the signed head and carried by the
/// canonical transfer. The signed descriptor records that transfer's length,
/// while the server restores the value into `request.body` before
/// dispatch. Authentication therefore hashes an empty head body in both
/// states. Attachment calls retain their typed head body unchanged.
pub(crate) fn daemon_auth_request_head_body(request: &crate::ObjectRpcRequest) -> &[u8] {
    let Ok(descriptor) = session_descriptor_from_request(request) else {
        return &request.body;
    };
    if descriptor.request_body_role == ObjectRpcSessionBodyRole::Value
        && descriptor.request_body.is_some()
    {
        &[]
    } else {
        &request.body
    }
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

    pub fn value(request: crate::ObjectRpcRequest) -> Self {
        Self::new(request, ObjectRpcIncomingBody::empty())
    }

    /// Rebinds a received relay body to an already-signed inner request. The
    /// signed descriptor must agree with body presence, role, route, operation,
    /// declared length. Attachment bodies remain streaming; value bodies are
    /// restored into the typed request before
    /// this method returns.
    pub async fn bind_signed_inner_request(
        self,
        request: crate::ObjectRpcRequest,
    ) -> Result<Self, NetworkError> {
        self.bind_signed_inner_request_common(request).await
    }

    /// Rebinds an origin-signed relay request received over a physically
    /// authenticated multiplexed channel.
    ///
    /// The channel validates ordered offsets and declared length before
    /// publishing successful EOF.
    pub async fn bind_signed_inner_request_on_authenticated_channel(
        self,
        request: crate::ObjectRpcRequest,
    ) -> Result<Self, NetworkError> {
        self.bind_signed_inner_request_common(request).await
    }

    async fn bind_signed_inner_request_common(
        mut self,
        mut request: crate::ObjectRpcRequest,
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
        }
        self.body.set_role(descriptor.request_body_role);
        if descriptor.request_body_role == ObjectRpcSessionBodyRole::Value {
            if !request.body.is_empty() {
                return Err(NetworkError::Serialization {
                    detail:
                        "relayed object RPC value is present in both the signed head and transfer"
                            .to_string(),
                });
            }
            request.body = self
                .body
                .collect_value_to_vec("materialize relayed object RPC request value")
                .await?;
            self.body = ObjectRpcIncomingBody::empty();
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
    pub fn new(mut response: crate::ObjectRpcResponse, mut body: ObjectRpcBodySource) -> Self {
        if body.materialized_value_forward {
            let value = std::mem::take(&mut response.body);
            if value.is_empty() {
                body.kind = ObjectRpcBodySourceKind::Empty;
                body.role = ObjectRpcSessionBodyRole::None;
            } else {
                body.kind = ObjectRpcBodySourceKind::Bytes(ReplayableBytes {
                    bytes: Bytes::from(value),
                });
                body.role = ObjectRpcSessionBodyRole::Value;
            }
            body.materialized_value_forward = false;
            Self { response, body }
        } else {
            Self {
                response,
                body: body.with_role(ObjectRpcSessionBodyRole::Attachment),
            }
        }
    }

    pub fn value(response: crate::ObjectRpcResponse) -> Self {
        Self {
            response,
            body: ObjectRpcBodySource::value_intent(),
        }
    }
}

/// Client-side received response head and body.
#[derive(Debug)]
pub struct ObjectRpcSessionResult {
    pub response: crate::ObjectRpcResponse,
    pub body: ObjectRpcIncomingBody,
    response_origin_verified: bool,
    response_link_verified: bool,
    // A multiplexed typed value is materialized into `response.body`. Keep its
    // peer-memory reservation with the application-facing result instead of
    // releasing it as soon as the materializer queues the result. Otherwise a
    // caller can retain completed, unread results while recycling the channel
    // call permit and bypass the aggregate materialized-value budget.
    materialized_value_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl ObjectRpcSessionResult {
    pub fn new(response: crate::ObjectRpcResponse, body: ObjectRpcIncomingBody) -> Self {
        Self {
            response,
            body,
            response_origin_verified: true,
            response_link_verified: true,
            materialized_value_permit: None,
        }
    }

    pub fn value(response: crate::ObjectRpcResponse) -> Self {
        Self::new(response, ObjectRpcIncomingBody::empty())
    }

    pub(crate) fn materialized_channel_value(
        response: crate::ObjectRpcResponse,
        total_bytes: Option<u64>,
    ) -> Self {
        Self::new(
            response,
            ObjectRpcIncomingBody::materialized_value_forward(total_bytes),
        )
    }

    /// Splits the application-visible response head and body while keeping
    /// target-identity verification state private to the routing layer.
    pub fn into_parts(self) -> (crate::ObjectRpcResponse, ObjectRpcIncomingBody) {
        (self.response, self.body)
    }

    /// Converts a received result into the response for the next relay hop.
    ///
    /// This is an internal carrier adapter: it preserves the public semantic
    /// response-plus-attachment model while moving received `Bytes` chunks
    /// directly into the outgoing producer.
    #[doc(hidden)]
    pub fn into_relay_response(self) -> Result<ObjectRpcSessionResponse, NetworkError> {
        let Self {
            response,
            body,
            materialized_value_permit,
            ..
        } = self;
        let mut source = ObjectRpcBodySource::from_incoming_chunk_stream(body);
        if let Some(permit) = materialized_value_permit {
            source = source.with_lifetime_guard(permit)?;
        }
        Ok(ObjectRpcSessionResponse::new(response, source))
    }

    fn with_response_verification(
        response: crate::ObjectRpcResponse,
        body: ObjectRpcIncomingBody,
        response_origin_verified: bool,
        response_link_verified: bool,
    ) -> Self {
        Self {
            response,
            body,
            response_origin_verified,
            response_link_verified,
            materialized_value_permit: None,
        }
    }

    pub(crate) fn with_materialized_value_permit(
        mut self,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        self.materialized_value_permit = Some(permit);
        self
    }

    pub(crate) const fn response_origin_verified(&self) -> bool {
        self.response_origin_verified
    }

    pub(crate) const fn response_link_verified(&self) -> bool {
        self.response_link_verified
    }
}

pub(crate) enum BodyProducer {
    Bytes(BytesBodyProducer),
    RepeatedBytes(RepeatedBytesBodyProducer),
    Sync(SyncBodyProducer),
    Async(AsyncBodyProducer),
    Incoming(IncomingBodyProducer),
}

impl BodyProducer {
    pub(crate) async fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        match self {
            Self::Bytes(producer) => producer.next(),
            Self::RepeatedBytes(producer) => producer.next(),
            Self::Sync(producer) => producer.next().await,
            Self::Async(producer) => producer.next().await,
            Self::Incoming(producer) => producer.next().await,
        }
    }

    pub(crate) async fn finish(self) -> Result<(), NetworkError> {
        match self {
            Self::Bytes(_) => Ok(()),
            Self::RepeatedBytes(_) => Ok(()),
            Self::Sync(producer) => producer.finish().await,
            Self::Async(_) => Ok(()),
            Self::Incoming(_) => Ok(()),
        }
    }
}

pub(crate) struct BytesBodyProducer {
    bytes: Bytes,
    offset: usize,
    chunk_bytes: usize,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl BytesBodyProducer {
    fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        if self.offset >= self.bytes.len() {
            return Ok(None);
        }
        let end = self
            .offset
            .saturating_add(self.chunk_bytes)
            .min(self.bytes.len());
        let bytes = self.bytes.slice(self.offset..end);
        self.offset = end;
        Ok(Some(bytes))
    }
}

pub(crate) struct RepeatedBytesBodyProducer {
    pattern: Bytes,
    pattern_offset: usize,
    remaining_bytes: u64,
    chunk_bytes: usize,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl RepeatedBytesBodyProducer {
    fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        if self.remaining_bytes == 0 {
            return Ok(None);
        }
        let available = self.pattern.len() - self.pattern_offset;
        let bytes = self
            .chunk_bytes
            .min(available)
            .min(usize::try_from(self.remaining_bytes).unwrap_or(usize::MAX));
        let chunk = self
            .pattern
            .slice(self.pattern_offset..self.pattern_offset + bytes);
        self.pattern_offset += bytes;
        if self.pattern_offset == self.pattern.len() {
            self.pattern_offset = 0;
        }
        self.remaining_bytes -= bytes as u64;
        Ok(Some(chunk))
    }
}

pub(crate) struct AsyncBodyProducer {
    reader: Pin<Box<dyn AsyncRead + Send>>,
    chunk_bytes: usize,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl AsyncBodyProducer {
    async fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        let mut bytes = pooled_body_buffer(self.chunk_bytes);
        let read = match self.reader.read(&mut bytes).await {
            Ok(read) => read,
            Err(source) => {
                drop(pooled_body_bytes(bytes));
                return Err(NetworkError::Io {
                    operation: "read asynchronous object RPC body source".to_string(),
                    detail: source.to_string(),
                });
            }
        };
        if read == 0 {
            drop(pooled_body_bytes(bytes));
            return Ok(None);
        }
        bytes.truncate(read);
        Ok(Some(pooled_body_bytes(bytes)))
    }
}

pub(crate) struct IncomingBodyProducer {
    stream: ObjectRpcIncomingBodyChunkStream,
    chunk_bytes: usize,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl IncomingBodyProducer {
    async fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        self.stream
            .next_chunk(self.chunk_bytes)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "read ownership-moving object RPC body source".to_string(),
                detail: source.to_string(),
            })
    }
}

pub(crate) struct SyncBodyProducer {
    receiver: tokio::sync::mpsc::Receiver<Result<Bytes, NetworkError>>,
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
        let task_lifetime_guards = Arc::clone(&lifetime_guards);
        let task = tokio::task::spawn_blocking(move || {
            // A cancelled consumer drops `SyncBodyProducer` without joining
            // this blocking task. Keep staged-file cleanup guards alive until
            // the reader itself closes, which is required on Windows where an
            // open file cannot be unlinked reliably.
            let _task_lifetime_guards = task_lifetime_guards;
            loop {
                let mut bytes = pooled_body_buffer(chunk_bytes);
                let read = match reader.read(&mut bytes) {
                    Ok(read) => read,
                    Err(source) if source.kind() == std::io::ErrorKind::Interrupted => {
                        drop(pooled_body_bytes(bytes));
                        continue;
                    }
                    Err(source) => {
                        drop(pooled_body_bytes(bytes));
                        let _ = sender.blocking_send(Err(NetworkError::Io {
                            operation: "read synchronous object RPC body source".to_string(),
                            detail: source.to_string(),
                        }));
                        return;
                    }
                };
                bytes.truncate(read);
                if sender.blocking_send(Ok(pooled_body_bytes(bytes))).is_err() || read == 0 {
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

    async fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
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

fn pooled_body_buffer(len: usize) -> BytesMut {
    shared_transfer_buffer_pool().acquire(len)
}

fn pooled_body_bytes(bytes: BytesMut) -> Bytes {
    shared_transfer_buffer_pool().into_bytes(bytes)
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
        SignedDaemonCommand, cached_daemon_signing_public_key, daemon_rpc_body_sha256,
        sign_daemon_rpc_request_from_data_dir,
    };
    use crate::transfer::TransferReceipt;
    use std::io::Cursor;
    use tokio::io::AsyncWriteExt as _;

    struct BlockingEofReader {
        entered: Option<std::sync::mpsc::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl Read for BlockingEofReader {
        fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
            let _ = self.release.recv();
            Ok(0)
        }
    }

    struct GuardDropSignal(std::sync::mpsc::Sender<()>);

    impl Drop for GuardDropSignal {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    struct PendingAsyncReader {
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        dropped: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl AsyncRead for PendingAsyncReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _output: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
            Poll::Pending
        }
    }

    impl Drop for PendingAsyncReader {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(());
            }
        }
    }

    #[test]
    fn session_wire_prefix_recognizes_partial_v2_magic_only() {
        assert!(!object_rpc_session_wire_prefix_matches(b""));
        for prefix in [b"S".as_slice(), b"SX", b"SXF", b"SXF2", b"SXF2more"] {
            assert!(object_rpc_session_wire_prefix_matches(prefix));
        }
        for prefix in [b"X".as_slice(), b"SXFR", b"SXF3"] {
            assert!(!object_rpc_session_wire_prefix_matches(prefix));
        }
    }

    #[tokio::test]
    async fn byte_body_producer_uses_exact_chunks_without_a_blocking_reader() {
        let empty = ObjectRpcBodySource::bytes(Vec::new());
        let mut empty_producer = empty.producer(2).expect("empty byte producer");
        assert!(matches!(&empty_producer, BodyProducer::Bytes(_)));
        assert_eq!(empty_producer.next().await.expect("empty EOF"), None);

        let source_bytes = vec![1_u8, 2, 3];
        let source_pointer = source_bytes.as_ptr();
        let source = ObjectRpcBodySource::bytes(source_bytes);
        let mut producer = source.producer(2).expect("byte producer");
        assert!(matches!(&producer, BodyProducer::Bytes(_)));
        let first = producer
            .next()
            .await
            .expect("first chunk")
            .expect("first bytes");
        assert_eq!(first.as_ref(), &[1, 2]);
        assert_eq!(
            first.as_ptr(),
            source_pointer,
            "contiguous producers must slice the owned buffer without copying"
        );
        assert_eq!(
            producer.next().await.expect("second chunk"),
            Some(Bytes::from_static(&[3]))
        );
        assert_eq!(producer.next().await.expect("EOF"), None);
        producer.finish().await.expect("finish byte producer");

        let exact = ObjectRpcBodySource::bytes(vec![1_u8, 2, 3, 4]);
        let mut exact_producer = exact.producer(2).expect("exact byte producer");
        assert_eq!(
            exact_producer.next().await.expect("first exact chunk"),
            Some(Bytes::from_static(&[1, 2]))
        );
        assert_eq!(
            exact_producer.next().await.expect("second exact chunk"),
            Some(Bytes::from_static(&[3, 4]))
        );
        assert_eq!(exact_producer.next().await.expect("exact EOF"), None);
    }

    #[tokio::test]
    async fn zero_body_source_replays_shared_bytes_without_async_reader_fills() {
        let source = ObjectRpcBodySource::zeroes(5);
        assert_eq!(source.total_bytes(), Some(5));
        assert!(source.is_replayable());

        let replay = source
            .try_clone_replayable()
            .expect("replayable zero source");
        for source in [&source, &replay] {
            let mut producer = source.producer(2).expect("zero producer");
            assert!(matches!(&producer, BodyProducer::RepeatedBytes(_)));
            assert_eq!(
                producer.next().await.expect("first zero chunk"),
                Some(Bytes::from_static(&[0, 0]))
            );
            assert_eq!(
                producer.next().await.expect("second zero chunk"),
                Some(Bytes::from_static(&[0, 0]))
            );
            assert_eq!(
                producer.next().await.expect("last zero chunk"),
                Some(Bytes::from_static(&[0]))
            );
            assert_eq!(producer.next().await.expect("zero EOF"), None);
        }

        assert!(ObjectRpcBodySource::zeroes(0).is_empty());
    }

    #[tokio::test]
    async fn incoming_chunk_producer_moves_received_bytes_without_copying() {
        let (mut sender, incoming) = ObjectRpcIncomingBody::multiplexed_channel(Some(5));
        let received = Bytes::from(vec![10_u8, 20, 30, 40, 50]);
        let received_pointer = received.as_ptr();
        sender.send(received).await.expect("queue incoming chunk");
        sender.finish(5).await.expect("finish incoming body");

        let forwarded = ObjectRpcSessionResult::new(
            crate::ObjectRpcResponse::from_encoded_body("/test/relay-zero-copy", Vec::new()),
            incoming,
        )
        .into_relay_response()
        .expect("relay response");
        assert!(!forwarded.body.is_replayable());
        let mut producer = forwarded.body.producer(2).expect("incoming chunk producer");
        assert!(matches!(&producer, BodyProducer::Incoming(_)));

        let first = producer
            .next()
            .await
            .expect("first incoming chunk")
            .expect("first bytes");
        assert_eq!(first.as_ref(), &[10, 20]);
        assert_eq!(first.as_ptr(), received_pointer);

        let second = producer
            .next()
            .await
            .expect("second incoming chunk")
            .expect("second bytes");
        assert_eq!(second.as_ref(), &[30, 40]);
        assert_eq!(second.as_ptr(), received_pointer.wrapping_add(2));

        let third = producer
            .next()
            .await
            .expect("third incoming chunk")
            .expect("third bytes");
        assert_eq!(third.as_ref(), &[50]);
        assert_eq!(third.as_ptr(), received_pointer.wrapping_add(4));
        assert_eq!(producer.next().await.expect("incoming EOF"), None);
        producer.finish().await.expect("finish incoming producer");
    }

    #[tokio::test]
    async fn relay_call_moves_received_bytes_without_async_read_copying() {
        let (mut sender, incoming) = ObjectRpcIncomingBody::multiplexed_channel(Some(5));
        let received = Bytes::from(vec![10_u8, 20, 30, 40, 50]);
        let received_pointer = received.as_ptr();
        sender.send(received).await.expect("queue incoming chunk");
        sender.finish(5).await.expect("finish incoming body");

        let request = crate::ObjectRpcRequest::encode_body(
            "/test/relay-request-zero-copy",
            "test",
            &"request head",
        )
        .expect("request head");
        let forwarded = ObjectRpcSessionCall::from_relay_body(request, incoming);
        assert_eq!(forwarded.body.role(), ObjectRpcSessionBodyRole::Attachment);
        assert_eq!(forwarded.body.total_bytes(), Some(5));
        assert!(!forwarded.body.is_replayable());
        let mut producer = forwarded.body.producer(2).expect("incoming chunk producer");
        assert!(matches!(&producer, BodyProducer::Incoming(_)));

        for (expected, offset) in [(&[10_u8, 20][..], 0), (&[30, 40][..], 2), (&[50][..], 4)] {
            let chunk = producer
                .next()
                .await
                .expect("relay request chunk")
                .expect("relay request bytes");
            assert_eq!(chunk.as_ref(), expected);
            assert_eq!(chunk.as_ptr(), received_pointer.wrapping_add(offset));
        }
        assert_eq!(producer.next().await.expect("incoming EOF"), None);
        producer.finish().await.expect("finish incoming producer");

        let value_request = crate::ObjectRpcRequest::encode_body(
            "/test/relay-request-value",
            "test",
            &"typed value",
        )
        .expect("value request");
        let value_body = value_request.body.clone();
        let forwarded =
            ObjectRpcSessionCall::from_relay_body(value_request, ObjectRpcIncomingBody::empty());
        assert!(forwarded.body.has_value_intent());
        assert_eq!(forwarded.request.body, value_body);
    }

    #[tokio::test]
    async fn discard_owned_chunks_counts_and_preserves_stream_completion() {
        let (mut sender, mut body) = ObjectRpcIncomingBody::multiplexed_channel(Some(7));
        let completion_release = Arc::new(tokio::sync::Notify::new());
        let (completion_finished_tx, completion_finished_rx) = tokio::sync::oneshot::channel();
        let task_release = Arc::clone(&completion_release);
        body.hold_completion_task(tokio::spawn(async move {
            task_release.notified().await;
            let _ = completion_finished_tx.send(());
        }));
        let delivery = tokio::spawn(async move {
            sender
                .send(Bytes::from_static(b"abc"))
                .await
                .expect("first body chunk");
            sender
                .send(Bytes::from_static(b"defg"))
                .await
                .expect("second body chunk");
            sender.finish(7).await.expect("body completion");
        });

        assert_eq!(body.discard_owned_chunks().await.expect("discard body"), 7);
        delivery.await.expect("body delivery");
        completion_release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), completion_finished_rx)
            .await
            .expect("natural EOF must detach the completion task")
            .expect("detached completion task must remain alive");

        let (mut sender, body) = ObjectRpcIncomingBody::multiplexed_channel(Some(3));
        let delivery = tokio::spawn(async move {
            sender
                .send(Bytes::from_static(b"abc"))
                .await
                .expect("body bytes before failure");
            sender.fail("forced discard terminal error");
        });
        let error = body
            .discard_owned_chunks()
            .await
            .expect_err("terminal failure must propagate");
        delivery.await.expect("failed body delivery");
        assert!(matches!(error, NetworkError::Io { .. }));
        assert!(error.to_string().contains("forced discard terminal error"));
    }

    #[tokio::test]
    async fn multiplexed_body_spills_only_the_stalled_suffix_in_order_and_cleans_up() {
        let root = sinomo_testdir::TestDir::new("sinomo-channel-body-spill");
        let first = Bytes::from_static(b"first-");
        let second = Bytes::from_static(b"second-");
        let third = Bytes::from_static(b"third");
        let total_bytes = first.len() + second.len() + third.len();
        let (mut sender, body) = ObjectRpcIncomingBody::multiplexed_channel_with_staging_root(
            Some(total_bytes as u64),
            Ok(root.path().to_path_buf()),
        );

        assert!(sender.send(first).await.expect("direct prefix"));
        assert!(sender.send(second).await.expect("start spilled suffix"));
        assert!(sender.send(third).await.expect("append spilled suffix"));
        let spool_dir = root.path().join("network-transfers");
        assert_eq!(
            std::fs::read_dir(&spool_dir)
                .expect("read active channel spool")
                .count(),
            1,
            "one stalled body must own exactly one staging file"
        );
        sender
            .finish(total_bytes as u64)
            .await
            .expect("complete spilled channel body");
        drop(sender);

        assert_eq!(
            body.collect_to_vec(total_bytes)
                .await
                .expect("collect direct prefix and spilled suffix"),
            b"first-second-third"
        );
        assert_eq!(
            std::fs::read_dir(&spool_dir)
                .expect("read cleaned channel spool")
                .count(),
            0,
            "the staging file must disappear after writer and tailer release it"
        );
    }

    #[tokio::test]
    async fn abandoned_multiplexed_body_discards_without_spooling() {
        let root = sinomo_testdir::TestDir::new("sinomo-channel-body-abandoned");
        let (mut sender, body) = ObjectRpcIncomingBody::multiplexed_channel_with_staging_root(
            Some(6),
            Ok(root.path().to_path_buf()),
        );
        assert!(
            sender
                .send(Bytes::from_static(b"abc"))
                .await
                .expect("initial body quantum")
        );
        drop(body);
        assert!(
            !sender
                .send(Bytes::from_static(b"def"))
                .await
                .expect("discard abandoned quantum"),
            "a dropped body must be reported to channel direction handling"
        );
        sender
            .finish(6)
            .await
            .expect("finish discarded request body");
        drop(sender);
        assert!(
            !root.path().join("network-transfers").exists(),
            "abandonment must release pooled bytes without touching disk"
        );
    }

    #[tokio::test]
    async fn incoming_body_natural_eof_detaches_completion_tasks_but_early_drop_aborts_them() {
        let (mut sender, mut body) = ObjectRpcIncomingBody::multiplexed_channel(Some(1));
        let natural_release = Arc::new(tokio::sync::Notify::new());
        let (natural_finished_tx, natural_finished_rx) = tokio::sync::oneshot::channel();
        let task_release = Arc::clone(&natural_release);
        body.hold_completion_task(tokio::spawn(async move {
            task_release.notified().await;
            let _ = natural_finished_tx.send(());
        }));
        sender
            .send(Bytes::from_static(b"x"))
            .await
            .expect("body byte");
        sender.finish(1).await.expect("body completion");
        assert_eq!(body.collect_to_vec(1).await.expect("natural EOF"), b"x");
        natural_release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), natural_finished_rx)
            .await
            .expect("naturally completed body task must stay alive")
            .expect("naturally completed body task must be detached");

        let mut body = ObjectRpcIncomingBody::bytes(vec![1]);
        let stream_release = Arc::new(tokio::sync::Notify::new());
        let (stream_finished_tx, stream_finished_rx) = tokio::sync::oneshot::channel();
        let task_release = Arc::clone(&stream_release);
        body.hold_completion_task(tokio::spawn(async move {
            task_release.notified().await;
            let _ = stream_finished_tx.send(());
        }));
        let mut stream = body.into_chunk_stream();
        assert_eq!(
            stream.next_chunk(1).await.expect("stream byte"),
            Some(Bytes::from_static(&[1]))
        );
        assert_eq!(stream.next_chunk(1).await.expect("stream EOF"), None);
        drop(stream);
        stream_release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), stream_finished_rx)
            .await
            .expect("naturally completed chunk-stream task must stay alive")
            .expect("naturally completed chunk-stream task must be detached");

        let mut body = ObjectRpcIncomingBody::bytes(vec![1]);
        let early_release = Arc::new(tokio::sync::Notify::new());
        let (early_started_tx, early_started_rx) = tokio::sync::oneshot::channel();
        let (early_finished_tx, early_finished_rx) = tokio::sync::oneshot::channel();
        let task_release = Arc::clone(&early_release);
        body.hold_completion_task(tokio::spawn(async move {
            let _ = early_started_tx.send(());
            task_release.notified().await;
            let _ = early_finished_tx.send(());
        }));
        early_started_rx.await.expect("early-drop task started");
        drop(body);
        early_release.notify_one();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), early_finished_rx)
                .await
                .expect("early-drop task cancellation must settle")
                .is_err(),
            "dropping an unread body must abort its completion tasks"
        );
    }

    fn proof_public_key(data_dir: &SinomoDataDir) -> Vec<u8> {
        cached_daemon_signing_public_key(data_dir).expect("session identity")
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

    fn receiver_bridge_call(request: crate::ObjectRpcRequest) -> ObjectRpcSessionCall {
        ObjectRpcSessionCall::value(request)
    }

    fn signed_attachment_request(
        data_dir: &SinomoDataDir,
        route: &str,
        signed_body: &[u8],
    ) -> crate::ObjectRpcRequest {
        let inner = crate::ObjectRpcRequest::encode_body(route, "test", &"signed attachment")
            .expect("inner request");
        let prepared =
            ObjectRpcSessionCall::new(inner, ObjectRpcBodySource::bytes(signed_body.to_vec()))
                .prepare_for_signing()
                .expect("prepare signed attachment");
        let mut signed = prepared.request;
        signed.auth = Some(
            sign_daemon_rpc_request_from_data_dir(data_dir, &signed)
                .expect("sign attachment request"),
        );
        signed
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
            .expect_err("body must exceed the caller's materialization bound");
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
        let (sender, body) = ObjectRpcIncomingBody::channel(
            Some(bytes.len() as u64),
            ObjectRpcSessionBodyRole::Attachment,
        );
        let ObjectRpcIncomingBodyInner::Channel { receiver, .. } = &body.inner else {
            panic!("incoming body must use the bounded delivery channel");
        };
        assert_eq!(receiver.max_capacity(), BODY_READER_BUFFER_DEPTH);
        assert!(matches!(
            sender.try_deliver(bytes.clone()),
            IncomingDirectDelivery::Delivered
        ));
        sender
            .complete(bytes.len() as u64, false)
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
    async fn incoming_body_waits_for_consumer_progress_before_spilling() {
        let root = sinomo_testdir::TestDir::new("sinomo-session-body-wait-direct");
        let bytes = b"abcdef".to_vec();
        let (sender, mut body) = ObjectRpcIncomingBody::channel(
            Some(bytes.len() as u64),
            ObjectRpcSessionBodyRole::Attachment,
        );
        assert!(matches!(
            sender.try_deliver(b"a".to_vec()),
            IncomingDirectDelivery::Delivered
        ));
        assert!(matches!(
            sender.try_deliver(b"b".to_vec()),
            IncomingDirectDelivery::Delivered
        ));

        let waits_before = BODY_READER_DIRECT_WAIT_TOTAL.load(Ordering::Relaxed);
        let delivered_before = BODY_READER_DIRECT_WAIT_DELIVERED_TOTAL.load(Ordering::Relaxed);
        let waiting_sender = sender.clone();
        let delivery_task = tokio::spawn(async move {
            waiting_sender
                .deliver_with_grace_for(b"cdef".to_vec(), std::time::Duration::from_secs(1))
                .await
        });
        while BODY_READER_DIRECT_WAIT_TOTAL.load(Ordering::Relaxed) == waits_before {
            tokio::task::yield_now().await;
        }
        assert!(
            !delivery_task.is_finished(),
            "the full queue must retain the frame during the direct-delivery grace"
        );

        let mut first = [0_u8; 1];
        body.read_exact(&mut first)
            .await
            .expect("consume first queued frame");
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), delivery_task)
            .await
            .expect("direct delivery must resume after consumer progress")
            .expect("direct delivery task");
        assert!(matches!(outcome, IncomingDirectDelivery::Delivered));
        assert!(BODY_READER_DIRECT_WAIT_DELIVERED_TOTAL.load(Ordering::Relaxed) > delivered_before);

        sender
            .complete(bytes.len() as u64, false)
            .expect("complete direct body after wait");
        drop(sender);
        let mut received = first.to_vec();
        received.extend(
            body.collect_to_vec(32)
                .await
                .expect("collect direct body after wait"),
        );
        assert_eq!(received, bytes);
        assert!(
            !root.path().join("network-transfers").exists(),
            "transient queue saturation with consumer progress must not select the spool"
        );
    }

    #[tokio::test]
    async fn incoming_body_direct_wait_is_bounded_before_spill() {
        let (sender, _body) =
            ObjectRpcIncomingBody::channel(Some(3), ObjectRpcSessionBodyRole::Attachment);
        assert!(matches!(
            sender.try_deliver(b"a".to_vec()),
            IncomingDirectDelivery::Delivered
        ));
        assert!(matches!(
            sender.try_deliver(b"b".to_vec()),
            IncomingDirectDelivery::Delivered
        ));
        let grace = std::time::Duration::from_millis(5);
        let started = tokio::time::Instant::now();
        let outcome = sender.deliver_with_grace_for(b"c".to_vec(), grace).await;
        assert!(matches!(outcome, IncomingDirectDelivery::Spill(bytes) if bytes.as_ref() == b"c"));
        assert!(started.elapsed() >= grace);
    }

    #[tokio::test]
    async fn incoming_body_close_ends_direct_wait_without_spilling() {
        let (sender, body) =
            ObjectRpcIncomingBody::channel(Some(3), ObjectRpcSessionBodyRole::Attachment);
        assert!(matches!(
            sender.try_deliver(b"a".to_vec()),
            IncomingDirectDelivery::Delivered
        ));
        assert!(matches!(
            sender.try_deliver(b"b".to_vec()),
            IncomingDirectDelivery::Delivered
        ));
        let waits_before = BODY_READER_DIRECT_WAIT_TOTAL.load(Ordering::Relaxed);
        let waiting_sender = sender.clone();
        let delivery_task = tokio::spawn(async move {
            waiting_sender
                .deliver_with_grace_for(b"c".to_vec(), std::time::Duration::from_secs(1))
                .await
        });
        while BODY_READER_DIRECT_WAIT_TOTAL.load(Ordering::Relaxed) == waits_before {
            tokio::task::yield_now().await;
        }
        drop(body);
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), delivery_task)
            .await
            .expect("consumer close must wake direct waiter")
            .expect("direct waiter task");
        assert!(matches!(outcome, IncomingDirectDelivery::ConsumerClosed));
    }

    #[tokio::test]
    async fn incoming_body_eof_is_published_only_after_the_wire_receipt_is_written() {
        let root = sinomo_testdir::TestDir::new("sinomo-session-completion-receipt-order");
        let data_dir = SinomoDataDir::new(root.path());
        let transfer_id = TransferId::generate();
        let bytes = b"body".to_vec();
        let receipt = TransferReceipt {
            transfer_id,
            completed_bytes: bytes.len() as u64,
        };
        let (sender, mut body) = ObjectRpcIncomingBody::channel(
            Some(bytes.len() as u64),
            ObjectRpcSessionBodyRole::Attachment,
        );
        assert!(matches!(
            sender.try_deliver(bytes.clone()),
            IncomingDirectDelivery::Delivered
        ));
        let mut received = vec![0_u8; bytes.len()];
        body.read_exact(&mut received).await.expect("body payload");
        assert_eq!(received, bytes);

        // A one-byte wire buffer forces the Complete control frame to
        // yield before it has been written. While that receipt is blocked, an
        // eager consumer must not observe EOF and drop/abort the reader task.
        let (write_stream, read_stream) = tokio::io::duplex(1);
        let writer = SessionWriterHandle::spawn(
            SessionFrameWriter::new(write_stream, data_dir, NetworkCallPolicy::default())
                .expect("completion writer"),
        );
        let mut reader = SessionFrameReader::new(read_stream, NetworkCallPolicy::default());
        let completion = tokio::spawn(acknowledge_incoming_completion(
            writer,
            Some(sender.clone()),
            receipt.clone(),
            false,
        ));
        let mut eof_probe = [0_u8; 1];
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                body.read(&mut eof_probe),
            )
            .await
            .is_err(),
            "body EOF became visible before the completion receipt could be written"
        );

        let frame = reader.receive().await.expect("completion receipt frame");
        assert!(matches!(
            frame,
            SessionInboundFrame::Payload(SessionWirePayload::Transfer(
                TransferControl::Complete(actual)
            )) if actual == receipt
        ));
        completion
            .await
            .expect("completion task")
            .expect("acknowledge completion");
        assert_eq!(body.read(&mut eof_probe).await.expect("published EOF"), 0);
    }

    #[tokio::test]
    async fn incoming_body_known_length_rejects_oversize_before_collection() {
        let body = ObjectRpcIncomingBody::bytes(vec![7_u8; 1024]);
        let error = body
            .collect_to_vec(100)
            .await
            .expect_err("known body length must enforce the collection bound");
        assert!(matches!(
            error,
            NetworkError::FrameTooLarge {
                actual_bytes: 1024,
                max_bytes: 100,
            }
        ));
    }

    #[tokio::test]
    async fn incoming_body_lazily_spills_after_the_bounded_channel_fills() {
        let root = sinomo_testdir::TestDir::new("sinomo-session-body-spill");
        let bytes = b"abcdef".to_vec();
        let (sender, body) = ObjectRpcIncomingBody::channel(
            Some(bytes.len() as u64),
            ObjectRpcSessionBodyRole::Attachment,
        );
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
            .complete(bytes.len() as u64, true)
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_sync_producer_holds_cleanup_guard_until_reader_closes() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (guard_tx, guard_rx) = std::sync::mpsc::channel();
        let source = ObjectRpcBodySource::from_reader(
            BlockingEofReader {
                entered: Some(entered_tx),
                release: release_rx,
            },
            None,
        )
        .with_lifetime_guard(GuardDropSignal(guard_tx))
        .expect("attach guard");
        let producer = source
            .producer(BODY_READER_BUFFER_BYTES)
            .expect("sync producer");
        drop(source);
        tokio::task::spawn_blocking(move || entered_rx.recv())
            .await
            .expect("join entered wait")
            .expect("reader entered");

        drop(producer);
        assert!(
            guard_rx.try_recv().is_err(),
            "dropping the consumer released cleanup while the reader was open"
        );
        release_tx.send(()).expect("release reader");
        tokio::task::spawn_blocking(move || {
            guard_rx.recv_timeout(std::time::Duration::from_secs(5))
        })
        .await
        .expect("join guard wait")
        .expect("reader task released cleanup guard");
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
            ObjectRpcSessionDescriptor::for_call(&ObjectRpcSessionCall::value(tampered.clone()))
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

    #[test]
    fn daemon_auth_digest_is_stable_after_value_materialization() {
        let request = crate::ObjectRpcRequest::from_encoded_body(
            "/test/session/materialized-auth-value",
            "test",
            vec![0x41; INLINE_VALUE_MAX_BYTES + 1],
        );
        let value = request.body.clone();
        let prepared = ObjectRpcSessionCall::value(request)
            .prepare_for_signing()
            .expect("prepare value call");
        assert!(prepared.request.body.is_empty());

        let signed_head_digest = daemon_rpc_body_sha256(&prepared.request);
        let mut materialized = prepared.request;
        materialized.body = value;
        assert_eq!(
            daemon_rpc_body_sha256(&materialized),
            signed_head_digest,
            "daemon authentication must continue to verify after the canonical session restores the ordinary value"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bootstrap_auth_denial_returns_a_signed_structured_error_head() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-auth-denial-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-auth-denial-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let route = "/test/session/auth-denial";
        let request = crate::ObjectRpcRequest::from_encoded_body(
            route,
            "test",
            vec![0x5a_u8; 256 * 1024 + 17],
        );
        let prepared = ObjectRpcSessionCall::value(request)
            .prepare_for_signing()
            .expect("prepare value request");
        assert!(
            prepared.descriptor.request_body.is_some(),
            "the denial must race an outbound request body"
        );
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(16 * 1024);
        let server = tokio::spawn(async move {
            let error = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                |_request, _claimed_node_id, _claimed_public_key| Err(NetworkError::AuthDenied),
            )
            .await
            .expect_err("bootstrap authorization must reject the session");
            assert!(matches!(error, NetworkError::AuthDenied));
        });

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            start_object_rpc_client_session(
                Box::new(client_stream),
                prepared,
                request_auth,
                client_data_dir,
                server_public_key.clone(),
                server_public_key,
                NetworkCallPolicy::default(),
            ),
        )
        .await
        .expect("authorization rejection must not wait for the request body")
        .expect("authorization rejection must arrive as a response head");

        assert_eq!(result.response.route, route);
        assert!(result.response.body.is_empty());
        assert!(result.body.is_empty());
        assert!(matches!(
            result.response.error_detail(),
            Err(NetworkError::AuthDenied)
        ));
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn signed_inner_body_contract_checks_shape_without_prebuffering() {
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
        let (sender, body) =
            ObjectRpcIncomingBody::channel(Some(4), ObjectRpcSessionBodyRole::Attachment);
        let bound = ObjectRpcSessionRequest::new(outer, body)
            .bind_signed_inner_request(signed_inner)
            .await
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
        sender.complete(4, true).expect("complete trusted body");
        drop(sender);
        drop(staging);

        let actual = bound
            .body
            .collect_to_vec(16)
            .await
            .expect("trusted transport bytes are not rehashed");
        assert_eq!(actual, b"evil");
    }

    #[tokio::test]
    async fn multiplexed_body_drains_queued_bytes_before_terminal_error() {
        let payload = b"queued before failure".to_vec();
        let (mut sender, mut body) =
            ObjectRpcIncomingBody::multiplexed_channel(Some(payload.len() as u64));
        sender
            .send(payload.clone())
            .await
            .expect("fill multiplexed body queue");

        sender.fail("forced multiplexed body failure");
        sender.fail("later failure must not replace the first");

        let mut delivered = vec![0_u8; payload.len()];
        body.read_exact(&mut delivered)
            .await
            .expect("drain valid queued bytes");
        assert_eq!(delivered, payload);
        let error = body
            .read_u8()
            .await
            .expect_err("terminal failure must not become clean EOF");
        assert!(
            error
                .to_string()
                .contains("forced multiplexed body failure")
        );
        assert_eq!(
            body.read_u8()
                .await
                .expect_err("closed body after terminal error")
                .kind(),
            std::io::ErrorKind::UnexpectedEof
        );

        let (sender, mut truncated) = ObjectRpcIncomingBody::multiplexed_channel(Some(1));
        drop(sender);
        let truncation = truncated
            .read_u8()
            .await
            .expect_err("sender drop before completion must not become clean EOF");
        assert_eq!(truncation.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(
            truncation
                .to_string()
                .contains("before declared completion")
        );
    }

    #[tokio::test]
    async fn authenticated_channel_binds_large_origin_signed_attachment() {
        let origin_root =
            sinomo_testdir::TestDir::new("sinomo-session-channel-signed-attachment-origin");
        let origin_data_dir = SinomoDataDir::new(origin_root.path());
        let payload = vec![0x5a_u8; 96 * 1024 + 37];
        let signed_inner = signed_attachment_request(
            &origin_data_dir,
            "/test/session/channel-signed-attachment",
            &payload,
        );
        let outer = crate::ObjectRpcRequest::encode_body(
            crate::CLUSTER_RELAY_ROUTE,
            "test",
            &"outer relay",
        )
        .expect("outer request");
        let (mut sender, body) =
            ObjectRpcIncomingBody::multiplexed_channel(Some(payload.len() as u64));
        let bound = ObjectRpcSessionRequest::new(outer, body)
            .bind_signed_inner_request_on_authenticated_channel(signed_inner)
            .await
            .expect("bind trusted channel contract");
        let delivered = payload.clone();
        let delivery = tokio::spawn(async move {
            for chunk in delivered.chunks(crate::OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES) {
                sender.send(chunk.to_vec()).await.expect("deliver chunk");
            }
            sender
                .finish(delivered.len() as u64)
                .await
                .expect("finish trusted channel body");
        });

        let actual = bound
            .body
            .collect_to_vec(2 * payload.len())
            .await
            .expect("collect trusted channel attachment");
        delivery.await.expect("delivery task");
        assert_eq!(actual, payload);
    }

    #[tokio::test]
    async fn authenticated_channel_rejects_origin_signed_length_mismatch() {
        let origin_root =
            sinomo_testdir::TestDir::new("sinomo-session-channel-signed-length-origin");
        let origin_data_dir = SinomoDataDir::new(origin_root.path());
        let payload = vec![0x71_u8; 96 * 1024 + 11];
        let signed_inner = signed_attachment_request(
            &origin_data_dir,
            "/test/session/channel-signed-length",
            &payload,
        );
        let outer = crate::ObjectRpcRequest::encode_body(
            crate::CLUSTER_RELAY_ROUTE,
            "test",
            &"outer relay",
        )
        .expect("outer request");
        let (_sender, body) =
            ObjectRpcIncomingBody::multiplexed_channel(Some(payload.len() as u64 + 1));
        let error = ObjectRpcSessionRequest::new(outer, body)
            .bind_signed_inner_request_on_authenticated_channel(signed_inner)
            .await
            .expect_err("signed length must match the trusted channel descriptor");
        assert!(matches!(error, NetworkError::Serialization { .. }));
        assert!(error.to_string().contains("signed inner length"));
    }

    #[tokio::test]
    async fn relayed_value_request_restores_the_origin_payload_after_binding() {
        let origin_root =
            sinomo_testdir::TestDir::new("sinomo-session-relayed-value-request-origin");
        let origin_data_dir = SinomoDataDir::new(origin_root.path());
        let payload = vec![0x43_u8; 344_208];
        let inner = crate::ObjectRpcRequest::from_encoded_body(
            "/test/session/relayed-value-request",
            "test",
            payload.clone(),
        );
        let prepared = receiver_bridge_call(inner)
            .prepare_for_signing()
            .expect("prepare value origin request");
        let mut signed_inner = prepared.request;
        signed_inner.auth = Some(
            sign_daemon_rpc_request_from_data_dir(&origin_data_dir, &signed_inner)
                .expect("sign value origin request"),
        );
        let outer = crate::ObjectRpcRequest::from_encoded_body(
            crate::CLUSTER_RELAY_ROUTE,
            "test",
            Vec::new(),
        );
        let (sender, body) = ObjectRpcIncomingBody::channel(
            Some(payload.len() as u64),
            ObjectRpcSessionBodyRole::Attachment,
        );
        assert!(matches!(
            sender.try_deliver(payload.clone()),
            IncomingDirectDelivery::Delivered
        ));
        sender
            .complete(payload.len() as u64, false)
            .expect("complete trusted body");
        drop(sender);
        let bound = ObjectRpcSessionRequest::new(outer, body)
            .bind_signed_inner_request(signed_inner)
            .await
            .expect("bind and restore value origin request");

        assert_eq!(bound.request.body, payload);
        assert!(bound.body.is_empty());
    }

    #[tokio::test]
    async fn response_identity_proof_survives_relay_without_attesting_content() {
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
            SessionFrameWriter::new(target_stream, target_data_dir, NetworkCallPolicy::default())
                .expect("target writer"),
        ));
        ensure_response_identity_proof(&target_writer, &logical_session_id, &mut response)
            .await
            .expect("target identity proof");
        assert!(response_origin_is_verified(
            &response,
            &target_public_key,
            &logical_session_id,
        ));
        let original_attestation = response
            .metadata
            .get(OBJECT_RPC_RESPONSE_IDENTITY_PROOF_METADATA_KEY)
            .cloned()
            .expect("response attestation");

        let (relay_stream, _relay_peer) = tokio::io::duplex(4096);
        let relay_writer = Arc::new(tokio::sync::Mutex::new(
            SessionFrameWriter::new(relay_stream, relay_data_dir, NetworkCallPolicy::default())
                .expect("relay writer"),
        ));
        ensure_response_identity_proof(&relay_writer, &TransferId::generate(), &mut response)
            .await
            .expect("relay preserves target identity proof");
        assert_eq!(
            response
                .metadata
                .get(OBJECT_RPC_RESPONSE_IDENTITY_PROOF_METADATA_KEY),
            Some(&original_attestation),
            "a relay must not replace the target attestation with its hop proof"
        );

        let mut tampered = response.clone();
        tampered.body = b"other-sizehead".to_vec();
        assert_eq!(tampered.body.len(), response.body.len());
        assert!(response_origin_is_verified(
            &tampered,
            &target_public_key,
            &logical_session_id,
        ));
        assert!(!response_origin_is_verified(
            &response,
            &relay_public_key,
            &logical_session_id,
        ));
        assert!(!response_origin_is_verified(
            &response,
            &target_public_key,
            &TransferId::generate(),
        ));
    }

    #[tokio::test]
    async fn response_identity_proof_can_authenticate_a_canonical_relay_hop() {
        let target_root = sinomo_testdir::TestDir::new("sinomo-session-head-proof-hybrid-target");
        let relay_root = sinomo_testdir::TestDir::new("sinomo-session-head-proof-hybrid-relay");
        let target_data_dir = SinomoDataDir::new(target_root.path());
        let relay_data_dir = SinomoDataDir::new(relay_root.path());
        let target_public_key = proof_public_key(&target_data_dir);
        let relay_public_key = proof_public_key(&relay_data_dir);
        let target_session_id = TransferId::generate();
        let relay_session_id = TransferId::generate();
        let mut response = crate::ObjectRpcResponse::from_encoded_body(
            "/test/session/hybrid-relay-proof",
            b"response".to_vec(),
        );
        response.mark_responder_node_id("node_hybrid_relay_target");

        // The relay received this result over an authenticated channel, which
        // carries target identity metadata but deliberately has no canonical
        // target proof. Its upstream canonical response therefore authenticates
        // the relay hop and leaves final-target validation to the routing layer.
        let (relay_stream, _relay_peer) = tokio::io::duplex(4096);
        let relay_writer = Arc::new(tokio::sync::Mutex::new(
            SessionFrameWriter::new(relay_stream, relay_data_dir, NetworkCallPolicy::default())
                .expect("relay writer"),
        ));
        ensure_response_identity_proof(&relay_writer, &relay_session_id, &mut response)
            .await
            .expect("relay identity proof");

        assert!(!response_origin_is_verified(
            &response,
            &target_public_key,
            &target_session_id,
        ));
        assert!(response_origin_is_verified(
            &response,
            &relay_public_key,
            &relay_session_id,
        ));
        assert_eq!(
            response.responder_node_id(),
            Some("node_hybrid_relay_target")
        );
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
        let (client_stream, server_stream) = tokio::io::duplex(16 * 1024);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn small_value_request_and_response_round_trip_inline() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-inline-value-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-inline-value-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/inline-value",
            "test",
            &"inline request",
        )
        .expect("request");
        let expected_request = request.body.clone();
        let prepared = ObjectRpcSessionCall::value(request)
            .prepare_for_signing()
            .expect("prepare inline value call");
        assert_eq!(prepared.request.body, expected_request);
        assert!(prepared.descriptor.request_body.is_none());
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let server = tokio::spawn(async move {
            let (request, responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                authenticate_test_request,
            )
            .await
            .expect("accept inline session");
            assert_eq!(request.request.body, expected_request);
            assert!(request.body.is_empty());
            responder
                .respond(ObjectRpcSessionResponse::value(
                    crate::ObjectRpcResponse::encode_body(
                        "/test/session/inline-value",
                        &"inline response",
                    )
                    .expect("response"),
                ))
                .await
                .expect("send inline response");
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
        .expect("inline value session");
        assert_eq!(
            result
                .response
                .decode_body::<String>()
                .expect("decode response"),
            "inline response"
        );
        assert!(result.body.is_empty());
        server.await.expect("server task");
    }

    /// Opt-in in-memory latency probe for the complete canonical small-value
    /// session. It includes descriptor preparation, request authentication,
    /// plain frame encoding, task scheduling, and response identity
    /// authentication, but deliberately excludes TCP and routing. There is no
    /// timing assertion because CI hosts are not stable benchmark systems.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "diagnostic microbenchmark; run explicitly with --ignored --nocapture"]
    async fn diagnostic_small_value_session_stage_costs() {
        const WARMUP: usize = 100;
        const ITERATIONS: usize = 1_000;
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-cost-diagnostic-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-cost-diagnostic-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session-cost-diagnostic",
            "test",
            &"small request",
        )
        .expect("diagnostic request");

        let mut prepare_us = Vec::with_capacity(ITERATIONS);
        let mut request_sign_us = Vec::with_capacity(ITERATIONS);
        let mut session_us = Vec::with_capacity(ITERATIONS);
        for iteration in 0..WARMUP + ITERATIONS {
            let prepare_started = std::time::Instant::now();
            let prepared = ObjectRpcSessionCall::value(request.clone())
                .prepare_for_signing()
                .expect("prepare diagnostic call");
            let prepared_elapsed = prepare_started.elapsed().as_micros() as u64;
            let sign_started = std::time::Instant::now();
            let request_auth =
                sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                    .expect("sign diagnostic request");
            let sign_elapsed = sign_started.elapsed().as_micros() as u64;
            let (client_stream, server_stream) = tokio::io::duplex(4096);
            let iteration_server_data_dir = server_data_dir.clone();
            let server = tokio::spawn(async move {
                let (_request, responder) = accept_object_rpc_server_session(
                    Box::new(server_stream),
                    iteration_server_data_dir,
                    NetworkCallPolicy::default(),
                    authenticate_test_request,
                )
                .await
                .expect("accept diagnostic session");
                responder
                    .respond(ObjectRpcSessionResponse::value(
                        crate::ObjectRpcResponse::encode_body(
                            "/test/session-cost-diagnostic",
                            &"small response",
                        )
                        .expect("diagnostic response"),
                    ))
                    .await
                    .expect("respond to diagnostic session");
            });
            let session_started = std::time::Instant::now();
            let result = start_object_rpc_client_session(
                Box::new(client_stream),
                prepared,
                request_auth,
                client_data_dir.clone(),
                server_public_key.clone(),
                server_public_key.clone(),
                NetworkCallPolicy::default(),
            )
            .await
            .expect("diagnostic session round trip");
            result
                .response
                .decode_body::<String>()
                .expect("decode diagnostic response");
            server.await.expect("diagnostic server task");
            let session_elapsed = session_started.elapsed().as_micros() as u64;
            if iteration >= WARMUP {
                prepare_us.push(prepared_elapsed);
                request_sign_us.push(sign_elapsed);
                session_us.push(session_elapsed);
            }
        }

        fn percentile(values: &mut [u64], percent: usize) -> u64 {
            values.sort_unstable();
            let rank = values.len().saturating_mul(percent).saturating_add(99) / 100;
            values[rank.saturating_sub(1).min(values.len() - 1)]
        }

        eprintln!(
            "small-session us: prepare-p50={} prepare-p95={} request-sign-p50={} request-sign-p95={} session-p50={} session-p95={}",
            percentile(&mut prepare_us.clone(), 50),
            percentile(&mut prepare_us, 95),
            percentile(&mut request_sign_us.clone(), 50),
            percentile(&mut request_sign_us, 95),
            percentile(&mut session_us.clone(), 50),
            percentile(&mut session_us, 95),
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn arbitrary_value_request_uses_transfer_body_and_restores_payload() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-value-request-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-value-request-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let request_bytes = vec![0x51_u8; 237_451];
        let expected_request = request_bytes.clone();
        let request = crate::ObjectRpcRequest::from_encoded_body(
            "/test/session/arbitrary-value-request",
            "test",
            request_bytes,
        );
        let prepared = receiver_bridge_call(request)
            .prepare_for_signing()
            .expect("prepare session call");
        assert!(prepared.request.body.is_empty());
        assert_eq!(prepared.body.total_bytes(), Some(237_451));
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
            assert_eq!(request.request.body, expected_request);
            assert!(request.body.is_empty());
            responder
                .respond(ObjectRpcSessionResponse::value(
                    crate::ObjectRpcResponse::encode_body(
                        "/test/session/arbitrary-value-request",
                        &"ok",
                    )
                    .expect("response"),
                ))
                .await
                .expect("send response");
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
        .expect("send transferred value request");

        assert_eq!(
            result
                .response
                .decode_body::<String>()
                .expect("decode response"),
            "ok"
        );
        server.await.expect("server task");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn eager_value_materialization_does_not_abort_the_response_receipt() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-eager-value-receipt-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-eager-value-receipt-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let route = "/test/session/eager-value-receipt";
        let request = crate::ObjectRpcRequest::from_encoded_body(route, "test", Vec::new());
        let prepared = ObjectRpcSessionCall::value(request)
            .prepare_for_signing()
            .expect("prepare session call");
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let policy = NetworkCallPolicy {
            health: crate::transfer::NetworkHealthPolicy::new(
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(1),
            ),
            overall_timeout: Some(std::time::Duration::from_secs(3)),
        };

        // The tiny duplex buffer makes the completion-control write yield,
        // while current-thread scheduling lets the eager value consumer run
        // immediately when EOF is published. Publishing EOF before Complete
        // would let that consumer drop the body and abort the receipt writer.
        let (client_stream, server_stream) = tokio::io::duplex(1);
        let server = tokio::spawn(async move {
            let (_request, responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                policy,
                authenticate_test_request,
            )
            .await
            .expect("accept session");
            responder
                .respond(ObjectRpcSessionResponse {
                    response: crate::ObjectRpcResponse::from_encoded_body(route, Vec::new()),
                    body: ObjectRpcBodySource::bytes(vec![0x7b])
                        .with_role(ObjectRpcSessionBodyRole::Value),
                })
                .await
        });
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            start_object_rpc_client_session(
                Box::new(client_stream),
                prepared,
                request_auth,
                client_data_dir,
                server_public_key.clone(),
                server_public_key,
                policy,
            ),
        )
        .await
        .expect("client session deadline")
        .expect("receive eager value response");
        assert_eq!(result.response.body, vec![0x7b]);
        tokio::time::timeout(std::time::Duration::from_secs(2), server)
            .await
            .expect("response receipt must not stall")
            .expect("server task")
            .expect("server received the response receipt");
    }

    #[test]
    fn small_values_stay_inline_while_large_values_stream() {
        for size in [0, 1, INLINE_VALUE_MAX_BYTES] {
            let request = crate::ObjectRpcRequest::from_encoded_body(
                "/test/session/value-role",
                "test",
                vec![0x5a; size],
            );
            let call = ObjectRpcSessionCall::value(request);
            assert_eq!(call.request.body.len(), size);
            let prepared = call.prepare_for_signing().expect("prepare value request");
            assert_eq!(prepared.request.body.len(), size);
            assert!(prepared.body.is_empty());
            assert_eq!(
                prepared.descriptor.request_body_role,
                ObjectRpcSessionBodyRole::None
            );
        }

        for size in [INLINE_VALUE_MAX_BYTES + 1, 237_451] {
            let request = crate::ObjectRpcRequest::from_encoded_body(
                "/test/session/value-role",
                "test",
                vec![0x5a; size],
            );
            let prepared = ObjectRpcSessionCall::value(request)
                .prepare_for_signing()
                .expect("prepare streamed value request");
            assert!(prepared.request.body.is_empty());
            assert_eq!(prepared.body.role(), ObjectRpcSessionBodyRole::Value);
            assert_eq!(prepared.body.total_bytes(), Some(size as u64));
        }

        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/attachment-role",
            "test",
            &"typed-head",
        )
        .expect("typed request head");
        let head_value = request.body.clone();
        let prepared =
            ObjectRpcSessionCall::new(request, ObjectRpcBodySource::bytes(vec![0x31; 91_019]))
                .prepare_for_signing()
                .expect("prepare attachment request");
        assert_eq!(prepared.request.body, head_value);
        assert_eq!(prepared.body.role(), ObjectRpcSessionBodyRole::Attachment);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn arbitrary_value_response_uses_transfer_body_and_restores_payload() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-value-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-value-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let response_bytes = vec![0x92_u8; 237_451];
        let expected_response = response_bytes.clone();
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/arbitrary-value-response",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared = ObjectRpcSessionCall::value(request)
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
            let response =
                ObjectRpcSessionResponse::value(crate::ObjectRpcResponse::from_encoded_body(
                    "/test/session/arbitrary-value-response",
                    response_bytes,
                ));
            responder
                .respond(response)
                .await
                .expect("send arbitrary value response");
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
        .expect("receive transferred value response");
        assert_eq!(result.response.body, expected_response);
        assert!(result.body.is_empty());
        assert!(
            result
                .response
                .metadata
                .contains_key(OBJECT_RPC_RESPONSE_IDENTITY_PROOF_METADATA_KEY)
        );
        let forwarded = ObjectRpcSessionResponse::new(
            result.response,
            ObjectRpcBodySource::from_incoming_body(result.body),
        );
        assert!(forwarded.response.body.is_empty());
        assert_eq!(forwarded.body.role(), ObjectRpcSessionBodyRole::Value);
        assert_eq!(forwarded.body.total_bytes(), Some(237_451));
        assert_eq!(
            forwarded
                .body
                .collect_to_vec(expected_response.len())
                .await
                .expect("forward restored response value"),
            expected_response
        );
        server.await.expect("server task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn large_metadata_only_response_has_no_control_frame_limit() {
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
        let prepared = ObjectRpcSessionCall::value(request)
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
                .respond(ObjectRpcSessionResponse::value(response))
                .await
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
        .expect("large metadata response");
        assert_eq!(
            result
                .response
                .metadata
                .get("test-oversized-metadata")
                .map(String::len),
            Some(128 * 1024)
        );
        assert!(result.body.is_empty());
        server.await.expect("server task").expect("send response");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn typed_response_head_and_attachment_remain_distinct() {
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
        let prepared = ObjectRpcSessionCall::value(request)
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
            let response = crate::ObjectRpcResponse::encode_body(
                "/test/session/reserved-marker",
                &"typed-head",
            )?;
            let response = ObjectRpcSessionResponse::new(
                response,
                ObjectRpcBodySource::bytes(b"semantic-stream".to_vec()),
            );
            responder.respond(response).await
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
        .expect("receive typed response and attachment");
        assert_eq!(
            result.response.decode_body::<String>().expect("typed head"),
            "typed-head"
        );
        assert_eq!(
            result.body.collect_to_vec(1024).await.expect("attachment"),
            b"semantic-stream"
        );
        server.await.expect("server task").expect("send response");
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
                .respond(ObjectRpcSessionResponse::value(response))
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_client_session_before_response_closes_transport() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-cancel-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-cancel-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let (body_reader, _body_writer) = tokio::io::duplex(64);
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/cancel-before-response",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared = ObjectRpcSessionCall::new(
            request,
            ObjectRpcBodySource::async_reader(body_reader, Some(1)),
        )
        .prepare_for_signing()
        .expect("prepare session call");
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
        let client = tokio::spawn(start_object_rpc_client_session(
            Box::new(client_stream),
            prepared,
            request_auth,
            client_data_dir,
            server_public_key.clone(),
            server_public_key,
            NetworkCallPolicy::default(),
        ));

        let mut server_reader =
            SessionFrameReader::new(server_stream, NetworkCallPolicy::default());
        assert!(matches!(
            server_reader.receive().await.expect("request head"),
            SessionInboundFrame::Payload(SessionWirePayload::RequestHead { .. })
        ));
        assert!(matches!(
            server_reader.receive().await.expect("request body open"),
            SessionInboundFrame::Payload(SessionWirePayload::Transfer(TransferControl::Open(_)))
        ));

        client.abort();
        assert!(
            client
                .await
                .expect_err("client task must be cancelled")
                .is_cancelled(),
            "the client task ended for a reason other than cancellation"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match server_reader.receive().await {
                    Err(_) => break,
                    Ok(SessionInboundFrame::Payload(SessionWirePayload::HealthPing { .. })) => {
                        continue;
                    }
                    Ok(_) => panic!("unexpected frame after client cancellation"),
                }
            }
        })
        .await
        .expect("cancelled session must promptly close its transport");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peer_fin_after_response_head_interrupts_blocked_request_producer() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-peer-fin-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-peer-fin-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let (source_entered_tx, source_entered_rx) = tokio::sync::oneshot::channel();
        let (source_dropped_tx, source_dropped_rx) = tokio::sync::oneshot::channel();
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/peer-fin-after-response-head",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared = ObjectRpcSessionCall::new(
            request,
            ObjectRpcBodySource::async_reader(
                PendingAsyncReader {
                    entered: Some(source_entered_tx),
                    dropped: Some(source_dropped_tx),
                },
                Some(1),
            ),
        )
        .prepare_for_signing()
        .expect("prepare session call");
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let (_request, mut responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                authenticate_test_request,
            )
            .await
            .expect("accept session");

            // Wait until OpenAck has reached the client and its one-pass body
            // producer is genuinely blocked. This is the lifecycle state that
            // used to ignore a later reader EOF while awaiting producer.next().
            source_entered_rx
                .await
                .expect("request body producer was polled");
            let mut response = crate::ObjectRpcResponse::encode_body(
                "/test/session/peer-fin-after-response-head",
                &"accepted",
            )
            .expect("response head");
            responder
                .writer
                .ensure_response_identity_proof(&responder.session_id, &mut response)
                .expect("response identity proof");
            responder
                .writer
                .send(SessionWirePayload::ResponseHead {
                    response,
                    body_role: ObjectRpcSessionBodyRole::None,
                    body: None,
                })
                .await
                .expect("write response head");

            // Model an authenticated peer that retires this route attempt
            // immediately after its response head. Aborting both server-side
            // session tasks drops every remaining writer handle and produces
            // a zero-byte FIN at the client reader.
            if let Some(task) = responder.reader_task.take() {
                task.abort();
            }
            if let Some(task) = responder.keepalive_task.take() {
                task.abort();
            }
        });

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            start_object_rpc_client_session(
                Box::new(client_stream),
                prepared,
                request_auth,
                client_data_dir,
                server_public_key.clone(),
                server_public_key,
                NetworkCallPolicy::default(),
            ),
        )
        .await
        .expect("peer FIN must retire the pending client session")
        .expect_err("an incomplete request body cannot become a successful call");
        assert!(
            error
                .to_string()
                .contains("session reader stopped before transfer acknowledgement"),
            "unexpected peer-FIN error: {error}"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), source_dropped_rx)
            .await
            .expect("peer FIN must cancel the blocked request producer")
            .expect("blocked request producer drop signal");
        server.await.expect("server task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn matching_transfer_cancel_interrupts_blocked_request_producer() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-session-transfer-cancel-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-session-transfer-cancel-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        let server_public_key = proof_public_key(&server_data_dir);
        let (source_entered_tx, source_entered_rx) = tokio::sync::oneshot::channel();
        let (source_dropped_tx, source_dropped_rx) = tokio::sync::oneshot::channel();
        let request = crate::ObjectRpcRequest::encode_body(
            "/test/session/matching-transfer-cancel",
            "test",
            &"request-head",
        )
        .expect("request head");
        let prepared = ObjectRpcSessionCall::new(
            request,
            ObjectRpcBodySource::async_reader(
                PendingAsyncReader {
                    entered: Some(source_entered_tx),
                    dropped: Some(source_dropped_tx),
                },
                Some(1),
            ),
        )
        .prepare_for_signing()
        .expect("prepare session call");
        let request_transfer_id = prepared
            .descriptor
            .request_body
            .as_ref()
            .expect("request transfer descriptor")
            .transfer_id
            .clone();
        let request_auth =
            sign_daemon_rpc_request_from_data_dir(&client_data_dir, &prepared.request)
                .expect("request auth");
        let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
        let cancel_reason = "peer cancelled blocked request body";
        let server = tokio::spawn(async move {
            let (_request, responder) = accept_object_rpc_server_session(
                Box::new(server_stream),
                server_data_dir,
                NetworkCallPolicy::default(),
                authenticate_test_request,
            )
            .await
            .expect("accept session");

            // OpenAck must reach the client before its producer is polled.
            // Cancel the exact transfer while producer.next() is pending.
            source_entered_rx
                .await
                .expect("request body producer was polled");
            responder
                .writer
                .send(SessionWirePayload::Transfer(TransferControl::Cancel {
                    transfer_id: request_transfer_id,
                    reason: cancel_reason.to_string(),
                }))
                .await
                .expect("send matching transfer cancellation");
        });

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            start_object_rpc_client_session(
                Box::new(client_stream),
                prepared,
                request_auth,
                client_data_dir,
                server_public_key.clone(),
                server_public_key,
                NetworkCallPolicy::default(),
            ),
        )
        .await
        .expect("matching transfer cancellation must retire the pending client session")
        .expect_err("a cancelled request body cannot become a successful call");
        assert!(
            error.to_string().contains(cancel_reason),
            "unexpected transfer-cancel error: {error}"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), source_dropped_rx)
            .await
            .expect("matching transfer cancellation must cancel the blocked producer")
            .expect("blocked request producer drop signal");
        server.await.expect("server task");
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
        let prepared = ObjectRpcSessionCall::value(request)
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
                .respond(ObjectRpcSessionResponse::value(response))
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
                .respond(ObjectRpcSessionResponse::value(response))
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
    async fn writer_scheduler_flushes_latest_ack_before_completion() {
        let root = sinomo_testdir::TestDir::new("sinomo-session-writer-ack-order");
        let data_dir = SinomoDataDir::new(root.path());
        let transfer_id = TransferId::generate();
        let (wire_writer, wire_reader) = tokio::io::duplex(4096);
        let writer = SessionWriterHandle::spawn(
            SessionFrameWriter::new(wire_writer, data_dir, NetworkCallPolicy::default())
                .expect("session writer"),
        );
        for received_contiguous_offset in [1_u64, 2, 3] {
            writer
                .acknowledge(TransferAck {
                    transfer_id: transfer_id.clone(),
                    received_contiguous_offset,
                    receive_window: TransferFlowWindow::default(),
                })
                .expect("queue cumulative acknowledgement");
        }
        writer
            .send_from_reader(SessionWirePayload::Transfer(TransferControl::Complete(
                TransferReceipt {
                    transfer_id: transfer_id.clone(),
                    completed_bytes: 3,
                },
            )))
            .await
            .expect("queue completion");

        let mut reader = SessionFrameReader::new(wire_reader, NetworkCallPolicy::default());
        assert!(matches!(
            reader.receive().await.expect("latest acknowledgement"),
            SessionInboundFrame::Payload(SessionWirePayload::Transfer(TransferControl::Ack(
                TransferAck {
                    received_contiguous_offset: 3,
                    ..
                }
            )))
        ));
        assert!(matches!(
            reader.receive().await.expect("completion"),
            SessionInboundFrame::Payload(SessionWirePayload::Transfer(TransferControl::Complete(
                TransferReceipt {
                    completed_bytes: 3,
                    ..
                }
            )))
        ));
    }

    #[tokio::test]
    async fn outgoing_body_pipelines_multiple_frames_before_the_first_ack() {
        let root = sinomo_testdir::TestDir::new("sinomo-session-pipelined-window");
        let data_dir = SinomoDataDir::new(root.path());
        let transfer_id = TransferId::generate();
        let frame_bytes = u64::from(crate::transfer::DEFAULT_TRANSFER_FRAME_BYTES);
        let body = vec![0x4a_u8; usize::try_from(frame_bytes * 3).expect("test body size")];
        let open = TransferOpen {
            transfer_id: transfer_id.clone(),
            route: "/test/session/pipelined-window".to_string(),
            operation_id: Some("pipeline-test".to_string()),
            total_bytes: Some(frame_bytes * 3),
            replayable: true,
            requested_window: TransferFlowWindow::default(),
        };
        let (wire_writer, wire_reader) = tokio::io::duplex(4 * 1024 * 1024);
        let writer = SessionWriterHandle::spawn(
            SessionFrameWriter::new(wire_writer, data_dir, NetworkCallPolicy::default())
                .expect("session writer"),
        );
        let (feedback_tx, feedback_rx) = tokio::sync::mpsc::channel(16);
        feedback_tx
            .send(TransferControl::OpenAck(crate::transfer::TransferOpenAck {
                transfer_id: transfer_id.clone(),
                receive_window: TransferFlowWindow::default(),
            }))
            .await
            .expect("open acknowledgement");
        let source = ObjectRpcBodySource::bytes(body);
        let send_task = tokio::spawn(pump_outgoing_body(
            writer.clone(),
            source,
            open.clone(),
            feedback_rx,
            NetworkCallPolicy::default(),
            NetworkPathEstimator::new(),
        ));
        let mut reader = SessionFrameReader::new(wire_reader, NetworkCallPolicy::default());
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
            SessionInboundFrame::Payload(SessionWirePayload::Finish(finish))
                if finish.total_bytes == frame_bytes * 3
        ));
        feedback_tx
            .send(TransferControl::Complete(TransferReceipt {
                transfer_id,
                completed_bytes: frame_bytes * 3,
            }))
            .await
            .expect("completion receipt");
        send_task
            .await
            .expect("sender task")
            .expect("pipelined transfer");
    }

    #[tokio::test]
    async fn session_transport_does_not_hash_data_frame_payloads() {
        let root = sinomo_testdir::TestDir::new("sinomo-session-trusted-payload");
        let data_dir = SinomoDataDir::new(root.path());
        let transfer_id = TransferId::generate();
        let valid = TransferDataFrame::new(transfer_id, 0, b"good", TransferFlowWindow::default())
            .expect("valid frame");
        let trusted = TransferDataFrame {
            header: valid.header,
            payload: b"evil",
        };
        let (sender, receiver) = tokio::io::duplex(4096);
        let mut writer = SessionFrameWriter::new(sender, data_dir, NetworkCallPolicy::default())
            .expect("session writer");
        let send = tokio::spawn(async move { writer.send_data(trusted).await });
        let mut reader = SessionFrameReader::new(receiver, NetworkCallPolicy::default());
        let received = reader.receive().await.expect("trusted transport payload");
        assert!(matches!(
            received,
            SessionInboundFrame::Data { payload, .. } if payload.as_ref() == b"evil"
        ));
        send.await
            .expect("sender task")
            .expect("send trusted bytes");
    }

    #[test]
    fn transfer_tuning_uses_bdp_for_window_chunk_and_credit_batching() {
        let estimator = NetworkPathEstimator::new();
        let initial = transfer_path_tuning(&estimator);
        assert_eq!(initial.flow_window.max_in_flight_bytes, 16 * 1024 * 1024);
        assert_eq!(initial.flow_window.max_frame_bytes, 1024 * 1024);
        assert_eq!(initial.credit_update_bytes, 8 * 1024 * 1024);

        estimator.record_round_trip_time(std::time::Duration::from_millis(1));
        estimator.record_bandwidth_bytes_per_second(50 * 1024 * 1024 * 1024);
        let measured = transfer_path_tuning(&estimator);
        assert_eq!(measured.flow_window.max_in_flight_bytes, 64 * 1024 * 1024);
        assert_eq!(measured.flow_window.max_frame_bytes, 4 * 1024 * 1024);
        assert_eq!(measured.credit_update_bytes, 16 * 1024 * 1024);

        let mut schedule = TransferCreditUpdateSchedule::new(initial);
        assert!(!schedule.due(7 * 1024 * 1024));
        assert!(schedule.due(8 * 1024 * 1024));
        schedule.mark(8 * 1024 * 1024);
        assert!(!schedule.due(12 * 1024 * 1024));
    }

    #[tokio::test(start_paused = true)]
    async fn session_reader_no_progress_is_a_typed_idle_timeout() {
        let (_sender, receiver) = tokio::io::duplex(64);
        let mut reader = SessionFrameReader::new(receiver, NetworkCallPolicy::default());
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
        let mut reader = SessionFrameReader::new(receiver, policy);
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
