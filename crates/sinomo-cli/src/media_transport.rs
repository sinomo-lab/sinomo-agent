//! Reaches audio devices attached to other nodes through the local daemon.
//!
//! Each device becomes one long-lived object-RPC call: capture reads the streamed response body
//! for as long as the call path wants frames, and playback writes the streamed request body. The
//! daemon owns routing, so this module only opens the call and moves PCM between the stream and
//! the channels behind [`RemoteAudioSource`] and [`RemoteAudioSink`].

use super::*;
use sinomo_audio::remote::{
    CaptureChunkAssembler, PcmFrameReader, RemoteAudioSink, RemoteAudioSource, capture_call,
    playback_call,
};
use sinomo_audio::service::{
    MEDIA_RPC_CATEGORY, MediaAudioCaptureRequest, MediaAudioDevice, MediaAudioPlaybackRequest,
};
use sinomo_runtime::api::DaemonApiClient;
use sinomo_runtime::network::ObjectRpcBodySource;
use std::time::Duration;

/// How long the daemon connection and route establishment may take before the device is reported
/// unreachable. Frames themselves are not bounded by this: the stream lives as long as the call.
const MEDIA_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounds how many frames queue between the call path and the outbound stream.
const MEDIA_FRAME_QUEUE_CAPACITY: usize = 32;

pub(crate) async fn open_desktop_video_source(
    data_dir: &SinomoDataDir,
    node_id: &NodeId,
    app: bool,
    session_id: String,
    encoding: sinomo_lm::RealtimeVideoInputConfig,
) -> Result<sinomo_video::stream::RemoteVideoSource, CliError> {
    use sinomo_actions::desktop::{DesktopModule, DesktopStreamRequest, MEDIA_CATEGORY};
    let kind = DesktopModule::AppUse;
    let client = media_daemon_client(data_dir, node_id).await?;
    let request = DesktopStreamRequest {
        resolution: None,
        capture_path: Default::default(),
        transport: sinomo_video::codec::VideoTransport::Auto,
        session_id,
        frames_per_second: 30,
        include_cursor: !app,
        encoding,
    };
    let call = network::ObjectRpcCall::encode_value(kind.capture_route(), MEDIA_CATEGORY, request)
        .map_err(|e| media_network_error(node_id, e))?;
    let result = client
        .object_rpc_call_to_target_async(node_id, call, MEDIA_CONNECT_TIMEOUT, true)
        .await
        .map_err(|e| media_network_error(node_id, e))?;
    result
        .response
        .error_detail()
        .map_err(|e| media_network_error(node_id, e))?;
    if result.response.route != kind.capture_route() {
        return Err(media_network_error(
            node_id,
            "video response route mismatch",
        ));
    }
    let metadata: serde_json::Value = result
        .response
        .decode_body()
        .map_err(|e| media_network_error(node_id, e))?;
    if !matches!(
        metadata.get("version").and_then(serde_json::Value::as_u64),
        Some(1 | 2 | 3)
    ) {
        return Err(media_network_error(
            node_id,
            "unsupported video stream version",
        ));
    }
    Ok(sinomo_video::stream::RemoteVideoSource::from_body(
        result.body,
    ))
}

fn media_network_error(node_id: &NodeId, detail: impl std::fmt::Display) -> CliError {
    CliError::Network {
        address: node_id.to_string(),
        detail: detail.to_string(),
    }
}

/// Connects to the local daemon, which routes the call to the node that owns the device.
async fn media_daemon_client(
    data_dir: &SinomoDataDir,
    node_id: &NodeId,
) -> Result<sinomo_runtime::api::DaemonApiClient, CliError> {
    let paths = sinomo_runtime::daemon::DaemonPaths::new(data_dir);
    sinomo_runtime::api::DaemonApiClient::connect_for_paths(&paths).map_err(|error| {
        media_network_error(
            node_id,
            format!("reaching a device on another node needs a running local daemon: {error}"),
        )
    })
}

/// One audio device found somewhere in the cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiscoveredAudioDevice {
    pub(crate) node_id: NodeId,
    pub(crate) device: MediaAudioDevice,
}

/// Returns the nodes that publish the media service.
///
/// Service instances are published into cluster KV by each node's daemon, so this is a local
/// observational read: it answers who *offers* media, and the per-node device call answers what
/// each of them actually serves.
fn media_service_nodes(client: &DaemonApiClient) -> Result<Vec<NodeId>, CliError> {
    let payload = sinomo_runtime::kv::cluster_kv_local_range_prefix_rpc_payload(
        &sinomo_runtime::service::cluster_service_prefix(MEDIA_RPC_CATEGORY),
    )?;
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = client
        .call_request(&request)
        .map_err(|source| CliError::Network {
            address: "local daemon".to_string(),
            detail: format!("discovering media service instances failed: {source}"),
        })?;
    let read = sinomo_runtime::kv::decode_cluster_kv_local_range_prefix_rpc_body(&response.body)?;
    let instances =
        sinomo_runtime::service::service_instances_from_entries(&read.value).map_err(|source| {
            CliError::Network {
                address: "local daemon".to_string(),
                detail: format!("decoding media service instances failed: {source}"),
            }
        })?;
    Ok(instances
        .into_iter()
        .map(|instance| instance.node_id)
        .collect())
}

/// Lists the audio devices every media-serving node in the cluster offers.
///
/// A node that is unreachable or serves nothing is skipped rather than failing the whole listing,
/// because one absent node should not hide the devices the rest of the cluster offers.
pub(crate) async fn discover_cluster_audio_devices(
    data_dir: &SinomoDataDir,
    kind: Option<&str>,
) -> Result<Vec<DiscoveredAudioDevice>, CliError> {
    let paths = sinomo_runtime::daemon::DaemonPaths::new(data_dir);
    let Ok(client) = sinomo_runtime::api::DaemonApiClient::connect_for_paths(&paths) else {
        // Without a daemon there is no cluster view; local devices are still listed separately.
        return Ok(Vec::new());
    };
    let mut discovered = Vec::new();
    for node_id in media_service_nodes(&client)? {
        let request = sinomo_audio::service::MediaAudioDevicesRequest {
            kind: kind.map(str::to_string),
        };
        let call = match network::ObjectRpcRequest::encode_body(
            sinomo_audio::service::MEDIA_AUDIO_DEVICES_ROUTE,
            MEDIA_RPC_CATEGORY,
            &request,
        ) {
            Ok(request) => network::ObjectRpcCall::value(request),
            Err(source) => return Err(media_network_error(&node_id, source)),
        };
        let result = client
            .object_rpc_call_to_target_async(
                &node_id,
                call,
                MEDIA_CONNECT_TIMEOUT,
                /*observe_connectivity*/ true,
            )
            .await;
        let Ok(result) = result else {
            tracing::debug!(
                target: "sinomo.cli.media",
                node_id = %node_id,
                "a media node did not answer a device listing; skipping it"
            );
            continue;
        };
        let Ok(response) = result
            .response
            .decode_body::<sinomo_audio::service::MediaAudioDevicesResponse>()
        else {
            continue;
        };
        discovered.extend(
            response
                .devices
                .into_iter()
                .map(|device| DiscoveredAudioDevice {
                    node_id: node_id.clone(),
                    device,
                }),
        );
    }
    discovered.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.device.kind.cmp(&right.device.kind))
            .then_with(|| left.device.selector.cmp(&right.device.selector))
    });
    Ok(discovered)
}

/// Finds the cluster device a selector names, when it is not attached to this node.
///
/// Matching accepts the selector or the display name so an operator can use whichever the device
/// listing showed them.
pub(crate) async fn resolve_cluster_audio_device(
    data_dir: &SinomoDataDir,
    kind: &str,
    selector: &str,
) -> Result<Option<DiscoveredAudioDevice>, CliError> {
    let selector = selector.trim();
    if selector.is_empty() {
        return Ok(None);
    }
    let devices = discover_cluster_audio_devices(data_dir, Some(kind)).await?;
    let mut matches = devices.into_iter().filter(|found| {
        found.device.selector.eq_ignore_ascii_case(selector)
            || found.device.name.eq_ignore_ascii_case(selector)
    });
    let Some(first) = matches.next() else {
        return Ok(None);
    };
    if let Some(second) = matches.next() {
        return Err(CliError::Parse {
            detail: format!(
                "`{selector}` names a {kind} on more than one node (`{}` and `{}`); pass the node explicitly",
                first.node_id, second.node_id
            ),
        });
    }
    Ok(Some(first))
}

/// Opens a capture stream on the node that owns the microphone.
///
/// The call stays open while the returned source is read; dropping the source lets the stream end,
/// which releases the microphone on the serving node.
pub(crate) async fn open_remote_audio_source(
    data_dir: &SinomoDataDir,
    node_id: &NodeId,
    request: MediaAudioCaptureRequest,
) -> Result<RemoteAudioSource, CliError> {
    let client = media_daemon_client(data_dir, node_id).await?;
    let call = capture_call(&request).map_err(|error| media_network_error(node_id, error))?;
    let result = client
        .object_rpc_call_to_target_async(
            node_id,
            call,
            MEDIA_CONNECT_TIMEOUT,
            /*observe_connectivity*/ true,
        )
        .await
        .map_err(|error| media_network_error(node_id, error))?;
    let (source, frames) = RemoteAudioSource::channel();
    let mut assembler = CaptureChunkAssembler::new(
        request.sample_rate_hz,
        request.sample_format,
        request.chunk_ms,
    );
    let mut body = result.body;
    let node_id = node_id.clone();
    tokio::spawn(async move {
        loop {
            match body.next_chunk().await {
                Ok(Some(chunk)) => {
                    for assembled in assembler.push(chunk.as_ref()) {
                        if frames.send(Ok(assembled)).is_err() {
                            // The call path stopped reading, so the stream has no reader left.
                            return;
                        }
                    }
                }
                Ok(None) => {
                    // A short final chunk is still audio the caller asked for.
                    if let Some(tail) = assembler.flush() {
                        let _ = frames.send(Ok(tail));
                    }
                    return;
                }
                Err(error) => {
                    let _ = frames.send(Err(format!(
                        "capture stream from node `{node_id}` failed: {error}"
                    )));
                    return;
                }
            }
        }
    });
    Ok(source)
}

/// Opens a playback stream on the node that owns the speaker.
///
/// Frames written to the returned sink travel as the call's request body. Dropping the sink ends
/// the body, which completes the call on the serving node.
pub(crate) async fn open_remote_audio_sink(
    data_dir: &SinomoDataDir,
    node_id: &NodeId,
    request: MediaAudioPlaybackRequest,
) -> Result<RemoteAudioSink, CliError> {
    let client = media_daemon_client(data_dir, node_id).await?;
    let (sink, queued_frames) = RemoteAudioSink::channel();
    let (stream_frames, stream_rx) = tokio::sync::mpsc::channel(MEDIA_FRAME_QUEUE_CAPACITY);
    // The sink is written from synchronous call-path code, so its queue is drained on a blocking
    // thread and handed to the stream rather than blocking a runtime worker.
    tokio::task::spawn_blocking(move || {
        while let Ok(frame) = queued_frames.recv() {
            if stream_frames.blocking_send(frame).is_err() {
                return;
            }
        }
    });
    let body = ObjectRpcBodySource::async_reader(
        PcmFrameReader::new(stream_rx),
        // The caller decides when speech ends, so the body length is unknown up front.
        /*total_bytes*/
        None,
    );
    let call =
        playback_call(&request, body).map_err(|error| media_network_error(node_id, error))?;
    let node_id = node_id.clone();
    tokio::spawn(async move {
        if let Err(error) = client
            .object_rpc_call_to_target_async(
                &node_id,
                call,
                MEDIA_CONNECT_TIMEOUT,
                /*observe_connectivity*/ true,
            )
            .await
        {
            tracing::warn!(
                target: "sinomo.cli.media",
                node_id = %node_id,
                error = %error,
                "playback stream to a remote speaker ended with an error"
            );
        }
    });
    Ok(sink)
}
