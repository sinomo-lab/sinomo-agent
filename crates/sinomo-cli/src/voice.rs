use super::*;
pub(crate) use sinomo_lm::cli::voice::{VoiceCallOptions, VoiceCliCommand};

use sinomo_runtime::cluster;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const MAX_CONSECUTIVE_NATIVE_VIDEO_FRAME_FAILURES: u8 = 3;

fn native_voice_video_failure_is_terminal(failures: &mut u8) -> bool {
    *failures = failures.saturating_add(1);
    *failures >= MAX_CONSECUTIVE_NATIVE_VIDEO_FRAME_FAILURES
}

#[cfg(test)]
fn parse_voice_schema_for_test(args: &[String]) -> Result<VoiceCliCommand, CliError> {
    sinomo_lm::cli::voice::parse_voice_command(args).map_err(Into::into)
}

fn video_format_from_path(path: &Path) -> Option<sinomo_lm::RealtimeVideoFrameFormat> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("jpg") | Some("jpeg") => Some(sinomo_lm::RealtimeVideoFrameFormat::Jpeg),
        Some("png") => Some(sinomo_lm::RealtimeVideoFrameFormat::Png),
        _ => None,
    }
}

pub(crate) fn run_voice_command(command: VoiceCliCommand) -> Result<(), CliError> {
    match command {
        VoiceCliCommand::Devices => run_voice_devices(),
        VoiceCliCommand::Call(options) => run_voice_call(options),
    }
}

fn run_voice_devices() -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let node_id = voice_local_node_id(&data_dir);
    let inventory = sinomo_runtime::device::enumerate_native_devices(node_id);
    println!("native devices for node {}", inventory.node_id);
    if inventory.devices.is_empty() {
        println!("  none");
    }
    for device in &inventory.devices {
        println!(
            "  {}{} [{}] {}",
            if device.is_default { "* " } else { "  " },
            device.native_id,
            native_device_kind_label(&device.profile.kind),
            device.profile.display_name
        );
        if let Some(detail) = device.detail.as_deref().filter(|detail| !detail.is_empty()) {
            println!("      {detail}");
        }
    }
    for issue in inventory.issues {
        println!("  warning: {issue}");
    }
    print_cluster_audio_devices(&data_dir, &inventory.node_id)?;
    Ok(())
}

/// Lists the audio devices other nodes serve to this cluster.
///
/// Only devices those nodes have exposed appear, so this is what is actually usable from here
/// rather than an inventory of everyone's hardware.
fn print_cluster_audio_devices(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
) -> Result<(), CliError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|source| CliError::Io {
            path: "tokio runtime".to_string(),
            detail: source.to_string(),
        })?;
    let discovered = runtime.block_on(crate::media_transport::discover_cluster_audio_devices(
        data_dir, None,
    ))?;
    let remote = discovered
        .into_iter()
        .filter(|found| &found.node_id != local_node_id)
        .collect::<Vec<_>>();
    if remote.is_empty() {
        return Ok(());
    }
    println!("cluster audio devices served by other nodes");
    for found in remote {
        println!(
            "  {}{}@{} [{}] {} ({})",
            if found.device.is_default { "* " } else { "  " },
            found.device.selector,
            found.node_id,
            found.device.kind,
            found.device.name,
            found.device.operations.join(", "),
        );
    }
    Ok(())
}

fn voice_local_node_id(data_dir: &SinomoDataDir) -> NodeId {
    let paths = ClusterPaths::new(data_dir);
    cluster::read_cluster_state(&paths)
        .map(|state| state.local_node_id)
        .unwrap_or_else(|_| NodeId::from_legacy_named("node_cli").expect("valid node id"))
}

fn native_device_kind_label(kind: &sinomo_runtime::device::DeviceKind) -> &'static str {
    match kind {
        sinomo_runtime::device::DeviceKind::Microphone => "microphone",
        sinomo_runtime::device::DeviceKind::Camera => "camera",
        sinomo_runtime::device::DeviceKind::Speaker => "speaker",
        sinomo_runtime::device::DeviceKind::Screen => "screen",
        sinomo_runtime::device::DeviceKind::Keyboard => "keyboard",
        sinomo_runtime::device::DeviceKind::Mouse => "mouse",
        sinomo_runtime::device::DeviceKind::Filesystem => "filesystem",
        sinomo_runtime::device::DeviceKind::Gpu => "gpu",
        sinomo_runtime::device::DeviceKind::NotificationSurface => "notification",
        sinomo_runtime::device::DeviceKind::Location => "location",
        sinomo_runtime::device::DeviceKind::Touchscreen => "touchscreen",
        sinomo_runtime::device::DeviceKind::MotionSensor => "motion-sensor",
        sinomo_runtime::device::DeviceKind::Battery => "battery",
        sinomo_runtime::device::DeviceKind::Haptic => "haptic",
        sinomo_runtime::device::DeviceKind::Other => "other",
    }
}

fn run_voice_call(options: VoiceCallOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let provider =
        sinomo_lm::RealtimeAudioProvider::from_id(&options.provider).ok_or_else(|| {
            CliError::Parse {
                detail: format!("unsupported realtime voice provider `{}`", options.provider),
            }
        })?;
    let model = options
        .model
        .clone()
        .unwrap_or_else(|| provider.default_model().to_string());
    let session_id = options
        .session_id
        .clone()
        .unwrap_or_else(SessionId::generate);
    let mut config = sinomo_lm::RealtimeAudioSessionConfig::new(provider, model.clone());
    config.instructions = options.instructions.clone();
    config.voice = options.voice.clone();
    config.turn_detection = options.turn_detection;
    if let Some(timeout) = options.receive_timeout_ms {
        config.receive_timeout_ms = Some(timeout);
    }
    if let Some(sample_rate) = options.input_sample_rate_hz {
        config.input_sample_rate_hz = sample_rate;
    }
    if let Some(format) = options.input_format {
        config.input_format = format;
    }
    if !options.video_frames.is_empty()
        || options.native_camera
        || options.desktop_session.is_some()
    {
        config.input_video = Some(sinomo_lm::RealtimeVideoInputConfig {
            frame_format: options
                .video_format
                .unwrap_or(sinomo_lm::RealtimeVideoFrameFormat::Jpeg),
            detail: options.video_detail,
            max_frame_rate_hz: Some(
                (1000_u64 / options.video_frame_ms.max(1))
                    .max(1)
                    .min(u32::MAX as u64) as u32,
            ),
            ..Default::default()
        });
    }
    if let Some(endpoint) = options.endpoint.as_deref() {
        if endpoint.starts_with("ws://") || endpoint.starts_with("wss://") {
            config.websocket_url = Some(endpoint.to_string());
        } else {
            config.api_base_url = Some(endpoint.to_string());
        }
    }
    if let Some(offer) = read_webrtc_offer(&options)? {
        config.transport = sinomo_lm::RealtimeAudioTransport::webrtc(offer);
    }
    authorize_realtime_voice_config(&data_dir, &mut config)?;

    let default_output = sinomo_agent::AgentStoragePaths::from_data_dir(&data_dir)
        .session_audio_dir_for_session(&session_id)
        .join("voice-output.raw");
    let output_path = options.output.clone().unwrap_or(default_output);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|source| CliError::Io {
            path: "tokio runtime".to_string(),
            detail: source.to_string(),
        })?;
    let report = runtime.block_on(run_voice_call_async(
        data_dir,
        session_id,
        config,
        options,
        output_path.clone(),
    ))?;
    if output_path == Path::new("-") {
        ::std::eprintln!("voice session: {}", report.session_id);
        ::std::eprintln!("provider: {} {}", report.provider, report.model);
        ::std::eprintln!("input chunks: {}", report.input_chunks);
        ::std::eprintln!("video frames: {}", report.input_video_frames);
        ::std::eprintln!("output chunks: {}", report.output_chunks);
        ::std::eprintln!("output audio: stdout");
    } else {
        println!("voice session: {}", report.session_id);
        println!("provider: {} {}", report.provider, report.model);
        println!("input chunks: {}", report.input_chunks);
        println!("video frames: {}", report.input_video_frames);
        println!("output chunks: {}", report.output_chunks);
        println!("output audio: {}", output_path.display());
    }
    Ok(())
}

/// Opens the capture device this call reads from, wherever it is attached.
///
/// Device selectors are node-scoped: a locator naming another node opens a routed capture stream
/// on that node instead of silently resolving against this node's microphones.
async fn open_voice_audio_source(
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    options: &VoiceCallOptions,
) -> Result<Option<Box<dyn sinomo_audio::RealtimeAudioSource>>, CliError> {
    if !options.native_microphone {
        return Ok(None);
    }
    let locator =
        resolve_voice_device_locator(voice_microphone_locator(options), "microphone").await?;
    if let Some(node_id) = remote_media_node(&locator)? {
        let source = crate::media_transport::open_remote_audio_source(
            &SinomoDataDir::from_env_or_home()?,
            &node_id,
            sinomo_audio::service::MediaAudioCaptureRequest {
                device: locator.device,
                sample_rate_hz: config.input_sample_rate_hz,
                sample_format: native_audio_format(config.input_format),
                chunk_ms: options.chunk_ms,
                max_chunks: None,
            },
        )
        .await?;
        return Ok(Some(Box::new(source)));
    }
    let mut capture_config = sinomo_audio::NativeAudioCaptureConfig::new(
        config.input_sample_rate_hz,
        native_audio_format(config.input_format),
    );
    capture_config.device = locator.device;
    capture_config.chunk_ms = options.chunk_ms;
    let capture = sinomo_audio::start_native_microphone_capture(capture_config)
        .map_err(native_audio_error)?;
    Ok(Some(Box::new(capture)))
}

/// Opens the playback device this call writes to, wherever it is attached.
async fn open_voice_audio_sink(
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    options: &VoiceCallOptions,
) -> Result<Option<Box<dyn sinomo_audio::RealtimeAudioSink>>, CliError> {
    if !options.native_speaker {
        return Ok(None);
    }
    let locator = resolve_voice_device_locator(voice_speaker_locator(options), "speaker").await?;
    if let Some(node_id) = remote_media_node(&locator)? {
        let sink = crate::media_transport::open_remote_audio_sink(
            &SinomoDataDir::from_env_or_home()?,
            &node_id,
            sinomo_audio::service::MediaAudioPlaybackRequest {
                device: locator.device,
                input_sample_rate_hz: config.output_sample_rate_hz,
                input_sample_format: native_audio_format(config.output_format),
            },
        )
        .await?;
        return Ok(Some(Box::new(sink)));
    }
    let playback =
        sinomo_audio::start_native_speaker_playback(sinomo_audio::NativeSpeakerPlaybackConfig {
            device: locator.device,
            input_sample_rate_hz: config.output_sample_rate_hz,
            input_sample_format: native_audio_format(config.output_format),
        })
        .map_err(native_audio_error)?;
    Ok(Some(Box::new(playback)))
}

fn voice_microphone_locator(options: &VoiceCallOptions) -> sinomo_audio::MediaDeviceLocator {
    match options.microphone_node.clone() {
        Some(node_id) => {
            sinomo_audio::MediaDeviceLocator::on_node(node_id, options.microphone_device.clone())
        }
        None => sinomo_audio::MediaDeviceLocator::local(options.microphone_device.clone()),
    }
}

fn voice_speaker_locator(options: &VoiceCallOptions) -> sinomo_audio::MediaDeviceLocator {
    match options.speaker_node.clone() {
        Some(node_id) => {
            sinomo_audio::MediaDeviceLocator::on_node(node_id, options.speaker_device.clone())
        }
        None => sinomo_audio::MediaDeviceLocator::local(options.speaker_device.clone()),
    }
}

/// Fills in the node a named device lives on when the caller did not say.
///
/// A device already attached to this node wins without consulting the cluster, so an operator who
/// names a local device never depends on a daemon or on discovery. Only a name this node does not
/// have is looked up across the cluster.
async fn resolve_voice_device_locator(
    locator: sinomo_audio::MediaDeviceLocator,
    kind: &str,
) -> Result<sinomo_audio::MediaDeviceLocator, CliError> {
    if locator.node_id.is_some() {
        return Ok(locator);
    }
    let Some(selector) = locator
        .device
        .as_deref()
        .map(str::trim)
        .filter(|selector| !selector.is_empty())
    else {
        return Ok(locator);
    };
    if local_audio_device_exists(kind, selector) {
        return Ok(locator);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let Some(found) =
        crate::media_transport::resolve_cluster_audio_device(&data_dir, kind, selector).await?
    else {
        return Ok(locator);
    };
    Ok(sinomo_audio::MediaDeviceLocator::on_node(
        found.node_id.to_string(),
        Some(found.device.selector),
    ))
}

/// Reports whether this node has an audio device matching a selector or display name.
fn local_audio_device_exists(kind: &str, selector: &str) -> bool {
    let Ok(data_dir) = SinomoDataDir::from_env_or_home() else {
        return false;
    };
    let inventory =
        sinomo_runtime::device::enumerate_native_devices(voice_local_node_id(&data_dir));
    inventory.devices.iter().any(|device| {
        native_device_kind_label(&device.profile.kind) == kind
            && (device.native_id.eq_ignore_ascii_case(selector)
                || device.profile.display_name.eq_ignore_ascii_case(selector))
    })
}

/// Returns the node a device stream must be opened on, or `None` when it is attached here.
///
/// A locator naming this node resolves to the local device rather than routing a call back to
/// ourselves.
fn remote_media_node(
    locator: &sinomo_audio::MediaDeviceLocator,
) -> Result<Option<NodeId>, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let local_node_id = voice_local_node_id(&data_dir).to_string();
    if !locator.is_remote(&local_node_id) {
        return Ok(None);
    }
    let node_id = locator.node_id.as_deref().unwrap_or_default();
    NodeId::from_legacy_named(node_id)
        .or_else(|_| NodeId::new(node_id))
        .map(Some)
        .map_err(|source| CliError::Parse {
            detail: format!("invalid media device node id `{node_id}`: {source}"),
        })
}

/// Reads the SDP offer that selects the WebRTC transport, if one was supplied.
fn read_webrtc_offer(options: &VoiceCallOptions) -> Result<Option<String>, CliError> {
    let offer = if options.webrtc_offer_stdin {
        let mut offer = String::new();
        io::stdin()
            .read_to_string(&mut offer)
            .map_err(|source| CliError::Io {
                path: "-".to_string(),
                detail: source.to_string(),
            })?;
        offer
    } else {
        let Some(path) = options.webrtc_offer.as_deref() else {
            return Ok(None);
        };
        fs::read_to_string(path).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?
    };
    if offer.trim().is_empty() {
        return Err(CliError::Parse {
            detail: "the WebRTC SDP offer is empty".to_string(),
        });
    }
    Ok(Some(offer))
}

/// Applies the active credential for the session's provider.
///
/// A Codex sign-in carries more than a key — the account it acts for travels as a header — so the
/// provider authorizes the whole configuration rather than returning a bare token.
fn authorize_realtime_voice_config(
    data_dir: &SinomoDataDir,
    config: &mut sinomo_lm::RealtimeAudioSessionConfig,
) -> Result<(), CliError> {
    let provider = config.provider;
    match provider {
        sinomo_lm::RealtimeAudioProvider::Codex => {
            let client = OpenAiResponsesCodexClient::new(OpenAiResponsesCodexConfig {
                credentials_path: Some(codex_credentials_file_in_data_dir(data_dir)),
                ..OpenAiResponsesCodexConfig::default()
            });
            client
                .authorize_realtime_session(config)
                .map_err(CliError::LanguageModel)?;
        }
        sinomo_lm::RealtimeAudioProvider::OpenAi => {
            config.api_key = Some(OpenAiProvider::default().api_key()?);
        }
        sinomo_lm::RealtimeAudioProvider::MiniCpmO => {
            config.api_key = match provider_api_key_from_env(provider.provider_id()) {
                Some((api_key, _source)) => Some(api_key),
                None => {
                    sinomo_lm::stored_provider_api_key_in_data_dir(data_dir, provider.provider_id())
                        .map_err(CliError::LanguageModel)?
                }
            };
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VoiceCallReport {
    session_id: SessionId,
    provider: String,
    model: String,
    input_chunks: u64,
    input_video_frames: u64,
    output_chunks: u64,
}

#[derive(Debug, Default)]
struct VoiceCallState {
    provider_session_id: Option<String>,
    output_sequence: u64,
}

#[derive(Debug, Default)]
struct NativeVoiceLoopReport {
    input_chunks: u64,
    input_video_frames: u64,
}

async fn run_voice_call_async(
    data_dir: SinomoDataDir,
    session_id: SessionId,
    config: sinomo_lm::RealtimeAudioSessionConfig,
    options: VoiceCallOptions,
    output_path: PathBuf,
) -> Result<VoiceCallReport, CliError> {
    let provider_id = config.provider.provider_id().to_string();
    let model = config.model.clone();
    let store = sinomo_agent::FileAgentSessionStore::for_data_dir(&data_dir);
    let mut session = sinomo_lm::connect_realtime_session(config.clone())
        .await
        .map_err(realtime_audio_error)?;
    let mut state = VoiceCallState::default();
    let mut output = open_voice_output(&output_path)?;
    if let Some(call) = session.negotiated_call().cloned() {
        return run_webrtc_voice_call(
            session,
            call,
            state,
            output,
            &data_dir,
            session_id,
            provider_id,
            model,
            &config,
            &options,
            &output_path,
        )
        .await;
    }
    let speaker = open_voice_audio_sink(&config, &options).await?;
    let input_video_frames = send_voice_video_frames(
        session.as_mut(),
        &mut state,
        &store,
        &session_id,
        &provider_id,
        &model,
        &config,
        &options,
        output.as_mut(),
        &output_path,
        speaker.as_deref(),
    )
    .await?;
    let input_sequence;
    let mut input_video_frames = input_video_frames;
    if options.native_microphone || options.native_camera || options.desktop_session.is_some() {
        let native_report = run_native_voice_loop(
            session.as_mut(),
            &mut state,
            &store,
            &session_id,
            &provider_id,
            &model,
            &config,
            &options,
            output.as_mut(),
            &output_path,
            speaker.as_deref(),
            input_video_frames,
        )
        .await?;
        input_sequence = native_report.input_chunks;
        input_video_frames = native_report.input_video_frames;
    } else {
        input_sequence = run_raw_voice_input_loop(
            session.as_mut(),
            &mut state,
            &store,
            &session_id,
            &provider_id,
            &model,
            &config,
            &options,
            output.as_mut(),
            &output_path,
            speaker.as_deref(),
        )
        .await?;
    }
    if input_sequence > 0 {
        session.flush_turn().await.map_err(realtime_audio_error)?;
    } else if input_video_frames > 0 {
        session
            .request_response()
            .await
            .map_err(realtime_audio_error)?;
    }

    loop {
        let event = session
            .receive_event()
            .await
            .map_err(realtime_audio_error)?;
        if handle_voice_event(
            event,
            &mut state,
            &store,
            &session_id,
            &provider_id,
            &model,
            &config,
            output.as_mut(),
            &output_path,
            speaker.as_deref(),
            true,
        )? {
            break;
        }
    }
    let _ = session.close().await;
    output.flush().map_err(|source| CliError::Io {
        path: output_path.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(VoiceCallReport {
        session_id,
        provider: provider_id,
        model,
        input_chunks: input_sequence,
        input_video_frames,
        output_chunks: state.output_sequence,
    })
}

/// Runs a call whose media rides a WebRTC peer connection.
///
/// The answer is reported before events are drained because negotiation is only half finished
/// until the peer connection owner applies it — nothing arrives on the sideband until it does.
#[allow(clippy::too_many_arguments)]
async fn run_webrtc_voice_call(
    mut session: Box<dyn sinomo_lm::RealtimeSession>,
    call: sinomo_lm::RealtimeWebrtcCall,
    mut state: VoiceCallState,
    mut output: Box<dyn Write>,
    data_dir: &SinomoDataDir,
    session_id: SessionId,
    provider_id: String,
    model: String,
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    options: &VoiceCallOptions,
    output_path: &Path,
) -> Result<VoiceCallReport, CliError> {
    let store = sinomo_agent::FileAgentSessionStore::for_data_dir(data_dir);
    let speaker = open_voice_audio_sink(config, options).await?;
    ::std::eprintln!("webrtc call id: {}", call.call_id);
    ::std::eprintln!("webrtc answer sdp:\n{}", call.answer_sdp);
    let duration = Duration::from_millis(options.call_ms.unwrap_or(30_000));
    receive_voice_events_for(
        session.as_mut(),
        &mut state,
        &store,
        &session_id,
        &provider_id,
        &model,
        config,
        output.as_mut(),
        output_path,
        speaker.as_deref(),
        duration,
        /*stop_on_turn_end*/ false,
    )
    .await?;
    let _ = session.close().await;
    output.flush().map_err(|source| CliError::Io {
        path: output_path.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(VoiceCallReport {
        session_id,
        provider: provider_id,
        model,
        input_chunks: 0,
        input_video_frames: 0,
        output_chunks: state.output_sequence,
    })
}

async fn run_raw_voice_input_loop(
    session: &mut dyn sinomo_lm::RealtimeSession,
    state: &mut VoiceCallState,
    store: &sinomo_agent::FileAgentSessionStore,
    session_id: &SessionId,
    provider_id: &str,
    model: &str,
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    options: &VoiceCallOptions,
    output: &mut dyn Write,
    output_path: &Path,
    speaker: Option<&dyn sinomo_audio::RealtimeAudioSink>,
) -> Result<u64, CliError> {
    let mut input = if options.audio_input {
        Some(open_voice_input(options.input.as_ref())?)
    } else {
        None
    };
    let chunk_bytes = voice_chunk_bytes(
        config.input_sample_rate_hz,
        config.input_format,
        options.chunk_ms,
    );
    let mut buffer = vec![0_u8; chunk_bytes];
    let mut input_sequence = 0_u64;
    while let Some(input) = input.as_mut() {
        let read = input.read(&mut buffer).map_err(|source| CliError::Io {
            path: options
                .input
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "stdin".to_string()),
            detail: source.to_string(),
        })?;
        if read == 0 {
            break;
        }
        let bytes = buffer[..read].to_vec();
        let mut segment = sinomo_agent::AgentAudioSegmentInput::new(
            sinomo_agent::AgentAudioDirection::Input,
            input_sequence,
            provider_id.to_string(),
            model.to_string(),
            config.input_sample_rate_hz,
            config.input_format,
        );
        segment.provider_session_id = state.provider_session_id.clone();
        segment.duration_ms = audio_duration_ms(
            bytes.len(),
            config.input_sample_rate_hz,
            config.input_format,
            1,
        );
        store
            .append_audio_segment(session_id, segment, &bytes)
            .map_err(agent_session_store_error)?;
        let mut chunk = sinomo_lm::RealtimeAudioInputChunk::new(bytes);
        chunk.force_listen = options.force_listen;
        session
            .send_audio(&chunk)
            .await
            .map_err(realtime_audio_error)?;
        input_sequence = input_sequence.saturating_add(1);
        if options.pace {
            if receive_voice_events_for(
                session,
                state,
                store,
                session_id,
                provider_id,
                model,
                config,
                output,
                output_path,
                speaker,
                Duration::from_millis(options.chunk_ms),
                false,
            )
            .await?
            {
                break;
            }
        } else {
            let _ = receive_voice_events_for(
                session,
                state,
                store,
                session_id,
                provider_id,
                model,
                config,
                output,
                output_path,
                speaker,
                Duration::from_millis(1),
                false,
            )
            .await?;
        }
    }
    Ok(input_sequence)
}

async fn run_native_voice_loop(
    session: &mut dyn sinomo_lm::RealtimeSession,
    state: &mut VoiceCallState,
    store: &sinomo_agent::FileAgentSessionStore,
    session_id: &SessionId,
    provider_id: &str,
    model: &str,
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    options: &VoiceCallOptions,
    output: &mut dyn Write,
    output_path: &Path,
    speaker: Option<&dyn sinomo_audio::RealtimeAudioSink>,
    initial_video_frames: u64,
) -> Result<NativeVoiceLoopReport, CliError> {
    let microphone = open_voice_audio_source(config, options).await?;
    let desktop = if let Some(desktop_session) = &options.desktop_session {
        let data_dir = SinomoDataDir::from_env_or_home()?;
        let node = options
            .desktop_node
            .as_deref()
            .map(NodeId::from_legacy_named)
            .transpose()
            .map_err(|e| CliError::Parse {
                detail: e.to_string(),
            })?
            .unwrap_or_else(|| voice_local_node_id(&data_dir));
        Some(
            crate::media_transport::open_desktop_video_source(
                &data_dir,
                &node,
                options.desktop_app,
                desktop_session.clone(),
                config.input_video.clone().unwrap_or_default(),
            )
            .await?,
        )
    } else {
        None
    };
    let camera = if options.native_camera {
        let camera_config = native_voice_camera_capture_config(options);
        Some(sinomo_video::start_native_camera_capture(camera_config).map_err(native_video_error)?)
    } else {
        None
    };
    let deadline = Instant::now() + Duration::from_millis(options.call_ms.unwrap_or(30_000).max(1));
    let mut input_sequence = 0_u64;
    let mut video_sequence = initial_video_frames;
    let video_send_interval = native_voice_model_frame_interval(config, options.video_frame_ms);
    let mut next_video_send_at = Instant::now();
    let mut latest_camera_sample = None;
    let mut video_conversion_failures = 0_u8;
    while Instant::now() < deadline {
        let mut progressed = false;
        if let Some(microphone) = &microphone
            && let Some(chunk) = microphone
                .recv_chunk(Duration::from_millis(5))
                .map_err(native_audio_error)?
        {
            send_voice_audio_chunk(
                session,
                state,
                store,
                session_id,
                provider_id,
                model,
                config,
                options,
                input_sequence,
                chunk.audio,
            )
            .await?;
            input_sequence = input_sequence.saturating_add(1);
            progressed = true;
        }
        if let Some(camera) = &camera {
            if let Some(sample) = camera
                .recv_sample_timeout(Duration::from_millis(1))
                .map_err(native_video_error)?
            {
                latest_camera_sample = Some(sample);
                progressed = true;
            }
            let now = Instant::now();
            if now >= next_video_send_at
                && let Some(sample) = latest_camera_sample.take()
            {
                let converted = tokio::task::spawn_blocking(move || {
                    sinomo_video::native_video_sample_to_surface_frame(&sample)
                })
                .await
                .map_err(|source| CliError::Parse {
                    detail: format!("native camera conversion task failed: {source}"),
                })?;
                let frame = match converted {
                    Ok(frame) => {
                        video_conversion_failures = 0;
                        frame
                    }
                    Err(error) => {
                        next_video_send_at = now + video_send_interval;
                        if native_voice_video_failure_is_terminal(&mut video_conversion_failures) {
                            return Err(native_video_error(error));
                        }
                        continue;
                    }
                };
                send_voice_raw_video_frame(
                    session,
                    state,
                    store,
                    session_id,
                    provider_id,
                    model,
                    options,
                    frame,
                    video_sequence,
                )
                .await?;
                video_sequence = video_sequence.saturating_add(1);
                next_video_send_at = now + video_send_interval;
                progressed = true;
            }
        }
        if let Some(desktop) = &desktop
            && Instant::now() >= next_video_send_at
            && let Some((metadata, mut frame)) = desktop
                .take_latest()
                .map_err(|detail| CliError::Parse { detail })?
        {
            frame.detail = options.video_detail;
            frame.sequence = video_sequence;
            session
                .send_encoded_video_frame(&frame)
                .await
                .map_err(realtime_audio_error)?;
            let mut record = sinomo_agent::AgentVideoFrameInput::new(
                sinomo_agent::AgentVideoDirection::Input,
                video_sequence,
                provider_id.to_string(),
                model.to_string(),
                frame.format,
            );
            record.provider_session_id = state.provider_session_id.clone();
            record.timestamp_ms = Some(metadata.timestamp_ns / 1_000_000);
            store
                .append_video_frame(session_id, record, &frame.image)
                .map_err(agent_session_store_error)?;
            video_sequence = video_sequence.saturating_add(1);
            next_video_send_at = Instant::now() + video_send_interval;
            progressed = true;
        }
        match session
            .receive_event_with_timeout(Duration::from_millis(1))
            .await
        {
            Ok(event) => {
                if handle_voice_event(
                    event,
                    state,
                    store,
                    session_id,
                    provider_id,
                    model,
                    config,
                    output,
                    output_path,
                    speaker,
                    false,
                )? {
                    break;
                }
                progressed = true;
            }
            Err(sinomo_lm::RealtimeAudioError::Timeout { .. }) => {}
            Err(error) => return Err(realtime_audio_error(error)),
        }
        if !progressed {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    Ok(NativeVoiceLoopReport {
        input_chunks: input_sequence,
        input_video_frames: video_sequence,
    })
}

fn native_voice_camera_capture_config(
    options: &VoiceCallOptions,
) -> sinomo_video::NativeVideoCaptureConfig {
    sinomo_video::NativeVideoCaptureConfig {
        device: options.camera_device.clone(),
        // Keep native capture at interactive video cadence. Model sampling is
        // independent and must not renegotiate the camera to a low frame rate.
        frame_interval_ms: sinomo_video::NATIVE_VIDEO_PREVIEW_FRAME_INTERVAL_MS,
        ..Default::default()
    }
}

fn native_voice_model_frame_interval(
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    fallback_ms: u64,
) -> Duration {
    let exact_interval = Duration::from_millis(fallback_ms.max(1));
    config
        .input_video
        .as_ref()
        .and_then(sinomo_lm::RealtimeVideoInputConfig::model_frame_interval)
        .map(|configured| configured.max(exact_interval))
        .unwrap_or(exact_interval)
}

async fn send_voice_audio_chunk(
    session: &mut dyn sinomo_lm::RealtimeSession,
    state: &mut VoiceCallState,
    store: &sinomo_agent::FileAgentSessionStore,
    session_id: &SessionId,
    provider_id: &str,
    model: &str,
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    options: &VoiceCallOptions,
    sequence: u64,
    bytes: Vec<u8>,
) -> Result<(), CliError> {
    let mut segment = sinomo_agent::AgentAudioSegmentInput::new(
        sinomo_agent::AgentAudioDirection::Input,
        sequence,
        provider_id.to_string(),
        model.to_string(),
        config.input_sample_rate_hz,
        config.input_format,
    );
    segment.provider_session_id = state.provider_session_id.clone();
    segment.duration_ms = audio_duration_ms(
        bytes.len(),
        config.input_sample_rate_hz,
        config.input_format,
        1,
    );
    store
        .append_audio_segment(session_id, segment, &bytes)
        .map_err(agent_session_store_error)?;
    let mut chunk = sinomo_lm::RealtimeAudioInputChunk::new(bytes);
    chunk.force_listen = options.force_listen;
    session
        .send_audio(&chunk)
        .await
        .map_err(realtime_audio_error)
}

#[allow(clippy::too_many_arguments)]
async fn send_voice_raw_video_frame(
    session: &mut dyn sinomo_lm::RealtimeSession,
    state: &mut VoiceCallState,
    store: &sinomo_agent::FileAgentSessionStore,
    session_id: &SessionId,
    provider_id: &str,
    model: &str,
    options: &VoiceCallOptions,
    frame: sinomo_video::NativeVideoSurfaceFrame,
    sequence: u64,
) -> Result<(), CliError> {
    let stride = usize::try_from(frame.width)
        .ok()
        .and_then(|width| width.checked_mul(4))
        .ok_or_else(|| CliError::Parse {
            detail: "native camera frame stride overflowed".to_string(),
        })?;
    let timestamp_ms = Some(frame.timestamp_ns / 1_000_000);
    let capture_sequence = frame.sequence;
    let captured_at_unix_ms = frame.captured_at_unix_ms;
    let mut frame_input = sinomo_lm::RealtimeVideoFrameInput::new(
        frame.rgba,
        sinomo_lm::RealtimeVideoPixelFormat::Rgba8,
        frame.width,
        frame.height,
        stride,
        sequence,
    );
    frame_input.detail = options.video_detail;
    frame_input.timestamp_ms = timestamp_ms;
    let encoded = session
        .send_video_frame_and_return_encoded(&frame_input)
        .await
        .map_err(realtime_audio_error)?;
    let mut record_input = sinomo_agent::AgentVideoFrameInput::new(
        sinomo_agent::AgentVideoDirection::Input,
        sequence,
        provider_id.to_string(),
        model.to_string(),
        encoded.format,
    );
    record_input.provider_session_id = state.provider_session_id.clone();
    record_input.timestamp_ms = encoded.timestamp_ms;
    record_input.width = encoded.width;
    record_input.height = encoded.height;
    record_input.metadata = serde_json::json!({
        "capture_sequence": capture_sequence,
        "captured_at_unix_ms": captured_at_unix_ms,
    });
    store
        .append_video_frame(session_id, record_input, encoded.image.as_ref())
        .map_err(agent_session_store_error)?;
    Ok(())
}

async fn send_voice_video_frames(
    session: &mut dyn sinomo_lm::RealtimeSession,
    state: &mut VoiceCallState,
    store: &sinomo_agent::FileAgentSessionStore,
    session_id: &SessionId,
    provider_id: &str,
    model: &str,
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    options: &VoiceCallOptions,
    output: &mut dyn Write,
    output_path: &Path,
    speaker: Option<&dyn sinomo_audio::RealtimeAudioSink>,
) -> Result<u64, CliError> {
    let mut sequence = 0_u64;
    for frame_path in &options.video_frames {
        let bytes = fs::read(frame_path).map_err(|source| CliError::Io {
            path: frame_path.display().to_string(),
            detail: source.to_string(),
        })?;
        let format = options
            .video_format
            .or_else(|| video_format_from_path(frame_path))
            .ok_or_else(|| CliError::Parse {
                detail: format!(
                    "cannot infer video frame format for {}; pass --video-format jpeg|png",
                    frame_path.display()
                ),
            })?;
        let mut frame_input =
            sinomo_lm::RealtimeEncodedVideoFrameInput::new(bytes.clone(), format, sequence);
        frame_input.detail = options.video_detail;
        frame_input.timestamp_ms = Some(sequence.saturating_mul(options.video_frame_ms));
        let mut record_input = sinomo_agent::AgentVideoFrameInput::new(
            sinomo_agent::AgentVideoDirection::Input,
            sequence,
            provider_id.to_string(),
            model.to_string(),
            format,
        );
        record_input.provider_session_id = state.provider_session_id.clone();
        record_input.timestamp_ms = frame_input.timestamp_ms;
        store
            .append_video_frame(session_id, record_input, &bytes)
            .map_err(agent_session_store_error)?;
        session
            .send_encoded_video_frame(&frame_input)
            .await
            .map_err(realtime_audio_error)?;
        sequence = sequence.saturating_add(1);
        if options.pace
            && receive_voice_events_for(
                session,
                state,
                store,
                session_id,
                provider_id,
                model,
                config,
                output,
                output_path,
                speaker,
                Duration::from_millis(options.video_frame_ms),
                false,
            )
            .await?
        {
            break;
        }
    }
    Ok(sequence)
}

async fn receive_voice_events_for(
    session: &mut dyn sinomo_lm::RealtimeSession,
    state: &mut VoiceCallState,
    store: &sinomo_agent::FileAgentSessionStore,
    session_id: &SessionId,
    provider_id: &str,
    model: &str,
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    output: &mut dyn Write,
    output_path: &Path,
    speaker: Option<&dyn sinomo_audio::RealtimeAudioSink>,
    duration: Duration,
    stop_on_turn_end: bool,
) -> Result<bool, CliError> {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let wait = remaining.min(Duration::from_millis(100));
        match session.receive_event_with_timeout(wait).await {
            Ok(event) => {
                if handle_voice_event(
                    event,
                    state,
                    store,
                    session_id,
                    provider_id,
                    model,
                    config,
                    output,
                    output_path,
                    speaker,
                    stop_on_turn_end,
                )? {
                    return Ok(true);
                }
            }
            Err(sinomo_lm::RealtimeAudioError::Timeout { .. }) => {}
            Err(error) => return Err(realtime_audio_error(error)),
        }
    }
    Ok(false)
}

fn handle_voice_event(
    event: sinomo_lm::RealtimeAudioEvent,
    state: &mut VoiceCallState,
    store: &sinomo_agent::FileAgentSessionStore,
    session_id: &SessionId,
    provider_id: &str,
    model: &str,
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    output: &mut dyn Write,
    output_path: &Path,
    speaker: Option<&dyn sinomo_audio::RealtimeAudioSink>,
    stop_on_turn_end: bool,
) -> Result<bool, CliError> {
    match event {
        sinomo_lm::RealtimeAudioEvent::SessionCreated {
            provider_session_id,
            ..
        } => {
            state.provider_session_id = provider_session_id;
        }
        sinomo_lm::RealtimeAudioEvent::OutputAudioDelta {
            text,
            audio,
            end_of_turn,
            kv_cache_length,
            ..
        } => {
            if !text.trim().is_empty() {
                ::std::eprintln!("voice> {text}");
            }
            output.write_all(&audio).map_err(|source| CliError::Io {
                path: output_path.display().to_string(),
                detail: source.to_string(),
            })?;
            if let Some(speaker) = speaker {
                speaker.write_chunk(&audio).map_err(native_audio_error)?;
            }
            let mut segment = sinomo_agent::AgentAudioSegmentInput::new(
                sinomo_agent::AgentAudioDirection::Output,
                state.output_sequence,
                provider_id.to_string(),
                model.to_string(),
                config.output_sample_rate_hz,
                config.output_format,
            );
            segment.provider_session_id = state.provider_session_id.clone();
            segment.text = if text.is_empty() { None } else { Some(text) };
            segment.end_of_turn = end_of_turn;
            segment.duration_ms = audio_duration_ms(
                audio.len(),
                config.output_sample_rate_hz,
                config.output_format,
                1,
            );
            segment.metadata = serde_json::json!({
                "kv_cache_length": kv_cache_length,
            });
            store
                .append_audio_segment(session_id, segment, &audio)
                .map_err(agent_session_store_error)?;
            state.output_sequence = state.output_sequence.saturating_add(1);
            if end_of_turn && stop_on_turn_end {
                return Ok(true);
            }
        }
        sinomo_lm::RealtimeAudioEvent::OutputTranscriptDelta { delta, .. } => {
            if !delta.trim().is_empty() {
                ::std::eprintln!("voice> {delta}");
            }
        }
        sinomo_lm::RealtimeAudioEvent::ResponseDone => {
            if stop_on_turn_end {
                return Ok(true);
            }
        }
        sinomo_lm::RealtimeAudioEvent::Closed { .. } => return Ok(true),
        sinomo_lm::RealtimeAudioEvent::Error { code, message } => {
            return Err(CliError::Parse {
                detail: format!("realtime voice error {code}: {message}"),
            });
        }
        _ => {}
    }
    Ok(false)
}

fn open_voice_input(path: Option<&PathBuf>) -> Result<Box<dyn Read + Send>, CliError> {
    match path {
        Some(path) => Ok(Box::new(File::open(path).map_err(|source| {
            CliError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?)),
        None => Ok(Box::new(io::stdin())),
    }
}

fn open_voice_output(path: &Path) -> Result<Box<dyn Write + Send>, CliError> {
    if path == Path::new("-") {
        return Ok(Box::new(io::stdout()));
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(Box::new(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .map_err(|source| CliError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?,
    ))
}

fn voice_chunk_bytes(
    sample_rate_hz: u32,
    format: sinomo_lm::RealtimeAudioSampleFormat,
    chunk_ms: u64,
) -> usize {
    let bytes = sample_rate_hz as u64 * format.bytes_per_sample() as u64 * chunk_ms / 1000;
    bytes.max(format.bytes_per_sample() as u64) as usize
}

fn audio_duration_ms(
    byte_len: usize,
    sample_rate_hz: u32,
    format: sinomo_lm::RealtimeAudioSampleFormat,
    channels: u8,
) -> Option<u64> {
    let frame_bytes = format
        .bytes_per_sample()
        .saturating_mul(channels.max(1) as usize);
    if sample_rate_hz == 0 || frame_bytes == 0 {
        return None;
    }
    Some(((byte_len / frame_bytes) as u64).saturating_mul(1000) / sample_rate_hz as u64)
}

fn realtime_audio_error(error: sinomo_lm::RealtimeAudioError) -> CliError {
    CliError::Parse {
        detail: error.to_string(),
    }
}

fn native_audio_format(
    format: sinomo_lm::RealtimeAudioSampleFormat,
) -> sinomo_audio::NativeAudioSampleFormat {
    match format {
        sinomo_lm::RealtimeAudioSampleFormat::Pcm16Le => {
            sinomo_audio::NativeAudioSampleFormat::Pcm16Le
        }
        sinomo_lm::RealtimeAudioSampleFormat::F32Le => sinomo_audio::NativeAudioSampleFormat::F32Le,
    }
}

fn native_audio_error(error: sinomo_audio::NativeAudioError) -> CliError {
    CliError::Parse {
        detail: error.to_string(),
    }
}

fn native_video_error(error: sinomo_video::NativeVideoError) -> CliError {
    CliError::Parse {
        detail: error.to_string(),
    }
}

fn agent_session_store_error(error: sinomo_agent::AgentSessionStoreError) -> CliError {
    CliError::Io {
        path: "agent session store".to_string(),
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desktop_tools_project_from_the_complete_product_catalog_and_withdraw_when_disabled() {
        use sinomo_actions::desktop::*;
        let root = sinomo_testdir::TestDir::new("desktop-agent-tool-projection");
        let data_dir = SinomoDataDir::new(&root);
        let projection = sinomo_agent::product_module_tool_projection(&data_dir).unwrap();
        let bindings = projection.project_bindings();
        for route in [COMPUTER_CONTROL_ROUTE, APP_CONTROL_ROUTE] {
            assert_eq!(
                bindings
                    .iter()
                    .filter(|binding| binding.route == route)
                    .count(),
                1
            );
        }
        assert!(!bindings.iter().any(|binding| {
            [COMPUTER_CAPTURE_ROUTE, APP_CAPTURE_ROUTE].contains(&binding.route.as_str())
        }));
        sinomo_runtime::module::set_native_rust_module_enabled(
            &data_dir.root(),
            &DesktopModule::ComputerUse.module_id(),
            false,
            "test",
            "tool withdrawal",
        )
        .unwrap();
        let projection = sinomo_agent::product_module_tool_projection(&data_dir).unwrap();
        assert!(
            !projection
                .project_bindings()
                .iter()
                .any(|binding| binding.route == COMPUTER_CONTROL_ROUTE)
        );
        assert!(
            projection
                .project_bindings()
                .iter()
                .any(|binding| binding.route == APP_CONTROL_ROUTE)
        );
        assert!(
            projection
                .registrations
                .iter()
                .flat_map(|r| &r.surfaces.declarative_resources)
                .any(|skill| skill.id == "computer-use")
        );
    }

    #[test]
    fn voice_call_options_parse_minicpm_audio_defaults() {
        let command = parse_voice_schema_for_test(&[
            "call".to_string(),
            "--provider".to_string(),
            "minicpm-o".to_string(),
            "--input-format".to_string(),
            "f32".to_string(),
            "--sample-rate".to_string(),
            "16000".to_string(),
            "--turn".to_string(),
            "manual".to_string(),
            "--no-pacing".to_string(),
        ])
        .expect("voice command");
        let VoiceCliCommand::Call(options) = command else {
            panic!("expected call");
        };
        assert_eq!(options.provider, "minicpm-o");
        assert_eq!(
            options.input_format,
            Some(sinomo_lm::RealtimeAudioSampleFormat::F32Le)
        );
        assert_eq!(options.input_sample_rate_hz, Some(16_000));
        assert_eq!(options.chunk_ms, 100);
        assert!(!options.pace);
    }

    #[test]
    fn voice_call_options_select_the_webrtc_transport_from_an_offer_file() {
        let dir = sinomo_testdir::TestDir::new("voice-webrtc-offer");
        let offer_path = dir.path().join("offer.sdp");
        fs::write(&offer_path, "v=0\r\no=- offer\r\n").expect("write offer");
        let command = parse_voice_schema_for_test(&[
            "call".to_string(),
            "--provider".to_string(),
            "codex".to_string(),
            "--webrtc-offer".to_string(),
            offer_path.display().to_string(),
        ])
        .expect("voice command");
        let VoiceCliCommand::Call(options) = command else {
            panic!("expected call");
        };
        assert_eq!(options.provider, "codex");
        assert_eq!(options.webrtc_offer.as_deref(), Some(offer_path.as_path()));
        assert!(!options.webrtc_offer_stdin);
        assert_eq!(
            read_webrtc_offer(&options).expect("read offer"),
            Some("v=0\r\no=- offer\r\n".to_string())
        );

        // Without the option the session keeps the socket transport.
        let default_options = VoiceCallOptions::default();
        assert_eq!(read_webrtc_offer(&default_options).expect("no offer"), None);
    }

    #[test]
    fn voice_call_options_carry_the_node_each_device_is_attached_to() {
        let command = parse_voice_schema_for_test(&[
            "call".to_string(),
            "--native".to_string(),
            "--mic-node".to_string(),
            "node_edge".to_string(),
            "--mic-device".to_string(),
            "usb-mic".to_string(),
            "--speaker-node".to_string(),
            "node_edge".to_string(),
        ])
        .expect("voice command");
        let VoiceCliCommand::Call(options) = command else {
            panic!("expected call");
        };

        let microphone = voice_microphone_locator(&options);
        assert_eq!(microphone.node_id.as_deref(), Some("node_edge"));
        assert_eq!(microphone.device.as_deref(), Some("usb-mic"));
        assert!(microphone.is_remote("node_agent"));
        assert!(!microphone.is_remote("node_edge"));

        let speaker = voice_speaker_locator(&options);
        assert_eq!(speaker.node_id.as_deref(), Some("node_edge"));
        assert!(speaker.device.is_none());

        // Device selectors without a node stay on the node running the call.
        let local = VoiceCallOptions {
            microphone_device: Some("default".to_string()),
            ..VoiceCallOptions::default()
        };
        assert!(!voice_microphone_locator(&local).is_remote("node_agent"));
    }

    #[test]
    fn an_explicit_node_or_an_unnamed_device_never_consults_cluster_discovery() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");

        // A locator that already names a node is returned untouched.
        let pinned =
            sinomo_audio::MediaDeviceLocator::on_node("node_edge", Some("mic".to_string()));
        let resolved = runtime
            .block_on(resolve_voice_device_locator(pinned.clone(), "microphone"))
            .expect("an explicit node needs no lookup");
        assert_eq!(resolved, pinned);

        // A locator naming no device has nothing to look up, so it stays local and default.
        let unnamed = sinomo_audio::MediaDeviceLocator::local(None);
        let resolved = runtime
            .block_on(resolve_voice_device_locator(unnamed.clone(), "microphone"))
            .expect("an unnamed device needs no lookup");
        assert_eq!(resolved, unnamed);
        assert!(resolved.node_id.is_none());

        // A blank selector is treated as unnamed rather than searched for.
        let blank = sinomo_audio::MediaDeviceLocator::local(Some("   ".to_string()));
        let resolved = runtime
            .block_on(resolve_voice_device_locator(blank.clone(), "microphone"))
            .expect("a blank selector needs no lookup");
        assert_eq!(resolved, blank);
    }

    #[test]
    fn a_device_on_this_node_resolves_locally_rather_than_routing_back_to_us() {
        let dir = sinomo_testdir::TestDir::new("voice-local-media-node");
        // SAFETY: the CLI resolves its data directory from the environment, and this test owns
        // the variable for the duration of the resolution it exercises.
        unsafe { std::env::set_var("SINOMO_DATA_DIR", dir.path()) };
        let local_node_id =
            voice_local_node_id(&SinomoDataDir::from_env_or_home().expect("data dir")).to_string();

        // No node at all, and this node by name, are both local.
        assert!(
            remote_media_node(&sinomo_audio::MediaDeviceLocator::local(None))
                .expect("resolve a local locator")
                .is_none()
        );
        assert!(
            remote_media_node(&sinomo_audio::MediaDeviceLocator::on_node(
                local_node_id.clone(),
                None
            ))
            .expect("resolve this node by name")
            .is_none()
        );

        // Another node resolves to a routable node id the media call is opened against.
        let remote = remote_media_node(&sinomo_audio::MediaDeviceLocator::on_node(
            "node_edge",
            None,
        ))
        .expect("resolve another node")
        .expect("another node is remote");
        assert_ne!(remote.to_string(), local_node_id);

        let error = remote_media_node(&sinomo_audio::MediaDeviceLocator::on_node("!!", None))
            .expect_err("an unusable node id is reported rather than routed");
        assert!(error.to_string().contains("node id"), "{error}");
        unsafe { std::env::remove_var("SINOMO_DATA_DIR") };
    }

    #[test]
    fn voice_call_rejects_an_empty_webrtc_offer() {
        let dir = sinomo_testdir::TestDir::new("voice-webrtc-offer-empty");
        let offer_path = dir.path().join("offer.sdp");
        fs::write(&offer_path, "   \n").expect("write offer");
        let options = VoiceCallOptions {
            webrtc_offer: Some(offer_path),
            ..VoiceCallOptions::default()
        };

        let error = read_webrtc_offer(&options).expect_err("empty offer is refused");

        assert!(error.to_string().contains("empty"), "{error}");
    }

    #[test]
    fn voice_call_options_accept_server_vad() {
        let command = parse_voice_schema_for_test(&[
            "call".to_string(),
            "--provider".to_string(),
            "openai".to_string(),
            "--turn".to_string(),
            "server-vad".to_string(),
        ])
        .expect("voice command");
        let VoiceCliCommand::Call(options) = command else {
            panic!("expected call");
        };
        assert_eq!(
            options.turn_detection,
            sinomo_lm::RealtimeAudioTurnDetection::ServerVad
        );
    }

    #[test]
    fn voice_call_options_accept_video_frames_without_stdin_audio() {
        let command = parse_voice_schema_for_test(&[
            "call".to_string(),
            "--provider".to_string(),
            "openai".to_string(),
            "--video-frame".to_string(),
            "frame-001.jpg".to_string(),
            "--video-frame".to_string(),
            "frame-002.png".to_string(),
            "--video-detail".to_string(),
            "low".to_string(),
        ])
        .expect("voice command");
        let VoiceCliCommand::Call(options) = command else {
            panic!("expected call");
        };
        assert_eq!(options.video_frames.len(), 2);
        assert_eq!(
            options.video_detail,
            sinomo_lm::RealtimeVideoFrameDetail::Low
        );
        assert!(!options.audio_input);
    }

    #[test]
    fn voice_call_options_keep_stdin_audio_when_explicit_with_video() {
        let command = parse_voice_schema_for_test(&[
            "call".to_string(),
            "--video-frame".to_string(),
            "frame-001.jpg".to_string(),
            "--input".to_string(),
            "-".to_string(),
        ])
        .expect("voice command");
        let VoiceCliCommand::Call(options) = command else {
            panic!("expected call");
        };
        assert_eq!(options.video_frames.len(), 1);
        assert!(options.audio_input);
        assert_eq!(options.input, None);
    }

    #[test]
    fn voice_call_options_accept_native_media_devices() {
        let command = parse_voice_schema_for_test(&[
            "call".to_string(),
            "--native-mic".to_string(),
            "--camera-device".to_string(),
            "/dev/video0".to_string(),
            "--speaker-device".to_string(),
            "default".to_string(),
            "--call-ms".to_string(),
            "1500".to_string(),
        ])
        .expect("voice command");
        let VoiceCliCommand::Call(options) = command else {
            panic!("expected call");
        };
        assert!(options.native_microphone);
        assert!(options.native_camera);
        assert!(options.native_speaker);
        assert_eq!(options.camera_device.as_deref(), Some("/dev/video0"));
        assert_eq!(options.speaker_device.as_deref(), Some("default"));
        assert_eq!(options.call_ms, Some(1500));

        let capture = native_voice_camera_capture_config(&options);
        assert_eq!(
            capture.frame_interval_ms,
            sinomo_video::NATIVE_VIDEO_PREVIEW_FRAME_INTERVAL_MS
        );
        assert_ne!(capture.frame_interval_ms, options.video_frame_ms);

        let mut session = sinomo_lm::RealtimeAudioSessionConfig::new(
            sinomo_lm::RealtimeAudioProvider::OpenAi,
            "gpt-realtime-2",
        );
        session.input_video = Some(sinomo_lm::RealtimeVideoInputConfig {
            max_frame_rate_hz: Some(4),
            ..Default::default()
        });
        assert_eq!(
            native_voice_model_frame_interval(&session, 250),
            Duration::from_millis(250)
        );
        session
            .input_video
            .as_mut()
            .expect("video config")
            .max_frame_rate_hz = Some(1);
        assert_eq!(
            native_voice_model_frame_interval(&session, 2_000),
            Duration::from_millis(2_000),
            "integer FPS configuration must not make a slow exact cadence run faster"
        );

        let mut failures = 0;
        assert!(!native_voice_video_failure_is_terminal(&mut failures));
        assert!(!native_voice_video_failure_is_terminal(&mut failures));
        assert!(native_voice_video_failure_is_terminal(&mut failures));
    }

    #[test]
    fn voice_chunk_bytes_respects_sample_format() {
        assert_eq!(
            voice_chunk_bytes(16_000, sinomo_lm::RealtimeAudioSampleFormat::F32Le, 1000),
            64_000
        );
        assert_eq!(
            voice_chunk_bytes(24_000, sinomo_lm::RealtimeAudioSampleFormat::Pcm16Le, 500),
            24_000
        );
    }
}
