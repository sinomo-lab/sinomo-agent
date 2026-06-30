use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum VoiceCliCommand {
    Help,
    Call(VoiceCallOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct VoiceCallOptions {
    pub provider: String,
    pub model: Option<String>,
    pub instructions: String,
    pub input: Option<PathBuf>,
    pub output: Option<PathBuf>,
    pub video_frames: Vec<PathBuf>,
    pub video_format: Option<sinomo_lm::RealtimeVideoFrameFormat>,
    pub video_detail: sinomo_lm::RealtimeVideoFrameDetail,
    pub video_frame_ms: u64,
    pub session_id: Option<SessionId>,
    pub endpoint: Option<String>,
    pub voice: Option<String>,
    pub chunk_ms: u64,
    pub input_sample_rate_hz: Option<u32>,
    pub input_format: Option<sinomo_lm::RealtimeAudioSampleFormat>,
    pub turn_detection: sinomo_lm::RealtimeAudioTurnDetection,
    pub pace: bool,
    pub force_listen: bool,
    pub audio_input: bool,
    pub receive_timeout_ms: Option<u64>,
}

impl Default for VoiceCallOptions {
    fn default() -> Self {
        Self {
            provider: "openai".to_string(),
            model: None,
            instructions: "You are Sinomo's realtime voice agent. Keep spoken responses concise."
                .to_string(),
            input: None,
            output: None,
            video_frames: Vec::new(),
            video_format: None,
            video_detail: sinomo_lm::RealtimeVideoFrameDetail::Auto,
            video_frame_ms: 500,
            session_id: None,
            endpoint: None,
            voice: None,
            chunk_ms: 100,
            input_sample_rate_hz: None,
            input_format: None,
            turn_detection: sinomo_lm::RealtimeAudioTurnDetection::Manual,
            pace: true,
            force_listen: false,
            audio_input: true,
            receive_timeout_ms: None,
        }
    }
}

pub(crate) fn parse_voice_command(args: &[String]) -> Result<VoiceCliCommand, CliError> {
    match args.split_first() {
        None => Ok(VoiceCliCommand::Help),
        Some((arg, [])) if matches!(arg.as_str(), "help" | "--help" | "-h") => {
            Ok(VoiceCliCommand::Help)
        }
        Some((command, rest)) if command == "call" => {
            Ok(VoiceCliCommand::Call(parse_voice_call_options(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

fn parse_voice_call_options(args: &[String]) -> Result<VoiceCallOptions, CliError> {
    let mut options = VoiceCallOptions::default();
    let mut input_specified = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--provider" => {
                index += 1;
                options.provider = required_value(args, index, "--provider")?.to_string();
            }
            "--model" => {
                index += 1;
                options.model = Some(required_value(args, index, "--model")?.to_string());
            }
            "--instructions" | "--prompt" => {
                index += 1;
                options.instructions = required_value(args, index, "--instructions")?.to_string();
            }
            "--input" => {
                index += 1;
                input_specified = true;
                let value = required_value(args, index, "--input")?;
                options.input = if value == "-" {
                    None
                } else {
                    Some(PathBuf::from(value))
                };
            }
            "--output" => {
                index += 1;
                let value = required_value(args, index, "--output")?;
                options.output = if value == "-" {
                    Some(PathBuf::from("-"))
                } else {
                    Some(PathBuf::from(value))
                };
            }
            "--video-frame" | "--image" => {
                index += 1;
                options.video_frames.push(PathBuf::from(required_value(
                    args,
                    index,
                    "--video-frame",
                )?));
            }
            "--video-format" | "--image-format" => {
                index += 1;
                options.video_format = Some(parse_video_format(required_value(
                    args,
                    index,
                    "--video-format",
                )?)?);
            }
            "--video-detail" | "--image-detail" => {
                index += 1;
                options.video_detail =
                    parse_video_detail(required_value(args, index, "--video-detail")?)?;
            }
            "--video-frame-ms" => {
                index += 1;
                options.video_frame_ms = parse_u64_option(args, index, "--video-frame-ms")?;
            }
            "--session" | "--session-id" => {
                index += 1;
                let value = required_value(args, index, "--session")?;
                options.session_id = Some(value.parse().map_err(|source| CliError::Parse {
                    detail: format!("invalid session id `{value}`: {source}"),
                })?);
            }
            "--endpoint" | "--url" => {
                index += 1;
                options.endpoint = Some(required_value(args, index, "--endpoint")?.to_string());
            }
            "--voice" => {
                index += 1;
                options.voice = Some(required_value(args, index, "--voice")?.to_string());
            }
            "--chunk-ms" => {
                index += 1;
                options.chunk_ms = parse_u64_option(args, index, "--chunk-ms")?;
            }
            "--sample-rate" | "--input-sample-rate" => {
                index += 1;
                options.input_sample_rate_hz =
                    Some(parse_u64_option(args, index, "--sample-rate")? as u32);
            }
            "--input-format" => {
                index += 1;
                options.input_format = Some(parse_audio_format(required_value(
                    args,
                    index,
                    "--input-format",
                )?)?);
            }
            "--turn" | "--turn-detection" => {
                index += 1;
                options.turn_detection =
                    parse_turn_detection(required_value(args, index, "--turn")?)?;
            }
            "--receive-timeout-ms" => {
                index += 1;
                options.receive_timeout_ms =
                    Some(parse_u64_option(args, index, "--receive-timeout-ms")?);
            }
            "--no-pacing" => {
                options.pace = false;
            }
            "--force-listen" => {
                options.force_listen = true;
            }
            "--no-audio" | "--no-audio-input" => {
                options.audio_input = false;
            }
            "--help" | "-h" => return Err(CliError::Usage),
            other => {
                return Err(CliError::Parse {
                    detail: format!("unknown voice call option `{other}`"),
                });
            }
        }
        index += 1;
    }
    if options.chunk_ms == 0 {
        return Err(CliError::Parse {
            detail: "--chunk-ms must be greater than zero".to_string(),
        });
    }
    if options.video_frame_ms == 0 {
        return Err(CliError::Parse {
            detail: "--video-frame-ms must be greater than zero".to_string(),
        });
    }
    if !options.video_frames.is_empty() && !input_specified && options.input.is_none() {
        options.audio_input = false;
    }
    if !options.audio_input && options.video_frames.is_empty() {
        return Err(CliError::Parse {
            detail: "voice call needs audio input or at least one --video-frame".to_string(),
        });
    }
    Ok(options)
}

fn required_value<'a>(args: &'a [String], index: usize, flag: &str) -> Result<&'a str, CliError> {
    args.get(index)
        .map(String::as_str)
        .filter(|value| !value.starts_with("--"))
        .ok_or_else(|| CliError::Parse {
            detail: format!("{flag} requires a value"),
        })
}

fn parse_u64_option(args: &[String], index: usize, flag: &str) -> Result<u64, CliError> {
    let value = required_value(args, index, flag)?;
    value.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid {flag} value `{value}`: {source}"),
    })
}

fn parse_audio_format(value: &str) -> Result<sinomo_lm::RealtimeAudioSampleFormat, CliError> {
    match value {
        "pcm16" | "pcm16-le" | "s16le" => Ok(sinomo_lm::RealtimeAudioSampleFormat::Pcm16Le),
        "f32" | "f32-le" | "float32" | "float32-le" => {
            Ok(sinomo_lm::RealtimeAudioSampleFormat::F32Le)
        }
        _ => Err(CliError::Parse {
            detail: format!("unsupported audio format `{value}`"),
        }),
    }
}

fn parse_turn_detection(value: &str) -> Result<sinomo_lm::RealtimeAudioTurnDetection, CliError> {
    match value {
        "manual" | "none" => Ok(sinomo_lm::RealtimeAudioTurnDetection::Manual),
        "semantic-vad" | "vad" => Ok(sinomo_lm::RealtimeAudioTurnDetection::SemanticVad),
        _ => Err(CliError::Parse {
            detail: format!("unsupported turn detection `{value}`"),
        }),
    }
}

fn parse_video_format(value: &str) -> Result<sinomo_lm::RealtimeVideoFrameFormat, CliError> {
    match value {
        "jpg" | "jpeg" | "image/jpeg" => Ok(sinomo_lm::RealtimeVideoFrameFormat::Jpeg),
        "png" | "image/png" => Ok(sinomo_lm::RealtimeVideoFrameFormat::Png),
        _ => Err(CliError::Parse {
            detail: format!("unsupported video frame format `{value}`"),
        }),
    }
}

fn parse_video_detail(value: &str) -> Result<sinomo_lm::RealtimeVideoFrameDetail, CliError> {
    match value {
        "auto" => Ok(sinomo_lm::RealtimeVideoFrameDetail::Auto),
        "low" => Ok(sinomo_lm::RealtimeVideoFrameDetail::Low),
        "high" => Ok(sinomo_lm::RealtimeVideoFrameDetail::High),
        _ => Err(CliError::Parse {
            detail: format!("unsupported video detail `{value}`"),
        }),
    }
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
        VoiceCliCommand::Help => {
            print_voice_help();
            Ok(())
        }
        VoiceCliCommand::Call(options) => run_voice_call(options),
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
    if !options.video_frames.is_empty() {
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
        });
    }
    if let Some(endpoint) = options.endpoint.as_deref() {
        if endpoint.starts_with("ws://") || endpoint.starts_with("wss://") {
            config.websocket_url = Some(endpoint.to_string());
        } else {
            config.api_base_url = Some(endpoint.to_string());
        }
    }
    config.api_key = realtime_voice_api_key(&data_dir, provider)?;

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

fn realtime_voice_api_key(
    data_dir: &SinomoDataDir,
    provider: sinomo_lm::RealtimeAudioProvider,
) -> Result<Option<String>, CliError> {
    match provider {
        sinomo_lm::RealtimeAudioProvider::OpenAi => Ok(Some(OpenAiProvider::default().api_key()?)),
        sinomo_lm::RealtimeAudioProvider::MiniCpmO => {
            if let Some((api_key, _source)) = provider_api_key_from_env(provider.provider_id()) {
                return Ok(Some(api_key));
            }
            sinomo_lm::stored_provider_api_key_in_data_dir(data_dir, provider.provider_id())
                .map_err(CliError::LanguageModel)
        }
    }
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
    let mut session = sinomo_lm::RealtimeAudioSession::connect(config.clone())
        .await
        .map_err(realtime_audio_error)?;
    let mut state = VoiceCallState::default();
    let mut output = open_voice_output(&output_path)?;
    let input_video_frames = send_voice_video_frames(
        &mut session,
        &mut state,
        &store,
        &session_id,
        &provider_id,
        &model,
        &config,
        &options,
        output.as_mut(),
        &output_path,
    )
    .await?;
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
            provider_id.clone(),
            model.clone(),
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
            .append_audio_segment(&session_id, segment, &bytes)
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
                &mut session,
                &mut state,
                &store,
                &session_id,
                &provider_id,
                &model,
                &config,
                output.as_mut(),
                &output_path,
                Duration::from_millis(options.chunk_ms),
                false,
            )
            .await?
            {
                break;
            }
        } else {
            let _ = receive_voice_events_for(
                &mut session,
                &mut state,
                &store,
                &session_id,
                &provider_id,
                &model,
                &config,
                output.as_mut(),
                &output_path,
                Duration::from_millis(1),
                false,
            )
            .await?;
        }
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

async fn send_voice_video_frames(
    session: &mut sinomo_lm::RealtimeAudioSession,
    state: &mut VoiceCallState,
    store: &sinomo_agent::FileAgentSessionStore,
    session_id: &SessionId,
    provider_id: &str,
    model: &str,
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    options: &VoiceCallOptions,
    output: &mut dyn Write,
    output_path: &Path,
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
            sinomo_lm::RealtimeVideoFrameInput::new(bytes.clone(), format, sequence);
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
            .send_video_frame(&frame_input)
            .await
            .map_err(realtime_audio_error)?;
        sequence = sequence.saturating_add(1);
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
                Duration::from_millis(options.video_frame_ms),
                false,
            )
            .await?
            {
                break;
            }
        }
    }
    Ok(sequence)
}

async fn receive_voice_events_for(
    session: &mut sinomo_lm::RealtimeAudioSession,
    state: &mut VoiceCallState,
    store: &sinomo_agent::FileAgentSessionStore,
    session_id: &SessionId,
    provider_id: &str,
    model: &str,
    config: &sinomo_lm::RealtimeAudioSessionConfig,
    output: &mut dyn Write,
    output_path: &Path,
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
        } => {
            if !text.trim().is_empty() {
                ::std::eprintln!("voice> {text}");
            }
            output.write_all(&audio).map_err(|source| CliError::Io {
                path: output_path.display().to_string(),
                detail: source.to_string(),
            })?;
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
        sinomo_lm::RealtimeAudioEvent::OutputTranscriptDelta { delta } => {
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
    fn voice_call_options_parse_minicpm_audio_defaults() {
        let command = parse_voice_command(&[
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
    fn voice_call_options_accept_video_frames_without_stdin_audio() {
        let command = parse_voice_command(&[
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
        let command = parse_voice_command(&[
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
