//! Realtime voice command grammar. Device and terminal execution stays with the caller.
use sinomo_runtime::{cli_registry::*, events::SessionId};
use std::path::PathBuf;
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum VoiceCliCommand {
    Devices,
    Call(VoiceCallOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VoiceCallOptions {
    pub provider: String,
    pub model: Option<String>,
    pub instructions: String,
    pub input: Option<PathBuf>,
    pub output: Option<PathBuf>,
    pub video_frames: Vec<PathBuf>,
    pub video_format: Option<crate::RealtimeVideoFrameFormat>,
    pub video_detail: crate::RealtimeVideoFrameDetail,
    pub video_frame_ms: u64,
    pub session_id: Option<SessionId>,
    pub endpoint: Option<String>,
    /// Path to a WebRTC SDP offer, or `-` for stdin, selecting the WebRTC transport.
    pub webrtc_offer: Option<PathBuf>,
    pub webrtc_offer_stdin: bool,
    pub voice: Option<String>,
    pub chunk_ms: u64,
    pub input_sample_rate_hz: Option<u32>,
    pub input_format: Option<crate::RealtimeAudioSampleFormat>,
    pub turn_detection: crate::RealtimeAudioTurnDetection,
    pub pace: bool,
    pub force_listen: bool,
    pub audio_input: bool,
    pub native_microphone: bool,
    pub microphone_device: Option<String>,
    /// Node the microphone is attached to; `None` means the node running the call.
    pub microphone_node: Option<String>,
    pub native_camera: bool,
    pub camera_device: Option<String>,
    pub desktop_session: Option<String>,
    pub desktop_node: Option<String>,
    pub desktop_app: bool,
    pub native_speaker: bool,
    pub speaker_device: Option<String>,
    /// Node the speaker is attached to; `None` means the node running the call.
    pub speaker_node: Option<String>,
    pub call_ms: Option<u64>,
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
            video_detail: crate::RealtimeVideoFrameDetail::Auto,
            video_frame_ms: 500,
            session_id: None,
            endpoint: None,
            webrtc_offer: None,
            webrtc_offer_stdin: false,
            voice: None,
            chunk_ms: 100,
            input_sample_rate_hz: None,
            input_format: None,
            turn_detection: crate::RealtimeAudioTurnDetection::Manual,
            pace: true,
            force_listen: false,
            audio_input: true,
            native_microphone: false,
            microphone_device: None,
            microphone_node: None,
            native_camera: false,
            camera_device: None,
            desktop_session: None,
            desktop_node: None,
            desktop_app: false,
            native_speaker: false,
            speaker_device: None,
            speaker_node: None,
            call_ms: None,
            receive_timeout_ms: None,
        }
    }
}

pub fn voice_cli_command_handler<E: Into<CliCommandError> + 'static>(
    run: fn(VoiceCliCommand) -> Result<(), E>,
) -> DeclarativeCommandHandler<VoiceCliCommand, E> {
    DeclarativeCommandHandler::new(
        "voice",
        CliCommandPolicy::local_capable(),
        voice_cli_spec,
        parse_voice_matches,
        run,
    )
}

fn voice_cli_spec() -> CliCommandSpec {
    let free = |name: &'static str, help: &'static str, placeholder: &'static str| {
        CliOptionSpec::value(name, help, CliValueSpec::new(placeholder, help))
    };
    let choices = |name: &'static str,
                   help: &'static str,
                   placeholder: &'static str,
                   values: &'static [&'static str]| {
        CliOptionSpec::value(
            name,
            help,
            CliValueSpec::new(placeholder, help)
                .with_choices(
                    values
                        .iter()
                        .map(|value| CliAutocompleteChoice::new(*value, help)),
                )
                .restricted(),
        )
    };
    CliCommandSpec::new("voice", "use realtime voice and native media devices")
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("devices", "list native microphones, cameras, and speakers"),
            CliCommandSpec::new("call", "run a realtime voice session").with_options([
                free("--provider", "realtime model provider", "provider"),
                free("--model", "realtime model id", "model"),
                free("--instructions", "session instructions", "text").with_aliases(["--prompt"]),
                free("--input", "raw PCM input path or -", "path|-"),
                free("--output", "raw PCM output path or -", "path|-"),
                free("--video-frame", "JPEG or PNG video frame", "path")
                    .with_aliases(["--image"])
                    .repeatable(),
                choices(
                    "--video-format",
                    "video frame format",
                    "format",
                    &["jpg", "jpeg", "image/jpeg", "png", "image/png"],
                )
                .with_aliases(["--image-format"]),
                choices(
                    "--video-detail",
                    "video frame detail",
                    "detail",
                    &["auto", "low", "high"],
                )
                .with_aliases(["--image-detail"]),
                free("--video-frame-ms", "video frame interval", "milliseconds"),
                free("--session", "session id", "session-id").with_aliases(["--session-id"]),
                free("--endpoint", "realtime endpoint URL", "url").with_aliases(["--url"]),
                free(
                    "--webrtc-offer",
                    "WebRTC SDP offer path or - for stdin",
                    "path|-",
                ),
                free("--voice", "voice id or reference audio", "voice"),
                free("--chunk-ms", "audio chunk duration", "milliseconds"),
                free("--sample-rate", "input sample rate", "hz")
                    .with_aliases(["--input-sample-rate"]),
                choices(
                    "--input-format",
                    "input audio sample format",
                    "format",
                    &[
                        "pcm16",
                        "pcm16-le",
                        "s16le",
                        "f32",
                        "f32-le",
                        "float32",
                        "float32-le",
                    ],
                ),
                choices(
                    "--turn",
                    "turn detection mode",
                    "mode",
                    &["manual", "none", "server-vad", "semantic-vad", "vad"],
                )
                .with_aliases(["--turn-detection"]),
                free("--receive-timeout-ms", "receive timeout", "milliseconds"),
                CliOptionSpec::flag("--native", "use native microphone and speaker"),
                CliOptionSpec::flag("--native-mic", "use a native microphone")
                    .with_aliases(["--microphone"]),
                free("--mic-device", "microphone selector", "selector")
                    .with_aliases(["--microphone-device"]),
                free(
                    "--mic-node",
                    "node the microphone is attached to",
                    "node-id",
                )
                .with_aliases(["--microphone-node"]),
                CliOptionSpec::flag("--native-camera", "use a native camera")
                    .with_aliases(["--camera"]),
                free("--camera-device", "camera selector", "selector"),
                free(
                    "--computer-stream",
                    "Computer Use capture session",
                    "session-id",
                ),
                free("--app-stream", "App Use capture session", "session-id"),
                free(
                    "--video-node",
                    "node hosting the desktop capture session",
                    "node-id",
                ),
                CliOptionSpec::flag("--native-speaker", "use a native speaker")
                    .with_aliases(["--speaker"]),
                free("--speaker-device", "speaker selector", "selector"),
                free(
                    "--speaker-node",
                    "node the speaker is attached to",
                    "node-id",
                ),
                free("--call-ms", "maximum call duration", "milliseconds")
                    .with_aliases(["--duration-ms"]),
                CliOptionSpec::flag("--no-pacing", "disable realtime input pacing"),
                CliOptionSpec::flag("--force-listen", "force response listening"),
                CliOptionSpec::flag("--no-audio", "disable audio input")
                    .with_aliases(["--no-audio-input"]),
            ]),
        ])
}

fn parse_voice_matches(matches: &CliCommandMatches) -> Result<VoiceCliCommand, CliParseError> {
    match matches.command_name() {
        "devices" => Ok(VoiceCliCommand::Devices),
        "call" => parse_voice_call_matches(matches).map(VoiceCliCommand::Call),
        _ => Err(CliParseError::Parse {
            detail: "voice requires a subcommand".to_string(),
        }),
    }
}

fn parse_voice_call_matches(
    matches: &CliCommandMatches,
) -> Result<VoiceCallOptions, CliParseError> {
    let mut options = VoiceCallOptions::default();
    let input_specified = matches.contains("--input");
    if let Some(value) = matches.value("--provider") {
        options.provider = value.to_string();
    }
    options.model = matches.value("--model").map(str::to_string);
    if let Some(value) = matches.value("--instructions") {
        options.instructions = value.to_string();
    }
    options.input = matches
        .value("--input")
        .and_then(|value| (value != "-").then(|| PathBuf::from(value)));
    options.output = matches.value("--output").map(PathBuf::from);
    options.video_frames = matches.values("--video-frame").map(PathBuf::from).collect();
    options.video_format = matches
        .value("--video-format")
        .map(parse_video_format)
        .transpose()
        .map_err(semantic_parse_error)?;
    if let Some(value) = matches.value("--video-detail") {
        options.video_detail = parse_video_detail(value).map_err(semantic_parse_error)?;
    }
    if let Some(value) = matches.value("--video-frame-ms") {
        options.video_frame_ms = parse_u64_value("--video-frame-ms", value)?;
    }
    options.session_id = matches
        .value("--session")
        .map(|value| {
            value.parse().map_err(|source| CliParseError::Parse {
                detail: format!("invalid session id `{value}`: {source}"),
            })
        })
        .transpose()?;
    options.endpoint = matches.value("--endpoint").map(str::to_string);
    options.webrtc_offer_stdin = matches.value("--webrtc-offer") == Some("-");
    options.webrtc_offer = matches
        .value("--webrtc-offer")
        .and_then(|value| (value != "-").then(|| PathBuf::from(value)));
    options.voice = matches.value("--voice").map(str::to_string);
    if let Some(value) = matches.value("--chunk-ms") {
        options.chunk_ms = parse_u64_value("--chunk-ms", value)?;
    }
    options.input_sample_rate_hz = matches
        .value("--sample-rate")
        .map(|value| parse_u64_value("--sample-rate", value).map(|value| value as u32))
        .transpose()?;
    options.input_format = matches
        .value("--input-format")
        .map(parse_audio_format)
        .transpose()
        .map_err(semantic_parse_error)?;
    if let Some(value) = matches.value("--turn") {
        options.turn_detection = parse_turn_detection(value).map_err(semantic_parse_error)?;
    }
    options.receive_timeout_ms = matches
        .value("--receive-timeout-ms")
        .map(|value| parse_u64_value("--receive-timeout-ms", value))
        .transpose()?;
    options.native_microphone = matches.contains("--native")
        || matches.contains("--native-mic")
        || matches.contains("--mic-device");
    options.microphone_device = matches.value("--mic-device").map(str::to_string);
    options.microphone_node = matches.value("--mic-node").map(str::to_string);
    options.native_camera =
        matches.contains("--native-camera") || matches.contains("--camera-device");
    options.camera_device = matches.value("--camera-device").map(str::to_string);
    if matches.contains("--computer-stream") && matches.contains("--app-stream") {
        return Err(CliParseError::Parse {
            detail: "choose --computer-stream or --app-stream".into(),
        });
    }
    options.desktop_session = matches
        .value("--computer-stream")
        .or_else(|| matches.value("--app-stream"))
        .map(str::to_string);
    options.desktop_app = matches.contains("--app-stream");
    options.desktop_node = matches.value("--video-node").map(str::to_string);
    if options.desktop_session.is_some() && options.native_camera {
        return Err(CliParseError::Parse {
            detail: "desktop streams and native camera are mutually exclusive".into(),
        });
    }
    if options.desktop_node.is_some() && options.desktop_session.is_none() {
        return Err(CliParseError::Parse {
            detail: "--video-node requires --computer-stream or --app-stream".into(),
        });
    }
    options.native_speaker = matches.contains("--native")
        || matches.contains("--native-speaker")
        || matches.contains("--speaker-device");
    options.speaker_device = matches.value("--speaker-device").map(str::to_string);
    options.speaker_node = matches.value("--speaker-node").map(str::to_string);
    options.call_ms = matches
        .value("--call-ms")
        .map(|value| parse_u64_value("--call-ms", value))
        .transpose()?;
    options.pace = !matches.contains("--no-pacing");
    options.force_listen = matches.contains("--force-listen");
    options.audio_input = !matches.contains("--no-audio");
    if options.chunk_ms == 0 {
        return Err(CliParseError::Parse {
            detail: "--chunk-ms must be greater than zero".to_string(),
        });
    }
    if options.video_frame_ms == 0 {
        return Err(CliParseError::Parse {
            detail: "--video-frame-ms must be greater than zero".to_string(),
        });
    }
    if options.call_ms == Some(0) {
        return Err(CliParseError::Parse {
            detail: "--call-ms must be greater than zero".to_string(),
        });
    }
    if options.native_microphone && input_specified {
        return Err(CliParseError::Parse {
            detail: "--native-mic cannot be combined with --input".to_string(),
        });
    }
    if !options.video_frames.is_empty() && !input_specified && options.input.is_none() {
        options.audio_input = false;
    }
    if !options.audio_input
        && options.video_frames.is_empty()
        && !options.native_camera
        && options.desktop_session.is_none()
    {
        return Err(CliParseError::Parse {
            detail: "voice call needs audio input, native camera, or at least one --video-frame"
                .to_string(),
        });
    }
    Ok(options)
}

fn parse_u64_value(flag: &str, value: &str) -> Result<u64, CliParseError> {
    value.parse::<u64>().map_err(|source| CliParseError::Parse {
        detail: format!("invalid {flag} value `{value}`: {source}"),
    })
}

fn parse_audio_format(value: &str) -> Result<crate::RealtimeAudioSampleFormat, CliParseError> {
    match value {
        "pcm16" | "pcm16-le" | "s16le" => Ok(crate::RealtimeAudioSampleFormat::Pcm16Le),
        "f32" | "f32-le" | "float32" | "float32-le" => Ok(crate::RealtimeAudioSampleFormat::F32Le),
        _ => Err(CliParseError::Parse {
            detail: format!("unsupported audio format `{value}`"),
        }),
    }
}

fn parse_turn_detection(value: &str) -> Result<crate::RealtimeAudioTurnDetection, CliParseError> {
    match value {
        "manual" | "none" => Ok(crate::RealtimeAudioTurnDetection::Manual),
        "server-vad" => Ok(crate::RealtimeAudioTurnDetection::ServerVad),
        "semantic-vad" | "vad" => Ok(crate::RealtimeAudioTurnDetection::SemanticVad),
        _ => Err(CliParseError::Parse {
            detail: format!("unsupported turn detection `{value}`"),
        }),
    }
}

fn parse_video_format(value: &str) -> Result<crate::RealtimeVideoFrameFormat, CliParseError> {
    match value {
        "jpg" | "jpeg" | "image/jpeg" => Ok(crate::RealtimeVideoFrameFormat::Jpeg),
        "png" | "image/png" => Ok(crate::RealtimeVideoFrameFormat::Png),
        _ => Err(CliParseError::Parse {
            detail: format!("unsupported video frame format `{value}`"),
        }),
    }
}

fn parse_video_detail(value: &str) -> Result<crate::RealtimeVideoFrameDetail, CliParseError> {
    match value {
        "auto" => Ok(crate::RealtimeVideoFrameDetail::Auto),
        "low" => Ok(crate::RealtimeVideoFrameDetail::Low),
        "high" => Ok(crate::RealtimeVideoFrameDetail::High),
        _ => Err(CliParseError::Parse {
            detail: format!("unsupported video detail `{value}`"),
        }),
    }
}
/// Parse this domain's arguments without executing a command.
pub fn parse_voice_command(args: &[String]) -> Result<VoiceCliCommand, CliParseError> {
    parse_command_args(voice_cli_spec(), args, parse_voice_matches)
}

#[cfg(test)]
mod desktop_tests {
    use super::*;
    fn parse(args: &[&str]) -> Result<VoiceCliCommand, CliParseError> {
        parse_voice_command(&args.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }
    #[test]
    fn desktop_stream_allows_video_only_calls_and_rejects_ambiguous_sources() {
        let VoiceCliCommand::Call(options) =
            parse(&["call", "--app-stream", "session", "--no-audio"]).unwrap()
        else {
            panic!("call");
        };
        assert_eq!(options.desktop_session.as_deref(), Some("session"));
        assert!(options.desktop_app);
        assert!(parse(&["call", "--app-stream", "a", "--computer-stream", "b"]).is_err());
        assert!(parse(&["call", "--app-stream", "a", "--native-camera"]).is_err());
        assert!(parse(&["call", "--video-node", "node"]).is_err());
    }
}
