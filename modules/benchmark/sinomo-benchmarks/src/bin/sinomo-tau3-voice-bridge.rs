//! JSON-lines bridge between tau3-bench's Python tick adapter and Sinomo's
//! provider-neutral realtime audio session.

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinomo_lm::{
    RealtimeAudioError, RealtimeAudioEvent, RealtimeAudioInputChunk, RealtimeAudioProvider,
    RealtimeAudioSession, RealtimeAudioSessionConfig, RealtimeAudioToolDefinition,
    RealtimeAudioTurnDetection, provider_api_key_from_env, stored_provider_api_key,
};
use std::io::{self, BufRead, Write};
use std::time::Duration;
use tokio::time::Instant;

const PROTOCOL: &str = "sinomo.benchmark.tau3-voice-bridge.v1";

#[derive(Debug, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum BridgeCommand {
    Connect {
        provider: String,
        model: String,
        instructions: String,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        voice: Option<String>,
        #[serde(default = "default_turn_detection")]
        turn_detection: RealtimeAudioTurnDetection,
        #[serde(default)]
        tools: Vec<RealtimeAudioToolDefinition>,
    },
    Tick {
        tick: u64,
        #[serde(default)]
        audio_base64: String,
        receive_ms: u64,
    },
    ToolResult {
        call_id: String,
        output: String,
        #[serde(default = "default_true")]
        request_response: bool,
    },
    Truncate {
        item_id: String,
        #[serde(default)]
        content_index: u64,
        audio_end_ms: u64,
    },
    Cancel,
    Close,
}

#[derive(Debug, Serialize)]
struct BridgeResponse {
    protocol: &'static str,
    ok: bool,
    #[serde(flatten)]
    body: Value,
}

fn default_turn_detection() -> RealtimeAudioTurnDetection {
    RealtimeAudioTurnDetection::ServerVad
}

const fn default_true() -> bool {
    true
}

#[tokio::main]
async fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut session = None;

    for line in stdin.lock().lines() {
        let response = match line {
            Ok(line) if line.trim().is_empty() => continue,
            Ok(line) => match serde_json::from_str::<BridgeCommand>(&line) {
                Ok(command) => execute(command, &mut session).await,
                Err(error) => Err(format!("invalid bridge command: {error}")),
            },
            Err(error) => Err(format!("read bridge command: {error}")),
        };
        let close = matches!(response, Ok(ref value) if value["type"] == "closed");
        let response = match response {
            Ok(body) => BridgeResponse {
                protocol: PROTOCOL,
                ok: true,
                body,
            },
            Err(error) => BridgeResponse {
                protocol: PROTOCOL,
                ok: false,
                body: json!({"type": "error", "error": error}),
            },
        };
        if serde_json::to_writer(&mut stdout, &response).is_err()
            || writeln!(&mut stdout).is_err()
            || stdout.flush().is_err()
        {
            break;
        }
        if close {
            break;
        }
    }
}

async fn execute(
    command: BridgeCommand,
    session: &mut Option<RealtimeAudioSession>,
) -> Result<Value, String> {
    match command {
        BridgeCommand::Connect {
            provider,
            model,
            instructions,
            endpoint,
            voice,
            turn_detection,
            tools,
        } => {
            if session.is_some() {
                return Err("bridge session is already connected".to_string());
            }
            let provider_kind = RealtimeAudioProvider::from_id(&provider)
                .ok_or_else(|| format!("unsupported realtime audio provider `{provider}`"))?;
            let provider_id = provider_kind.provider_id();
            let mut config = RealtimeAudioSessionConfig::new(provider_kind, model);
            config.instructions = instructions;
            config.api_base_url = endpoint;
            config.voice = voice;
            config.turn_detection = turn_detection;
            config.tools = tools;
            config.api_key = provider_api_key_from_env(provider_id)
                .map(|(key, _)| key)
                .or_else(|| stored_provider_api_key(provider_id).ok().flatten());
            let connected = RealtimeAudioSession::connect(config)
                .await
                .map_err(|error| error.to_string())?;
            let body = json!({
                "type": "connected",
                "provider": connected.config().provider.provider_id(),
                "model": connected.config().model,
                "input_sample_rate_hz": connected.config().input_sample_rate_hz,
                "output_sample_rate_hz": connected.config().output_sample_rate_hz
            });
            *session = Some(connected);
            Ok(body)
        }
        BridgeCommand::Tick {
            tick,
            audio_base64,
            receive_ms,
        } => {
            let session = connected_session(session)?;
            if !audio_base64.is_empty() {
                let audio = base64::engine::general_purpose::STANDARD
                    .decode(audio_base64)
                    .map_err(|error| format!("decode input audio: {error}"))?;
                session
                    .send_audio(&RealtimeAudioInputChunk::new(audio))
                    .await
                    .map_err(|error| error.to_string())?;
            }
            let deadline = Instant::now() + Duration::from_millis(receive_ms);
            let mut events = Vec::new();
            while Instant::now() < deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let poll = remaining.min(Duration::from_millis(20));
                match session.receive_event_with_timeout(poll).await {
                    Ok(event) => events.push(bridge_event(event)),
                    Err(RealtimeAudioError::Timeout { .. }) => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
            Ok(json!({"type": "tick", "tick": tick, "events": events}))
        }
        BridgeCommand::ToolResult {
            call_id,
            output,
            request_response,
        } => {
            connected_session(session)?
                .send_tool_result(&call_id, &output, request_response)
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({"type": "ack", "command": "tool_result"}))
        }
        BridgeCommand::Truncate {
            item_id,
            content_index,
            audio_end_ms,
        } => {
            connected_session(session)?
                .truncate_output(&item_id, content_index, audio_end_ms)
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({"type": "ack", "command": "truncate"}))
        }
        BridgeCommand::Cancel => {
            connected_session(session)?
                .cancel_response()
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({"type": "ack", "command": "cancel"}))
        }
        BridgeCommand::Close => {
            if let Some(mut connected) = session.take() {
                connected.close().await.map_err(|error| error.to_string())?;
            }
            Ok(json!({"type": "closed"}))
        }
    }
}

fn connected_session(
    session: &mut Option<RealtimeAudioSession>,
) -> Result<&mut RealtimeAudioSession, String> {
    session
        .as_mut()
        .ok_or_else(|| "bridge session is not connected".to_string())
}

fn bridge_event(event: RealtimeAudioEvent) -> Value {
    match event {
        RealtimeAudioEvent::OutputAudioDelta {
            text,
            audio,
            end_of_turn,
            item_id,
            kv_cache_length,
        } => json!({
            "type": "output_audio_delta",
            "text": text,
            "audio_base64": base64::engine::general_purpose::STANDARD.encode(audio),
            "end_of_turn": end_of_turn,
            "item_id": item_id,
            "kv_cache_length": kv_cache_length
        }),
        RealtimeAudioEvent::OutputTranscriptDelta { delta, item_id } => {
            json!({"type": "output_transcript_delta", "delta": delta, "item_id": item_id})
        }
        RealtimeAudioEvent::InputSpeechStarted {
            audio_start_ms,
            item_id,
        } => json!({
            "type": "input_speech_started",
            "audio_start_ms": audio_start_ms,
            "item_id": item_id
        }),
        RealtimeAudioEvent::InputSpeechStopped {
            audio_end_ms,
            item_id,
        } => json!({
            "type": "input_speech_stopped",
            "audio_end_ms": audio_end_ms,
            "item_id": item_id
        }),
        RealtimeAudioEvent::ToolCallStarted {
            item_id,
            call_id,
            name,
        } => json!({
            "type": "tool_call_started",
            "item_id": item_id,
            "call_id": call_id,
            "name": name
        }),
        RealtimeAudioEvent::ToolCallArgumentsDelta {
            item_id,
            call_id,
            name,
            delta,
        } => json!({
            "type": "tool_call_arguments_delta",
            "item_id": item_id,
            "call_id": call_id,
            "name": name,
            "delta": delta
        }),
        RealtimeAudioEvent::ToolCallCompleted {
            item_id,
            call_id,
            name,
            arguments,
        } => json!({
            "type": "tool_call_completed",
            "item_id": item_id,
            "call_id": call_id,
            "name": name,
            "arguments": arguments
        }),
        RealtimeAudioEvent::ResponseCancelled { response_id } => {
            json!({"type": "response_cancelled", "response_id": response_id})
        }
        RealtimeAudioEvent::OutputTruncated {
            item_id,
            audio_end_ms,
        } => json!({
            "type": "output_truncated",
            "item_id": item_id,
            "audio_end_ms": audio_end_ms
        }),
        RealtimeAudioEvent::SessionQueued {
            position,
            estimated_wait_s,
        } => json!({
            "type": "session_queued",
            "position": position,
            "estimated_wait_s": estimated_wait_s
        }),
        RealtimeAudioEvent::SessionCreated {
            provider_session_id,
            prompt_length,
        } => json!({
            "type": "session_created",
            "provider_session_id": provider_session_id,
            "prompt_length": prompt_length
        }),
        RealtimeAudioEvent::SessionUpdated => json!({"type": "session_updated"}),
        RealtimeAudioEvent::Listening { kv_cache_length } => {
            json!({"type": "listening", "kv_cache_length": kv_cache_length})
        }
        RealtimeAudioEvent::ResponseDone => json!({"type": "response_done"}),
        RealtimeAudioEvent::Closed { reason } => {
            json!({"type": "closed", "reason": reason})
        }
        RealtimeAudioEvent::Error { code, message } => {
            json!({"type": "provider_error", "code": code, "message": message})
        }
        RealtimeAudioEvent::Raw { event_type, value } => {
            json!({"type": "raw", "event_type": event_type, "value": value})
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_audio_event_uses_base64_and_preserves_item_identity() {
        assert_eq!(
            bridge_event(RealtimeAudioEvent::OutputAudioDelta {
                text: "hello".to_string(),
                audio: vec![0, 1, 2, 3],
                end_of_turn: false,
                item_id: Some("item_123".to_string()),
                kv_cache_length: None,
            }),
            json!({
                "type": "output_audio_delta",
                "text": "hello",
                "audio_base64": "AAECAw==",
                "end_of_turn": false,
                "item_id": "item_123",
                "kv_cache_length": null
            })
        );
    }

    #[test]
    fn bridge_rejects_commands_before_connect() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let error = runtime
            .block_on(execute(
                BridgeCommand::Cancel,
                &mut Option::<RealtimeAudioSession>::None,
            ))
            .expect_err("cancel without session");
        assert!(error.contains("not connected"));
    }
}
