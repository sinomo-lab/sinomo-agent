//! End-to-end Codex realtime session over the socket transport, against the live API.
//!
//! Ignored by default because it spends a real ChatGPT subscription credential and reaches the
//! network. Run it deliberately:
//!
//! ```bash
//! SINOMO_CODEX_CREDENTIALS_FILE=/path/to/codex.json \
//!   cargo test -p sinomo-lm --test codex_realtime_socket_e2e -- --ignored --nocapture
//! ```
//!
//! The credentials file must be a ChatGPT sign-in (`auth_mode: "chatgpt"`). A token close to expiry
//! is refreshed, which rotates the refresh token, so point it at a copy rather than a live Codex CLI
//! login.
//!
//! This is the plain realtime socket, not a negotiated call: a ChatGPT sign-in token is audienced
//! for `api.openai.com`, and the realtime API accepts it there, so a Codex voice session needs no
//! WebRTC stack. What it proves is that the session document this crate builds is accepted — a
//! rejected `session.update` answers with an `error` event and leaves the session's own defaults in
//! place, so the run asserts the update was applied rather than merely sent.

use sinomo_lm::{
    OpenAiResponsesCodexClient, OpenAiResponsesCodexConfig, RealtimeAudioEvent,
    RealtimeAudioProvider, RealtimeAudioSessionConfig,
};
use std::time::Duration;

#[test]
#[ignore = "spends a real ChatGPT credential and reaches the network"]
fn codex_realtime_runs_a_socket_session_and_answers() {
    let credentials_path = std::env::var("SINOMO_CODEX_CREDENTIALS_FILE")
        .map(std::path::PathBuf::from)
        .expect("set SINOMO_CODEX_CREDENTIALS_FILE to a ChatGPT sign-in copy");
    assert!(
        credentials_path.exists(),
        "credentials file {} does not exist",
        credentials_path.display()
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let client = OpenAiResponsesCodexClient::new(OpenAiResponsesCodexConfig {
        credentials_path: Some(credentials_path),
        ..OpenAiResponsesCodexConfig::default()
    });
    let mut config = RealtimeAudioSessionConfig::new(RealtimeAudioProvider::Codex, "");
    config.instructions = "Say the single word ready, then stop.".to_string();
    client
        .authorize_realtime_session(&mut config)
        .expect("authorize the realtime session with the stored ChatGPT sign-in");
    eprintln!(
        "authorized: model={} socket={}",
        config.model,
        config.websocket_url().expect("socket url")
    );

    let mut session = runtime
        .block_on(sinomo_lm::connect_realtime_session(config))
        .expect("connect the Codex realtime socket");

    // Nothing is spoken, so the turn is driven directly; the point is whether the provider accepts
    // this crate's session and answers on it.
    runtime
        .block_on(session.request_response())
        .expect("request a response");

    let mut updated = false;
    let mut transcript = String::new();
    let mut audio_bytes = 0_usize;
    for _ in 0..80 {
        let event = match runtime
            .block_on(session.receive_event_with_timeout(Duration::from_millis(500)))
        {
            Ok(event) => event,
            Err(error) => panic!("read the realtime socket: {error}"),
        };
        match event {
            RealtimeAudioEvent::SessionUpdated => updated = true,
            RealtimeAudioEvent::OutputTranscriptDelta { delta, .. } => transcript.push_str(&delta),
            RealtimeAudioEvent::OutputAudioDelta { audio, .. } => audio_bytes += audio.len(),
            RealtimeAudioEvent::ResponseDone => break,
            RealtimeAudioEvent::Error { message, .. } => {
                panic!("the provider refused the session: {message}")
            }
            RealtimeAudioEvent::Closed { .. } => break,
            _ => {}
        }
    }
    eprintln!("transcript={transcript:?} audio_bytes={audio_bytes}");

    // A session document the API refuses is reported as an error event and silently discarded, so
    // the acknowledgement is what proves the document is the current one.
    assert!(
        updated,
        "the provider never acknowledged the session update"
    );
    assert!(
        !transcript.trim().is_empty(),
        "the provider produced no spoken output"
    );
    assert!(audio_bytes > 0, "the provider produced no output audio");
    runtime
        .block_on(session.close())
        .expect("close the realtime session");
}
