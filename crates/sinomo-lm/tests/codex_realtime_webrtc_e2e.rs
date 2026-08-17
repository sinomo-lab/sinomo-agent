//! End-to-end negotiation of a Codex realtime WebRTC call against the live backend.
//!
//! Ignored by default because it spends a real ChatGPT subscription credential and reaches the
//! network. Run it deliberately:
//!
//! ```bash
//! SINOMO_CODEX_CREDENTIALS_FILE=/path/to/codex.json \
//!   cargo test -p sinomo-lm --features webrtc-media --test codex_realtime_webrtc_e2e -- --ignored --nocapture
//! ```
//!
//! The credentials file must be a ChatGPT sign-in (`auth_mode: "chatgpt"`). A token close to expiry
//! is refreshed, which rotates the refresh token, so point it at a copy rather than a live Codex CLI
//! login.
//!
//! The call goes to the OpenAI realtime API, not the Codex backend: a ChatGPT sign-in token is
//! audienced for `api.openai.com` and the realtime API accepts it there, so this exercises the same
//! endpoint an API key would reach and is expected to negotiate a real call rather than skip.

#![cfg(feature = "webrtc-media")]

use sinomo_lm::{
    OpenAiResponsesCodexClient, OpenAiResponsesCodexConfig, RealtimeAudioProvider,
    RealtimeAudioSessionConfig, RealtimeMediaTransport as _,
};
use std::time::Duration;

#[test]
#[ignore = "spends a real ChatGPT credential and reaches the network"]
fn codex_realtime_negotiates_a_webrtc_call_and_carries_audio() {
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
    client
        .authorize_realtime_session(&mut config)
        .expect("authorize the realtime session with the stored ChatGPT sign-in");
    eprintln!(
        "authorized: model={} call={}",
        config.model,
        config.realtime_call_url()
    );

    let (mut media, call) = runtime
        .block_on(sinomo_lm::realtime_webrtc::connect_webrtc_media(&config))
        .expect("negotiate a realtime WebRTC call");
    eprintln!(
        "negotiated call_id={} answer_sdp_bytes={}",
        call.call_id,
        call.answer_sdp.len()
    );
    assert!(!call.call_id.is_empty(), "the call id identifies the media");
    assert!(
        call.answer_sdp.to_lowercase().contains("opus"),
        "the answer should negotiate opus:\n{}",
        call.answer_sdp
    );

    // Push a short burst so the outbound path runs end to end, then look for anything coming back.
    // A provider that has not started speaking is not a failure.
    let silence = vec![0_u8; 480 * 2];
    for _ in 0..25 {
        runtime
            .block_on(media.send_audio(&silence))
            .expect("send capture audio over the peer connection");
    }
    let mut received = 0_usize;
    for _ in 0..40 {
        if let Some(frame) = runtime
            .block_on(media.recv_audio(Duration::from_millis(100)))
            .expect("read the peer connection")
        {
            received += frame.len();
        }
    }
    eprintln!("received {received} bytes of decoded audio from the provider");
    runtime
        .block_on(media.close())
        .expect("close the media path");
}
