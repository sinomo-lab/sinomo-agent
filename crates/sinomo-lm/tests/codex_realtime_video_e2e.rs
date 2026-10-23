//! Small opt-in subscription-authenticated Realtime visual probe.
//! Run with SINOMO_CODEX_CREDENTIALS_FILE and a fresh
//! SINOMO_REALTIME_VIDEO_ARTIFACTS directory, then `cargo test -p sinomo-lm
//! --test codex_realtime_video_e2e -- --ignored --nocapture`.
//! It uses gpt-realtime-2, sends four synthetic frames and requests two answers.
//! No microphone, desktop capture, cluster, credential copy or refresh is used.
use base64::Engine;
use image::{Rgb, RgbImage};
use ring::{
    digest,
    rand::{SecureRandom, SystemRandom},
};
use serde_json::{Value, json};
use sinomo_lm::*;
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MODEL: &str = "gpt-realtime-2";
const INSTRUCTIONS: &str = "This is a visual input check. When a response is requested, report every image received since your previous answer, in order. For each image say only its exact four-digit code, shape color, shape name, and horizontal position (left, center, or right). Use digits in the transcript for the code. Do not invent images. Keep the answer brief. The codes and answers are supplied only in the image pixels.";

#[derive(Default)]
struct Evidence {
    session_updated: bool,
    session_model: Option<String>,
    completed: Vec<Value>,
    sent: Vec<Value>,
    responses: Vec<Value>,
}

fn save(path: &Path, value: &impl serde::Serialize) {
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}
fn sha256(bytes: &[u8]) -> String {
    digest::digest(&digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn redact(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            let audio =
                fields.get("type").and_then(Value::as_str) == Some("response.output_audio.delta");
            for (key, value) in fields {
                if matches!(
                    key.as_str(),
                    "access_token"
                        | "refresh_token"
                        | "api_key"
                        | "authorization"
                        | "client_secret"
                ) || (audio && key == "delta")
                {
                    *value = json!({"redacted":true,"encoded_bytes":value.as_str().map(str::len)});
                } else {
                    redact(value);
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(redact),
        Value::String(text) if text.starts_with("data:") => {
            *value = json!({"redacted_media":true,"encoded_bytes":text.len()})
        }
        _ => {}
    }
}

fn fixture(code: &str, color: [u8; 3], shape: &str, position: &str) -> RealtimeVideoFrameInput {
    let mut image = RgbImage::from_pixel(800, 480, Rgb([18, 25, 35]));
    let segments = [0x3f, 0x06, 0x5b, 0x4f, 0x66, 0x6d, 0x7d, 0x07, 0x7f, 0x6f];
    for (i, digit) in code.bytes().enumerate() {
        let x = 235 + i as u32 * 85;
        for (bit, (dx, dy, w, h)) in [
            (12, 0, 48, 10),
            (60, 10, 10, 48),
            (60, 70, 10, 48),
            (12, 118, 48, 10),
            (2, 70, 10, 48),
            (2, 10, 10, 48),
            (12, 59, 48, 10),
        ]
        .into_iter()
        .enumerate()
        {
            if segments[usize::from(digit - b'0')] & (1 << bit) != 0 {
                for py in 40 + dy..40 + dy + h {
                    for px in x + dx..x + dx + w {
                        image.put_pixel(px, py, Rgb([245, 245, 245]));
                    }
                }
            }
        }
    }
    let cx = match position {
        "left" => 145,
        "right" => 655,
        _ => 400,
    };
    for y in 230i32..410 {
        for x in 40i32..760 {
            let dx = x - cx;
            let dy = y - 320;
            let inside = match shape {
                "circle" => dx * dx + dy * dy <= 75 * 75,
                "square" => dx.abs() <= 72 && dy.abs() <= 72,
                _ => (-80..=80).contains(&dy) && dx.abs() <= (dy + 80) / 2,
            };
            if inside {
                image.put_pixel(x as u32, y as u32, Rgb(color));
            }
        }
    }
    RealtimeVideoFrameInput::new(
        image.into_raw().into(),
        RealtimeVideoPixelFormat::Rgb8,
        800,
        480,
        800 * 3,
        0,
    )
}
fn save_wav(path: &Path, audio: &[u8]) {
    let mut file = File::create(path).unwrap();
    file.write_all(b"RIFF").unwrap();
    file.write_all(&(36 + audio.len() as u32).to_le_bytes())
        .unwrap();
    file.write_all(b"WAVEfmt ").unwrap();
    file.write_all(&16u32.to_le_bytes()).unwrap();
    file.write_all(&1u16.to_le_bytes()).unwrap();
    file.write_all(&1u16.to_le_bytes()).unwrap();
    file.write_all(&24000u32.to_le_bytes()).unwrap();
    file.write_all(&48000u32.to_le_bytes()).unwrap();
    file.write_all(&2u16.to_le_bytes()).unwrap();
    file.write_all(&16u16.to_le_bytes()).unwrap();
    file.write_all(b"data").unwrap();
    file.write_all(&(audio.len() as u32).to_le_bytes()).unwrap();
    file.write_all(audio).unwrap();
}

async fn answer(
    session: &mut RealtimeAudioSession,
    evidence: &Arc<Mutex<Evidence>>,
    directory: &Path,
    started: Instant,
    index: usize,
) -> Result<(), String> {
    let previous = evidence.lock().unwrap().completed.len();
    session
        .request_response()
        .await
        .map_err(|e| e.to_string())?;
    let mut transcript = String::new();
    let mut audio = Vec::new();
    let mut audio_at = None;
    let deadline = Instant::now() + Duration::from_secs(30);
    let result = loop {
        if Instant::now() >= deadline {
            break Err("model response deadline exceeded".into());
        }
        match session
            .receive_event_with_timeout(Duration::from_millis(500))
            .await
        {
            Ok(RealtimeAudioEvent::OutputTranscriptDelta { delta, .. }) => {
                transcript.push_str(&delta)
            }
            Ok(RealtimeAudioEvent::OutputAudioDelta { audio: chunk, .. }) => {
                audio_at.get_or_insert(started.elapsed().as_millis());
                audio.extend(chunk);
            }
            Ok(RealtimeAudioEvent::Error { code, message }) => {
                break Err(format!("provider error {code}: {message}"));
            }
            Ok(RealtimeAudioEvent::Closed { reason }) => {
                break Err(format!("provider closed: {reason}"));
            }
            Ok(_) => {}
            Err(RealtimeAudioError::Timeout { .. }) => continue,
            Err(error) => break Err(error.to_string()),
        }
        if evidence.lock().unwrap().completed.len() > previous {
            let completed = evidence.lock().unwrap().completed.last().unwrap().clone();
            break if completed["status"] == "completed" {
                Ok(())
            } else {
                Err(format!(
                    "response was not completed: {}",
                    completed["status"]
                ))
            };
        }
    };
    save_wav(&directory.join(format!("response-{index}.wav")), &audio);
    evidence.lock().unwrap().responses.push(json!({"index":index,"transcript":transcript,"audio_bytes":audio.len(),"first_audio_ms":audio_at,"finished_ms":started.elapsed().as_millis()}));
    result
}

#[test]
#[ignore = "uses stored Codex auth for four image frames and two short live Realtime responses"]
fn codex_realtime_sees_video_sequence_and_later_updates() {
    let credentials = std::path::PathBuf::from(
        std::env::var("SINOMO_CODEX_CREDENTIALS_FILE").expect("set credential path"),
    );
    let directory = std::path::PathBuf::from(
        std::env::var("SINOMO_REALTIME_VIDEO_ARTIFACTS").expect("set fresh artifact directory"),
    );
    fs::create_dir(&directory).expect("artifact directory must be new");
    let original_credentials = fs::read(&credentials).unwrap();
    let client = OpenAiResponsesCodexClient::new(OpenAiResponsesCodexConfig {
        credentials_path: Some(credentials.clone()),
        default_model: MODEL.into(),
        allow_credential_refresh: false,
        transport_health_timeout: Duration::from_secs(10),
        ..Default::default()
    });
    let tokens = client
        .chatgpt_tokens()
        .expect("AUTH_STOP: stored Codex credential unavailable");
    let claims: Value = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(
                tokens
                    .access_token
                    .split('.')
                    .nth(1)
                    .expect("JWT")
                    .trim_end_matches('='),
            )
            .expect("JWT claims"),
    )
    .unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        claims["exp"].as_u64().unwrap_or(0) > now + 90,
        "AUTH_STOP: credential expired or too near expiry; no refresh attempted"
    );
    drop(tokens);
    let mut config = RealtimeAudioSessionConfig::new(RealtimeAudioProvider::Codex, MODEL)
        .with_video_input(RealtimeVideoInputConfig {
            encoding_quality: 92,
            max_dimension: Some(800),
            max_frame_rate_hz: Some(2),
            ..Default::default()
        });
    config.instructions = INSTRUCTIONS.into();
    config.connect_timeout_ms = Some(10_000);
    config.receive_timeout_ms = Some(1_000);
    client
        .authorize_realtime_session(&mut config)
        .expect("AUTH_STOP: cannot authorize without credential refresh");
    save(
        &directory.join("request.json"),
        &json!({"provider":"codex","model":MODEL,"url":config.websocket_url().unwrap(),"instructions":INSTRUCTIONS,"credential_expiry_unix":claims["exp"],"refresh_allowed":false,"frame_count":4,"response_limit":2,"desktop_or_microphone_capture":false}),
    );
    let usage_before = client.usage().map_err(|e| e.to_string());
    save(&directory.join("usage-before.json"), &usage_before);
    let rng = SystemRandom::new();
    let mut codes = Vec::new();
    while codes.len() < 4 {
        let mut bytes = [0u8; 2];
        rng.fill(&mut bytes).unwrap();
        let code = format!("{}", 1000 + u16::from_le_bytes(bytes) % 9000);
        if !codes.contains(&code) {
            codes.push(code);
        }
    }
    let specs = [
        ("red", [235, 35, 45], "circle", "left"),
        ("blue", [35, 100, 245], "square", "right"),
        ("green", [35, 205, 90], "triangle", "center"),
        ("yellow", [245, 215, 30], "circle", "right"),
    ];
    let expected:Vec<_>=specs.iter().enumerate().map(|(i,(color,_,shape,position))|json!({"code":codes[i],"color":color,"shape":shape,"position":position})).collect();
    save(&directory.join("expected.json"), &expected);
    let evidence = Arc::new(Mutex::new(Evidence::default()));
    let observed = evidence.clone();
    let started = Instant::now();
    let mut events = File::create(directory.join("server-events.jsonl")).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result=runtime.block_on(async { tokio::time::timeout(Duration::from_secs(75),async {
        let mut session=RealtimeAudioSession::connect(config).await.map_err(|e|e.to_string())?.with_server_event_observer(move |event|{
            let mut state=observed.lock().unwrap();
            if event["type"]=="session.updated" {state.session_updated=true;state.session_model=event.pointer("/session/model").and_then(Value::as_str).map(str::to_string);}
            if event["type"]=="response.done" {state.completed.push(event["response"].clone());}
            let mut safe=event.clone();redact(&mut safe);writeln!(events,"{}",json!({"elapsed_ms":started.elapsed().as_millis(),"event":safe})).unwrap();
        });
        let probe=async {
            while !evidence.lock().unwrap().session_updated {
                if let RealtimeAudioEvent::Error{code,message}=session.receive_event().await.map_err(|e|e.to_string())? {return Err(format!("session update rejected {code}: {message}"));}
            }
            for (i,(_,color,shape,position)) in specs.iter().enumerate() {
                let mut frame=fixture(&codes[i],*color,shape,position);frame.sequence=i as u64;frame.timestamp_ms=Some(started.elapsed().as_millis() as u64);
                let sent_ms=started.elapsed().as_millis();
                let encoded=session.send_video_frame_and_return_encoded(&frame).await.map_err(|e|e.to_string())?;
                fs::write(directory.join(format!("frame-{i:03}.jpg")),&encoded.image).map_err(|e|e.to_string())?;
                evidence.lock().unwrap().sent.push(json!({"sequence":i,"sent_ms":sent_ms,"bytes":encoded.image.len(),"sha256":sha256(&encoded.image)}));
                if i==2||i==3 {answer(&mut session,&evidence,&directory,started,if i==2{0}else{1}).await?;}else{tokio::time::sleep(Duration::from_millis(500)).await;}
            }
            Ok::<(),String>(())
        }.await;
        let close=session.close().await.map_err(|e|e.to_string());
        probe.and(close)
    }).await }).map_err(|_|"overall Realtime probe deadline exceeded".to_string()).and_then(|r|r);
    let usage_after = client.usage().map_err(|e| e.to_string());
    save(&directory.join("usage-after.json"), &usage_after);
    let state = evidence.lock().unwrap();
    let texts: Vec<String> = state
        .responses
        .iter()
        .map(|r| {
            r["transcript"]
                .as_str()
                .unwrap_or_default()
                .to_ascii_lowercase()
        })
        .collect();
    let first = texts.first().cloned().unwrap_or_default();
    let second = texts.get(1).cloned().unwrap_or_default();
    let ordered_codes = codes[..3]
        .iter()
        .try_fold(0, |offset, code| {
            first[offset..]
                .find(code)
                .map(|at| offset + at + code.len())
        })
        .is_some();
    let visual_sequence = ordered_codes
        && [
            "red", "circle", "left", "blue", "square", "right", "green", "triangle", "center",
        ]
        .iter()
        .all(|s| first.contains(s));
    let later_update = second.contains(&codes[3])
        && ["yellow", "circle", "right"]
            .iter()
            .all(|s| second.contains(s));
    let unchanged = fs::read(&credentials).unwrap() == original_credentials;
    let passed = result.is_ok()
        && state.session_updated
        && state.sent.len() == 4
        && state.completed.len() == 2
        && visual_sequence
        && later_update
        && unchanged
        && state
            .responses
            .iter()
            .all(|r| r["audio_bytes"].as_u64().unwrap_or(0) > 0);
    let report = json!({"status":if passed{"passed"}else{"failed"},"model_requested":MODEL,"model_reported":state.session_model,"error":result.err(),"session_updated":state.session_updated,"frames":state.sent,"responses":state.responses,"completed_responses":state.completed,"visual_sequence_verified":visual_sequence,"later_update_verified":later_update,"credentials_unchanged":unchanged,"duration_ms":started.elapsed().as_millis(),"scope":"synthetic timed image frames over the production Realtime WebSocket adapter and shared encoder; no native WebRTC video track","usage_caveat":"response usage is observed API telemetry; account percentages are shared, delayed, and do not establish subscription billing rules"});
    save(&directory.join("report.json"), &report);
    println!(
        "{}",
        json!({"status":report["status"],"model":MODEL,"responses":texts,"artifacts":directory})
    );
    assert!(
        passed,
        "live visual probe did not meet its assertions; inspect report.json"
    );
}
