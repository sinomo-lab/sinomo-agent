use super::*;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(future)
}

/// One second of a 440 Hz tone at a given rate, as the session's 16-bit PCM.
fn tone_pcm(rate_hz: u32, millis: usize) -> Vec<u8> {
    let samples = (rate_hz as usize / 1_000) * millis;
    let mut bytes = Vec::with_capacity(samples * 2);
    for index in 0..samples {
        let phase = index as f64 / f64::from(rate_hz) * 440.0 * std::f64::consts::TAU;
        let sample = (phase.sin() * 8_000.0) as i16;
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    bytes
}

#[test]
fn a_codec_requires_the_session_pcm_format_it_can_carry() {
    assert!(OpusVoiceCodec::new(24_000, RealtimeAudioSampleFormat::Pcm16Le).is_ok());

    let Err(error) = OpusVoiceCodec::new(24_000, RealtimeAudioSampleFormat::F32Le) else {
        panic!("float session audio must be refused");
    };
    assert!(error.to_string().contains("16-bit PCM"), "{error}");

    let Err(error) = OpusVoiceCodec::new(0, RealtimeAudioSampleFormat::Pcm16Le) else {
        panic!("a zero sample rate must be refused");
    };
    assert!(error.to_string().contains("sample rate"), "{error}");
}

#[test]
fn capture_audio_becomes_whole_opus_frames_and_the_remainder_is_kept() {
    let mut codec = OpusVoiceCodec::new(24_000, RealtimeAudioSampleFormat::Pcm16Le).expect("codec");

    // 50 ms of session audio is two whole 20 ms frames with 10 ms left over.
    let frames = codec.encode(&tone_pcm(24_000, 50)).expect("encode");
    assert_eq!(frames.len(), 2);
    assert!(frames.iter().all(|frame| !frame.is_empty()));
    assert_eq!(codec.pending_samples(), OPUS_FRAME_SAMPLES / 2);

    // The retained remainder joins the next chunk instead of being padded or dropped.
    let frames = codec
        .encode(&tone_pcm(24_000, 10))
        .expect("encode the rest");
    assert_eq!(frames.len(), 1);
    assert_eq!(codec.pending_samples(), 0);

    // A chunk shorter than a frame produces nothing yet.
    assert!(
        codec
            .encode(&tone_pcm(24_000, 5))
            .expect("encode")
            .is_empty()
    );
}

#[test]
fn a_frame_round_trips_back_to_session_audio_at_the_session_rate() {
    let mut codec = OpusVoiceCodec::new(24_000, RealtimeAudioSampleFormat::Pcm16Le).expect("codec");
    let frames = codec.encode(&tone_pcm(24_000, 20)).expect("encode");
    assert_eq!(frames.len(), 1);

    let decoded = codec.decode(&frames[0]).expect("decode");

    // 20 ms at the session rate is 480 samples of 16-bit PCM, whatever the wire clock was.
    assert_eq!(decoded.len(), 480 * 2);
}

#[test]
fn a_session_running_at_the_wire_rate_needs_no_resampling() {
    let mut codec = OpusVoiceCodec::new(48_000, RealtimeAudioSampleFormat::Pcm16Le).expect("codec");

    let frames = codec.encode(&tone_pcm(48_000, 20)).expect("encode");
    assert_eq!(frames.len(), 1);
    let decoded = codec.decode(&frames[0]).expect("decode");
    assert_eq!(decoded.len(), OPUS_FRAME_SAMPLES * 2);
}

#[test]
fn resampling_preserves_duration_and_leaves_matching_rates_untouched() {
    let samples = (0..480_i16).collect::<Vec<_>>();

    // Doubling the rate doubles the sample count, so the audio keeps its duration.
    assert_eq!(resample(&samples, 24_000, 48_000).len(), 960);
    assert_eq!(resample(&samples, 48_000, 24_000).len(), 240);
    // An unchanged rate is a pass-through rather than an interpolation pass.
    assert_eq!(resample(&samples, 24_000, 24_000), samples);
    assert!(resample(&[], 24_000, 48_000).is_empty());
}

#[test]
fn the_transport_encodes_outbound_audio_and_decodes_inbound_frames() {
    let codec = OpusVoiceCodec::new(24_000, RealtimeAudioSampleFormat::Pcm16Le).expect("codec");
    let (outbound, mut outbound_rx) = tokio::sync::mpsc::channel(8);
    let (inbound_tx, inbound) = tokio::sync::mpsc::channel(8);
    let mut transport = WebrtcMediaTransport::new(codec, outbound, inbound);

    // Session audio leaves as encoded frames, not raw PCM.
    block_on(transport.send_audio(&tone_pcm(24_000, 20))).expect("send audio");
    let frame = outbound_rx.try_recv().expect("a frame was queued");
    assert!(!frame.is_empty());
    assert!(frame.len() < 24_000, "frames are compressed, not raw PCM");

    // A frame arriving from the peer comes back as session PCM.
    inbound_tx.try_send(frame).expect("queue an inbound frame");
    let decoded = block_on(transport.recv_audio(Duration::from_millis(50)))
        .expect("receive audio")
        .expect("a frame is pending");
    assert_eq!(decoded.len(), 480 * 2);

    // Silence is a timeout, not a failure.
    assert!(
        block_on(transport.recv_audio(Duration::from_millis(5)))
            .expect("an idle peer is not a failure")
            .is_none()
    );
}

#[test]
fn a_closed_transport_stops_accepting_audio() {
    let codec = OpusVoiceCodec::new(24_000, RealtimeAudioSampleFormat::Pcm16Le).expect("codec");
    let (outbound, _outbound_rx) = tokio::sync::mpsc::channel(8);
    let (_inbound_tx, inbound) = tokio::sync::mpsc::channel(8);
    let mut transport = WebrtcMediaTransport::new(codec, outbound, inbound);

    block_on(transport.close()).expect("close");

    let error = block_on(transport.send_audio(&tone_pcm(24_000, 20)))
        .expect_err("a closed peer connection refuses audio");
    assert!(error.to_string().contains("closed"), "{error}");
}

#[test]
fn a_dropped_peer_reports_rather_than_silently_discarding_audio() {
    let codec = OpusVoiceCodec::new(24_000, RealtimeAudioSampleFormat::Pcm16Le).expect("codec");
    let (outbound, outbound_rx) = tokio::sync::mpsc::channel(8);
    let (_inbound_tx, inbound) = tokio::sync::mpsc::channel(8);
    let mut transport = WebrtcMediaTransport::new(codec, outbound, inbound);
    drop(outbound_rx);

    let error = block_on(transport.send_audio(&tone_pcm(24_000, 20)))
        .expect_err("a gone peer connection is reported");
    assert!(error.to_string().contains("stopped accepting"), "{error}");
}
