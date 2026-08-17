use super::*;

#[test]
fn capture_chunk_bytes_follow_the_negotiated_format_and_duration() {
    // 24 kHz PCM16 for 100 ms is 2400 frames of 2 bytes.
    assert_eq!(
        capture_chunk_bytes(24_000, NativeAudioSampleFormat::Pcm16Le, 100),
        4_800
    );
    assert_eq!(
        capture_chunk_bytes(16_000, NativeAudioSampleFormat::F32Le, 100),
        6_400
    );
    // A duration too short to hold a frame still yields one frame rather than an empty chunk.
    assert_eq!(
        capture_chunk_bytes(24_000, NativeAudioSampleFormat::Pcm16Le, 0),
        2
    );
}

#[test]
fn the_assembler_rebuilds_chunks_across_arbitrary_packet_boundaries() {
    let mut assembler = CaptureChunkAssembler::new(1_000, NativeAudioSampleFormat::Pcm16Le, 2);
    // 1000 Hz for 2 ms is 2 frames of 2 bytes.
    let chunk_bytes = capture_chunk_bytes(1_000, NativeAudioSampleFormat::Pcm16Le, 2);
    assert_eq!(chunk_bytes, 4);

    // A partial packet completes nothing yet.
    assert!(assembler.push(&[1, 2, 3]).is_empty());
    // One packet spanning several chunk boundaries yields every whole chunk it completed.
    let chunks = assembler.push(&[4, 5, 6, 7, 8, 9]);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].audio, vec![1, 2, 3, 4]);
    assert_eq!(chunks[0].sequence, 0);
    assert_eq!(chunks[0].sample_rate_hz, 1_000);
    assert_eq!(chunks[1].audio, vec![5, 6, 7, 8]);
    assert_eq!(chunks[1].sequence, 1);

    // The byte left over stays buffered and joins the next packet.
    let chunks = assembler.push(&[10, 11, 12, 13, 14]);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].audio, vec![9, 10, 11, 12]);
    assert_eq!(chunks[0].sequence, 2);

    // The trailing partial chunk survives to the end of the stream.
    let tail = assembler.flush().expect("a partial tail is kept");
    assert_eq!(tail.audio, vec![13, 14]);
    assert_eq!(tail.sequence, 3);
    assert!(assembler.flush().is_none());
}

#[test]
fn a_remote_source_reports_frames_timeouts_and_a_closed_stream_distinctly() {
    let (source, sender) = RemoteAudioSource::channel();
    let chunk = NativeAudioChunk {
        sequence: 0,
        sample_rate_hz: 24_000,
        sample_format: NativeAudioSampleFormat::Pcm16Le,
        channels: 1,
        captured_at_unix_ms: 0,
        audio: vec![7, 7],
    };
    sender.send(Ok(chunk.clone())).expect("queue a chunk");

    let received = source
        .recv_chunk(Duration::from_millis(1))
        .expect("read a chunk")
        .expect("a chunk is pending");
    assert_eq!(received.audio, vec![7, 7]);

    // Nothing pending is a timeout, not an error.
    assert!(
        source
            .recv_chunk(Duration::from_millis(1))
            .expect("an empty queue is not a failure")
            .is_none()
    );

    sender
        .send(Err("microphone was unplugged".to_string()))
        .expect("queue a failure");
    let error = source
        .recv_chunk(Duration::from_millis(1))
        .expect_err("a reported failure surfaces");
    assert!(error.to_string().contains("unplugged"), "{error}");

    // A serving node that ends the stream reads as end-of-stream, not a failure.
    drop(sender);
    assert!(
        source
            .recv_chunk(Duration::from_millis(1))
            .expect("a closed stream ends cleanly")
            .is_none()
    );
}

#[test]
fn a_remote_sink_queues_frames_and_reports_a_closed_stream() {
    let (sink, frames) = RemoteAudioSink::channel();

    sink.write_chunk(&[1, 2, 3]).expect("queue a frame");
    sink.write_chunk(&[4]).expect("queue another frame");
    assert_eq!(frames.recv().expect("first frame"), vec![1, 2, 3]);
    assert_eq!(frames.recv().expect("second frame"), vec![4]);

    drop(frames);
    let error = sink
        .write_chunk(&[5])
        .expect_err("writing to a closed stream fails");
    assert!(error.to_string().contains("closed"), "{error}");
}

#[tokio::test]
async fn the_frame_reader_streams_queued_frames_and_ends_when_the_queue_closes() {
    use tokio::io::AsyncReadExt as _;

    let (frames, receiver) = tokio::sync::mpsc::channel(4);
    frames.send(vec![1, 2, 3]).await.expect("queue a frame");
    frames.send(vec![4, 5]).await.expect("queue another frame");
    drop(frames);

    let mut reader = PcmFrameReader::new(receiver);
    let mut streamed = Vec::new();
    reader
        .read_to_end(&mut streamed)
        .await
        .expect("read the whole body");

    // Frames concatenate into one byte stream; the closed queue ends the body.
    assert_eq!(streamed, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn the_frame_reader_splits_a_frame_larger_than_the_read_buffer() {
    use tokio::io::AsyncReadExt as _;

    let (frames, receiver) = tokio::sync::mpsc::channel(1);
    frames.send(vec![9; 5]).await.expect("queue a frame");
    drop(frames);

    let mut reader = PcmFrameReader::new(receiver);
    let mut first = [0_u8; 2];
    reader
        .read_exact(&mut first)
        .await
        .expect("read part of a frame");
    assert_eq!(first, [9, 9]);

    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).await.expect("read the rest");
    assert_eq!(rest, vec![9; 3]);
}

#[test]
fn media_calls_address_the_media_routes() {
    let capture = capture_call(&MediaAudioCaptureRequest {
        device: Some("usb-mic".to_string()),
        sample_rate_hz: 24_000,
        sample_format: NativeAudioSampleFormat::Pcm16Le,
        chunk_ms: 100,
        max_chunks: Some(4),
    })
    .expect("build a capture call");
    assert_eq!(capture.request.route, MEDIA_AUDIO_CAPTURE_ROUTE);
    assert_eq!(capture.request.category, MEDIA_RPC_CATEGORY);

    let playback = playback_call(
        &MediaAudioPlaybackRequest {
            device: None,
            input_sample_rate_hz: 24_000,
            input_sample_format: NativeAudioSampleFormat::Pcm16Le,
        },
        ObjectRpcBodySource::bytes(vec![1, 2, 3]),
    )
    .expect("build a playback call");
    assert_eq!(playback.request.route, MEDIA_AUDIO_PLAYBACK_ROUTE);
    assert_eq!(playback.request.category, MEDIA_RPC_CATEGORY);
}
