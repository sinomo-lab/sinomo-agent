//! Carries realtime audio over a WebRTC peer connection.
//!
//! This is one [`RealtimeMediaTransport`](crate::RealtimeMediaTransport) implementation, not a
//! second audio path: the session, the call path, and the capture and playback devices are the
//! same ones every other provider uses. What lives here is only what WebRTC additionally needs —
//! turning the session's PCM into Opus frames, negotiating a peer connection, and turning the
//! remote track back into PCM.
//!
//! Built only with the `webrtc-media` feature, because the WebRTC stack and the C Opus codec are
//! a large dependency that most builds never need.

use crate::RealtimeAudioError;
use crate::realtime_audio::{
    RealtimeAudioSampleFormat, RealtimeMediaTransport, RealtimeSessionFuture,
};
use std::time::Duration;

/// Opus in WebRTC always runs on a 48 kHz clock regardless of the session's own sample rate.
const OPUS_CLOCK_RATE_HZ: u32 = 48_000;
/// Frame duration every realtime peer accepts, and the unit RTP timestamps advance by.
const OPUS_FRAME_MS: usize = 20;
/// Samples in one encoded frame at the Opus clock rate.
const OPUS_FRAME_SAMPLES: usize = (OPUS_CLOCK_RATE_HZ as usize / 1_000) * OPUS_FRAME_MS;
/// Upper bound for one encoded Opus packet.
const OPUS_MAX_PACKET_BYTES: usize = 4_000;

/// Converts between a session's PCM and the Opus frames a peer connection carries.
///
/// A session speaks whatever sample rate its provider configured; Opus on the wire does not. This
/// owns that mismatch so the rest of the realtime code never sees a codec or a resampler.
pub struct OpusVoiceCodec {
    encoder: audiopus::coder::Encoder,
    decoder: audiopus::coder::Decoder,
    session_rate_hz: u32,
    /// Session audio resampled to the Opus clock and waiting for a whole frame.
    pending: Vec<i16>,
}

impl OpusVoiceCodec {
    /// Creates a codec for a session running at `session_rate_hz` with 16-bit PCM.
    pub fn new(
        session_rate_hz: u32,
        session_format: RealtimeAudioSampleFormat,
    ) -> Result<Self, RealtimeAudioError> {
        if session_format != RealtimeAudioSampleFormat::Pcm16Le {
            return Err(RealtimeAudioError::InvalidConfig {
                detail: "WebRTC realtime audio requires 16-bit PCM little-endian session audio"
                    .to_string(),
            });
        }
        if session_rate_hz == 0 {
            return Err(RealtimeAudioError::InvalidConfig {
                detail: "WebRTC realtime audio requires a non-zero session sample rate".to_string(),
            });
        }
        let encoder = audiopus::coder::Encoder::new(
            audiopus::SampleRate::Hz48000,
            audiopus::Channels::Mono,
            audiopus::Application::Voip,
        )
        .map_err(codec_error)?;
        let decoder =
            audiopus::coder::Decoder::new(audiopus::SampleRate::Hz48000, audiopus::Channels::Mono)
                .map_err(codec_error)?;
        Ok(Self {
            encoder,
            decoder,
            session_rate_hz,
            pending: Vec::new(),
        })
    }

    /// Turns session PCM into whole Opus frames.
    ///
    /// Capture chunks rarely divide evenly into 20 ms, so a partial frame is retained and joined
    /// with the next chunk rather than padded with silence, which would inject clicks.
    pub fn encode(&mut self, session_pcm: &[u8]) -> Result<Vec<Vec<u8>>, RealtimeAudioError> {
        let samples = pcm16_to_samples(session_pcm);
        let resampled = resample(&samples, self.session_rate_hz, OPUS_CLOCK_RATE_HZ);
        self.pending.extend_from_slice(&resampled);
        let mut frames = Vec::new();
        while self.pending.len() >= OPUS_FRAME_SAMPLES {
            let frame = self
                .pending
                .drain(..OPUS_FRAME_SAMPLES)
                .collect::<Vec<i16>>();
            let mut packet = vec![0_u8; OPUS_MAX_PACKET_BYTES];
            let written = self
                .encoder
                .encode(&frame, &mut packet)
                .map_err(codec_error)?;
            packet.truncate(written);
            frames.push(packet);
        }
        Ok(frames)
    }

    /// Turns one Opus packet back into session PCM.
    pub fn decode(&mut self, packet: &[u8]) -> Result<Vec<u8>, RealtimeAudioError> {
        let mut samples = vec![0_i16; OPUS_FRAME_SAMPLES];
        let decoded = self
            .decoder
            .decode(Some(packet), &mut samples, /*fec*/ false)
            .map_err(codec_error)?;
        samples.truncate(decoded);
        let resampled = resample(&samples, OPUS_CLOCK_RATE_HZ, self.session_rate_hz);
        Ok(samples_to_pcm16(&resampled))
    }

    /// Returns how much session audio is buffered short of a whole frame.
    pub fn pending_samples(&self) -> usize {
        self.pending.len()
    }
}

fn codec_error(error: audiopus::Error) -> RealtimeAudioError {
    RealtimeAudioError::Protocol {
        detail: format!("opus codec: {error}"),
    }
}

fn pcm16_to_samples(bytes: &[u8]) -> Vec<i16> {
    bytes
        .chunks_exact(2)
        .map(|pair| i16::from_le_bytes([pair[0], pair[1]]))
        .collect()
}

fn samples_to_pcm16(samples: &[i16]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    bytes
}

/// Resamples mono PCM by linear interpolation.
///
/// This matches the interpolation the native audio pipeline already applies between device and
/// model rates, so audio crossing the peer connection is not resampled by a different rule than
/// audio crossing a socket.
fn resample(samples: &[i16], source_rate_hz: u32, target_rate_hz: u32) -> Vec<i16> {
    if samples.is_empty() || source_rate_hz == target_rate_hz {
        return samples.to_vec();
    }
    let target_len = ((samples.len() as u64 * u64::from(target_rate_hz))
        / u64::from(source_rate_hz).max(1)) as usize;
    if target_len == 0 {
        return Vec::new();
    }
    let ratio = (samples.len() as f64 - 1.0).max(0.0) / (target_len as f64 - 1.0).max(1.0);
    (0..target_len)
        .map(|index| {
            let position = index as f64 * ratio;
            let left = position.floor() as usize;
            let right = (left + 1).min(samples.len() - 1);
            let weight = position - left as f64;
            let blended =
                f64::from(samples[left]) * (1.0 - weight) + f64::from(samples[right]) * weight;
            blended
                .round()
                .clamp(f64::from(i16::MIN), f64::from(i16::MAX)) as i16
        })
        .collect()
}

/// Moves audio between a session and an established peer connection.
///
/// Outbound frames are encoded and handed to the sender; inbound frames are decoded and queued for
/// the session's event stream. Both directions run against bounded queues so a stalled peer
/// applies backpressure instead of growing without limit.
pub struct WebrtcMediaTransport {
    codec: OpusVoiceCodec,
    outbound: tokio::sync::mpsc::Sender<Vec<u8>>,
    inbound: tokio::sync::mpsc::Receiver<Vec<u8>>,
    closed: bool,
}

impl WebrtcMediaTransport {
    /// Creates a transport over an already-negotiated media path.
    ///
    /// `outbound` receives encoded Opus frames to packetize onto the local track and `inbound`
    /// supplies Opus frames arriving on the remote track, so this type stays independent of which
    /// peer-connection implementation drives them.
    pub fn new(
        codec: OpusVoiceCodec,
        outbound: tokio::sync::mpsc::Sender<Vec<u8>>,
        inbound: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) -> Self {
        Self {
            codec,
            outbound,
            inbound,
            closed: false,
        }
    }
}

impl RealtimeMediaTransport for WebrtcMediaTransport {
    fn send_audio<'a>(&'a mut self, audio: &'a [u8]) -> RealtimeSessionFuture<'a, ()> {
        Box::pin(async move {
            if self.closed {
                return Err(RealtimeAudioError::Protocol {
                    detail: "the realtime peer connection is closed".to_string(),
                });
            }
            for frame in self.codec.encode(audio)? {
                self.outbound
                    .send(frame)
                    .await
                    .map_err(|_| RealtimeAudioError::Protocol {
                        detail: "the realtime peer connection stopped accepting audio".to_string(),
                    })?;
            }
            Ok(())
        })
    }

    fn recv_audio(&mut self, timeout: Duration) -> RealtimeSessionFuture<'_, Option<Vec<u8>>> {
        Box::pin(async move {
            let Ok(frame) = tokio::time::timeout(timeout, self.inbound.recv()).await else {
                return Ok(None);
            };
            // A closed inbound queue means the peer stopped sending, which is an ordinary end of
            // stream rather than a failure.
            let Some(frame) = frame else {
                return Ok(None);
            };
            self.codec.decode(&frame).map(Some)
        })
    }

    fn close(&mut self) -> RealtimeSessionFuture<'_, ()> {
        Box::pin(async move {
            self.closed = true;
            self.inbound.close();
            Ok(())
        })
    }
}

/// Bounds how many encoded frames queue in either direction before backpressure applies.
const MEDIA_FRAME_QUEUE_CAPACITY: usize = 64;
/// Public STUN server used when a configuration supplies none.
const DEFAULT_STUN_URL: &str = "stun:stun.l.google.com:19302";

/// Negotiates a peer connection for a realtime session and returns its media transport.
///
/// The SDP exchange reuses the session's existing call negotiation, so the provider-specific
/// signaling stays in one place; what this adds is the local peer connection that terminates the
/// media. The returned transport is attached to the session with
/// [`RealtimeAudioSession::attach_media_transport`](crate::RealtimeAudioSession::attach_media_transport),
/// after which the call path sends and receives audio exactly as it does for a socket provider.
///
/// Exercised against the live realtime API by the ignored `codex_realtime_webrtc_e2e` test, which
/// negotiates a real call and decodes audio off the answered peer connection.
pub async fn connect_webrtc_media(
    config: &crate::RealtimeAudioSessionConfig,
) -> Result<(WebrtcMediaTransport, crate::RealtimeWebrtcCall), RealtimeAudioError> {
    use rtc::rtp_transceiver::rtp_sender::{
        RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind,
    };
    use webrtc::media_stream::MediaStreamTrack;
    use webrtc::media_stream::track_local::static_sample::TrackLocalStaticSample;
    use webrtc::peer_connection::{
        MediaEngine, PeerConnection, PeerConnectionBuilder, RTCConfigurationBuilder, RTCIceServer,
        RTCSessionDescription,
    };

    let codec = OpusVoiceCodec::new(config.input_sample_rate_hz, config.input_format)?;
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(MEDIA_FRAME_QUEUE_CAPACITY);
    let (outbound_tx, mut outbound_rx) =
        tokio::sync::mpsc::channel::<Vec<u8>>(MEDIA_FRAME_QUEUE_CAPACITY);

    let handler = RealtimeTrackHandler {
        inbound: inbound_tx,
    };
    // The default engine registers no codecs, so a track would find no transceiver to match.
    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs().map_err(peer_error)?;
    let peer = PeerConnectionBuilder::new()
        .with_media_engine(media_engine)
        .with_configuration(
            RTCConfigurationBuilder::default()
                .with_ice_servers(vec![RTCIceServer {
                    urls: vec![DEFAULT_STUN_URL.to_owned()],
                    ..Default::default()
                }])
                .build(),
        )
        .with_handler(std::sync::Arc::new(handler))
        .with_udp_addrs(vec!["0.0.0.0:0"])
        .build()
        .await
        .map_err(peer_error)?;

    // This must match the registered codec exactly or the transceiver rejects the track. Opus is
    // declared with two channels by convention even when the payload is mono, which it is here.
    let opus = RTCRtpCodec {
        mime_type: "audio/opus".to_string(),
        clock_rate: OPUS_CLOCK_RATE_HZ,
        channels: 2,
        sdp_fmtp_line: "minptime=10;useinbandfec=1".to_string(),
        rtcp_feedback: Vec::new(),
    };
    let ssrc = rand_ssrc();
    let track = std::sync::Arc::new(
        TrackLocalStaticSample::new(MediaStreamTrack::new(
            "sinomo-realtime".to_owned(),
            "sinomo-realtime-audio".to_owned(),
            "microphone".to_owned(),
            RtpCodecKind::Audio,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(ssrc),
                    ..Default::default()
                },
                codec: opus,
                ..Default::default()
            }],
        ))
        .map_err(peer_error)?,
    );
    let sender = peer
        .add_track(
            track.clone() as std::sync::Arc<dyn webrtc::media_stream::track_local::TrackLocal>
        )
        .await
        .map_err(peer_error)?;

    let offer = peer.create_offer(None).await.map_err(peer_error)?;
    peer.set_local_description(offer.clone())
        .await
        .map_err(peer_error)?;

    // The provider answers the offer through the session's own call negotiation.
    let mut call_config = config.clone();
    call_config.transport = crate::RealtimeAudioTransport::webrtc(offer.sdp.clone());
    let call = crate::create_realtime_webrtc_call(&call_config).await?;
    peer.set_remote_description(
        RTCSessionDescription::answer(call.answer_sdp.clone()).map_err(peer_error)?,
    )
    .await
    .map_err(peer_error)?;

    // The payload type is only known once the answer is applied, so the send pump starts after
    // negotiation rather than being configured up front.
    let payload_type = sender
        .get_parameters()
        .await
        .map_err(peer_error)?
        .rtp_parameters
        .codecs
        .first()
        .map(|codec| codec.payload_type)
        .unwrap_or(111);
    tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            let sample = rtc::media::Sample {
                data: frame.into(),
                duration: Duration::from_millis(OPUS_FRAME_MS as u64),
                ..Default::default()
            };
            if track
                .sample_writer(ssrc, payload_type)
                .write_sample(&sample)
                .await
                .is_err()
            {
                return;
            }
        }
    });

    Ok((
        WebrtcMediaTransport::new(codec, outbound_tx, inbound_rx),
        call,
    ))
}

/// Forwards inbound RTP payloads from the remote track into the transport's queue.
struct RealtimeTrackHandler {
    inbound: tokio::sync::mpsc::Sender<Vec<u8>>,
}

#[async_trait::async_trait]
impl webrtc::peer_connection::PeerConnectionEventHandler for RealtimeTrackHandler {
    async fn on_track(
        &self,
        track: std::sync::Arc<dyn webrtc::media_stream::track_remote::TrackRemote>,
    ) {
        let inbound = self.inbound.clone();
        tokio::spawn(async move {
            while let Some(event) = track.poll().await {
                use webrtc::media_stream::track_remote::TrackRemoteEvent;
                match event {
                    TrackRemoteEvent::OnRtpPacket(packet) => {
                        // The RTP payload is one Opus frame; decoding belongs to the codec.
                        if inbound.send(packet.payload.to_vec()).await.is_err() {
                            return;
                        }
                    }
                    TrackRemoteEvent::OnEnded | TrackRemoteEvent::OnError => return,
                    _ => {}
                }
            }
        });
    }
}

fn peer_error(error: impl std::fmt::Display) -> RealtimeAudioError {
    RealtimeAudioError::Protocol {
        detail: format!("realtime peer connection: {error}"),
    }
}

/// Returns a random synchronization source identifier for the outbound track.
fn rand_ssrc() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos())
        .unwrap_or(1);
    nanos | 1
}

#[cfg(test)]
#[path = "realtime_webrtc_tests.rs"]
mod tests;
