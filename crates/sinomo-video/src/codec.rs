//! Cluster transport codecs. Model providers continue to receive JPEG/PNG.
//!
//! Desktop hardware encoding is opt-in with `sinomo-video/desktop-gpu` and
//! requires system FFmpeg development libraries discoverable by pkg-config.
//! Auto probes hardware once per process; explicit H264 fails if unavailable.
//! Image remains the default. The backend uploads CPU pixels; it does not
//! eliminate native capture readback. Android retains its host-installed codec.
use serde::{Deserialize, Serialize};
use sinomo_lm::RealtimeVideoFrameInput;
use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VideoTransport {
    /// Select the installed hardware backend, otherwise retain image transport.
    Auto,
    #[default]
    Image,
    H264,
}

/// A stream-local encoder, called and dropped on one blocking media worker.
/// Each call produces one Annex-B access unit; the first includes SPS/PPS + IDR.
pub trait VideoEncoder {
    fn encode(&mut self, frame: &RealtimeVideoFrameInput) -> Result<Vec<u8>, String>;
}
pub type EncoderFactory = fn(u32, u32, u32) -> Result<Box<dyn VideoEncoder>, String>;
#[cfg(all(
    feature = "desktop-gpu",
    any(target_os = "linux", target_os = "windows", target_os = "macos")
))]
#[path = "desktop_gpu.rs"]
mod desktop_gpu;

fn available() -> bool {
    if ENCODER.get().is_some() {
        return true;
    }
    #[cfg(all(
        feature = "desktop-gpu",
        any(target_os = "linux", target_os = "windows", target_os = "macos")
    ))]
    {
        return desktop_gpu::available();
    }
    #[allow(unreachable_code)]
    false
}

static ENCODER: OnceLock<EncoderFactory> = OnceLock::new();
/// The Android app installs its MediaCodec adapter as part of media host startup.
pub fn install_h264_encoder(factory: EncoderFactory) {
    let _ = ENCODER.set(factory);
}
pub fn select(transport: VideoTransport, grid: bool) -> Result<VideoTransport, String> {
    match transport {
        VideoTransport::Auto if !grid && available() => Ok(VideoTransport::H264),
        VideoTransport::Auto => Ok(VideoTransport::Image),
        VideoTransport::H264 if grid => {
            Err("H264 transport does not support a coordinate grid; select image transport".into())
        }
        VideoTransport::H264 if !available() => {
            Err("H264 hardware encoder is unavailable on this node".into())
        }
        value => Ok(value),
    }
}
pub fn encoder(width: u32, height: u32, fps: u32) -> Result<Box<dyn VideoEncoder>, String> {
    if let Some(factory) = ENCODER.get() {
        return factory(width, height, fps);
    }
    #[cfg(all(
        feature = "desktop-gpu",
        any(target_os = "linux", target_os = "windows", target_os = "macos")
    ))]
    {
        return desktop_gpu::open(width, height, fps);
    }
    #[allow(unreachable_code)]
    Err("H264 hardware encoder is unavailable".into())
}
/// Bound decoder memory before accepting compressed data. Transport dimensions
/// are deliberately limited to a screen preview, not arbitrary media files.
pub fn dimensions(width: u32, height: u32) -> Result<(), String> {
    if width < 2 || height < 2 || width > 1920 || height > 1920 || width % 2 != 0 || height % 2 != 0
    {
        return Err("H264 dimensions must be even and within 2..1920".into());
    }
    Ok(())
}

/// Inspect SPS before native decoder allocation. This stream contract permits
/// progressive Baseline 4:2:0 only, with bounded coded geometry/reference memory.
pub(crate) fn validate_access_unit(
    bytes: &[u8],
    width: u32,
    height: u32,
    first: bool,
) -> Result<(), String> {
    use h264_reader::nal::{Nal, RefNal};
    let (mut sps_seen, mut pps_seen, mut idr, mut slices) = (false, false, false, 0);
    if !bytes.starts_with(&[0, 0, 1]) && !bytes.starts_with(&[0, 0, 0, 1]) {
        return Err("H264 access unit must use Annex-B framing".into());
    }
    for unit in openh264::nal_units(bytes) {
        let raw = &unit[3..];
        let header = *raw.first().ok_or("empty H264 NAL")?;
        if header & 0x80 != 0 {
            return Err("invalid H264 NAL header".into());
        }
        match header & 31 {
            7 => {
                if raw.len() > 4096 {
                    return Err("H264 SPS exceeds limit".into());
                }
                // Reject other profiles before parsing their optional structures.
                if raw.get(1) != Some(&66) {
                    return Err("H264 stream requires Baseline profile".into());
                }
                let nal = RefNal::new(raw, &[], true);
                let sps = h264_reader::nal::sps::SeqParameterSet::from_bits(nal.rbsp_bits())
                    .map_err(|e| format!("invalid SPS: {e:?}"))?;
                if sps.pic_width_in_mbs_minus1 >= 120
                    || sps.pic_height_in_map_units_minus1 >= 120
                    || sps.max_num_ref_frames > 4
                    || !matches!(
                        sps.frame_mbs_flags,
                        h264_reader::nal::sps::FrameMbsFlags::Frames
                    )
                    || sps
                        .pixel_dimensions()
                        .map_err(|e| format!("invalid SPS dimensions: {e:?}"))?
                        != (width, height)
                {
                    return Err("H264 SPS exceeds stream geometry/reference bounds".into());
                }
                sps_seen = true;
            }
            8 => {
                if raw.len() > 4096 {
                    return Err("H264 PPS exceeds limit".into());
                }
                pps_seen = true;
            }
            5 => {
                idr = true;
                slices += 1;
            }
            1 => slices += 1,
            6 | 9 | 12 => {}
            _ => return Err("unsupported H264 NAL type".into()),
        }
    }
    if slices == 0 || (first && !(sps_seen && pps_seen && idr)) {
        return Err("H264 stream must start with configuration and an IDR frame".into());
    }
    Ok(())
}
