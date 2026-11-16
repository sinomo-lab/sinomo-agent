//! Optional system FFmpeg hardware encoder. CPU upload is explicit; this is not
//! a native-surface capture backend. Encoders must produce immediate Baseline
//! Annex-B access units matching the existing receiver contract.
use super::{VideoEncoder, dimensions, validate_access_unit};
use sinomo_lm::{RealtimeVideoFrameInput, RealtimeVideoPixelFormat};
use std::{
    ffi::{CString, c_char, c_int, c_void},
    ptr::NonNull,
    sync::OnceLock,
};

unsafe extern "C" {
    fn sinomo_gpu_open(
        name: *const c_char,
        width: c_int,
        height: c_int,
        fps: c_int,
        error: *mut c_int,
    ) -> *mut c_void;
    fn sinomo_gpu_close(state: *mut c_void);
    fn sinomo_gpu_encode(
        state: *mut c_void,
        data: *const u8,
        width: c_int,
        height: c_int,
        stride: c_int,
        format: c_int,
        output: *mut *const u8,
        length: *mut c_int,
    ) -> c_int;
}

struct Encoder {
    state: NonNull<c_void>,
    width: u32,
    height: u32,
    first: bool,
}
impl Encoder {
    fn open(name: &str, width: u32, height: u32, fps: u32) -> Result<Self, String> {
        dimensions(width, height)?;
        if !(1..=60).contains(&fps) {
            return Err("GPU encoder fps must be 1..60".into());
        }
        let name = CString::new(name).map_err(|e| e.to_string())?;
        let mut error = 0;
        // SAFETY: bounded dimensions, valid string and writable error slot. The
        // opaque allocation is exclusively owned and freed by Drop.
        let state = unsafe {
            sinomo_gpu_open(
                name.as_ptr(),
                width as i32,
                height as i32,
                fps as i32,
                &mut error,
            )
        };
        Ok(Self {
            state: NonNull::new(state)
                .ok_or_else(|| format!("hardware encoder open: FFmpeg {error}"))?,
            width,
            height,
            first: true,
        })
    }
}
impl VideoEncoder for Encoder {
    fn encode(&mut self, frame: &RealtimeVideoFrameInput) -> Result<Vec<u8>, String> {
        let (format, channels) = match frame.pixel_format {
            RealtimeVideoPixelFormat::Rgb8 => (0, 3),
            RealtimeVideoPixelFormat::Rgba8 => (1, 4),
            RealtimeVideoPixelFormat::Bgra8 => (2, 4),
        };
        if frame.width == 0
            || frame.height == 0
            || frame.width > 8192
            || frame.height > 8192
            || frame.stride < frame.width as usize * channels
            || frame.stride > i32::MAX as usize
            || frame
                .stride
                .checked_mul(frame.height as usize)
                .is_none_or(|n| n > frame.pixels.len())
        {
            return Err("invalid GPU encoder input geometry/stride/length".into());
        }
        let mut output = std::ptr::null();
        let mut length = 0;
        // SAFETY: checked input extent and stride; C borrows pixels for this call
        // and returns packet memory owned by state until its next invocation.
        let result = unsafe {
            sinomo_gpu_encode(
                self.state.as_ptr(),
                frame.pixels.as_ptr(),
                frame.width as i32,
                frame.height as i32,
                frame.stride as i32,
                format,
                &mut output,
                &mut length,
            )
        };
        if result < 0 {
            return Err(format!(
                "hardware encoder requires immediate output: FFmpeg {result}"
            ));
        }
        if output.is_null()
            || length <= 0
            || length as usize > crate::stream::MAX_ENCODED_FRAME_BYTES
        {
            return Err("invalid hardware encoder packet length".into());
        }
        // SAFETY: successful C call exposes length initialized bytes; copy before
        // allowing another encoder call to replace that packet.
        let bytes = unsafe { std::slice::from_raw_parts(output, length as usize) }.to_vec();
        validate_access_unit(&bytes, self.width, self.height, self.first)?;
        self.first = false;
        Ok(bytes)
    }
}
impl Drop for Encoder {
    fn drop(&mut self) {
        // SAFETY: this is the sole owner; no borrowed packet escapes encode.
        unsafe { sinomo_gpu_close(self.state.as_ptr()) }
    }
}

static BACKEND: OnceLock<Result<&'static str, String>> = OnceLock::new();
fn backend() -> Result<&'static str, String> {
    BACKEND
        .get_or_init(|| {
            #[cfg(target_os = "linux")]
            let candidates = ["h264_vaapi", "h264_nvenc", "h264_qsv"];
            #[cfg(target_os = "windows")]
            let candidates = ["h264_nvenc", "h264_qsv", "h264_amf"];
            #[cfg(target_os = "macos")]
            let candidates = ["h264_videotoolbox"];
            let frame = RealtimeVideoFrameInput::new(
                vec![80; 128 * 128 * 3].into(),
                RealtimeVideoPixelFormat::Rgb8,
                128,
                128,
                128 * 3,
                0,
            );
            let mut errors = Vec::new();
            for name in candidates {
                let probe = Encoder::open(name, 128, 128, 30).and_then(|mut encoder| {
                    for _ in 0..3 {
                        encoder.encode(&frame)?;
                    }
                    Ok(())
                });
                match probe {
                    Ok(()) => return Ok(name),
                    Err(error) => errors.push(format!("{name}: {error}")),
                }
            }
            Err(errors.join("; "))
        })
        .clone()
}
pub(super) fn available() -> bool {
    backend().is_ok()
}
pub(super) fn open(width: u32, height: u32, fps: u32) -> Result<Box<dyn VideoEncoder>, String> {
    Ok(Box::new(Encoder::open(backend()?, width, height, fps)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    // Uses the real driver and receiver wire contract; never silently skips on
    // hosts without hardware. Ordinary unit suites do not require a GPU.
    #[test]
    #[ignore = "requires a working hardware H264 encoder and FFmpeg libraries"]
    fn hardware_frames_decode_and_reject_invalid_input() {
        let mut encoder = open(128, 128, 30).unwrap();
        let mut decoder = openh264::decoder::Decoder::new().unwrap();
        for sequence in 0..65 {
            let (format, channels) = match sequence % 3 {
                0 => (RealtimeVideoPixelFormat::Rgb8, 3),
                1 => (RealtimeVideoPixelFormat::Rgba8, 4),
                _ => (RealtimeVideoPixelFormat::Bgra8, 4),
            };
            // Exercise scaling and padded input rows as used by native capture.
            let stride = 256 * channels + 16;
            let frame = RealtimeVideoFrameInput::new(
                vec![(sequence * 3) as u8; stride * 192].into(),
                format,
                256,
                192,
                stride,
                sequence,
            );
            let bytes = encoder.encode(&frame).unwrap();
            assert!(decoder.decode(&bytes).unwrap().is_some());
        }
        let invalid = RealtimeVideoFrameInput::new(
            vec![0; 3].into(),
            RealtimeVideoPixelFormat::Rgb8,
            128,
            128,
            384,
            66,
        );
        assert!(encoder.encode(&invalid).is_err());
    }
}
