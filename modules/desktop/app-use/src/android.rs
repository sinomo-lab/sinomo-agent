//! Private, same-UID IPC to the Android OS service packaged in the application.
//! The service owns MediaProjection/accessibility; it has no Sinomo transport,
//! agent, encoder, or authority store. This module remains the consent owner.
use super::*;
use serde_json::{Value, json};
use sinomo_device::{NativeVideoPlane, NativeVideoSampleFormat};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;

const MAX_CONTROL_BYTES: usize = 64 * 1024;
fn platform(error: impl std::fmt::Display) -> DesktopError {
    DesktopError::Platform(error.to_string())
}
struct Bridge(UnixStream);
impl Bridge {
    fn connect() -> DesktopResult<Self> {
        let uid = unsafe { libc::getuid() };
        let name = format!("sinomo-app-use:{uid}");
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
        if fd < 0 {
            return Err(platform(std::io::Error::last_os_error()));
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        address.sun_family = libc::AF_UNIX as _;
        for (dest, source) in address.sun_path[1..].iter_mut().zip(name.as_bytes()) {
            *dest = *source as _;
        }
        let len = std::mem::offset_of!(libc::sockaddr_un, sun_path) + 1 + name.len();
        if unsafe {
            libc::connect(
                fd.as_raw_fd(),
                (&address as *const libc::sockaddr_un).cast(),
                len as _,
            )
        } != 0
        {
            return Err(DesktopError::Unsupported(
                "enable Sinomo Accessibility or start screen sharing in local settings".into(),
            ));
        }
        let mut peer: libc::ucred = unsafe { std::mem::zeroed() };
        let mut size = std::mem::size_of_val(&peer) as libc::socklen_t;
        if unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut peer as *mut libc::ucred).cast(),
                &mut size,
            )
        } != 0
            || peer.uid != uid
        {
            return Err(DesktopError::PermissionDenied(
                "Android OS bridge must have the host application's UID".into(),
            ));
        }
        let stream = UnixStream::from(fd);
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .map_err(platform)?;
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .map_err(platform)?;
        Ok(Self(stream))
    }
    fn call(&mut self, request: Value) -> DesktopResult<Value> {
        let bytes = serde_json::to_vec(&request).map_err(platform)?;
        if bytes.len() > MAX_CONTROL_BYTES {
            return Err(DesktopError::Invalid(
                "Android control message exceeds limit".into(),
            ));
        }
        self.0
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(platform)?;
        self.0.write_all(&bytes).map_err(platform)?;
        let mut header = [0; 4];
        self.0.read_exact(&mut header).map_err(platform)?;
        let length = u32::from_be_bytes(header) as usize;
        if length > MAX_CONTROL_BYTES {
            return Err(platform("Android control response exceeds limit"));
        }
        let mut bytes = vec![0; length];
        self.0.read_exact(&mut bytes).map_err(platform)?;
        let response: Value = serde_json::from_slice(&bytes).map_err(platform)?;
        if let Some(error) = response.get("error").and_then(Value::as_str) {
            return Err(match response.get("code").and_then(Value::as_str) {
                Some("permission_denied") => DesktopError::PermissionDenied(error.into()),
                Some("target_unavailable") => DesktopError::TargetUnavailable(error.into()),
                Some("unsupported") => DesktopError::Unsupported(error.into()),
                Some("invalid") => DesktopError::Invalid(error.into()),
                _ => platform(error),
            });
        }
        Ok(response)
    }
}
pub fn capabilities() -> DesktopCapabilities {
    let mut caps = DesktopCapabilities { frame_rate_ceiling: false, accessibility: false, capture_formats: vec![], platform: "android".into(), capture: false, keyboard_mouse: false, background_window_input: false, gamepad: "unsupported".into(), limitations: vec![
        "Raw mode only: Android does not provide background window message injection".into(),
        "Prefer persistent Accessibility observation and input. Screen video uses a separate MediaProjection grant".into(),
        "Left-button down/move/up submits a finite touch gesture on release; release_all discards an unfinished gesture".into(),
        "Text edits the focused accessible editor; raw key support is Escape/Back, Backspace and Enter; protected surfaces remain protected".into(),
    ] };
    match Bridge::connect().and_then(|mut b| b.call(json!({"op":"capabilities"}))) {
        Ok(value) => {
            caps.frame_rate_ceiling = value["frame_rate_ceiling"].as_bool().unwrap_or(false);
            caps.accessibility = value["accessibility"].as_bool().unwrap_or(false);
            caps.capture = value["capture"].as_bool().unwrap_or(false)
                && value["capture_protocol"].as_u64() == Some(3);
            caps.capture_formats =
                serde_json::from_value(value["capture_formats"].clone()).unwrap_or_default();
            caps.keyboard_mouse = value["input"].as_bool().unwrap_or(false);
        }
        Err(error) => caps.limitations.push(error.to_string()),
    }
    caps
}
pub fn enumerate_targets(windows: bool) -> DesktopResult<Vec<DesktopTargetInfo>> {
    if windows {
        return Err(DesktopError::Unsupported(
            "Android requires explicit raw mode".into(),
        ));
    }
    let response = Bridge::connect()?.call(json!({"op":"targets"}))?;
    serde_json::from_value(response["targets"].clone()).map_err(platform)
}
pub fn enumerate_child_windows(_: &DesktopTarget) -> DesktopResult<Vec<DesktopTargetInfo>> {
    Err(DesktopError::Unsupported(
        "Android background windows are unavailable".into(),
    ))
}
pub fn validate_target(target: &DesktopTarget) -> DesktopResult<()> {
    Bridge::connect()?
        .call(json!({"op":"validate", "target":target}))
        .map(|_| ())
}
pub fn initialize_input_thread() -> DesktopResult<()> {
    Ok(())
}
pub struct Input {
    bridge: Bridge,
    target: DesktopTarget,
}
impl Input {
    pub fn new(target: &DesktopTarget) -> DesktopResult<Self> {
        let mut bridge = Bridge::connect()?;
        bridge.call(json!({"op":"open_input", "target":target}))?;
        Ok(Self {
            bridge,
            target: target.clone(),
        })
    }
    pub fn send(&mut self, event: &DesktopInput) -> DesktopResult<()> {
        self.bridge
            .call(json!({"op":"input", "target":self.target, "event":event}))
            .map(|_| ())
    }
    pub fn cancel_pending(&mut self) {
        let _ = self.bridge.call(json!({"op":"release"}));
    }
    pub fn release_gamepad(&mut self) {}
}
impl Drop for Input {
    fn drop(&mut self) {
        self.cancel_pending();
    }
}

pub fn capture(
    config: DesktopCaptureConfig,
    permissions: LocalNodePermissionStore,
    mailbox: &CaptureMailbox,
    stop: mpsc::Receiver<()>,
    ready: mpsc::SyncSender<DesktopResult<()>>,
) -> DesktopResult<()> {
    let opened = Bridge::connect().and_then(|mut bridge| {
        let negotiated=bridge.call(json!({"op":"open_capture", "capture_protocol":3, "target":config.target,
            "include_cursor":config.include_cursor,"resolution":config.resolution,"pixel_format":config.pixel_format,"fps":config.frames_per_second}))?;
        if negotiated["capture_protocol"].as_u64()!=Some(3) { return Err(platform("Android capture bridge v3 required; update the application")); }
        Ok(bridge)
    });
    let mut bridge = match opened {
        Ok(bridge) => bridge,
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    let _ = ready.send(Ok(()));
    let started = Instant::now();
    let cadence = Duration::from_secs_f64(1.0 / f64::from(config.frames_per_second));
    let deadline = started + Duration::from_millis(config.lease_ms.min(300_000));
    let mut after = 0;
    while Instant::now() < deadline {
        if !mailbox.wait_for_request(Duration::from_millis(100)) {
            if stop.try_recv() != Err(mpsc::TryRecvError::Empty) {
                return Ok(());
            }
            continue;
        }
        authorize(&permissions, &config.target, "capture")?;
        let value = bridge.call(json!({"op":"frame", "target":config.target, "after":after,
            "wait_ms":100}))?;
        if value["available"].as_bool() == Some(true) {
            let format: NativeVideoSampleFormat =
                serde_json::from_value(value["format"].clone()).map_err(platform)?;
            if !matches!(
                format,
                NativeVideoSampleFormat::Rgba8
                    | NativeVideoSampleFormat::Bgra8
                    | NativeVideoSampleFormat::H264
            ) || config.pixel_format.is_some_and(|wanted| wanted != format)
            {
                return Err(platform("Android capture format negotiation mismatch"));
            }
            let number = |name: &str| {
                value[name]
                    .as_u64()
                    .ok_or_else(|| platform(format!("invalid Android frame {name}")))
            };
            let width = u32::try_from(number("width")?).map_err(platform)?;
            let height = u32::try_from(number("height")?).map_err(platform)?;
            let len = number("bytes")?;
            let native_width = u32::try_from(number("native_width")?).map_err(platform)?;
            let native_height = u32::try_from(number("native_height")?).map_err(platform)?;
            if width == 0
                || height == 0
                || len == 0
                || len > MAX_FRAME_BYTES as u64
                || (format != NativeVideoSampleFormat::H264
                    && len != u64::from(width) * u64::from(height) * 4)
            {
                return Err(platform("invalid Android frame dimensions"));
            }
            if let Some(bounds) = config.resolution {
                if width > bounds.width || height > bounds.height {
                    return Err(platform("Android capture exceeded negotiated resolution"));
                }
            }
            let sequence = number("sequence")?;
            if sequence <= after {
                return Err(platform("non-monotonic Android frame"));
            }
            let read_started = Instant::now();
            // Read into the final shared allocation: no Vec-to-Arc frame copy.
            // Read's safe API requires initialized storage, even though this
            // concrete socket reader only writes. Zero directly in the final
            // allocation rather than introducing a temporary Vec.
            // SAFETY: zero is a valid value for every u8 in this allocation.
            let mut bytes = unsafe { Arc::<[u8]>::new_zeroed_slice(len as usize).assume_init() };
            bridge
                .0
                .read_exact(Arc::get_mut(&mut bytes).expect("new capture allocation is unique"))
                .map_err(platform)?;
            let read_us = read_started.elapsed().as_micros();
            let publish_started = Instant::now();
            authorize(&permissions, &config.target, "capture")?;
            after = sequence;
            mailbox.publish(NativeVideoSample {
                native_dimensions: Some([native_width, native_height]),
                sequence,
                format,
                width,
                height,
                timestamp_ns: number("timestamp_ns")?,
                duration_ns: cadence.as_nanos() as u64,
                captured_at_unix_ms: u128::from(number("captured_at_unix_ms")?),
                data: bytes,
                planes: Arc::from([NativeVideoPlane {
                    offset: 0,
                    len: len as usize,
                    width,
                    height,
                    stride: if format == NativeVideoSampleFormat::H264 {
                        0
                    } else {
                        width * 4
                    },
                }]),
            });
            if frame_trace_enabled() {
                trace_frame(json!({"kind":"capture", "sequence":sequence,
                    "captured_at_unix_ms":number("captured_at_unix_ms")?,
                    "read_us":read_us, "normalize_us":0,
                    "publish_us":publish_started.elapsed().as_micros()}));
            }
        }
        // The OS bridge wakes on frame availability or its single FPS ceiling.
        // Timeouts above only bound cancellation/authority checks, never pacing.
        if stop.try_recv() != Err(mpsc::TryRecvError::Empty) {
            return Ok(());
        }
    }
    Ok(())
}

#[link(name = "log")]
unsafe extern "C" {
    fn __android_log_write(
        priority: libc::c_int,
        tag: *const libc::c_char,
        message: *const libc::c_char,
    ) -> libc::c_int;
}

/// Local, opt-in native diagnostics using the same Android log tag as capture.
pub(crate) fn frame_trace_enabled() -> bool {
    let mut value = [0 as libc::c_char; 92]; // Android PROP_VALUE_MAX.
    unsafe {
        libc::__system_property_get(c"log.tag.SinomoMediaStage".as_ptr(), value.as_mut_ptr());
    }
    matches!(value[0] as u8, b'V' | b'v')
}
pub(crate) fn trace_frame(value: serde_json::Value) {
    if let Ok(message) = std::ffi::CString::new(value.to_string()) {
        unsafe {
            __android_log_write(2, c"SinomoMediaStage".as_ptr(), message.as_ptr());
        }
    }
}

pub fn accessibility_observe(
    target: &DesktopTarget,
    max_nodes: u32,
) -> DesktopResult<accessibility::AccessibilitySnapshot> {
    let response =
        Bridge::connect()?.call(json!({"op":"observe", "target":target,"max_nodes":max_nodes}))?;
    serde_json::from_value(response["snapshot"].clone()).map_err(platform)
}
pub fn accessibility_act(
    target: &DesktopTarget,
    snapshot_id: &str,
    node_id: &str,
    action: &accessibility::AccessibilityAction,
) -> DesktopResult<()> {
    Bridge::connect()?.call(json!({"op":"act","target":target,"snapshot_id":snapshot_id,"node_id":node_id,"action":action})).map(|_| ())
}
pub fn video_targets() -> DesktopResult<Vec<DesktopTargetInfo>> {
    let response = Bridge::connect()?.call(json!({"op":"video_targets"}))?;
    serde_json::from_value(response["targets"].clone()).map_err(platform)
}
pub fn stop_video(target: &DesktopTarget) -> DesktopResult<()> {
    Bridge::connect()?
        .call(json!({"op":"stop_video","target":target}))
        .map(|_| ())
}
