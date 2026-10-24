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
            return Err(DesktopError::Unsupported("enable Sinomo App Use in Android Accessibility settings and start screen sharing from its local consent activity".into()));
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
    let mut caps = DesktopCapabilities { platform: "android".into(), capture: false, keyboard_mouse: false, background_window_input: false, gamepad: "unsupported".into(), limitations: vec![
        "Raw mode only: Android does not provide background window message injection".into(),
        "Screen capture requires a local MediaProjection grant; input requires a user-enabled AccessibilityService".into(),
        "Left-button down/move/up submits a finite touch gesture on release; release_all discards an unfinished gesture".into(),
        "Text edits the focused accessible editor; raw key support is Escape/Back, Backspace and Enter; protected surfaces remain protected".into(),
    ] };
    match Bridge::connect().and_then(|mut b| b.call(json!({"op":"capabilities"}))) {
        Ok(value) => {
            caps.capture = value["capture"].as_bool().unwrap_or(false);
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
        bridge.call(json!({"op":"open_capture", "target":config.target, "include_cursor":config.include_cursor}))?;
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
        authorize(&permissions, &config.target, "capture")?;
        let value = bridge.call(json!({"op":"frame", "target":config.target, "after":after}))?;
        if value["available"].as_bool() == Some(true) {
            if value["format"].as_str() != Some("rgba8") {
                return Err(platform("unsupported Android frame format"));
            }
            let number = |name: &str| {
                value[name]
                    .as_u64()
                    .ok_or_else(|| platform(format!("invalid Android frame {name}")))
            };
            let width = u32::try_from(number("width")?).map_err(platform)?;
            let height = u32::try_from(number("height")?).map_err(platform)?;
            let len = u64::from(width) * u64::from(height) * 4;
            if width == 0 || height == 0 || len > MAX_FRAME_BYTES as u64 || number("bytes")? != len
            {
                return Err(platform("invalid Android frame dimensions"));
            }
            let sequence = number("sequence")?;
            if sequence <= after {
                return Err(platform("non-monotonic Android frame"));
            }
            let mut bytes = vec![0; len as usize];
            bridge.0.read_exact(&mut bytes).map_err(platform)?;
            // Keep the shared native-media contract BGRA; swizzle after the
            // bounded row copy rather than per-byte JNI/Java pixel operations.
            for pixel in bytes.chunks_exact_mut(4) {
                pixel.swap(0, 2);
                pixel[3] = 255;
            }
            authorize(&permissions, &config.target, "capture")?;
            after = sequence;
            mailbox.publish(NativeVideoSample {
                sequence,
                format: NativeVideoSampleFormat::Bgra8,
                width,
                height,
                timestamp_ns: number("timestamp_ns")?,
                duration_ns: cadence.as_nanos() as u64,
                captured_at_unix_ms: u128::from(number("captured_at_unix_ms")?),
                data: bytes.into(),
                planes: Arc::from([NativeVideoPlane {
                    offset: 0,
                    len: len as usize,
                    width,
                    height,
                    stride: width * 4,
                }]),
            });
        }
        match stop.recv_timeout(cadence.min(Duration::from_millis(100))) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
    Ok(())
}
