//! Platform-neutral app-use contracts. Native OS integration lives in the
//! independently loaded modules/desktop/app-use provider.
use crate::NativeVideoSample;
use serde::{Deserialize, Serialize};
use sinomo_core::node::LocalNodePermissionStore;
use std::time::Duration;
pub mod protocol;
pub mod surfaces;

/// Background messages are preferred. Raw input is an explicit fallback on a
/// monitor target and can affect the user's pointer, keyboard and foreground.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DesktopControlMode {
    #[default]
    Background,
    Raw,
}
impl DesktopControlMode {
    pub fn validate_target(self, target: &DesktopTarget) -> DesktopResult<()> {
        if target.is_window() != (self == Self::Background) {
            return Err(DesktopError::Invalid("background mode requires a window; raw mode requires a monitor and explicit raw selection".into()));
        }
        Ok(())
    }
}

pub const MAX_INPUT_EVENTS: usize = 256;
pub const MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;
pub const MAX_TARGETS: usize = 1024;

#[derive(Clone, Debug, thiserror::Error, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "code", content = "detail", rename_all = "snake_case")]
pub enum DesktopError {
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("desktop interaction required: {0}")]
    InteractionRequired(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("target unavailable: {0}")]
    TargetUnavailable(String),
    #[error("platform operation failed: {0}")]
    Platform(String),
    #[error("{applied} input events applied before failure: {detail}; do not replay the batch")]
    PartialInput { applied: usize, detail: String },
    #[error("capture closed: {0}")]
    Closed(String),
}
pub type DesktopResult<T> = Result<T, DesktopError>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DesktopTarget {
    Monitor {
        device_name: String,
    },
    Window {
        handle: u64,
        process_id: u32,
        thread_id: u32,
        process_started: u64,
    },
}
impl DesktopTarget {
    pub fn resource_id(&self) -> String {
        match self {
            Self::Monitor { device_name } => format!("display:{device_name}"),
            Self::Window {
                handle,
                process_id,
                process_started,
                ..
            } => format!("window:{process_id}:{process_started}:{handle}"),
        }
    }
    pub fn is_window(&self) -> bool {
        matches!(self, Self::Window { .. })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopTargetInfo {
    pub target: DesktopTarget,
    pub resource_id: String,
    pub title: String,
    /// Target coordinates: physical screen pixels on Windows/X11, stream-local
    /// portal coordinates on Wayland. App input uses client pixels.
    pub bounds: [i32; 4],
    /// Physical screen rectangle of the client area, for mapping captured
    /// whole-window pixels to app input coordinates. Absent for monitors.
    pub client_bounds: Option<[i32; 4]>,
    pub minimized: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopCapabilities {
    pub platform: String,
    pub capture: bool,
    pub keyboard_mouse: bool,
    pub background_window_input: bool,
    pub gamepad: String,
    pub limitations: Vec<String>,
}
pub fn authorize_host(store: &LocalNodePermissionStore) -> DesktopResult<()> {
    let permission = store
        .load()
        .map_err(|e| DesktopError::PermissionDenied(e.to_string()))?;
    if !permission.remote_management_enabled() {
        return Err(DesktopError::PermissionDenied(
            "node-local remote-management consent is required".into(),
        ));
    }
    Ok(())
}
pub fn authorize(
    store: &LocalNodePermissionStore,
    target: &DesktopTarget,
    operation: &str,
) -> DesktopResult<()> {
    authorize_resource(store, &target.resource_id(), operation)
}
pub fn authorize_resource(
    store: &LocalNodePermissionStore,
    resource: &str,
    operation: &str,
) -> DesktopResult<()> {
    let permission = store
        .load()
        .map_err(|e| DesktopError::PermissionDenied(e.to_string()))?;
    if !permission.remote_management_enabled()
        || !permission.resource_operation_exposed(resource, operation)
    {
        return Err(DesktopError::PermissionDenied(format!(
            "remote-management and {} / {operation} exposure are required",
            resource
        )));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum MouseButton {
    Left,
    Middle,
    Right,
    X1,
    X2,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DesktopInput {
    /// Set-1 scan code for raw mode; virtual-key code for background mode.
    Key {
        code: u16,
        down: bool,
        #[serde(default)]
        extended: bool,
    },
    Text {
        text: String,
    },
    /// Raw coordinates are physical desktop pixels, or portal-advertised stream
    /// coordinates on Wayland. Background mode uses client pixels.
    Move {
        x: i32,
        y: i32,
    },
    /// App use: fractions [0,1] of the entire captured window (including chrome).
    /// Source dimensions, before encoding, guard against a stale resized frame.
    MoveInFrame {
        x: f64,
        y: f64,
        source_width: u32,
        source_height: u32,
    },
    RelativeMove {
        dx: i32,
        dy: i32,
    },
    Button {
        button: MouseButton,
        down: bool,
    },
    Wheel {
        delta: i16,
        #[serde(default)]
        horizontal: bool,
    },
    /// Complete state; axes [-1,1], triggers [0,1], Windows GamepadButtons mask.
    Gamepad {
        buttons: u32,
        left_x: f64,
        left_y: f64,
        right_x: f64,
        right_y: f64,
        left_trigger: f64,
        right_trigger: f64,
    },
    ReleaseAll,
}
pub fn validate_inputs(target: &DesktopTarget, events: &[DesktopInput]) -> DesktopResult<()> {
    if events.is_empty() || events.len() > MAX_INPUT_EVENTS {
        return Err(DesktopError::Invalid(
            "input batch must contain 1..256 events".into(),
        ));
    }
    for event in events {
        match event {
            DesktopInput::MoveInFrame {
                x,
                y,
                source_width,
                source_height,
            } => {
                if !target.is_window() {
                    return Err(DesktopError::Unsupported(
                        "frame-relative input requires an app window".into(),
                    ));
                }
                validate_frame_point(*x, *y, [*source_width, *source_height])?;
            }
            DesktopInput::Key { code, .. } if *code == 0 || *code > 255 => {
                return Err(DesktopError::Invalid("key code must be 1..255".into()));
            }
            DesktopInput::Text { text }
                if text.is_empty() || text.encode_utf16().count() > 4096 =>
            {
                return Err(DesktopError::Invalid(
                    "text must contain 1..4096 UTF-16 units".into(),
                ));
            }
            DesktopInput::Move { x, y }
                if target.is_window()
                    && (i16::try_from(*x).is_err() || i16::try_from(*y).is_err()) =>
            {
                return Err(DesktopError::Invalid(
                    "app client coordinates must fit signed 16 bits".into(),
                ));
            }
            DesktopInput::RelativeMove { .. } | DesktopInput::Gamepad { .. }
                if target.is_window() =>
            {
                return Err(DesktopError::Unsupported(
                    "app use supports window messages, not relative hardware motion or gamepads"
                        .into(),
                ));
            }
            DesktopInput::Gamepad {
                buttons,
                left_x,
                left_y,
                right_x,
                right_y,
                left_trigger,
                right_trigger,
            } if *buttons & !0x3fff != 0
                || [left_x, left_y, right_x, right_y]
                    .into_iter()
                    .any(|v| !(-1.0..=1.0).contains(v))
                || [left_trigger, right_trigger]
                    .into_iter()
                    .any(|v| !(0.0..=1.0).contains(v)) =>
            {
                return Err(DesktopError::Invalid(
                    "invalid gamepad button mask or axes".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_frame_point(x: f64, y: f64, source: [u32; 2]) -> DesktopResult<()> {
    if !(0.0..=1.0).contains(&x)
        || !(0.0..=1.0).contains(&y)
        || source.contains(&0)
        || u64::from(source[0]) * u64::from(source[1]) > (MAX_FRAME_BYTES / 4) as u64
    {
        return Err(DesktopError::Invalid(
            "frame coordinates must be finite fractions in [0,1] with valid source dimensions"
                .into(),
        ));
    }
    Ok(())
}

/// Map fractions of a full-window image to physical client pixels. Encoding or
/// model-side aspect-preserving resizing does not change these fractions. Keep
/// geometry at the device boundary instead of asking the model to subtract
/// screen origins, decoration sizes, or DPI offsets.
pub fn frame_point_to_client(
    x: f64,
    y: f64,
    source: [u32; 2],
    bounds: [i32; 4],
    client: [i32; 4],
) -> DesktopResult<(i32, i32)> {
    validate_frame_point(x, y, source)?;
    let width = i64::from(bounds[2]) - i64::from(bounds[0]);
    let height = i64::from(bounds[3]) - i64::from(bounds[1]);
    if width != i64::from(source[0]) || height != i64::from(source[1]) {
        return Err(DesktopError::TargetUnavailable(format!(
            "window geometry changed: frame {}x{}, current {width}x{height}; observe a fresh frame before input",
            source[0], source[1]
        )));
    }
    let screen_x = i64::from(bounds[0]) + (x * f64::from(source[0] - 1)).round() as i64;
    let screen_y = i64::from(bounds[1]) + (y * f64::from(source[1] - 1)).round() as i64;
    if screen_x < i64::from(client[0])
        || screen_x >= i64::from(client[2])
        || screen_y < i64::from(client[1])
        || screen_y >= i64::from(client[3])
    {
        return Err(DesktopError::Invalid(
            "frame point is outside the window client area; app input cannot click window chrome"
                .into(),
        ));
    }
    let x = i16::try_from(screen_x - i64::from(client[0]));
    let y = i16::try_from(screen_y - i64::from(client[1]));
    match (x, y) {
        (Ok(x), Ok(y)) => Ok((i32::from(x), i32::from(y))),
        _ => Err(DesktopError::Invalid(
            "app client coordinates must fit signed 16 bits".into(),
        )),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesktopCaptureConfig {
    pub target: DesktopTarget,
    #[serde(default = "default_fps")]
    pub frames_per_second: u32,
    #[serde(default = "default_cursor")]
    pub include_cursor: bool,
    #[serde(default = "default_capture_lease_ms")]
    pub lease_ms: u64,
}
fn default_capture_lease_ms() -> u64 {
    300_000
}
fn default_fps() -> u32 {
    30
}
fn default_cursor() -> bool {
    true
}

/// Continuous native samples consumed by the shared media encoder.
pub trait DesktopFrameSource: Send {
    fn identity(&self) -> Option<surfaces::DesktopCaptureIdentity> {
        None
    }
    fn tiles(&self) -> Vec<surfaces::DesktopOverviewTile> {
        Vec::new()
    }
    fn next(
        &self,
        after_sequence: u64,
        timeout: Duration,
    ) -> DesktopResult<Option<NativeVideoSample>>;
}
impl<T: DesktopFrameSource + ?Sized> DesktopFrameSource for Box<T> {
    fn identity(&self) -> Option<surfaces::DesktopCaptureIdentity> {
        (**self).identity()
    }
    fn tiles(&self) -> Vec<surfaces::DesktopOverviewTile> {
        (**self).tiles()
    }
    fn next(
        &self,
        after_sequence: u64,
        timeout: Duration,
    ) -> DesktopResult<Option<NativeVideoSample>> {
        (**self).next(after_sequence, timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn background_is_default_and_raw_cannot_be_implicit() {
        assert_eq!(
            DesktopControlMode::default(),
            DesktopControlMode::Background
        );
        let monitor = DesktopTarget::Monitor {
            device_name: "test".into(),
        };
        assert!(
            DesktopControlMode::Background
                .validate_target(&monitor)
                .is_err()
        );
        assert!(DesktopControlMode::Raw.validate_target(&monitor).is_ok());
    }
    #[test]
    fn frame_coordinates_map_chrome_and_nonzero_origins_without_encoded_size() {
        // Whole-window image point (200, 200), client begins at (2, 45).
        for (left, top) in [(229, 110), (-1800, -400)] {
            assert_eq!(
                frame_point_to_client(
                    200.0 / 911.0,
                    200.0 / 680.0,
                    [912, 681],
                    [left, top, left + 912, top + 681],
                    [left + 2, top + 45, left + 910, top + 679],
                )
                .unwrap(),
                (198, 155)
            );
        }
        // The same fractions work when source pixels are twice as dense, even
        // if shared media later encodes that source at a smaller resolution.
        assert_eq!(
            frame_point_to_client(
                0.5,
                0.5,
                [1801, 1201],
                [0, 0, 1801, 1201],
                [4, 90, 1797, 1197]
            )
            .unwrap(),
            (896, 510)
        );
    }

    #[test]
    fn frame_coordinates_reject_stale_geometry_chrome_and_invalid_values() {
        let map =
            |x, y, size| frame_point_to_client(x, y, size, [0, 0, 912, 681], [2, 45, 910, 679]);
        assert!(matches!(
            map(0.5, 0.5, [1132, 781]),
            Err(DesktopError::TargetUnavailable(_))
        ));
        for (x, y, size) in [
            (0.5, 0.01, [912, 681]),
            (1.0, 0.5, [912, 681]),
            (f64::NAN, 0.5, [912, 681]),
            (0.5, f64::INFINITY, [912, 681]),
            (-0.1, 0.5, [912, 681]),
            (0.5, 1.01, [912, 681]),
            (0.5, 0.5, [0, 681]),
            (0.5, 0.5, [u32::MAX, u32::MAX]),
        ] {
            assert!(map(x, y, size).is_err());
        }
        let event = DesktopInput::MoveInFrame {
            x: 0.5,
            y: 0.5,
            source_width: 912,
            source_height: 681,
        };
        assert!(matches!(
            validate_inputs(
                &DesktopTarget::Monitor {
                    device_name: "test".into()
                },
                &[event]
            ),
            Err(DesktopError::Unsupported(_))
        ));
    }
    #[test]
    fn input_validation_rejects_partial_invalid_batches_and_hardware_app_events() {
        let target = DesktopTarget::Window {
            handle: 1,
            process_id: 1,
            thread_id: 1,
            process_started: 1,
        };
        assert!(
            validate_inputs(
                &target,
                &[
                    DesktopInput::Text { text: "ok".into() },
                    DesktopInput::Key {
                        code: 0,
                        down: true,
                        extended: false
                    }
                ]
            )
            .is_err()
        );
        assert!(validate_inputs(&target, &[DesktopInput::RelativeMove { dx: 1, dy: 1 }]).is_err());
        assert!(validate_inputs(&target, &[DesktopInput::Move { x: 32768, y: 0 }]).is_err());
    }
}
