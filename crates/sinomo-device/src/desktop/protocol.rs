//! Versioned OS boundary using the native module SOBJ ABI. Continuous capture
//! returns raw BGRA samples for the common media encoder, never encoded images.
use super::*;
use crate::{NativeVideoPlane, NativeVideoSampleFormat};
use std::sync::Arc;

pub const APP_USE_OS_MODULE_ID: &str = "module.app-use.os";
pub const APP_USE_OS_INTERFACE: &str = "sinomo.app-use.os";
pub const APP_USE_OS_VERSION: &str = "1.0.0";
pub const APP_USE_OS_OPERATION: &str = "app-use.os";
pub const APP_USE_OS_SERVICE: &str = "app-use-os";
pub const APP_USE_OS_CACHE_KEY: &str = "module/desktop/app-use";
pub fn permission_store_capability(path: &str) -> String {
    format!("app-use.permission-store:{path}")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DesktopProviderRequest {
    /// Bound by a live host callback before opening this permission store.
    pub permission_store: String,
    pub command: DesktopProviderCommand,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum DesktopProviderCommand {
    Capabilities,
    Targets {
        mode: DesktopControlMode,
    },
    Children {
        target: DesktopTarget,
    },
    OpenInput {
        target: DesktopTarget,
        mode: DesktopControlMode,
        lease_ms: u64,
    },
    Input {
        id: String,
        events: Vec<DesktopInput>,
    },
    CloseInput {
        id: String,
    },
    OpenCapture {
        config: DesktopCaptureConfig,
    },
    Frame {
        id: String,
        after_sequence: u64,
        timeout_ms: u64,
    },
    CloseCapture {
        id: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DesktopProviderResponse {
    Capabilities {
        capabilities: DesktopCapabilities,
    },
    Targets {
        targets: Vec<DesktopTargetInfo>,
    },
    Opened {
        id: String,
    },
    Applied {
        events: usize,
    },
    /// Raw pixels travel through the native ABI's bounded output callback;
    /// the ordinary response stays small instead of nesting a byte sequence.
    Frame {
        available: bool,
    },
    Closed,
    Failed {
        error: DesktopError,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DesktopRawFrame {
    pub sequence: u64,
    pub width: u32,
    pub height: u32,
    pub stride: u32,
    pub timestamp_ns: u64,
    pub duration_ns: u64,
    pub captured_at_unix_ms: u64,
    #[serde(with = "serde_bytes")]
    pub pixels: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DesktopFrameDelivery {
    pub capture_id: String,
    pub frame: DesktopRawFrame,
}
impl DesktopRawFrame {
    pub fn from_sample(sample: NativeVideoSample) -> DesktopResult<Self> {
        sample
            .validate()
            .map_err(|e| DesktopError::Platform(e.to_string()))?;
        if sample.format != NativeVideoSampleFormat::Bgra8
            || sample.planes.len() != 1
            || sample.planes[0].offset != 0
            || sample.data.len() > MAX_FRAME_BYTES
        {
            return Err(DesktopError::Invalid(
                "OS provider requires one bounded BGRA plane".into(),
            ));
        }
        Ok(Self {
            sequence: sample.sequence,
            width: sample.width,
            height: sample.height,
            stride: sample.planes[0].stride,
            timestamp_ns: sample.timestamp_ns,
            duration_ns: sample.duration_ns,
            captured_at_unix_ms: u64::try_from(sample.captured_at_unix_ms).map_err(|_| {
                DesktopError::Invalid("capture timestamp exceeds the native wire range".into())
            })?,
            pixels: sample.data.to_vec(),
        })
    }
    pub fn into_sample(self) -> DesktopResult<NativeVideoSample> {
        if self.pixels.len() > MAX_FRAME_BYTES {
            return Err(DesktopError::Invalid("OS frame exceeds limit".into()));
        }
        let sample = NativeVideoSample {
            sequence: self.sequence,
            format: NativeVideoSampleFormat::Bgra8,
            width: self.width,
            height: self.height,
            timestamp_ns: self.timestamp_ns,
            duration_ns: self.duration_ns,
            captured_at_unix_ms: u128::from(self.captured_at_unix_ms),
            planes: Arc::from([NativeVideoPlane {
                offset: 0,
                len: self.pixels.len(),
                width: self.width,
                height: self.height,
                stride: self.stride,
            }]),
            data: self.pixels.into(),
        };
        sample
            .validate()
            .map_err(|e| DesktopError::Invalid(e.to_string()))?;
        Ok(sample)
    }
}
