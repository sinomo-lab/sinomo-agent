//! Versioned OS boundary using the native module SOBJ ABI. Continuous capture
//! preserves sample format and native geometry through an explicitly versioned contract.
use super::surfaces::*;
use super::*;
use crate::{NativeVideoPlane, NativeVideoSampleFormat};
use std::sync::Arc;

pub const APP_USE_OS_MODULE_ID: &str = "module.app-use.os";
pub const APP_USE_OS_INTERFACE: &str = "sinomo.app-use.os";
pub const APP_USE_OS_VERSION: &str = "2.0.0";
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
    SurfaceCapabilities,
    Applications,
    OpenApplication {
        application: DesktopApplication,
        lease_ms: u64,
    },
    ApplicationSurfaces {
        application_id: String,
    },
    CloseApplication {
        application_id: String,
    },
    ResolveApplicationWindow {
        binding: ApplicationWindowBinding,
        operation: String,
    },
    OpenApplicationInput {
        binding: ApplicationWindowBinding,
        lease_ms: u64,
    },
    OpenApplicationCapture {
        binding: ApplicationWindowBinding,
        config: DesktopCaptureConfig,
    },
    TrayItems,
    InvokeTray {
        item_id: String,
        operation: DesktopSemanticAction,
        allow_desktop_interaction: bool,
    },
    MenuItems {
        binding: ApplicationWindowBinding,
    },
    InvokeMenuItem {
        binding: ApplicationWindowBinding,
        item_id: String,
        allow_desktop_interaction: bool,
    },
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
    SurfaceCapabilities {
        capabilities: DesktopSurfaceCapabilities,
    },
    Applications {
        applications: Vec<DesktopApplicationInfo>,
    },
    ApplicationOpened {
        application_id: String,
        snapshot: DesktopApplicationSnapshot,
    },
    ApplicationSurfaces {
        snapshot: DesktopApplicationSnapshot,
    },
    ApplicationWindow {
        window: DesktopWindowSurface,
    },
    SemanticItems {
        snapshot: DesktopSemanticSnapshot,
    },
    SemanticInvoked,
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
    pub pixel_format: NativeVideoSampleFormat,
    pub native_width: u32,
    pub native_height: u32,
    pub sequence: u64,
    pub width: u32,
    pub height: u32,
    pub stride: u32,
    pub timestamp_ns: u64,
    pub duration_ns: u64,
    pub captured_at_unix_ms: u64,
    #[serde(with = "shared_bytes")]
    pub pixels: Arc<[u8]>,
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
        if !matches!(
            sample.format,
            NativeVideoSampleFormat::Bgra8
                | NativeVideoSampleFormat::Rgba8
                | NativeVideoSampleFormat::H264
        ) || sample.planes.len() != 1
            || sample.planes[0].offset != 0
            || sample.data.len() > MAX_FRAME_BYTES
        {
            return Err(DesktopError::Invalid(
                "OS provider requires one bounded BGRA, RGBA or H264 plane".into(),
            ));
        }
        let [native_width, native_height] = sample.native_dimensions();
        Ok(Self {
            pixel_format: sample.format,
            native_width,
            native_height,
            sequence: sample.sequence,
            width: sample.width,
            height: sample.height,
            stride: sample.planes[0].stride,
            timestamp_ns: sample.timestamp_ns,
            duration_ns: sample.duration_ns,
            captured_at_unix_ms: u64::try_from(sample.captured_at_unix_ms).map_err(|_| {
                DesktopError::Invalid("capture timestamp exceeds the native wire range".into())
            })?,
            pixels: sample.data,
        })
    }
    pub fn into_sample(self) -> DesktopResult<NativeVideoSample> {
        if self.pixels.len() > MAX_FRAME_BYTES {
            return Err(DesktopError::Invalid("OS frame exceeds limit".into()));
        }
        let sample = NativeVideoSample {
            native_dimensions: Some([self.native_width, self.native_height]),
            sequence: self.sequence,
            format: self.pixel_format,
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
            data: self.pixels,
        };
        sample
            .validate()
            .map_err(|e| DesktopError::Invalid(e.to_string()))?;
        Ok(sample)
    }
}

// Decode directly into host-owned shared storage; do not allocate an intermediate
// Vec and copy it again when handing the frame to the video pipeline.
mod shared_bytes {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        bytes: &Arc<[u8]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(bytes)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Arc<[u8]>, D::Error> {
        struct Bytes;
        impl<'de> serde::de::Visitor<'de> for Bytes {
            type Value = Arc<[u8]>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("bounded native frame bytes")
            }
            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
                if bytes.len() > MAX_FRAME_BYTES {
                    return Err(E::custom("native frame exceeds limit"));
                }
                Ok(Arc::from(bytes))
            }
            fn visit_byte_buf<E: serde::de::Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
                self.visit_bytes(&bytes)
            }
        }
        deserializer.deserialize_bytes(Bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_frame_handoff_preserves_shared_storage_format_and_geometry() {
        for format in [
            NativeVideoSampleFormat::Rgba8,
            NativeVideoSampleFormat::Bgra8,
            NativeVideoSampleFormat::H264,
        ] {
            let data: Arc<[u8]> = Arc::from([1, 2, 3, 4, 5, 6, 7, 8]);
            let frame = DesktopRawFrame {
                pixel_format: format,
                native_width: 1080,
                native_height: 2280,
                sequence: 4,
                width: 2,
                height: 1,
                stride: if format == NativeVideoSampleFormat::H264 {
                    0
                } else {
                    8
                },
                timestamp_ns: 5,
                duration_ns: 6,
                captured_at_unix_ms: 7,
                pixels: data.clone(),
            };
            let sample = frame.into_sample().unwrap();
            assert!(Arc::ptr_eq(&data, &sample.data));
            assert_eq!(sample.native_dimensions(), [1080, 2280]);
            assert_eq!(sample.format, format);
            let frame = DesktopRawFrame::from_sample(sample).unwrap();
            assert!(Arc::ptr_eq(&data, &frame.pixels));
            assert_eq!((frame.width, frame.height), (2, 1));
        }
    }
}
