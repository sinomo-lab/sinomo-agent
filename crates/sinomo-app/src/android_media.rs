//! Local user-initiated Android dialogs. Remote device requests never call this adapter.
use jni::{
    jni_sig, jni_str,
    objects::{JObject, JString},
    refs::Global,
};
use std::sync::OnceLock;

static BRIDGE: OnceLock<(jni::JavaVM, Global<JObject<'static>>)> = OnceLock::new();

pub(crate) fn install(vm: jni::JavaVM, callback: Global<JObject<'static>>) {
    let _ = BRIDGE.set((vm, callback));
    sinomo_runtime::device::install_android_camera(open_camera);
    sinomo_video::codec::install_h264_encoder(open_video_encoder);
    sinomo_runtime::device::sensors::install_sensor_reader(read_sensors);
}

/// Runs on a media/picker worker while Android owns the permission dialog.
pub(crate) fn request(operation: &str) -> Result<String, String> {
    let (vm, callback) = BRIDGE
        .get()
        .ok_or("Android media UI is not ready; retry shortly")?;
    let result = vm
        .attach_current_thread(|env| -> jni::errors::Result<String> {
            let operation = env.new_string(operation)?;
            let value = env
                .call_method(
                    callback,
                    jni_str!("mediaRequest"),
                    jni_sig!("(Ljava/lang/String;)Ljava/lang/String;"),
                    &[(&operation).into()],
                )?
                .l()?;
            JString::cast_local(env, value)?.try_to_string(env)
        })
        .map_err(|error| format!("Android media request failed: {error}"))?;
    if let Some(error) = result.strip_prefix("error:") {
        Err(error.to_string())
    } else {
        Ok(result)
    }
}

use sinomo_runtime::device::{
    AndroidCameraSession, NativeCameraCaptureConfig, NativeDeviceError, NativeVideoPlane,
    NativeVideoSample, NativeVideoSampleFormat,
};

fn camera_error(error: impl std::fmt::Display) -> NativeDeviceError {
    NativeDeviceError::Camera {
        detail: error.to_string(),
    }
}

pub(crate) fn open_camera(
    config: NativeCameraCaptureConfig,
) -> Result<Box<dyn AndroidCameraSession>, NativeDeviceError> {
    let (vm, callback) = BRIDGE
        .get()
        .ok_or_else(|| camera_error("Android media host unavailable"))?;
    let object = vm.attach_current_thread(|env| -> jni::errors::Result<_> {
        let selector = env.new_string(config.device.as_deref().unwrap_or(""))?;
        let object = env.call_method(callback, jni_str!("openCamera"),
            jni_sig!("(Ljava/lang/String;II)Lnet/sinomo/app/runtime/SinomoLifecycleCallbacks$CameraStream;"),
            &[(&selector).into(), (config.width.clamp(2, 1920) as i32).into(), (config.height.clamp(2, 1920) as i32).into()])?.l()?;
        env.new_global_ref(object)
    }).map_err(camera_error)?;
    Ok(Box::new(Camera {
        object,
        sequence: 0,
        started: std::time::Instant::now(),
        duration_ns: config.frame_interval_ms.max(1) * 1_000_000,
    }))
}

struct Camera {
    object: Global<JObject<'static>>,
    sequence: u64,
    started: std::time::Instant,
    duration_ns: u64,
}
impl AndroidCameraSession for Camera {
    fn next_sample(&mut self) -> Result<Option<NativeVideoSample>, NativeDeviceError> {
        let (vm, _) = BRIDGE
            .get()
            .ok_or_else(|| camera_error("Android media host unavailable"))?;
        let packet = vm
            .attach_current_thread(|env| -> jni::errors::Result<_> {
                let value = env
                    .call_method(&self.object, jni_str!("next"), jni_sig!("()[B"), &[])?
                    .l()?;
                if value.is_null() {
                    return Ok(None);
                }
                let bytes = jni::objects::JByteArray::cast_local(env, value)?;
                env.convert_byte_array(&bytes).map(Some)
            })
            .map_err(camera_error)?;
        let Some(packet) = packet else {
            return Ok(None);
        };
        if packet.len() < 8 {
            return Err(camera_error("Truncated Android camera frame"));
        }
        let width = u32::from_be_bytes(packet[0..4].try_into().unwrap());
        let height = u32::from_be_bytes(packet[4..8].try_into().unwrap());
        if width == 0
            || height == 0
            || width > 1920
            || height > 1920
            || width % 2 != 0
            || height % 2 != 0
        {
            return Err(camera_error("Invalid Android camera dimensions"));
        }
        let y_len = (width * height) as usize;
        if packet.len() != 8 + y_len * 3 / 2 {
            return Err(camera_error("Invalid Android camera frame length"));
        }
        let sample = NativeVideoSample {
            sequence: self.sequence,
            format: NativeVideoSampleFormat::Nv12,
            width,
            height,
            timestamp_ns: self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            duration_ns: self.duration_ns,
            captured_at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            data: packet[8..].to_vec().into(),
            planes: vec![
                NativeVideoPlane {
                    offset: 0,
                    len: y_len,
                    width,
                    height,
                    stride: width,
                },
                NativeVideoPlane {
                    offset: y_len,
                    len: y_len / 2,
                    width: width / 2,
                    height: height / 2,
                    stride: width,
                },
            ]
            .into(),
        };
        sample.validate()?;
        self.sequence += 1;
        Ok(Some(sample))
    }
}
impl Drop for Camera {
    fn drop(&mut self) {
        if let Some((vm, _)) = BRIDGE.get() {
            let _ = vm.attach_current_thread(|env| {
                env.call_method(&self.object, jni_str!("close"), jni_sig!("()V"), &[])
                    .map(|_| ())
            });
        }
    }
}

fn read_sensors() -> Result<Vec<sinomo_runtime::device::sensors::SensorReading>, String> {
    let (vm, callback) = BRIDGE.get().ok_or("Android media host unavailable")?;
    let json = vm
        .attach_current_thread(|env| -> jni::errors::Result<String> {
            let value = env
                .call_method(
                    callback,
                    jni_str!("readSensors"),
                    jni_sig!("()Ljava/lang/String;"),
                    &[],
                )?
                .l()?;
            JString::cast_local(env, value)?.try_to_string(env)
        })
        .map_err(|error| error.to_string())?;
    serde_json::from_str(&json).map_err(|error| error.to_string())
}

fn open_video_encoder(
    width: u32,
    height: u32,
    fps: u32,
) -> Result<Box<dyn sinomo_video::codec::VideoEncoder>, String> {
    let (vm, callback) = BRIDGE.get().ok_or("Android media host unavailable")?;
    let object = vm
        .attach_current_thread(|env| -> jni::errors::Result<_> {
            let object = env
                .call_method(
                    callback,
                    jni_str!("openVideoEncoder"),
                    jni_sig!("(III)Lnet/sinomo/app/runtime/SinomoVideoEncoder;"),
                    &[
                        (width as i32).into(),
                        (height as i32).into(),
                        (fps as i32).into(),
                    ],
                )?
                .l()?;
            env.new_global_ref(object)
        })
        .map_err(|e| e.to_string())?;
    Ok(Box::new(VideoEncoder { object }))
}
struct VideoEncoder {
    object: Global<JObject<'static>>,
}
impl sinomo_video::codec::VideoEncoder for VideoEncoder {
    fn encode(&mut self, frame: &sinomo_lm::RealtimeVideoFrameInput) -> Result<Vec<u8>, String> {
        use sinomo_lm::RealtimeVideoPixelFormat;
        let (vm, _) = BRIDGE.get().ok_or("Android media host unavailable")?;
        let pixel_format = match frame.pixel_format {
            RealtimeVideoPixelFormat::Rgb8 => 0,
            RealtimeVideoPixelFormat::Rgba8 => 1,
            RealtimeVideoPixelFormat::Bgra8 => 2,
        };
        let row = frame.width as usize * frame.pixel_format.bytes_per_pixel();
        if frame.stride < row || frame.pixels.len() < frame.stride * frame.height as usize {
            return Err("invalid native video stride/length".into());
        }
        let packed;
        let pixels = if frame.stride == row {
            &frame.pixels[..row * frame.height as usize]
        } else {
            packed = frame
                .pixels
                .chunks(frame.stride)
                .take(frame.height as usize)
                .flat_map(|row_bytes| row_bytes[..row].iter().copied())
                .collect::<Vec<_>>();
            &packed
        };
        vm.attach_current_thread(|env| -> jni::errors::Result<_> {
            // Java only reads this buffer synchronously during texture upload;
            // the immutable source stays alive until the call returns.
            let buffer =
                unsafe { env.new_direct_byte_buffer(pixels.as_ptr() as *mut u8, pixels.len())? };
            let result = env
                .call_method(
                    &self.object,
                    jni_str!("encode"),
                    jni_sig!("(Ljava/nio/ByteBuffer;III)[B"),
                    &[
                        (&buffer).into(),
                        (frame.width as i32).into(),
                        (frame.height as i32).into(),
                        pixel_format.into(),
                    ],
                )?
                .l()?;
            let bytes = jni::objects::JByteArray::cast_local(env, result)?;
            env.convert_byte_array(&bytes)
        })
        .map_err(|e| e.to_string())
    }
}
impl Drop for VideoEncoder {
    fn drop(&mut self) {
        if let Some((vm, _)) = BRIDGE.get() {
            let _ = vm.attach_current_thread(|env| {
                env.call_method(&self.object, jni_str!("close"), jni_sig!("()V"), &[])
                    .map(|_| ())
            });
        }
    }
}
