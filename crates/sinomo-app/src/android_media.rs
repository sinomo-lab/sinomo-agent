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
    sinomo_video::decoder::install_h264_decoder(open_video_decoder);
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
            native_dimensions: None,
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

fn open_video_decoder(
    width: u32,
    height: u32,
) -> Result<Box<dyn sinomo_video::decoder::VideoDecoder>, String> {
    let (vm, callback) = BRIDGE.get().ok_or("Android media host unavailable")?;
    let object = vm
        .attach_current_thread(|env| -> jni::errors::Result<_> {
            let result = env.call_method(
                callback,
                jni_str!("openVideoDecoder"),
                jni_sig!("(II)Lnet/sinomo/app/runtime/SinomoVideoDecoder;"),
                &[(width as i32).into(), (height as i32).into()],
            );
            // Fallback must leave this worker usable for subsequent JNI calls.
            if result.is_err() && env.exception_check() {
                env.exception_clear();
            }
            env.new_global_ref(result?.l()?)
        })
        .map_err(|e| format!("Android hardware decoder unavailable: {e}"))?;
    Ok(Box::new(VideoDecoder {
        object,
        width,
        height,
    }))
}
struct VideoDecoder {
    object: Global<JObject<'static>>,
    width: u32,
    height: u32,
}
impl sinomo_video::decoder::VideoDecoder for VideoDecoder {
    fn backend(&self) -> &'static str {
        "android-mediacodec"
    }
    fn decode(
        &mut self,
        access: &[u8],
    ) -> Result<sinomo_video::decoded_frame::DecodedYuvFrame, String> {
        let (vm, _) = BRIDGE.get().ok_or("Android media host unavailable")?;
        vm.attach_current_thread(|env| -> jni::errors::Result<Result<sinomo_video::decoded_frame::DecodedYuvFrame, String>> {
            // Java reads access synchronously; its allocation outlives this call.
            let input = unsafe { env.new_direct_byte_buffer(access.as_ptr() as *mut u8, access.len())? };
            let decode_started = std::time::Instant::now();
            let result = env.call_method(&self.object, jni_str!("decode"),
                jni_sig!("(Ljava/nio/ByteBuffer;)Lnet/sinomo/app/runtime/SinomoVideoDecoder$DecodedFrame;"), &[(&input).into()]);
            if result.is_err() && env.exception_check() { env.exception_clear(); }
            let frame = result?.l()?;
            let codec_us = decode_started.elapsed().as_micros() as u64;
            let planes_started = std::time::Instant::now();
            let converted = (|| -> jni::errors::Result<Result<sinomo_video::decoded_frame::DecodedYuvFrame, String>> {
                let layout = env.call_method(&frame, jni_str!("layout"), jni_sig!("()[I"), &[])?.l()?;
                let layout = jni::objects::JIntArray::cast_local(env, layout)?;
                if layout.len(env)? != 14 { return Ok(Err("invalid Android image layout".into())); }
                let mut values = [0i32; 14]; layout.get_region(env, 0, &mut values)?;
                let mut buffers = Vec::with_capacity(3);
                for index in 0..3i32 {
                    let buffer = env.call_method(&frame, jni_str!("plane"), jni_sig!("(I)Ljava/nio/ByteBuffer;"), &[index.into()])?.l()?;
                    buffers.push(jni::objects::JByteBuffer::cast_local(env, buffer)?);
                }
                let mut planes = Vec::with_capacity(3);
                for (index, buffer) in buffers.iter().enumerate() {
                    let v = &values[index*4..index*4+4];
                    if v.iter().any(|value| *value < 0) { return Ok(Err("negative Android plane layout".into())); }
                    let capacity = env.get_direct_buffer_capacity(buffer)?;
                    if v[3] as usize > capacity { return Ok(Err("Android plane limit exceeds allocation".into())); }
                    let pointer = env.get_direct_buffer_address(buffer)?;
                    if pointer.is_null() { return Ok(Err("null Android image plane".into())); }
                    // The DecodedFrame retains its Image and codec output buffer.
                    // We read only the validated ByteBuffer limit, and close it
                    // after the bounded plane copy; no borrowed plane escapes this closure.
                    let data = unsafe { std::slice::from_raw_parts(pointer, v[3] as usize) };
                    planes.push(sinomo_video::decoder::AndroidYuvPlane {data,offset:v[0] as usize,row_stride:v[1] as usize,pixel_stride:v[2] as usize});
                }
                let planes: [_;3] = planes.try_into().map_err(|_| jni::errors::Error::NullPtr("image planes"))?;
                let plane_access_us = planes_started.elapsed().as_micros() as u64;
                let copy_started = std::time::Instant::now();
                let native = sinomo_video::decoded_frame::YuvColorSpace::from_android(values[12],values[13]).and_then(|color| sinomo_video::decoded_frame::DecodedYuvFrame::copy_from_planes(&planes,self.width,self.height,color));
                tracing::trace!(target: "sinomo.perf", stage="media.frame.hardware_decode", codec_us, plane_access_us,
                    copy_us=copy_started.elapsed().as_micros() as u64, width=self.width,height=self.height,
                    "Android hardware decode stages");
                Ok(native)
            })();
            if converted.is_err() && env.exception_check() { env.exception_clear(); }
            // Return the codec buffer even when plane validation/copy fails.
            let closed = env.call_method(&frame, jni_str!("close"), jni_sig!("()V"), &[]);
            if closed.is_err() && env.exception_check() { env.exception_clear(); }
            closed?;
            converted
        }).map_err(|e| format!("Android hardware decode failed: {e}"))?
    }
}
impl Drop for VideoDecoder {
    fn drop(&mut self) {
        if let Some((vm, _)) = BRIDGE.get() {
            let _ = vm.attach_current_thread(|env| -> jni::errors::Result<()> {
                let result = env.call_method(&self.object, jni_str!("close"), jni_sig!("()V"), &[]);
                if result.is_err() && env.exception_check() {
                    env.exception_clear();
                }
                result.map(|_| ())
            });
        }
    }
}
