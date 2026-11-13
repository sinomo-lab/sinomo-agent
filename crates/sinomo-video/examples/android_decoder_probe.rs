//! On-device JNI verification using the actual application adapter and stream reader.
#![cfg(target_os = "android")]
#[path = "../../sinomo-app/src/android_media.rs"]
mod android_media;
use jni::{
    EnvUnowned,
    objects::{JObject, JString},
};
use sinomo_video::stream::{ReceivedFrame, VideoStreamReader};
use std::time::Instant;

fn check_conversion() -> Result<(), String> {
    use sinomo_video::decoder::{AndroidYuvPlane, android_yuv420_rgb};
    for width in [2usize, 8, 10, 640] {
        let height = 4usize;
        let count = width * height;
        let y: Vec<u8> = (0..count).map(|i| (i * 73 % 256) as u8).collect();
        let mut spaced = vec![0u8; count * 2];
        for (i, v) in y.iter().enumerate() {
            spaced[i * 2] = *v;
        }
        let uv: Vec<u8> = (0..count / 2).map(|i| (i * 51 % 256) as u8).collect();
        for standard in [1, 2, 4] {
            for range in [1, 2] {
                let mut planes = [
                    AndroidYuvPlane {
                        data: &y,
                        offset: 0,
                        row_stride: width,
                        pixel_stride: 1,
                    },
                    AndroidYuvPlane {
                        data: &uv,
                        offset: 0,
                        row_stride: width,
                        pixel_stride: 2,
                    },
                    AndroidYuvPlane {
                        data: &uv,
                        offset: 1,
                        row_stride: width,
                        pixel_stride: 2,
                    },
                ];
                let vector =
                    android_yuv420_rgb(&planes, width as u32, height as u32, standard, range)?;
                planes[0] = AndroidYuvPlane {
                    data: &spaced,
                    offset: 0,
                    row_stride: width * 2,
                    pixel_stride: 2,
                };
                let scalar =
                    android_yuv420_rgb(&planes, width as u32, height as u32, standard, range)?;
                if vector != scalar {
                    return Err("SIMD/scalar conversion mismatch".into());
                }
            }
        }
    }
    Ok(())
}

fn run(path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(std::io::stderr)
        .try_init();
    check_conversion()?;
    let input = std::fs::read(path)?;
    let mut bytes = input.as_slice();
    let mut reader = VideoStreamReader::default();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let mut rows = Vec::new();
    let mut sequence = 0u64;
    while !bytes.is_empty() {
        if bytes.len() < 4 {
            return Err("truncated fixture".into());
        }
        let len = u32::from_be_bytes(bytes[..4].try_into()?) as usize;
        bytes = &bytes[4..];
        if len > bytes.len() {
            return Err("truncated frame".into());
        }
        let metadata = serde_json::to_vec(&serde_json::json!({
            "stream_id":"decoder-probe", "sequence":sequence+1, "timestamp_ns":sequence*100_000_000,
            "duration_ns":100_000_000,"captured_at_unix_ms":1,"width":640,"height":360,
            "source_width":640,"source_height":360,"format":"jpeg","coordinate_grid":false,
            "capture_format":"h264","transport":"h264","packet_sequence":sequence
        }))?;
        let mut wire = (metadata.len() as u32).to_le_bytes().to_vec();
        wire.extend(metadata);
        wire.extend((len as u32).to_le_bytes());
        wire.extend(&bytes[..len]);
        bytes = &bytes[len..];
        let start = Instant::now();
        let (_, frame) = runtime
            .block_on(reader.read_decoded(&mut wire.as_slice()))?
            .ok_or("no output")?;
        let wall_us = start.elapsed().as_micros();
        let ReceivedFrame::Pixels(frame) = frame else {
            return Err("not pixels".into());
        };
        // Match the fixture's RGB quadrants, excluding chroma boundaries.
        let mut error = 0u64;
        let mut count = 0u64;
        for y in 4..356usize {
            for x in 4..636usize {
                if (x as i32 - 320).abs() < 4 || (y as i32 - 180).abs() < 4 {
                    continue;
                }
                let expected = [
                    if x < 320 { 200 } else { 40 },
                    if y < 180 { 60 } else { 200 },
                    40 + (sequence % 100) as u8,
                ];
                for c in 0..3 {
                    error += u64::from(frame.pixels[(y * 640 + x) * 3 + c].abs_diff(expected[c]));
                    count += 1;
                }
            }
        }
        let mae = error as f64 / count as f64;
        if mae > 12.0 {
            return Err(format!("RGB/crop mismatch at {sequence}: MAE {mae}").into());
        }
        rows.push(serde_json::json!({"frame":sequence,"warmup":sequence<5,"decode_rgb_us":reader.last_decode_us,"wall_us":wall_us,"rgb_mae":mae}));
        sequence += 1;
    }
    if sequence != 105 {
        return Err("expected 105 reference-dependent frames".into());
    }
    Ok(
        serde_json::json!({"backend":reader.decoder_backend(),"frames":sequence,"samples":rows})
            .to_string(),
    )
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_net_sinomo_probe_DecoderProbe_run<'local>(
    mut env: EnvUnowned<'local>,
    _class: jni::objects::JClass<'local>,
    callback: JObject<'local>,
    path: JString<'local>,
    hardware: bool,
) -> jni::sys::jstring {
    env.with_env(|env| -> jni::errors::Result<_> {
        if hardware {
            let vm = env.get_java_vm()?;
            let callback = env.new_global_ref(callback)?;
            android_media::install(vm, callback);
        }
        let result = run(&path.try_to_string(env)?)
            .unwrap_or_else(|e| serde_json::json!({"error":e.to_string()}).to_string());
        Ok(env.new_string(result)?.into_raw())
    })
    .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}
