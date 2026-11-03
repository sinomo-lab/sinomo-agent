//! Opt-in, model-free attribution of the shared video encoder on the target CPU.
//! Run this ignored test with --nocapture. Each measured stage uses the same
//! helpers/codecs as production; byte equality checks the staged reconstruction.
use super::*;
use std::{hint::black_box, sync::Arc, time::Instant};

fn micros(started: Instant) -> u128 {
    started.elapsed().as_micros()
}

#[test]
#[ignore = "target-device latency diagnostic; emits raw samples and invokes no provider"]
fn measure_video_encoding_stages() {
    let samples = std::env::var("SINOMO_MEDIA_BENCH_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(6)
        .clamp(1, 100);
    for (width, height) in [(1080_u32, 2280_u32), (303, 640)] {
        let mut pixels = vec![255_u8; (width * height * 4) as usize];
        // Deterministic UI-like color blocks; construction is outside timing.
        for y in 0..height {
            for x in 0..width {
                let i = ((y * width + x) * 4) as usize;
                if y > height / 4 && y < height / 2 {
                    pixels[i..i + 4].copy_from_slice(if x < width / 3 {
                        &[0, 255, 255, 255]
                    } else {
                        &[210, 120, 15, 255]
                    });
                }
            }
        }
        let frame = RealtimeVideoFrameInput::new(
            Arc::<[u8]>::from(pixels),
            RealtimeVideoPixelFormat::Bgra8,
            width,
            height,
            width as usize * 4,
            0,
        );
        let config = RealtimeVideoInputConfig {
            max_dimension: Some(640),
            frame_format: RealtimeVideoFrameFormat::Jpeg,
            ..Default::default()
        };
        for iteration in 0..=samples {
            let full_start = Instant::now();
            let full = encode_realtime_video_frame(black_box(&frame), black_box(&config)).unwrap();
            let full_us = micros(full_start);

            let staged_start = Instant::now();
            let started = Instant::now();
            let mut image = raw_video_frame_image(black_box(&frame)).unwrap();
            let pack_us = micros(started);
            let started = Instant::now();
            let source_max = width.max(height);
            if source_max > 640 {
                let scaled = |n: u32| {
                    ((u64::from(n) * 640 + u64::from(source_max) / 2) / u64::from(source_max))
                        .max(1) as u32
                };
                image = image.resize_exact(scaled(width), scaled(height), FilterType::Triangle);
            }
            let resize_us = micros(started);
            let started = Instant::now();
            let rgb = image.to_rgb8();
            let rgb_us = micros(started);
            let started = Instant::now();
            let mut encoded = Vec::new();
            JpegEncoder::new_with_quality(&mut encoded, config.encoding_quality)
                .encode(
                    rgb.as_raw(),
                    rgb.width(),
                    rgb.height(),
                    image::ExtendedColorType::Rgb8,
                )
                .unwrap();
            let jpeg_us = micros(started);
            let staged_us = micros(staged_start);
            assert_eq!(
                full.image.as_ref(),
                encoded.as_slice(),
                "staged path must match the production encoder"
            );
            println!(
                "MEDIA_LATENCY {}",
                serde_json::json!({
                    "case":format!("{width}x{height}-to-640-jpeg"),
                    "iteration":iteration,"warmup":iteration==0,"source_bytes":frame.pixels.len(),
                    "output_width":full.width,"output_height":full.height,"encoded_bytes":encoded.len(),
                    "production_encode_us":full_us,"staged_encode_us":staged_us,
                    "pack_bgra_to_rgba_us":pack_us,"resize_triangle_us":resize_us,
                    "rgba_to_rgb_us":rgb_us,"jpeg_us":jpeg_us,"output_matches":true
                })
            );
        }
    }
}

/// Compare codec-ready frames with the Android MediaCodec probe's reference.
/// Capture and scaling are intentionally separate from this common-input lane.
#[test]
#[ignore = "requires SINOMO_MEDIA_CODEC_FIXTURE_DIR from the Android codec probe"]
fn compare_fixture_image_codecs() {
    use std::{path::PathBuf, time::Duration};
    let root =
        PathBuf::from(std::env::var("SINOMO_MEDIA_CODEC_FIXTURE_DIR").expect("fixture directory"));
    println!(
        "IMAGE_CODEC_ENV {}",
        serde_json::json!({"pid":std::process::id(),"cgroup":std::fs::read_to_string("/proc/self/cgroup").ok()})
    );
    let width = 304_u32;
    let height = 640_u32;
    let original = std::fs::read(root.join("reference.bgra")).unwrap();
    assert_eq!(original.len(), (width * height * 4) as usize);
    let luma = |r: u8, g: u8, b: u8| {
        16.0 + 219.0 / 255.0
            * (0.2126 * f64::from(r) + 0.7152 * f64::from(g) + 0.0722 * f64::from(b))
    };
    for quality in [75, 85, 95] {
        for moving in [false, true] {
            let lane = format!(
                "jpeg-{quality}-{}",
                if moving { "stream" } else { "independent" }
            );
            let output = root.join(&lane);
            std::fs::create_dir_all(&output).unwrap();
            let schedule = Instant::now();
            let mut previous = schedule;
            for frame_index in 0..35 {
                let deadline = schedule + Duration::from_millis(frame_index * 100);
                std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
                let mut pixels = original.clone();
                if moving {
                    let x = (frame_index as u32 * 7) % (width - 8);
                    for y in 0..height {
                        for col in x..x + 8 {
                            let start = ((y * width + col) * 4) as usize;
                            pixels[start..start + 4].copy_from_slice(&[0, 0, 255, 255]);
                        }
                    }
                }
                let input = RealtimeVideoFrameInput::new(
                    Arc::<[u8]>::from(pixels),
                    RealtimeVideoPixelFormat::Bgra8,
                    width,
                    height,
                    width as usize * 4,
                    frame_index,
                );
                let config = RealtimeVideoInputConfig {
                    encoding_quality: quality,
                    max_dimension: None,
                    ..Default::default()
                };
                let submitted = Instant::now();
                let encoded = encode_realtime_video_frame(black_box(&input), &config).unwrap();
                let encode_us = submitted.elapsed().as_micros();
                let decode_start = Instant::now();
                let decoded = image::load_from_memory(&encoded.image).unwrap().to_rgb8();
                let decode_us = decode_start.elapsed().as_micros();
                let total_us = submitted.elapsed().as_micros();
                assert_eq!(decoded.dimensions(), (width, height));
                let mut squared = 0.0;
                for (source, decoded) in input
                    .pixels
                    .chunks_exact(4)
                    .zip(decoded.as_raw().chunks_exact(3))
                {
                    let error = luma(source[2], source[1], source[0])
                        - luma(decoded[0], decoded[1], decoded[2]);
                    squared += error * error;
                }
                let psnr = if squared == 0.0 {
                    99.0
                } else {
                    10.0 * (255.0_f64.powi(2) * f64::from(width * height) / squared).log10()
                };
                if frame_index == 0 || frame_index == 34 {
                    std::fs::write(output.join(format!("{frame_index:04}.jpg")), &encoded.image)
                        .unwrap();
                }
                println!(
                    "IMAGE_CODEC_SAMPLE {}",
                    serde_json::json!({"lane":lane,"frame":frame_index,
                    "warmup":frame_index<5,"encode_us":encode_us,"decode_us":decode_us,
                    "encode_decode_us":total_us,"bytes":encoded.image.len(),"luma_psnr_db":psnr,
                    "source_gap_us":submitted.duration_since(previous).as_micros(),"width":width,"height":height})
                );
                previous = submitted;
            }
        }
    }
}

#[test]
#[ignore = "requires SINOMO_MEDIA_CODEC_FIXTURE_DIR; measures the full source-image path"]
fn compare_full_source_image_pipeline() {
    use std::{path::PathBuf, time::Duration};
    let root =
        PathBuf::from(std::env::var("SINOMO_MEDIA_CODEC_FIXTURE_DIR").expect("fixture directory"));
    let pixels = std::fs::read(root.join("source.bgra")).unwrap();
    assert_eq!(pixels.len(), 1080 * 2280 * 4);
    let frame = RealtimeVideoFrameInput::new(
        Arc::<[u8]>::from(pixels),
        RealtimeVideoPixelFormat::Bgra8,
        1080,
        2280,
        1080 * 4,
        0,
    );
    let config = RealtimeVideoInputConfig {
        max_dimension: Some(640),
        ..Default::default()
    };
    let schedule = Instant::now();
    let mut previous = schedule;
    println!(
        "IMAGE_CODEC_ENV {}",
        serde_json::json!({"pid":std::process::id(),"cgroup":std::fs::read_to_string("/proc/self/cgroup").ok()})
    );
    for index in 0..35 {
        let deadline = schedule + Duration::from_millis(index * 100);
        std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
        let started = Instant::now();
        let encoded = encode_realtime_video_frame(&frame, &config).unwrap();
        let encode_us = started.elapsed().as_micros();
        let decode = Instant::now();
        let image = image::load_from_memory(&encoded.image).unwrap().to_rgb8();
        let decode_us = decode.elapsed().as_micros();
        let total_us = started.elapsed().as_micros();
        assert_eq!(image.dimensions(), (303, 640));
        if index == 34 {
            std::fs::write(root.join("jpeg-full-source.jpg"), &encoded.image).unwrap();
        }
        println!(
            "IMAGE_CODEC_SAMPLE {}",
            serde_json::json!({"lane":"jpeg-85-full-source","frame":index,
            "warmup":index<5,"encode_us":encode_us,"decode_us":decode_us,"encode_decode_us":total_us,
            "bytes":encoded.image.len(),"source_gap_us":started.duration_since(previous).as_micros(),
            "input_width":1080,"input_height":2280,"width":303,"height":640})
        );
        previous = started;
    }
}
