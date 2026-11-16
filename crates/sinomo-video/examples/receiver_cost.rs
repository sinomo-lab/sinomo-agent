//! Synthetic receiver stage measurement, excluding capture and network latency.
//! cargo run --release -p sinomo-video --example receiver_cost -- 1280 720 100
//! Add --features desktop-gpu and a final `gpu` argument to validate GPU encoding.
use openh264::{
    encoder::Encoder,
    formats::{RgbSliceU8, YUVBuffer},
};
use sinomo_lm::{RealtimeVideoFrameFormat, RealtimeVideoFrameInput, RealtimeVideoPixelFormat};
use sinomo_video::{
    codec,
    stream::{VideoFrameMetadata, VideoStreamReader},
};
use std::time::Instant;

fn stats(values: &[u128]) -> serde_json::Value {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    serde_json::json!({"median_us": sorted[sorted.len()/2], "p95_us": sorted[(sorted.len()*95).div_ceil(100)-1], "max_us": sorted[sorted.len()-1]})
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let width: usize = args.get(1).map(|v| v.parse()).transpose()?.unwrap_or(1280);
    let height: usize = args.get(2).map(|v| v.parse()).transpose()?.unwrap_or(720);
    let count: usize = args.get(3).map(|v| v.parse()).transpose()?.unwrap_or(100);
    if width > 1920 || height > 1920 || count == 0 || count > 10000 {
        return Err("invalid benchmark bounds".into());
    }
    codec::dimensions(width as u32, height as u32)?;
    let gpu = args.get(4).is_some_and(|v| v == "gpu");
    let mut hardware = if gpu {
        Some(codec::encoder(width as u32, height as u32, 30)?)
    } else {
        None
    };
    let mut software = Encoder::with_api_config(
        openh264::OpenH264API::from_source(),
        openh264::encoder::EncoderConfig::new().skip_frames(false),
    )?;
    let mut decoder = VideoStreamReader::default();
    let mut encodes = Vec::new();
    let mut decodes = Vec::new();
    let mut reads = Vec::new();
    let mut jpegs = Vec::new();
    let mut pngs = Vec::new();
    let mut rows = Vec::new();
    for sequence in 0..count + 10 {
        // Moving edges and textured pixels exercise more than a constant color.
        let mut rgb = vec![0u8; width * height * 3];
        for y in 0..height {
            for x in 0..width {
                let i = (y * width + x) * 3;
                rgb[i] = ((x + sequence * 3) % 256) as u8;
                rgb[i + 1] = ((y + sequence * 5) % 256) as u8;
                rgb[i + 2] = (((x / 16 + y / 16 + sequence) % 2) * 180) as u8;
            }
        }
        let started = Instant::now();
        let bytes = if let Some(encoder) = hardware.as_mut() {
            encoder.encode(&RealtimeVideoFrameInput::new(
                rgb.into(),
                RealtimeVideoPixelFormat::Rgb8,
                width as u32,
                height as u32,
                width * 3,
                sequence as u64,
            ))?
        } else {
            software
                .encode(&YUVBuffer::from_rgb_source(RgbSliceU8::new(
                    &rgb,
                    (width, height),
                )))?
                .to_vec()
        };
        let encode_us = started.elapsed().as_micros();
        let metadata = VideoFrameMetadata {
            stream_id: None,
            content_bounds: None,
            notifications: Vec::new(),
            identity: None,
            tiles: vec![],
            sequence: sequence as u64,
            timestamp_ns: sequence as u64 * 33_333_333,
            duration_ns: 33_333_333,
            captured_at_unix_ms: 0,
            width: width as u32,
            height: height as u32,
            source_width: width as u32,
            source_height: height as u32,
            format: RealtimeVideoFrameFormat::Jpeg,
            coordinate_grid: false,
            capture_format: None,
            transport: codec::VideoTransport::H264,
            packet_sequence: Some(sequence as u64),
        };
        let metadata = serde_json::to_vec(&metadata)?;
        let mut record = Vec::new();
        record.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
        record.extend(metadata);
        record.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        record.extend(&bytes);
        let started = Instant::now();
        let (_, frame) = decoder
            .read_decoded(&mut record.as_slice())
            .await?
            .ok_or("missing frame")?;
        let read_us = started.elapsed().as_micros();
        let started = Instant::now();
        let jpeg = frame
            .clone()
            .into_model_image(RealtimeVideoFrameFormat::Jpeg)?;
        let jpeg_us = started.elapsed().as_micros();
        let started = Instant::now();
        let png = frame.into_model_image(RealtimeVideoFrameFormat::Png)?;
        let png_us = started.elapsed().as_micros();
        if sequence >= 10 {
            encodes.push(encode_us);
            decodes.push(decoder.last_decode_us);
            reads.push(read_us);
            jpegs.push(jpeg_us);
            pngs.push(png_us);
            rows.push(serde_json::json!({"sequence":sequence,"encode_us":encode_us,"decode_native_us":decoder.last_decode_us,
                "read_wall_us":read_us,"jpeg_us":jpeg_us,"png_us":png_us,"h264_bytes":bytes.len(),
                "jpeg_bytes":jpeg.image.len(),"png_bytes":png.image.len()}));
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "width":width,"height":height,"frames":count,"warmup_frames":10,"gpu":gpu,
            "encode_including_conversion_upload":stats(&encodes),"decode_native":stats(&decodes),
            "read_including_worker_scheduling":stats(&reads),"jpeg":stats(&jpegs),"png":stats(&pngs),"samples":rows
        }))?
    );
    Ok(())
}
