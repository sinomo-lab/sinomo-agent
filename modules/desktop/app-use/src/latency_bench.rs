//! Opt-in timing of the Android pixel loop and the actual native-frame codec.
use super::*;
use sinomo_device::{NativeVideoPlane, NativeVideoSample, NativeVideoSampleFormat};
use std::hint::black_box;

#[test]
#[ignore = "target-device latency diagnostic; no OS capture or input effects"]
fn measure_native_frame_transfer_stages() {
    let width = 1080;
    let height = 2280;
    for iteration in 0..7 {
        let mut pixels = vec![127; width as usize * height as usize * 4];
        let started = Instant::now();
        // Same RGBA-to-BGRA normalization as the Android bridge worker.
        for pixel in black_box(&mut pixels).chunks_exact_mut(4) {
            pixel.swap(0, 2);
            pixel[3] = 255;
        }
        let swizzle_us = started.elapsed().as_micros();
        let len = pixels.len();
        let sample = NativeVideoSample {
            native_dimensions: None,
            sequence: 1,
            format: NativeVideoSampleFormat::Bgra8,
            width,
            height,
            timestamp_ns: 1,
            duration_ns: 100_000_000,
            captured_at_unix_ms: 1,
            data: pixels.into(),
            planes: Arc::from([NativeVideoPlane {
                offset: 0,
                len,
                width,
                height,
                stride: width * 4,
            }]),
        };
        let all = Instant::now();
        let started = Instant::now();
        let frame = DesktopRawFrame::from_sample(black_box(sample)).unwrap();
        let copy_us = started.elapsed().as_micros();
        let started = Instant::now();
        let bytes = encode_sobj(&DesktopFrameDelivery {
            capture_id: "capture".into(),
            frame,
        })
        .unwrap();
        let encode_us = started.elapsed().as_micros();
        let started = Instant::now();
        let delivery: DesktopFrameDelivery = decode_sobj(black_box(&bytes)).unwrap();
        let decode_us = started.elapsed().as_micros();
        let started = Instant::now();
        let sample = delivery.frame.into_sample().unwrap();
        let publish_us = started.elapsed().as_micros();
        let transfer_us = all.elapsed().as_micros();
        assert_eq!(sample.data.len(), len);
        assert!(
            sample
                .data
                .chunks_exact(4)
                .all(|pixel| pixel == [127, 127, 127, 255])
        );
        println!(
            "NATIVE_MEDIA_LATENCY {}",
            serde_json::json!({
                "iteration":iteration,"warmup":iteration==0,"source_bytes":len,
                "encoded_bytes":bytes.len(),"rgba_to_bgra_us":swizzle_us,
                "copy_to_wire_frame_us":copy_us,"sobj_encode_us":encode_us,
                "sobj_decode_us":decode_us,"publish_sample_us":publish_us,
                "native_transfer_us":transfer_us,"roundtrip_matches":true
            })
        );
    }
}
