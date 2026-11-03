//! Capture output negotiation; coordinates remain in native resource space.
use super::{DesktopError, DesktopResult};
use crate::{NativeVideoPlane, NativeVideoSample, NativeVideoSampleFormat};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaptureResolution {
    pub width: u32,
    pub height: u32,
}
impl CaptureResolution {
    pub fn validate(self) -> DesktopResult<()> {
        if self.width == 0
            || self.height == 0
            || self.width > 8192
            || self.height > 8192
            || u64::from(self.width) * u64::from(self.height) > 32 * 1024 * 1024
        {
            return Err(DesktopError::Invalid(
                "capture resolution must be nonzero, at most 8192 per edge and 32 megapixels"
                    .into(),
            ));
        }
        Ok(())
    }
    pub fn fit(self, width: u32, height: u32, alignment: u32) -> DesktopResult<[u32; 2]> {
        self.validate()?;
        if width == 0 || height == 0 || !matches!(alignment, 1 | 2) {
            return Err(DesktopError::Invalid(
                "invalid capture geometry/alignment".into(),
            ));
        }
        let (w, h) = if width <= self.width && height <= self.height {
            (width, height)
        } else if u64::from(self.width) * u64::from(height)
            <= u64::from(self.height) * u64::from(width)
        {
            (
                self.width,
                (u64::from(height) * u64::from(self.width) / u64::from(width)) as u32,
            )
        } else {
            (
                (u64::from(width) * u64::from(self.height) / u64::from(height)) as u32,
                self.height,
            )
        };
        let result = [w / alignment * alignment, h / alignment * alignment];
        if result.contains(&0) {
            return Err(DesktopError::Invalid(
                "capture resolution is too small for source aspect ratio".into(),
            ));
        }
        Ok(result)
    }
}

/// Portable fallback for providers without native scaled surfaces. It runs in
/// the capture provider, before the native ABI transfer, and retains pixel order.
pub fn fit_capture_sample(
    mut sample: NativeVideoSample,
    bounds: CaptureResolution,
) -> DesktopResult<NativeVideoSample> {
    sample
        .validate()
        .map_err(|e| DesktopError::Invalid(e.to_string()))?;
    let [width, height] = bounds.fit(sample.width, sample.height, 1)?;
    if [width, height] == [sample.width, sample.height] {
        return Ok(sample);
    }
    if !matches!(
        sample.format,
        NativeVideoSampleFormat::Rgba8 | NativeVideoSampleFormat::Bgra8
    ) {
        return Err(DesktopError::Unsupported(
            "scaled capture requires packed RGBA/BGRA output".into(),
        ));
    }
    let native = sample.native_dimensions();
    let source = sample
        .plane_data(0)
        .ok_or_else(|| DesktopError::Invalid("missing capture plane".into()))?;
    let stride = sample.planes[0].stride as usize;
    let mut pixels = vec![0; width as usize * height as usize * 4];
    for y in 0..height as usize {
        let sy = ((y as f64 + 0.5) * sample.height as f64 / height as f64 - 0.5).max(0.0);
        let y0 = sy.floor() as usize;
        let y1 = (y0 + 1).min(sample.height as usize - 1);
        let fy = sy - y0 as f64;
        for x in 0..width as usize {
            let sx = ((x as f64 + 0.5) * sample.width as f64 / width as f64 - 0.5).max(0.0);
            let x0 = sx.floor() as usize;
            let x1 = (x0 + 1).min(sample.width as usize - 1);
            let fx = sx - x0 as f64;
            for c in 0..4 {
                let top = source[y0 * stride + x0 * 4 + c] as f64 * (1.0 - fx)
                    + source[y0 * stride + x1 * 4 + c] as f64 * fx;
                let bottom = source[y1 * stride + x0 * 4 + c] as f64 * (1.0 - fx)
                    + source[y1 * stride + x1 * 4 + c] as f64 * fx;
                pixels[(y * width as usize + x) * 4 + c] =
                    (top * (1.0 - fy) + bottom * fy).round() as u8;
            }
        }
    }
    sample.native_dimensions = Some(native);
    sample.width = width;
    sample.height = height;
    sample.data = pixels.into();
    sample.planes = Arc::from([NativeVideoPlane {
        offset: 0,
        len: sample.data.len(),
        width,
        height,
        stride: width * 4,
    }]);
    Ok(sample)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn resolution_bounds_preserve_aspect_and_never_upscale() {
        assert_eq!(
            CaptureResolution {
                width: 640,
                height: 640
            }
            .fit(1080, 2280, 1)
            .unwrap(),
            [303, 640]
        );
        assert_eq!(
            CaptureResolution {
                width: 640,
                height: 640
            }
            .fit(1080, 2280, 2)
            .unwrap(),
            [302, 640]
        );
        assert_eq!(
            CaptureResolution {
                width: 400,
                height: 100
            }
            .fit(1920, 1080, 1)
            .unwrap(),
            [177, 100]
        );
        assert_eq!(
            CaptureResolution {
                width: 640,
                height: 640
            }
            .fit(100, 200, 1)
            .unwrap(),
            [100, 200]
        );
        assert!(
            CaptureResolution {
                width: 0,
                height: 200
            }
            .fit(100, 200, 1)
            .is_err()
        );
    }
    #[test]
    fn scaled_sample_keeps_native_geometry_and_pixel_order() {
        let sample = NativeVideoSample {
            native_dimensions: None,
            sequence: 7,
            format: NativeVideoSampleFormat::Rgba8,
            width: 4,
            height: 2,
            timestamp_ns: 1,
            duration_ns: 1,
            captured_at_unix_ms: 1,
            data: [20, 40, 60, 255].repeat(8).into(),
            planes: Arc::from([NativeVideoPlane {
                offset: 0,
                len: 32,
                width: 4,
                height: 2,
                stride: 16,
            }]),
        };
        let scaled = fit_capture_sample(
            sample,
            CaptureResolution {
                width: 2,
                height: 2,
            },
        )
        .unwrap();
        assert_eq!([scaled.width, scaled.height], [2, 1]);
        assert_eq!(scaled.native_dimensions(), [4, 2]);
        assert_eq!(&*scaled.data, &[20, 40, 60, 255, 20, 40, 60, 255]);
        assert_eq!(scaled.sequence, 7);
    }
}
