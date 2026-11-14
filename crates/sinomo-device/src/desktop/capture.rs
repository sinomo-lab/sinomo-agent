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
    let pixels = resize_capture_pixels(source, sample.width, sample.height, stride, width, height)?;
    sample.native_dimensions = Some(native);
    sample.width = width;
    sample.height = height;
    sample.data = pixels;
    sample.planes = Arc::from([NativeVideoPlane {
        offset: 0,
        len: sample.data.len(),
        width,
        height,
        stride: width * 4,
    }]);
    Ok(sample)
}

/// Half-pixel bilinear resampling of packed four-channel pixels into their final
/// shared allocation. Channel order is preserved. Fixed-point weights differ
/// from ideal floating-point interpolation by at most one channel value.
pub fn resize_capture_pixels(
    source: &[u8],
    source_width: u32,
    source_height: u32,
    stride: usize,
    width: u32,
    height: u32,
) -> DesktopResult<Arc<[u8]>> {
    CaptureResolution { width, height }.validate()?;
    if source_width == 0
        || source_height == 0
        || width == 0
        || height == 0
        || width > source_width
        || height > source_height
        || u64::from(source_width) * u64::from(source_height) > 32 * 1024 * 1024
        || stride < source_width as usize * 4
        || stride
            .checked_mul(source_height as usize - 1)
            .and_then(|len| len.checked_add(source_width as usize * 4))
            .is_none_or(|len| len > source.len())
    {
        return Err(DesktopError::Invalid(
            "invalid packed capture resampling geometry".into(),
        ));
    }
    // Compute each axis once. The inner loop contains no division, coordinate
    // conversion, allocation or floating-point channel arithmetic.
    fn axis(source: u32, target: u32) -> Vec<(usize, usize, u32)> {
        (0..target)
            .map(|i| {
                let position =
                    ((f64::from(i) + 0.5) * f64::from(source) / f64::from(target) - 0.5).max(0.0);
                let first = position.floor() as usize;
                (
                    first,
                    (first + 1).min(source as usize - 1),
                    ((position - first as f64) * 65536.0).round() as u32,
                )
            })
            .collect()
    }
    let xs = axis(source_width, width);
    let ys = axis(source_height, height);
    // SAFETY: zero is valid for u8. Fill the final Arc allocation directly.
    let mut pixels = unsafe {
        Arc::<[u8]>::new_zeroed_slice(width as usize * height as usize * 4).assume_init()
    };
    let output = Arc::get_mut(&mut pixels).expect("new allocation is unique");
    for (y, &(y0, y1, fy)) in ys.iter().enumerate() {
        let row0 = &source[y0 * stride..y0 * stride + source_width as usize * 4];
        let row1 = &source[y1 * stride..y1 * stride + source_width as usize * 4];
        for (pixel, &(x0, x1, fx)) in output[y * width as usize * 4..(y + 1) * width as usize * 4]
            .chunks_exact_mut(4)
            .zip(&xs)
        {
            for (channel, out) in pixel.iter_mut().enumerate() {
                let top = u32::from(row0[x0 * 4 + channel]) * (65536 - fx)
                    + u32::from(row0[x1 * 4 + channel]) * fx;
                let bottom = u32::from(row1[x0 * 4 + channel]) * (65536 - fx)
                    + u32::from(row1[x1 * 4 + channel]) * fx;
                *out = ((u64::from(top) * u64::from(65536 - fy)
                    + u64::from(bottom) * u64::from(fy)
                    + (1u64 << 31))
                    >> 32) as u8;
            }
        }
    }
    Ok(pixels)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_point_resize_matches_bilinear_with_padding_and_keeps_channels() {
        for (sw, sh, w, h) in [(13, 9, 7, 5), (8, 6, 1, 1), (3, 7, 2, 4), (32, 18, 16, 9)] {
            let stride = sw * 4 + 7;
            let mut data = vec![0u8; stride * (sh - 1) + sw * 4];
            for y in 0..sh {
                for x in 0..sw {
                    for c in 0..4 {
                        data[y * stride + x * 4 + c] = ((x * 37 + y * 51 + c * 89) % 256) as u8;
                    }
                }
            }
            let output =
                resize_capture_pixels(&data, sw as u32, sh as u32, stride, w as u32, h as u32)
                    .unwrap();
            for y in 0..h {
                for x in 0..w {
                    let sy = ((y as f64 + 0.5) * sh as f64 / h as f64 - 0.5).max(0.0);
                    let y0 = sy.floor() as usize;
                    let y1 = (y0 + 1).min(sh - 1);
                    let fy = sy - y0 as f64;
                    let sx = ((x as f64 + 0.5) * sw as f64 / w as f64 - 0.5).max(0.0);
                    let x0 = sx.floor() as usize;
                    let x1 = (x0 + 1).min(sw - 1);
                    let fx = sx - x0 as f64;
                    for c in 0..4 {
                        let top = f64::from(data[y0 * stride + x0 * 4 + c]) * (1.0 - fx)
                            + f64::from(data[y0 * stride + x1 * 4 + c]) * fx;
                        let bottom = f64::from(data[y1 * stride + x0 * 4 + c]) * (1.0 - fx)
                            + f64::from(data[y1 * stride + x1 * 4 + c]) * fx;
                        assert!(
                            output[(y * w + x) * 4 + c]
                                .abs_diff((top * (1.0 - fy) + bottom * fy).round() as u8)
                                <= 1
                        );
                    }
                }
            }
            assert!(
                resize_capture_pixels(
                    &data[..data.len() - 1],
                    sw as u32,
                    sh as u32,
                    stride,
                    w as u32,
                    h as u32
                )
                .is_err()
            );
        }
        assert!(resize_capture_pixels(&[0; 16], 2, 2, usize::MAX, 1, 1).is_err());
        assert!(resize_capture_pixels(&[0; 16], 2, 2, 8, 3, 2).is_err());
    }

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
