//! Stream-local decoder selection. Fallback is safe only before the first output.
use crate::decoded_frame::{DecodedYuvFrame, YuvColorSpace};
use std::sync::OnceLock;

pub trait VideoDecoder: Send {
    /// Decode one validated access unit into owned native YUV planes.
    fn decode(&mut self, access_unit: &[u8]) -> Result<DecodedYuvFrame, String>;
    fn backend(&self) -> &'static str;
}
pub type DecoderFactory = fn(u32, u32) -> Result<Box<dyn VideoDecoder>, String>;
static HARDWARE: OnceLock<DecoderFactory> = OnceLock::new();

/// Installed by the Android media host; other hosts retain software decoding.
pub fn install_h264_decoder(factory: DecoderFactory) {
    let _ = HARDWARE.set(factory);
}

pub(crate) struct H264Decoder {
    active: Box<dyn VideoDecoder>,
    dimensions: (u32, u32),
    fallback_allowed: bool,
}
impl H264Decoder {
    pub(crate) fn new(width: u32, height: u32) -> Result<Self, String> {
        Self::with_factory(width, height, HARDWARE.get().copied())
    }
    fn with_factory(
        width: u32,
        height: u32,
        factory: Option<DecoderFactory>,
    ) -> Result<Self, String> {
        crate::codec::dimensions(width, height)?;
        if let Some(factory) = factory {
            match factory(width, height) {
                Ok(active) => {
                    return Ok(Self {
                        active,
                        dimensions: (width, height),
                        fallback_allowed: true,
                    });
                }
                Err(error) => {
                    tracing::debug!(%error, "hardware decoder unavailable; using OpenH264")
                }
            }
        }
        Ok(Self {
            active: software(width, height)?,
            dimensions: (width, height),
            fallback_allowed: false,
        })
    }
    pub(crate) fn backend(&self) -> &'static str {
        self.active.backend()
    }
    pub(crate) fn decode(&mut self, bytes: &[u8]) -> Result<DecodedYuvFrame, String> {
        let result = self
            .active
            .decode(bytes)
            .and_then(|frame| self.check_frame(frame));
        let result = match result {
            Err(error) if self.fallback_allowed => {
                tracing::warn!(%error, "hardware decoder rejected first frame; using OpenH264");
                self.active = software(self.dimensions.0, self.dimensions.1)?;
                self.active
                    .decode(bytes)
                    .and_then(|frame| self.check_frame(frame))
            }
            result => result,
        };
        self.fallback_allowed = false;
        result
    }
    fn check_frame(&self, frame: DecodedYuvFrame) -> Result<DecodedYuvFrame, String> {
        if (frame.width(), frame.height()) != self.dimensions {
            return Err("decoder frame does not match stream geometry".into());
        }
        Ok(frame)
    }
}
struct Software {
    decoder: openh264::decoder::Decoder,
    width: u32,
    height: u32,
}
fn software(width: u32, height: u32) -> Result<Box<dyn VideoDecoder>, String> {
    Ok(Box::new(Software {
        decoder: openh264::decoder::Decoder::new().map_err(|e| e.to_string())?,
        width,
        height,
    }))
}
impl VideoDecoder for Software {
    fn backend(&self) -> &'static str {
        "openh264"
    }
    fn decode(&mut self, bytes: &[u8]) -> Result<DecodedYuvFrame, String> {
        use openh264::formats::YUVSource;
        let decoded = self
            .decoder
            .decode(bytes)
            .map_err(|e| e.to_string())?
            .ok_or("H264 access unit did not produce one frame")?;
        if decoded.dimensions() != (self.width as usize, self.height as usize) {
            return Err("H264 decoded dimensions do not match metadata".into());
        }
        let (ys, us, vs) = decoded.strides();
        let planes = [
            AndroidYuvPlane {
                data: decoded.y(),
                offset: 0,
                row_stride: ys,
                pixel_stride: 1,
            },
            AndroidYuvPlane {
                data: decoded.u(),
                offset: 0,
                row_stride: us,
                pixel_stride: 1,
            },
            AndroidYuvPlane {
                data: decoded.v(),
                offset: 0,
                row_stride: vs,
                pixel_stride: 1,
            },
        ];
        // Preserve the existing OpenH264 RGB adapter's BT.601 full-range interpretation.
        DecodedYuvFrame::copy_from_planes(
            &planes,
            self.width,
            self.height,
            YuvColorSpace::from_android(4, 1)?,
        )
    }
}

/// Borrowed Android Image plane. The image owner must outlive conversion.
pub struct AndroidYuvPlane<'a> {
    pub data: &'a [u8],
    pub offset: usize,
    pub row_stride: usize,
    pub pixel_stride: usize,
}
pub(crate) fn validate_yuv_planes(
    planes: &[AndroidYuvPlane<'_>; 3],
    width: u32,
    height: u32,
) -> Result<(), String> {
    crate::codec::dimensions(width, height)?;
    for (index, plane) in planes.iter().enumerate() {
        let shift = usize::from(index > 0);
        let w = (width as usize) >> shift;
        let h = (height as usize) >> shift;
        let end = (h - 1)
            .checked_mul(plane.row_stride)
            .and_then(|n| {
                (w - 1)
                    .checked_mul(plane.pixel_stride)
                    .and_then(|m| n.checked_add(m))
            })
            .and_then(|n| n.checked_add(plane.offset))
            .and_then(|n| n.checked_add(1));
        if plane.row_stride == 0
            || plane.pixel_stride == 0
            || end.is_none_or(|end| end > plane.data.len())
        {
            return Err("invalid Android YUV plane bounds".into());
        }
    }
    Ok(())
}
/// Android color-standard 1/2/4 is BT.709/BT.601 PAL/BT.601 NTSC;
/// color-range 1/2 is full/limited. Convert directly without packing/copying YUV.
pub fn android_yuv420_rgb(
    planes: &[AndroidYuvPlane<'_>; 3],
    width: u32,
    height: u32,
    standard: i32,
    range: i32,
) -> Result<Vec<u8>, String> {
    validate_yuv_planes(planes, width, height)?;
    YuvColorSpace::from_android(standard, range)?;
    let (rv, gu, gv, bu) = match (standard, range) {
        (1, 1) => (403, -48, -120, 475),
        (1, 2) => (459, -55, -136, 541),
        (_, 1) => (359, -88, -183, 454),
        _ => (409, -100, -208, 516),
    };
    let mut rgb = vec![0; width as usize * height as usize * 3];
    let [yp, up, vp] = planes;
    for row in 0..height as usize {
        #[allow(unused_mut)]
        let mut start_col = 0;
        #[cfg(target_arch = "aarch64")]
        if yp.pixel_stride == 1 {
            // AArch64 guarantees NEON. All plane ranges were checked above;
            // each vector reads eight Y samples and four U/V samples and writes
            // exactly 24 RGB bytes. Unusual chroma pixel strides remain valid.
            start_col = unsafe {
                neon_row(
                    planes,
                    row,
                    width as usize,
                    range,
                    [rv, gu, gv, bu],
                    &mut rgb,
                )
            };
        }
        for col in start_col..width as usize {
            let i = row * width as usize + col;
            let u = i32::from(
                up.data[up.offset + (row / 2) * up.row_stride + (col / 2) * up.pixel_stride],
            ) - 128;
            let v = i32::from(
                vp.data[vp.offset + (row / 2) * vp.row_stride + (col / 2) * vp.pixel_stride],
            ) - 128;
            let luma = i32::from(yp.data[yp.offset + row * yp.row_stride + col * yp.pixel_stride]);
            let y = if range == 2 {
                (luma - 16) * 298
            } else {
                luma * 256
            };
            for (channel, value) in [y + rv * v, y + gu * u + gv * v, y + bu * u]
                .into_iter()
                .enumerate()
            {
                rgb[i * 3 + channel] = ((value + 128) >> 8).clamp(0, 255) as u8;
            }
        }
    }
    Ok(rgb)
}

#[cfg(target_arch = "aarch64")]
unsafe fn neon_row(
    planes: &[AndroidYuvPlane<'_>; 3],
    row: usize,
    width: usize,
    range: i32,
    matrix: [i32; 4],
    rgb: &mut [u8],
) -> usize {
    use std::arch::aarch64::*;
    let [yp, up, vp] = planes;
    let y_base = yp.offset + row * yp.row_stride;
    let u_base = up.offset + (row / 2) * up.row_stride;
    let v_base = vp.offset + (row / 2) * vp.row_stride;
    let end = width / 8 * 8;
    // Safety: caller has checked all source indices and output geometry. The
    // eight-luma/four-chroma blocks never extend beyond the active image width.
    unsafe {
        for col in (0..end).step_by(8) {
            let y = vreinterpretq_s16_u16(vmovl_u8(vld1_u8(yp.data.as_ptr().add(y_base + col))));
            let y = vsubq_s16(y, vdupq_n_s16(if range == 2 { 16 } else { 0 }));
            let u = neon_chroma(up, u_base + col / 2 * up.pixel_stride);
            let v = neon_chroma(vp, v_base + col / 2 * vp.pixel_stride);
            let scale = if range == 2 { 298 } else { 256 };
            let lo = vmull_n_s16(vget_low_s16(y), scale);
            let hi = vmull_n_s16(vget_high_s16(y), scale);
            let rlo = vmlal_n_s16(lo, vget_low_s16(v), matrix[0] as i16);
            let rhi = vmlal_n_s16(hi, vget_high_s16(v), matrix[0] as i16);
            let glo = vmlal_n_s16(
                vmlal_n_s16(lo, vget_low_s16(u), matrix[1] as i16),
                vget_low_s16(v),
                matrix[2] as i16,
            );
            let ghi = vmlal_n_s16(
                vmlal_n_s16(hi, vget_high_s16(u), matrix[1] as i16),
                vget_high_s16(v),
                matrix[2] as i16,
            );
            let blo = vmlal_n_s16(lo, vget_low_s16(u), matrix[3] as i16);
            let bhi = vmlal_n_s16(hi, vget_high_s16(u), matrix[3] as i16);
            let r = vqmovn_u16(vcombine_u16(
                vqrshrun_n_s32::<8>(rlo),
                vqrshrun_n_s32::<8>(rhi),
            ));
            let g = vqmovn_u16(vcombine_u16(
                vqrshrun_n_s32::<8>(glo),
                vqrshrun_n_s32::<8>(ghi),
            ));
            let b = vqmovn_u16(vcombine_u16(
                vqrshrun_n_s32::<8>(blo),
                vqrshrun_n_s32::<8>(bhi),
            ));
            vst3_u8(
                rgb.as_mut_ptr().add((row * width + col) * 3),
                uint8x8x3_t(r, g, b),
            );
        }
    }
    end
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn neon_chroma(plane: &AndroidYuvPlane<'_>, offset: usize) -> std::arch::aarch64::int16x8_t {
    use std::arch::aarch64::*;
    // Caller validated four active chroma samples. A widened NV12/NV21 read
    // additionally checks the full eight-byte range, including the final byte
    // that some Android plane views omit. That last block uses scalar gathers.
    unsafe {
        let packed = if plane.pixel_stride == 1 {
            vcreate_u8(u64::from(std::ptr::read_unaligned(
                plane.data.as_ptr().add(offset).cast::<u32>(),
            )))
        } else if plane.pixel_stride == 2 && plane.data.len() - offset >= 8 {
            let interleaved = vld1_u8(plane.data.as_ptr().add(offset));
            vuzp1_u8(interleaved, interleaved)
        } else {
            let mut values = [0u8; 8];
            for (i, value) in values[..4].iter_mut().enumerate() {
                *value = *plane.data.get_unchecked(offset + i * plane.pixel_stride);
            }
            vld1_u8(values.as_ptr())
        };
        vsubq_s16(
            vreinterpretq_s16_u16(vmovl_u8(vzip1_u8(packed, packed))),
            vdupq_n_s16(128),
        )
    }
}

#[cfg(test)]
mod tests {
    fn android_i420_rgb(
        yuv: &[u8],
        width: u32,
        height: u32,
        standard: i32,
        range: i32,
    ) -> Result<Vec<u8>, String> {
        let pixels = width as usize * height as usize;
        if yuv.len() != pixels * 3 / 2 {
            return Err("truncated I420".into());
        }
        let planes = [
            super::AndroidYuvPlane {
                data: yuv,
                offset: 0,
                row_stride: width as usize,
                pixel_stride: 1,
            },
            super::AndroidYuvPlane {
                data: yuv,
                offset: pixels,
                row_stride: width as usize / 2,
                pixel_stride: 1,
            },
            super::AndroidYuvPlane {
                data: yuv,
                offset: pixels + pixels / 4,
                row_stride: width as usize / 2,
                pixel_stride: 1,
            },
        ];
        super::android_yuv420_rgb(&planes, width, height, standard, range)
    }

    use super::*;
    fn access_unit() -> Vec<u8> {
        use openh264::{
            encoder::Encoder,
            formats::{RgbSliceU8, YUVBuffer},
        };
        let rgb = vec![90; 16 * 16 * 3];
        let yuv = YUVBuffer::from_rgb_source(RgbSliceU8::new(&rgb, (16, 16)));
        Encoder::new().unwrap().encode(&yuv).unwrap().to_vec()
    }
    struct Hardware {
        calls: usize,
        reject_first: bool,
    }
    impl VideoDecoder for Hardware {
        fn backend(&self) -> &'static str {
            "test-hardware"
        }
        fn decode(&mut self, _: &[u8]) -> Result<DecodedYuvFrame, String> {
            self.calls += 1;
            if self.reject_first || self.calls > 1 {
                Err("device failure".into())
            } else {
                software(16, 16)?.decode(&access_unit())
            }
        }
    }
    #[test]
    fn selects_hardware_and_never_switches_reference_state_midstream() {
        let mut decoder = H264Decoder::with_factory(
            16,
            16,
            Some(|_, _| {
                Ok(Box::new(Hardware {
                    calls: 0,
                    reject_first: false,
                }))
            }),
        )
        .unwrap();
        assert_eq!(
            decoder.decode(&access_unit()).unwrap().storage_len(),
            16 * 16 * 3 / 2
        );
        assert_eq!(decoder.backend(), "test-hardware");
        assert!(
            decoder
                .decode(&access_unit())
                .unwrap_err()
                .contains("device failure")
        );
        assert_eq!(decoder.backend(), "test-hardware");
    }
    #[test]
    fn unavailable_or_first_frame_failure_falls_back_to_software() {
        for factory in [
            (|_, _| Err("unsupported".into())) as DecoderFactory,
            (|_, _| {
                Ok(Box::new(Hardware {
                    calls: 0,
                    reject_first: true,
                }))
            }) as DecoderFactory,
        ] {
            let mut decoder = H264Decoder::with_factory(16, 16, Some(factory)).unwrap();
            assert_eq!(
                decoder.decode(&access_unit()).unwrap().storage_len(),
                16 * 16 * 3 / 2
            );
            assert_eq!(decoder.backend(), "openh264");
        }
    }
    #[test]
    fn android_planes_handle_padded_cropped_interleaved_chroma_and_reject_overflow() {
        let y = vec![81; 36];
        let mut uv = vec![128; 12];
        for i in [1, 3, 7, 9] {
            uv[i] = 90;
            uv[i + 1] = 240;
        }
        let mut planes = [
            AndroidYuvPlane {
                data: &y,
                offset: 7,
                row_stride: 6,
                pixel_stride: 1,
            },
            AndroidYuvPlane {
                data: &uv,
                offset: 1,
                row_stride: 6,
                pixel_stride: 2,
            },
            AndroidYuvPlane {
                data: &uv,
                offset: 2,
                row_stride: 6,
                pixel_stride: 2,
            },
        ];
        let rgb = android_yuv420_rgb(&planes, 4, 4, 4, 2).unwrap();
        for pixel in rgb.chunks_exact(3) {
            assert!(pixel[0] >= 250 && pixel[1] <= 2 && pixel[2] <= 2);
        }
        planes[2].data = &uv[..10];
        assert!(android_yuv420_rgb(&planes, 4, 4, 4, 2).is_err());
        planes[2].data = &uv;
        planes[0].row_stride = usize::MAX;
        assert!(android_yuv420_rgb(&planes, 4, 4, 4, 2).is_err());
    }

    #[test]
    fn android_conversion_respects_range_chroma_and_layout_bounds() {
        for (range, black, white) in [(1, 0, 255), (2, 16, 235)] {
            for standard in [1, 2, 4] {
                assert_eq!(
                    android_i420_rgb(
                        &[black, black, black, black, 128, 128],
                        2,
                        2,
                        standard,
                        range
                    )
                    .unwrap(),
                    vec![0; 12]
                );
                assert_eq!(
                    android_i420_rgb(
                        &[white, white, white, white, 128, 128],
                        2,
                        2,
                        standard,
                        range
                    )
                    .unwrap(),
                    vec![255; 12]
                );
            }
        }
        let red = android_i420_rgb(&[81, 81, 81, 81, 90, 240], 2, 2, 4, 2).unwrap();
        assert!(red[0] >= 250 && red[1] <= 2 && red[2] <= 2);
        assert!(android_i420_rgb(&[0; 5], 2, 2, 4, 2).is_err());
        assert!(android_i420_rgb(&[0; 6], 2, 2, 6, 2).is_err());
        assert!(android_i420_rgb(&[0; 6], 2, 2, 4, 0).is_err());
    }
}
