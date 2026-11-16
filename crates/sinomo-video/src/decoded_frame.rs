//! Owned native decoded pixels. No frame retains a scarce decoder output buffer.
use crate::decoder::{AndroidYuvPlane, android_yuv420_rgb, validate_yuv_planes};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum YuvMatrix {
    Bt601Pal,
    Bt601Ntsc,
    Bt709,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum YuvRange {
    Full,
    Limited,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct YuvColorSpace {
    pub matrix: YuvMatrix,
    pub range: YuvRange,
}
impl YuvColorSpace {
    pub fn from_android(standard: i32, range: i32) -> Result<Self, String> {
        Ok(Self {
            matrix: match standard {
                1 => YuvMatrix::Bt709,
                2 => YuvMatrix::Bt601Pal,
                4 => YuvMatrix::Bt601Ntsc,
                _ => return Err("unsupported YUV matrix".into()),
            },
            range: match range {
                1 => YuvRange::Full,
                2 => YuvRange::Limited,
                _ => return Err("unsupported YUV range".into()),
            },
        })
    }
    fn android(self) -> (i32, i32) {
        (
            match self.matrix {
                YuvMatrix::Bt601Pal => 2,
                YuvMatrix::Bt601Ntsc => 4,
                YuvMatrix::Bt709 => 1,
            },
            match self.range {
                YuvRange::Full => 1,
                YuvRange::Limited => 2,
            },
        )
    }
}

/// Cropped, tightly packed I420. Clones share immutable storage. Source padding
/// and crop offsets are normalized during the bounded copy, before codec release.
#[derive(Clone, Debug)]
pub struct DecodedYuvFrame {
    data: Arc<[u8]>,
    width: u32,
    height: u32,
    color: YuvColorSpace,
}
impl DecodedYuvFrame {
    pub fn copy_from_planes(
        planes: &[AndroidYuvPlane<'_>; 3],
        width: u32,
        height: u32,
        color: YuvColorSpace,
    ) -> Result<Self, String> {
        validate_yuv_planes(planes, width, height)?;
        let pixels = width as usize * height as usize;
        // SAFETY: all-zero bytes are valid u8 values. Allocate directly into
        // final shared storage rather than copying a temporary Vec into an Arc.
        let mut data = unsafe { Arc::<[u8]>::new_zeroed_slice(pixels * 3 / 2).assume_init() };
        let target = Arc::get_mut(&mut data).expect("new frame storage is unique");
        let mut cursor = 0;
        for (index, plane) in planes.iter().enumerate() {
            let shift = usize::from(index > 0);
            let w = (width as usize) >> shift;
            let h = (height as usize) >> shift;
            for row in 0..h {
                let start = plane.offset + row * plane.row_stride;
                let output = &mut target[cursor..cursor + w];
                if plane.pixel_stride == 1 {
                    output.copy_from_slice(&plane.data[start..start + w]);
                } else {
                    #[allow(unused_mut)]
                    let mut copied = 0;
                    #[cfg(target_arch = "aarch64")]
                    if plane.pixel_stride == 2 {
                        // Bounds above cover all active samples. Check the extra
                        // interleaved byte too: some final U/V views omit it.
                        use std::arch::aarch64::{vld2q_u8, vst1q_u8};
                        while copied + 16 <= output.len()
                            && plane.data.len() - (start + copied * 2) >= 32
                        {
                            // SAFETY: 32 source bytes and 16 output bytes are in
                            // bounds; AArch64 guarantees unaligned NEON loads.
                            unsafe {
                                let samples = vld2q_u8(plane.data.as_ptr().add(start + copied * 2));
                                vst1q_u8(output.as_mut_ptr().add(copied), samples.0);
                            }
                            copied += 16;
                        }
                    }
                    for (col, value) in output[copied..].iter_mut().enumerate() {
                        *value = plane.data[start + (copied + col) * plane.pixel_stride];
                    }
                }
                cursor += w;
            }
        }
        Ok(Self {
            data,
            width,
            height,
            color,
        })
    }
    pub fn width(&self) -> u32 {
        self.width
    }
    pub fn height(&self) -> u32 {
        self.height
    }
    pub fn color_space(&self) -> YuvColorSpace {
        self.color
    }
    pub fn storage_len(&self) -> usize {
        self.data.len()
    }
    pub fn planes(&self) -> [AndroidYuvPlane<'_>; 3] {
        let pixels = self.width as usize * self.height as usize;
        [
            AndroidYuvPlane {
                data: &self.data[..pixels],
                offset: 0,
                row_stride: self.width as usize,
                pixel_stride: 1,
            },
            AndroidYuvPlane {
                data: &self.data[pixels..pixels + pixels / 4],
                offset: 0,
                row_stride: self.width as usize / 2,
                pixel_stride: 1,
            },
            AndroidYuvPlane {
                data: &self.data[pixels + pixels / 4..],
                offset: 0,
                row_stride: self.width as usize / 2,
                pixel_stride: 1,
            },
        ]
    }
    /// Explicit RGB materialization for consumers that cannot use native YUV.
    pub fn to_rgb(&self) -> Result<Vec<u8>, String> {
        let (standard, range) = self.color.android();
        android_yuv420_rgb(&self.planes(), self.width, self.height, standard, range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owned_planes_normalize_crop_stride_and_outlive_source() {
        let frame = {
            let y = vec![81; 36];
            let mut uv = vec![128; 12];
            for i in [1, 3, 7, 9] {
                uv[i] = 90;
                uv[i + 1] = 240;
            }
            let planes = [
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
            DecodedYuvFrame::copy_from_planes(
                &planes,
                4,
                4,
                YuvColorSpace::from_android(4, 2).unwrap(),
            )
            .unwrap()
        };
        let clone = frame.clone();
        assert!(Arc::ptr_eq(&clone.data, &frame.data));
        assert_eq!(frame.storage_len(), 24);
        assert_eq!(frame.planes()[1].data, &[90; 4]);
        drop(frame);
        for p in clone.to_rgb().unwrap().as_chunks::<3>().0 {
            assert!(p[0] >= 250 && p[1] <= 2 && p[2] <= 2);
        }
    }
}
