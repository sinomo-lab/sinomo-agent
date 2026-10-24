//! Optional, content-independent coordinate guides for visual tool grounding.
//! Draw inside the final encoded viewport: never crop, pad or move source pixels.
use image::{DynamicImage, GenericImage, GenericImageView, Rgba};

pub(super) fn draw_coordinate_grid(image: &mut DynamicImage) {
    let (width, height) = image.dimensions();
    if width == 0 || height == 0 {
        return;
    }
    let scale = ((height + 150) / 200).clamp(1, 3);
    // Tenths share the input contract's endpoint convention: 0 and 1 are the
    // centers of the first and last pixels, not an extra padded ruler margin.
    for tenth in 1..10 {
        let x = ((u64::from(width - 1) * tenth + 5) / 10) as u32;
        let y = ((u64::from(height - 1) * tenth + 5) / 10) as u32;
        for row in 0..height {
            guide_pixel(image, x, row);
        }
        for column in 0..width {
            guide_pixel(image, column, y);
        }
    }
    for tenth in 0..=10 {
        let x = ((u64::from(width - 1) * tenth + 5) / 10) as u32;
        let y = ((u64::from(height - 1) * tenth + 5) / 10) as u32;
        let text = format!("{}.{:01}", tenth / 10, tenth % 10);
        label(image, x.saturating_sub(13 * scale / 2), 0, &text, scale);
        if tenth > 0 {
            label(image, 0, y.saturating_sub(7 * scale / 2), &text, scale);
        }
    }
}

fn guide_pixel(image: &mut DynamicImage, x: u32, y: u32) {
    let mut pixel = image.get_pixel(x, y);
    for channel in &mut pixel.0[..3] {
        *channel = ((u16::from(*channel) * 3 + 220) / 4) as u8;
    }
    image.put_pixel(x, y, pixel);
}

fn label(image: &mut DynamicImage, x: u32, y: u32, text: &str, scale: u32) {
    const DIGITS: [[u8; 5]; 10] = [
        [7, 5, 5, 5, 7],
        [2, 6, 2, 2, 7],
        [7, 1, 7, 4, 7],
        [7, 1, 7, 1, 7],
        [5, 5, 7, 1, 1],
        [7, 4, 7, 1, 7],
        [7, 4, 7, 5, 7],
        [7, 1, 1, 1, 1],
        [7, 5, 7, 5, 7],
        [7, 5, 7, 1, 7],
    ];
    let width = (text.len() as u32 * 4 + 1) * scale;
    let height = 7 * scale;
    let x = x.min(image.width().saturating_sub(width));
    let y = y.min(image.height().saturating_sub(height));
    for row in 0..height.min(image.height()) {
        for column in 0..width.min(image.width()) {
            image.put_pixel(x + column, y + row, Rgba([8, 16, 24, 255]));
        }
    }
    for (index, character) in text.bytes().enumerate() {
        let glyph = if character == b'.' {
            [0, 0, 0, 0, 2]
        } else {
            DIGITS[usize::from(character - b'0')]
        };
        for row in 0..5 {
            for column in 0..3 {
                if glyph[row as usize] & (4 >> column) == 0 {
                    continue;
                }
                for dy in 0..scale {
                    for dx in 0..scale {
                        let px = x + (1 + index as u32 * 4 + column) * scale + dx;
                        let py = y + (1 + row) * scale + dy;
                        if px < image.width() && py < image.height() {
                            image.put_pixel(px, py, Rgba([255, 240, 120, 255]));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use std::sync::Arc;

    #[test]
    fn coordinate_grid_is_opt_in_and_preserves_resized_viewport_and_timing() {
        let mut source = RealtimeVideoFrameInput::new(
            Arc::<[u8]>::from([20, 40, 60, 255].repeat(900 * 600)),
            RealtimeVideoPixelFormat::Rgba8,
            900,
            600,
            900 * 4,
            17,
        );
        source.timestamp_ms = Some(1234);
        let mut config = RealtimeVideoInputConfig {
            max_dimension: Some(450),
            frame_format: RealtimeVideoFrameFormat::Png,
            ..Default::default()
        };
        let plain = encode_realtime_video_frame(&source, &config).unwrap();
        config.coordinate_grid = true;
        let guided = encode_realtime_video_frame(&source, &config).unwrap();
        assert_eq!((guided.width, guided.height), (Some(450), Some(300)));
        assert_eq!(guided.sequence, 17);
        assert_eq!(guided.timestamp_ms, Some(1234));
        let plain = image::load_from_memory(&plain.image).unwrap().to_rgba8();
        let guided = image::load_from_memory(&guided.image).unwrap().to_rgba8();
        // A source feature inside a grid cell has exactly the same position.
        assert_eq!(guided.get_pixel(210, 165), plain.get_pixel(210, 165));
        // The half-width guide is in encoded coordinates, not source pixels.
        assert_ne!(guided.get_pixel(225, 165), plain.get_pixel(225, 165));
        // The source buffer itself remains untouched for other consumers.
        assert_eq!(&source.pixels[..4], &[20, 40, 60, 255]);
    }
}
