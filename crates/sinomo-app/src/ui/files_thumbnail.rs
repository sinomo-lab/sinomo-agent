//! Bounded raster-thumbnail decoding and UI-side cache primitives for the Files tab.

use std::collections::{BTreeMap, VecDeque};
use std::io::Cursor;
use std::sync::Arc;

use image::metadata::Orientation;
use image::{DynamicImage, ImageDecoder, ImageReader, Limits};

use super::files::FilesSourceKind;

pub(super) const THUMBNAIL_MAX_SOURCE_BYTES: u64 = 32 * 1024 * 1024;
const THUMBNAIL_MAX_DIMENSION: u32 = 16_384;
const THUMBNAIL_MAX_PIXELS: u64 = 32 * 1024 * 1024;
const THUMBNAIL_MAX_DECODE_ALLOC: u64 = 128 * 1024 * 1024;
pub(super) const THUMBNAIL_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;
pub(super) const THUMBNAIL_CACHE_MAX_ENTRIES: usize = 2_048;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum ThumbnailBucket {
    Medium,
    Large,
}

impl ThumbnailBucket {
    pub(super) const fn target_side(self) -> u32 {
        match self {
            Self::Medium => 128,
            Self::Large => 256,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum ThumbnailIdentity {
    /// Local listings do not carry a stable modification fingerprint. Their epoch changes for
    /// every non-append reload, so a refreshed folder never reuses an unverified bitmap.
    Listing(u64),
    /// SIFS listing metadata already exposes immutable object-version identity.
    Sifs { object_id: String, version: String },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ThumbnailRequestKey {
    pub source: FilesSourceKind,
    pub path: String,
    pub identity: ThumbnailIdentity,
    pub bucket: ThumbnailBucket,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ThumbnailRequest {
    pub key: ThumbnailRequestKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ThumbnailBitmap {
    pub width: u32,
    pub height: u32,
    pub rgba: Arc<[u8]>,
}

impl ThumbnailBitmap {
    pub(super) fn byte_len(&self) -> usize {
        self.rgba.len()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ThumbnailCacheEntry {
    Ready(Arc<ThumbnailBitmap>),
    Unavailable,
}

impl ThumbnailCacheEntry {
    fn byte_len(&self) -> usize {
        match self {
            Self::Ready(bitmap) => bitmap.byte_len(),
            Self::Unavailable => 0,
        }
    }
}

#[derive(Debug)]
pub(super) struct ThumbnailCache {
    entries: BTreeMap<ThumbnailRequestKey, ThumbnailCacheEntry>,
    order: VecDeque<ThumbnailRequestKey>,
    total_bytes: usize,
    max_bytes: usize,
    max_entries: usize,
}

impl ThumbnailCache {
    pub(super) fn new(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            order: VecDeque::new(),
            total_bytes: 0,
            max_bytes: max_bytes.max(1),
            max_entries: max_entries.max(1),
        }
    }

    pub(super) fn contains(&self, key: &ThumbnailRequestKey) -> bool {
        self.entries.contains_key(key)
    }

    pub(super) fn ready(&self, key: &ThumbnailRequestKey) -> Option<Arc<ThumbnailBitmap>> {
        match self.entries.get(key) {
            Some(ThumbnailCacheEntry::Ready(bitmap)) => Some(Arc::clone(bitmap)),
            Some(ThumbnailCacheEntry::Unavailable) | None => None,
        }
    }

    pub(super) fn touch(&mut self, key: &ThumbnailRequestKey) {
        if !self.entries.contains_key(key) {
            return;
        }
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.clone());
    }

    pub(super) fn insert(&mut self, key: ThumbnailRequestKey, entry: ThumbnailCacheEntry) {
        if let Some(previous) = self.entries.remove(&key) {
            self.total_bytes = self.total_bytes.saturating_sub(previous.byte_len());
        }
        self.order.retain(|candidate| candidate != &key);
        self.total_bytes = self.total_bytes.saturating_add(entry.byte_len());
        self.entries.insert(key.clone(), entry);
        self.order.push_back(key);
        self.evict_to_limits();
    }

    pub(super) fn remove_unavailable(&mut self) {
        let unavailable = self
            .entries
            .iter()
            .filter(|(_, entry)| matches!(entry, ThumbnailCacheEntry::Unavailable))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in unavailable {
            self.entries.remove(&key);
            self.order.retain(|candidate| candidate != &key);
        }
    }

    fn evict_to_limits(&mut self) {
        while self.total_bytes > self.max_bytes || self.entries.len() > self.max_entries {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.remove(&oldest) {
                self.total_bytes = self.total_bytes.saturating_sub(entry.byte_len());
            }
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) fn total_bytes(&self) -> usize {
        self.total_bytes
    }
}

impl Default for ThumbnailCache {
    fn default() -> Self {
        Self::new(THUMBNAIL_CACHE_MAX_BYTES, THUMBNAIL_CACHE_MAX_ENTRIES)
    }
}

pub(super) fn thumbnail_candidate(name: &str) -> bool {
    let extension = std::path::Path::new(name)
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    matches!(
        extension.as_deref(),
        Some("jpg" | "jpeg" | "jfif" | "png" | "webp")
    )
}

pub(super) fn decode_thumbnail(
    bytes: &[u8],
    bucket: ThumbnailBucket,
) -> Result<Arc<ThumbnailBitmap>, String> {
    decode_raster_preview(bytes, bucket.target_side())
}

pub(super) fn decode_raster_preview(
    bytes: &[u8],
    target_side: u32,
) -> Result<Arc<ThumbnailBitmap>, String> {
    if bytes.is_empty() {
        return Err("image is empty".to_string());
    }
    let mut reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|error| format!("cannot inspect image format: {error}"))?;
    let format = reader
        .format()
        .ok_or_else(|| "unsupported image format".to_string())?;
    if !matches!(
        format,
        image::ImageFormat::Jpeg | image::ImageFormat::Png | image::ImageFormat::WebP
    ) {
        return Err(format!("unsupported image format: {format:?}"));
    }
    let mut limits = Limits::default();
    limits.max_image_width = Some(THUMBNAIL_MAX_DIMENSION);
    limits.max_image_height = Some(THUMBNAIL_MAX_DIMENSION);
    limits.max_alloc = Some(THUMBNAIL_MAX_DECODE_ALLOC);
    reader.limits(limits);
    let mut decoder = reader
        .into_decoder()
        .map_err(|error| format!("cannot create image decoder: {error}"))?;
    let (width, height) = decoder.dimensions();
    let pixels = u64::from(width)
        .checked_mul(u64::from(height))
        .ok_or_else(|| "image dimensions overflow".to_string())?;
    if pixels > THUMBNAIL_MAX_PIXELS {
        return Err(format!(
            "image has {pixels} pixels; thumbnail limit is {THUMBNAIL_MAX_PIXELS}"
        ));
    }
    let orientation = decoder.orientation().unwrap_or(Orientation::NoTransforms);
    let mut decoded = DynamicImage::from_decoder(decoder)
        .map_err(|error| format!("cannot decode image: {error}"))?;
    decoded.apply_orientation(orientation);
    let thumbnail = decoded.thumbnail(target_side, target_side);
    let rgba = thumbnail.to_rgba8();
    let (width, height) = rgba.dimensions();
    Ok(Arc::new(ThumbnailBitmap {
        width,
        height,
        rgba: Arc::from(rgba.into_raw()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitmap(side: u32, value: u8) -> Arc<ThumbnailBitmap> {
        Arc::new(ThumbnailBitmap {
            width: side,
            height: side,
            rgba: Arc::from(vec![value; side as usize * side as usize * 4]),
        })
    }

    fn key(path: &str) -> ThumbnailRequestKey {
        ThumbnailRequestKey {
            source: FilesSourceKind::Local,
            path: path.to_string(),
            identity: ThumbnailIdentity::Listing(1),
            bucket: ThumbnailBucket::Medium,
        }
    }

    #[test]
    fn raster_candidate_filter_is_deliberately_narrow() {
        for supported in ["photo.JPG", "image.jpeg", "asset.png", "cover.webp"] {
            assert!(thumbnail_candidate(supported), "{supported}");
        }
        for unsupported in ["notes.txt", "vector.svg", "movie.mp4", "no-extension"] {
            assert!(!thumbnail_candidate(unsupported), "{unsupported}");
        }
    }

    #[test]
    fn thumbnail_decode_preserves_aspect_ratio_and_bounds_output() {
        let source = image::RgbaImage::from_pixel(400, 200, image::Rgba([220, 20, 40, 255]));
        let mut encoded = Cursor::new(Vec::new());
        DynamicImage::ImageRgba8(source)
            .write_to(&mut encoded, image::ImageFormat::Png)
            .expect("encode PNG fixture");

        let thumbnail = decode_thumbnail(&encoded.into_inner(), ThumbnailBucket::Medium)
            .expect("decode thumbnail");
        assert_eq!((thumbnail.width, thumbnail.height), (128, 64));
        assert_eq!(thumbnail.rgba.len(), 128 * 64 * 4);
    }

    #[test]
    fn generated_image_preview_decodes_provider_formats_and_rejects_invalid_data() {
        for format in [
            image::ImageFormat::Png,
            image::ImageFormat::Jpeg,
            image::ImageFormat::WebP,
        ] {
            let source = image::RgbImage::from_pixel(900, 600, image::Rgb([20, 210, 40]));
            let mut encoded = Cursor::new(Vec::new());
            DynamicImage::ImageRgb8(source)
                .write_to(&mut encoded, format)
                .unwrap();
            let preview = decode_raster_preview(&encoded.into_inner(), 512).unwrap();
            assert_eq!((preview.width, preview.height), (512, 341));
        }
        assert!(decode_raster_preview(b"not an image", 512).is_err());
        assert!(decode_raster_preview(&[], 512).is_err());
    }

    #[test]
    fn thumbnail_cache_evicts_old_entries_by_byte_budget() {
        let mut cache = ThumbnailCache::new(128, 8);
        cache.insert(key("first.png"), ThumbnailCacheEntry::Ready(bitmap(4, 1)));
        cache.insert(key("second.png"), ThumbnailCacheEntry::Ready(bitmap(4, 2)));
        cache.touch(&key("first.png"));
        cache.insert(key("third.png"), ThumbnailCacheEntry::Ready(bitmap(4, 3)));

        assert_eq!(cache.len(), 2);
        assert!(cache.contains(&key("first.png")));
        assert!(!cache.contains(&key("second.png")));
        assert!(cache.contains(&key("third.png")));
        assert_eq!(cache.total_bytes(), 128);
    }
}
