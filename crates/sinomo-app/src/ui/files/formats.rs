//! Lossless document representations and bounded, passive format previews.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use image::{DynamicImage, ImageDecoder, ImageReader, Limits};
use sui::{
    Alignment, ArrangeCtx, Constraints, Event, EventCtx, ImageSource, Label, MeasureCtx, PaintCtx,
    Rect, RegisteredImage, RichDocumentModel, RichDocumentView, ScrollView, SemanticsCtx,
    SemanticsNode, SemanticsRole, Size, SizedBox, Stack, Table, TableColumn, TableRow, TextSurface,
    TextWrap, Widget, WidgetPod,
};

use super::{FILE_DOCUMENT_MAX_BYTES, ThemeReader, clone_reader};

const PREVIEW_TEXT_BYTES: usize = 64 * 1024;
const TABLE_ROWS: usize = 128;
const TABLE_COLUMNS: usize = 24;
const TABLE_CELL_CHARS: usize = 256;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) enum DocumentFormat {
    #[default]
    Text,
    Markdown,
    Json,
    Csv,
    Tsv,
    Image,
    Hex,
}

impl DocumentFormat {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Text => "Text",
            Self::Markdown => "Markdown",
            Self::Json => "JSON",
            Self::Csv => "CSV",
            Self::Tsv => "TSV",
            Self::Image => "Image",
            Self::Hex => "Hex",
        }
    }

    pub(super) fn has_preview(self) -> bool {
        !matches!(self, Self::Text | Self::Hex)
    }
}

/// Called on the document load worker. Raster validation warms the bounded preview cache.
pub(super) fn document_from_bytes(path: &str, bytes: &[u8]) -> (DocumentFormat, String) {
    if bytes.len() as u64 <= FILE_DOCUMENT_MAX_BYTES
        && image::guess_format(bytes).is_ok_and(supported_image)
        && image_bitmap(bytes).is_ok()
    {
        return (DocumentFormat::Image, encode_hex(bytes));
    }
    if super::preview_bytes_are_text(bytes) {
        let extension = Path::new(path)
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let format = match extension.as_str() {
            "md" | "markdown" | "mdown" => DocumentFormat::Markdown,
            "json" | "ipynb" => DocumentFormat::Json,
            "csv" => DocumentFormat::Csv,
            "tsv" | "tab" => DocumentFormat::Tsv,
            _ => DocumentFormat::Text,
        };
        // Classification above validates UTF-8; preserve BOM, CRLF and trailing newlines.
        return (format, std::str::from_utf8(bytes).unwrap().to_string());
    }
    (DocumentFormat::Hex, encode_hex(bytes))
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len().saturating_mul(3));
    for (index, byte) in bytes.iter().enumerate() {
        if index != 0 {
            text.push(if index % 16 == 0 { '\n' } else { ' ' });
        }
        write!(text, "{byte:02X}").expect("writing to a string");
    }
    text
}

pub(super) fn document_bytes(format: DocumentFormat, text: &str) -> Result<Vec<u8>, String> {
    let limit = FILE_DOCUMENT_MAX_BYTES as usize;
    if !matches!(format, DocumentFormat::Hex | DocumentFormat::Image) {
        return if text.len() <= limit {
            Ok(text.as_bytes().to_vec())
        } else {
            Err("The edited document exceeds the 2 MiB document limit.".into())
        };
    }
    if text.len() > limit.saturating_mul(6) {
        return Err("The hex input exceeds the document editor limit.".into());
    }
    let mut bytes = Vec::with_capacity((text.len() / 3).min(limit));
    for (index, pair) in text.split_ascii_whitespace().enumerate() {
        if bytes.len() == limit {
            return Err("The decoded file exceeds the 2 MiB document limit.".into());
        }
        if pair.len() != 2 || !pair.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(format!(
                "Invalid hex byte {}. Enter two hexadecimal digits per byte, separated by spaces or newlines.",
                index + 1
            ));
        }
        bytes.push(u8::from_str_radix(pair, 16).expect("validated hex pair"));
    }
    Ok(bytes)
}

fn bounded_text(text: &str) -> (&str, bool) {
    let mut end = text.len().min(PREVIEW_TEXT_BYTES);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (&text[..end], end < text.len())
}

fn note(text: impl Into<String>, reader: &ThemeReader) -> WidgetPod {
    WidgetPod::new(Label::new(text).theme_when(clone_reader(reader)))
}

fn text_preview(text: String, reader: &ThemeReader) -> WidgetPod {
    WidgetPod::new(
        TextSurface::new("formatted-document-preview")
            .value(text)
            .read_only(true)
            .wrap(TextWrap::NoWrap)
            .theme_when(clone_reader(reader))
            .text_style_when(|theme| theme.mono_text_style(theme.palette.text)),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonPreviewLimit {
    Complete,
    SourcePrefix,
    FormattedPrefix,
}

#[derive(Default)]
struct JsonPreviewWriter {
    bytes: Vec<u8>,
    limited: bool,
}

impl std::io::Write for JsonPreviewWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let accepted = bytes.len().min(PREVIEW_TEXT_BYTES - self.bytes.len());
        self.bytes.extend_from_slice(&bytes[..accepted]);
        if accepted < bytes.len() {
            self.limited = true;
            return Err(std::io::Error::other("JSON preview output limit reached"));
        }
        Ok(accepted)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Only JSON structural whitespace changes. Number spelling, key order, duplicate keys,
/// string escapes and Unicode are copied directly from the validated source.
fn write_pretty_json_tokens(text: &str, writer: &mut JsonPreviewWriter) -> std::io::Result<()> {
    use std::io::Write as _;

    fn newline(writer: &mut JsonPreviewWriter, depth: usize) -> std::io::Result<()> {
        writer.write_all(b"\n")?;
        for _ in 0..depth {
            writer.write_all(b"  ")?;
        }
        Ok(())
    }

    let bytes = text.as_bytes();
    let mut depth = 0;
    let mut in_string = false;
    let mut escaped = false;
    let mut previous = None;
    for (index, &byte) in bytes.iter().enumerate() {
        if in_string {
            writer.write_all(&[byte])?;
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b' ' | b'\n' | b'\r' | b'\t' => continue,
            b'"' => {
                in_string = true;
                writer.write_all(&[byte])?;
            }
            b'{' | b'[' => {
                writer.write_all(&[byte])?;
                depth += 1;
                let close = if byte == b'{' { b'}' } else { b']' };
                if bytes[index + 1..]
                    .iter()
                    .find(|byte| !byte.is_ascii_whitespace())
                    != Some(&close)
                {
                    newline(writer, depth)?;
                }
            }
            b'}' | b']' => {
                depth -= 1;
                let open = if byte == b'}' { b'{' } else { b'[' };
                if previous != Some(open) {
                    newline(writer, depth)?;
                }
                writer.write_all(&[byte])?;
            }
            b',' => {
                writer.write_all(b",")?;
                newline(writer, depth)?;
            }
            b':' => writer.write_all(b": ")?,
            _ => writer.write_all(&[byte])?,
        }
        previous = Some(byte);
    }
    Ok(())
}

fn json_preview_content(text: &str) -> Result<(String, JsonPreviewLimit), String> {
    // A large source is never parsed on the UI thread. In particular, parsing a complete
    // document before truncating its formatted output would not bound preview work.
    if text.len() > PREVIEW_TEXT_BYTES {
        return Ok((
            bounded_text(text).0.to_string(),
            JsonPreviewLimit::SourcePrefix,
        ));
    }
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    serde_json::from_str::<serde::de::IgnoredAny>(text).map_err(|error| error.to_string())?;
    let mut writer = JsonPreviewWriter::default();
    let result = write_pretty_json_tokens(text, &mut writer);
    if writer.limited {
        // The final write can end inside a UTF-8 scalar; omit that partial scalar.
        if let Err(error) = std::str::from_utf8(&writer.bytes) {
            writer.bytes.truncate(error.valid_up_to());
        }
        return Ok((
            String::from_utf8(writer.bytes).unwrap(),
            JsonPreviewLimit::FormattedPrefix,
        ));
    }
    result.map_err(|error| error.to_string())?;
    Ok((
        String::from_utf8(writer.bytes).unwrap(),
        JsonPreviewLimit::Complete,
    ))
}

pub(super) struct PreviewWidget(WidgetPod);

impl Widget for PreviewWidget {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.0.measure(ctx, constraints)
    }
    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.0.arrange(ctx, bounds);
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        self.0.paint(ctx);
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.0.semantics(ctx);
    }
    fn visit_children(&self, visitor: &mut dyn sui::WidgetPodVisitor) {
        visitor.visit(&self.0);
    }
    fn visit_children_mut(&mut self, visitor: &mut dyn sui::WidgetPodMutVisitor) {
        visitor.visit(&mut self.0);
    }
}

pub(super) fn preview_widget(
    format: DocumentFormat,
    text: &str,
    reader: &ThemeReader,
) -> Option<PreviewWidget> {
    Some(PreviewWidget(match format {
        DocumentFormat::Text | DocumentFormat::Hex => return None,
        DocumentFormat::Image => match document_bytes(format, text) {
            Ok(bytes) => WidgetPod::new(DocumentImage::new(bytes, reader)),
            Err(error) => note(error, reader),
        },
        DocumentFormat::Markdown => {
            let (prefix, truncated) = bounded_text(text);
            // The stock renderer has no link callback, image resolver, or attachment provider.
            // Links and images are passive content; opening a file never fetches resources.
            let rendered = RichDocumentView::new(RichDocumentModel::from_markdown(prefix))
                .theme_when(clone_reader(reader));
            let mut body = Stack::vertical().alignment(Alignment::Stretch).gap(8.0);
            if truncated {
                body = body.with_child(PreviewWidget(note(
                    "Preview limited to 64 KiB. Source contains the complete document.",
                    reader,
                )));
            }
            WidgetPod::new(body.with_child(rendered))
        }
        DocumentFormat::Json => match json_preview_content(text) {
            Ok((preview, limit)) => {
                let shown = match limit {
                    JsonPreviewLimit::Complete => preview,
                    JsonPreviewLimit::SourcePrefix => format!(
                        "Incomplete JSON preview: showing the first 64 KiB without parsing or formatting. Source contains the complete document.\n\n{preview}"
                    ),
                    JsonPreviewLimit::FormattedPrefix => format!(
                        "Incomplete JSON preview: formatted output is limited to 64 KiB. Source contains the complete document.\n\n{preview}"
                    ),
                };
                text_preview(shown, reader)
            }
            Err(error) => note(
                format!("JSON preview unavailable: {error}. Use Source to edit."),
                reader,
            ),
        },
        DocumentFormat::Csv | DocumentFormat::Tsv => {
            let delimiter = if format == DocumentFormat::Csv {
                ','
            } else {
                '\t'
            };
            match delimited_rows(text, delimiter) {
                Ok((rows, limited)) => {
                    let column_count = rows.iter().map(Vec::len).max().unwrap_or(1).max(1);
                    let columns = (1..=column_count)
                        .map(|column| TableColumn::new(format!("Column {column}")).width(140.0));
                    let table = Table::new("Delimited file preview")
                        .columns(columns)
                        .rows(rows.into_iter().map(TableRow::new))
                        .theme_when(clone_reader(reader));
                    let mut body = Stack::vertical().alignment(Alignment::Stretch).gap(8.0);
                    if limited {
                        body = body.with_child(PreviewWidget(note(
                            "Preview limited to 128 rows, 24 columns and 256 characters per cell. Source retains all data.",
                            reader,
                        )));
                    }
                    WidgetPod::new(
                        body.with_child(ScrollView::horizontal(
                            SizedBox::new()
                                .width(column_count as f32 * 140.0)
                                .height(420.0)
                                .child(table),
                        )),
                    )
                }
                Err(error) => note(
                    format!("Table preview unavailable: {error}. Use Source to edit."),
                    reader,
                ),
            }
        }
    }))
}

/// A bounded RFC 4180-style parser used only for display; source bytes are never rewritten.
fn delimited_rows(text: &str, delimiter: char) -> Result<(Vec<Vec<String>>, bool), String> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut cell = String::new();
    let mut cell_len = 0;
    let mut quoted = false;
    let mut quote_closed = false;
    let mut limited = false;
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '"' {
            if quoted && chars.peek() == Some(&'"') {
                chars.next();
            } else if quoted {
                quoted = false;
                quote_closed = true;
                continue;
            } else if cell_len == 0 && !quote_closed {
                quoted = true;
                continue;
            } else {
                return Err("Unexpected quote inside an unquoted field".into());
            }
        } else if !quoted && (ch == delimiter || ch == '\n' || ch == '\r') {
            if row.len() < TABLE_COLUMNS {
                row.push(std::mem::take(&mut cell));
            } else {
                cell.clear();
                limited = true;
            }
            cell_len = 0;
            quote_closed = false;
            if ch != delimiter {
                if ch == '\r' && chars.peek() == Some(&'\n') {
                    chars.next();
                }
                rows.push(std::mem::take(&mut row));
                if rows.len() == TABLE_ROWS {
                    limited |= chars.peek().is_some();
                    return Ok((rows, limited));
                }
            }
            continue;
        } else if quote_closed {
            return Err("Unexpected text after a closing quote".into());
        }
        if cell_len < TABLE_CELL_CHARS {
            cell.push(ch);
        } else {
            limited = true;
        }
        cell_len += 1;
    }
    if quoted {
        return Err("Unclosed quoted field".into());
    }
    if !row.is_empty() || cell_len > 0 || quote_closed || text.ends_with(delimiter) {
        if row.len() < TABLE_COLUMNS {
            row.push(cell);
        } else {
            limited = true;
        }
        rows.push(row);
    }
    Ok((rows, limited))
}

#[derive(Clone)]
struct Bitmap {
    width: u32,
    height: u32,
    rgba: Arc<[u8]>,
}

type ImageCache = VecDeque<(blake3::Hash, Arc<Bitmap>)>;
static IMAGE_CACHE: OnceLock<Mutex<ImageCache>> = OnceLock::new();
static IMAGE_DECODERS: (Mutex<usize>, Condvar) = (Mutex::new(0), Condvar::new());

struct DecodePermit;

impl DecodePermit {
    fn acquire() -> Self {
        let mut count = IMAGE_DECODERS.0.lock().unwrap();
        while *count >= 2 {
            count = IMAGE_DECODERS.1.wait(count).unwrap();
        }
        *count += 1;
        Self
    }
}

impl Drop for DecodePermit {
    fn drop(&mut self) {
        *IMAGE_DECODERS.0.lock().unwrap() -= 1;
        IMAGE_DECODERS.1.notify_one();
    }
}

fn supported_image(format: image::ImageFormat) -> bool {
    matches!(
        format,
        image::ImageFormat::Png
            | image::ImageFormat::Jpeg
            | image::ImageFormat::WebP
            | image::ImageFormat::Gif
            | image::ImageFormat::Bmp
            | image::ImageFormat::Ico
            | image::ImageFormat::Tiff
    )
}

fn cached_bitmap(bytes: &[u8]) -> Option<Arc<Bitmap>> {
    let hash = blake3::hash(bytes);
    IMAGE_CACHE
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .iter()
        .find(|(key, _)| *key == hash)
        .map(|(_, bitmap)| Arc::clone(bitmap))
}

fn image_bitmap(bytes: &[u8]) -> Result<Arc<Bitmap>, String> {
    if let Some(bitmap) = cached_bitmap(bytes) {
        return Ok(bitmap);
    }
    let _permit = DecodePermit::acquire();
    let mut reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|error| error.to_string())?;
    if !reader.format().is_some_and(supported_image) {
        return Err("Unsupported raster format".into());
    }
    let mut limits = Limits::default();
    limits.max_image_width = Some(8192);
    limits.max_image_height = Some(8192);
    limits.max_alloc = Some(64 * 1024 * 1024);
    reader.limits(limits);
    let mut decoder = reader.into_decoder().map_err(|error| error.to_string())?;
    let (width, height) = decoder.dimensions();
    if width == 0 || height == 0 {
        return Err("Image dimensions are empty".into());
    }
    if u64::from(width) * u64::from(height) > 16 * 1024 * 1024 {
        return Err("Image exceeds the 16 megapixel preview limit".into());
    }
    let orientation = decoder
        .orientation()
        .unwrap_or(image::metadata::Orientation::NoTransforms);
    let mut decoded = DynamicImage::from_decoder(decoder).map_err(|error| error.to_string())?;
    decoded.apply_orientation(orientation);
    let rgba = if decoded.width() > 1024 || decoded.height() > 1024 {
        decoded.thumbnail(1024, 1024).to_rgba8()
    } else {
        decoded.to_rgba8()
    };
    let bitmap = Arc::new(Bitmap {
        width: rgba.width(),
        height: rgba.height(),
        rgba: Arc::from(rgba.into_raw()),
    });
    let mut cache = IMAGE_CACHE.get_or_init(Default::default).lock().unwrap();
    cache.push_back((blake3::hash(bytes), Arc::clone(&bitmap)));
    while cache.len() > 4 {
        cache.pop_front();
    }
    Ok(bitmap)
}

struct DocumentImage {
    state: Arc<Mutex<Option<Result<Arc<Bitmap>, String>>>>,
    placeholder: WidgetPod,
    reader: ThemeReader,
    displayed_error: Option<String>,
    pending: bool,
}

impl DocumentImage {
    fn new(bytes: Vec<u8>, reader: &ThemeReader) -> Self {
        let cached = cached_bitmap(&bytes).map(Ok);
        let state = Arc::new(Mutex::new(cached));
        if state.lock().unwrap().is_none() {
            let result = Arc::clone(&state);
            if std::thread::Builder::new()
                .name("document-image-preview".into())
                .spawn(move || {
                    let decoded = image_bitmap(&bytes);
                    *result.lock().unwrap() = Some(decoded);
                })
                .is_err()
            {
                *state.lock().unwrap() = Some(Err("Image preview worker could not start".into()));
            }
        }
        let pending = state.lock().unwrap().is_none();
        Self {
            state,
            placeholder: note("Loading image preview…", reader),
            reader: std::rc::Rc::clone(reader),
            displayed_error: None,
            pending,
        }
    }
}

impl Widget for DocumentImage {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if self.pending && matches!(event, Event::Wake(_)) {
            if self.state.lock().unwrap().is_none() {
                ctx.request_animation_frame();
            } else {
                self.pending = false;
                ctx.request_measure();
                ctx.request_paint();
                ctx.request_semantics();
            }
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let error = self
            .state
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .cloned();
        if error != self.displayed_error {
            self.displayed_error = error.clone();
            if let Some(error) = error {
                self.placeholder = note(
                    format!("Image preview unavailable: {error}. Use Hex to inspect the bytes."),
                    &self.reader,
                );
            }
        }
        if self.pending {
            ctx.request_animation_frame();
        }
        self.placeholder.measure(ctx, constraints);
        let size = match self.state.lock().unwrap().as_ref() {
            Some(Ok(bitmap)) => {
                let scale = (constraints.max.width / bitmap.width as f32)
                    .min(constraints.max.height / bitmap.height as f32)
                    .min(1.0);
                Size::new(bitmap.width as f32 * scale, bitmap.height as f32 * scale)
            }
            _ => Size::new(320.0, 240.0),
        };
        constraints.clamp(size)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.placeholder.arrange(ctx, bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let state = self.state.lock().unwrap();
        let Some(Ok(bitmap)) = state.as_ref() else {
            self.placeholder.paint(ctx);
            return;
        };
        let handle = ctx.widget_image_handle(0);
        if !ctx.image_registered(handle) {
            if let Ok(image) =
                RegisteredImage::from_rgba8(bitmap.width, bitmap.height, bitmap.rgba.to_vec())
            {
                ctx.register_image(handle, image);
            } else {
                return;
            }
        }
        let bounds = ctx.bounds();
        let scale =
            (bounds.width() / bitmap.width as f32).min(bounds.height() / bitmap.height as f32);
        let width = bitmap.width as f32 * scale;
        let height = bitmap.height as f32 * scale;
        ctx.draw_image_source(
            Rect::new(
                bounds.x() + (bounds.width() - width) / 2.0,
                bounds.y() + (bounds.height() - height) / 2.0,
                width,
                height,
            ),
            ImageSource::new(handle),
        );
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Image, ctx.bounds());
        node.name = Some("File image preview".into());
        node.description = Some(match self.state.lock().unwrap().as_ref() {
            Some(Ok(bitmap)) => format!(
                "{} × {} pixel preview; use Hex to inspect original bytes",
                bitmap.width, bitmap.height
            ),
            Some(Err(error)) => {
                format!("Image preview unavailable: {error}. Use Hex to inspect the bytes.")
            }
            None => "Loading image preview".into(),
        });
        ctx.push(node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_preview_preserves_number_lexemes_strings_and_key_order() {
        let source = r#"{ "z":1234567890123456789012345678901234567890,"a":0.123456789012345678901234567890,"e":1.234567890123456789E+999,"negative":-0.00e-12,"z":2,"string":"escaped \" quote, braces {} and \\ path; \u4e16界","empty":[{},[]] }"#;
        let (pretty, limit) = json_preview_content(source).unwrap();
        assert_eq!(limit, JsonPreviewLimit::Complete);
        for lexeme in [
            "1234567890123456789012345678901234567890",
            "0.123456789012345678901234567890",
            "1.234567890123456789E+999",
            "-0.00e-12",
            r#""string": "escaped \" quote, braces {} and \\ path; \u4e16界""#,
        ] {
            assert!(pretty.contains(lexeme), "missing exact lexeme: {lexeme}");
        }
        assert!(pretty.find("\"z\"").unwrap() < pretty.find("\"a\"").unwrap());
        assert_eq!(pretty.matches("\"z\"").count(), 2);
        assert!(serde_json::from_str::<serde::de::IgnoredAny>(&pretty).is_ok());
        assert_eq!(json_preview_content(&pretty).unwrap().0, pretty);
    }

    #[test]
    fn json_preview_bounds_source_parsing_and_formatted_expansion() {
        let source = "not JSON 🦀 ".repeat(PREVIEW_TEXT_BYTES);
        let (prefix, limit) = json_preview_content(&source).unwrap();
        assert_eq!(limit, JsonPreviewLimit::SourcePrefix);
        assert!(prefix.len() <= PREVIEW_TEXT_BYTES);
        assert!(source.starts_with(&prefix));

        // Small input with deep indentation would otherwise expand to hundreds of KiB.
        let source = format!(
            "{}{}{}",
            "[".repeat(100),
            vec!["0"; 4000].join(","),
            "]".repeat(100)
        );
        assert!(source.len() < PREVIEW_TEXT_BYTES);
        let (prefix, limit) = json_preview_content(&source).unwrap();
        assert_eq!(limit, JsonPreviewLimit::FormattedPrefix);
        assert_eq!(prefix.len(), PREVIEW_TEXT_BYTES);

        let (pretty, limit) = json_preview_content("{\"answer\":42}").unwrap();
        assert_eq!(limit, JsonPreviewLimit::Complete);
        assert!(pretty.contains("\n  \"answer\": 42\n"));
        assert!(json_preview_content("{broken}").is_err());
    }

    #[test]
    fn binary_hex_round_trip_is_lossless_and_strict() {
        let bytes: Vec<u8> = (0..=255).collect();
        let (format, text) = document_from_bytes("unknown.bin", &bytes);
        assert_eq!(format, DocumentFormat::Hex);
        assert!(text.starts_with("00 01 02"));
        assert_eq!(document_bytes(format, &text).unwrap(), bytes);
        for invalid in ["0", "0001", "GG", "00 0x01", "00 01", "0 1", "00:01"] {
            assert!(
                document_bytes(DocumentFormat::Hex, invalid).is_err(),
                "{invalid}"
            );
        }
        assert_eq!(
            document_bytes(DocumentFormat::Hex, "aB\r\n00\tFF").unwrap(),
            [0xab, 0, 255]
        );
    }

    #[test]
    fn recognized_text_formats_preserve_every_byte() {
        let text = "\u{feff}Hello, 世界\r\n\r\n";
        for (path, expected) in [
            ("a.md", DocumentFormat::Markdown),
            ("a.JSON", DocumentFormat::Json),
            ("a.csv", DocumentFormat::Csv),
            ("a.tsv", DocumentFormat::Tsv),
            ("a.rs", DocumentFormat::Text),
        ] {
            let (format, source) = document_from_bytes(path, text.as_bytes());
            assert_eq!(format, expected);
            assert_eq!(document_bytes(format, &source).unwrap(), text.as_bytes());
        }
        assert_eq!(
            document_from_bytes("invalid.txt", &[0xff, 0xfe]).0,
            DocumentFormat::Hex
        );
        assert_eq!(
            document_from_bytes("empty", &[]),
            (DocumentFormat::Text, String::new())
        );
    }

    #[test]
    fn raster_detection_uses_bytes_and_rejects_corruption() {
        for format in [
            image::ImageFormat::Png,
            image::ImageFormat::Jpeg,
            image::ImageFormat::WebP,
            image::ImageFormat::Gif,
            image::ImageFormat::Bmp,
            image::ImageFormat::Ico,
            image::ImageFormat::Tiff,
        ] {
            let bitmap = DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(
                16,
                8,
                image::Rgba([20, 60, 180, 255]),
            ));
            let mut output = Cursor::new(Vec::new());
            if format == image::ImageFormat::Jpeg {
                DynamicImage::ImageRgb8(bitmap.to_rgb8())
                    .write_to(&mut output, format)
                    .unwrap();
            } else {
                bitmap.write_to(&mut output, format).unwrap();
            }
            let bytes = output.into_inner();
            let (kind, source) = document_from_bytes("extensionless", &bytes);
            assert_eq!(kind, DocumentFormat::Image, "{format:?}");
            assert_eq!(document_bytes(kind, &source).unwrap(), bytes);
        }
        assert_eq!(
            document_from_bytes("broken.png", b"\x89PNG\r\n\x1a\ninvalid").0,
            DocumentFormat::Hex
        );
    }

    #[test]
    fn delimited_preview_handles_quotes_newlines_and_budgets() {
        let (rows, limited) =
            delimited_rows("a,\"b,c\",\"d\"\"e\"\r\n\"line1\nline2\",,\r\n", ',').unwrap();
        assert_eq!(
            rows,
            vec![vec!["a", "b,c", "d\"e"], vec!["line1\nline2", "", ""]]
        );
        assert!(!limited);
        assert!(delimited_rows("\"unclosed", ',').is_err());
        assert!(delimited_rows("\"closed\"junk", ',').is_err());
        let (rows, limited) = delimited_rows(&"a,b\n".repeat(200), ',').unwrap();
        assert_eq!(rows.len(), TABLE_ROWS);
        assert!(limited);
        let (rows, limited) = delimited_rows(&"a".repeat(500), '\t').unwrap();
        assert_eq!(rows[0][0].len(), TABLE_CELL_CHARS);
        assert!(limited);
    }

    #[test]
    fn encoded_and_text_documents_enforce_the_same_byte_limit() {
        let over = "x".repeat(FILE_DOCUMENT_MAX_BYTES as usize + 1);
        assert!(document_bytes(DocumentFormat::Text, &over).is_err());
        assert!(
            document_bytes(
                DocumentFormat::Hex,
                &"00 ".repeat(FILE_DOCUMENT_MAX_BYTES as usize + 1)
            )
            .is_err()
        );
    }
}
