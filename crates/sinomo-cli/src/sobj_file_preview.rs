use std::fmt::Write as _;

use crate::file_manager::{
    FILE_MANAGER_PREVIEW_MAX_BYTES, FILE_MANAGER_PREVIEW_MAX_LINES, FileManagerEntry,
    FileManagerPreview, FileManagerPreviewer, FileManagerRead, format_file_size,
};
use sinomo_runtime::object::{DocumentView, ElementType, ElementView, PayloadView, SchemaType};

const SOBJ_PREVIEW_MAX_CONTAINER_ITEMS: usize = 32;
const SOBJ_PREVIEW_MAX_STRING_CHARS: usize = 200;
const SOBJ_PREVIEW_MAX_INLINE_BYTES: usize = 32;
const SOBJ_PREVIEW_MAX_DEPTH: usize = 8;

#[derive(Clone, Debug, Default)]
pub(crate) struct SobjFilePreviewer;

impl FileManagerPreviewer for SobjFilePreviewer {
    fn preview_file(
        &self,
        entry: &FileManagerEntry,
        read: &FileManagerRead,
    ) -> Option<FileManagerPreview> {
        if file_manager_entry_is_sobj(entry)
            || read.bytes.starts_with(sinomo_runtime::object::MAGIC)
        {
            Some(sobj_file_preview(entry, read))
        } else {
            None
        }
    }
}

fn file_manager_entry_is_sobj(entry: &FileManagerEntry) -> bool {
    entry
        .name
        .rsplit_once('.')
        .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("sobj"))
}

fn sobj_file_preview(entry: &FileManagerEntry, read: &FileManagerRead) -> FileManagerPreview {
    let title = format!("SOBJ {}", entry.name);
    if read.truncated && read.bytes.starts_with(sinomo_runtime::object::MAGIC) {
        let mut lines = vec![
            "SOBJ document".to_string(),
            "preview requires the complete document".to_string(),
            format!(
                "read limit: {}",
                format_file_size(FILE_MANAGER_PREVIEW_MAX_BYTES)
            ),
        ];
        if let Some(size) = entry.size {
            lines.push(format!("file size: {}", format_file_size(size)));
        }
        return FileManagerPreview::new(title, lines);
    }

    match DocumentView::parse(&read.bytes) {
        Ok(document) => FileManagerPreview::new(title, format_sobj_document_preview(&document)),
        Err(error) => FileManagerPreview::new(
            title,
            vec![
                "invalid SOBJ document".to_string(),
                format!("error: {error}"),
            ],
        ),
    }
}

fn format_sobj_document_preview(document: &DocumentView<'_>) -> Vec<String> {
    let mut lines = SobjPreviewLines::default();
    lines.push("SOBJ document");
    lines.push(format!("version: {}", document.metadata.version_id));
    lines.push(format!("length mode: {:?}", document.metadata.length_mode));
    lines.push(format!("alignment: {:?}", document.metadata.alignment_hint));
    lines.push(format!(
        "document size: {}",
        format_file_size(document.metadata.document_length)
    ));
    write_sobj_schema_preview(&mut lines, &document.schema.types);
    lines.push("payload:");
    match &document.payload {
        PayloadView::Dynamic(element) => write_sobj_element_preview(&mut lines, element, 1, 0),
        PayloadView::Schema {
            root_schema_type_id,
            root_count,
            data,
        } => {
            lines.push(format!("  schema root: #{root_schema_type_id}"));
            lines.push(format!("  root count: {root_count}"));
            lines.push(format!("  data: {}", format_sobj_bytes_summary(data)));
        }
    }
    lines.finish()
}

fn write_sobj_schema_preview(lines: &mut SobjPreviewLines, schema_types: &[SchemaType]) {
    if schema_types.is_empty() {
        lines.push("schema: none");
        return;
    }

    lines.push(format!("schema types: {}", schema_types.len()));
    for schema_type in schema_types.iter().take(SOBJ_PREVIEW_MAX_CONTAINER_ITEMS) {
        if lines.is_full() {
            lines.mark_truncated();
            return;
        }
        lines.push(format!(
            "  #{} {} payload={} fields={}",
            schema_type.id,
            schema_type.name,
            format_file_size(schema_type.payload_size),
            schema_type.fields.len()
        ));
        for field in schema_type
            .fields
            .iter()
            .take(SOBJ_PREVIEW_MAX_CONTAINER_ITEMS)
        {
            lines.push(format!(
                "    {}: {}",
                field.name,
                format_sobj_element_type(&field.element_type)
            ));
        }
        if schema_type.fields.len() > SOBJ_PREVIEW_MAX_CONTAINER_ITEMS {
            lines.push(format!(
                "    ... {} more fields",
                schema_type.fields.len() - SOBJ_PREVIEW_MAX_CONTAINER_ITEMS
            ));
        }
    }
    if schema_types.len() > SOBJ_PREVIEW_MAX_CONTAINER_ITEMS {
        lines.push(format!(
            "  ... {} more schema types",
            schema_types.len() - SOBJ_PREVIEW_MAX_CONTAINER_ITEMS
        ));
    }
}

fn write_sobj_element_preview(
    lines: &mut SobjPreviewLines,
    element: &ElementView<'_>,
    indent: usize,
    depth: usize,
) {
    if depth >= SOBJ_PREVIEW_MAX_DEPTH {
        lines.push(format!(
            "{}... nested value omitted",
            sobj_preview_indent(indent)
        ));
        return;
    }
    if let Some(value) = format_sobj_inline_element(element) {
        lines.push(format!("{}{}", sobj_preview_indent(indent), value));
        return;
    }

    match element {
        ElementView::Object(object) => {
            if object.fields.is_empty() {
                lines.push(format!("{}{{}}", sobj_preview_indent(indent)));
                return;
            }
            lines.push(format!("{}{{", sobj_preview_indent(indent)));
            for field in object.fields.iter().take(SOBJ_PREVIEW_MAX_CONTAINER_ITEMS) {
                if lines.is_full() {
                    lines.mark_truncated();
                    return;
                }
                let key = format!("{:?}", field.key);
                if let Some(value) = format_sobj_inline_element(&field.value) {
                    lines.push(format!(
                        "{}{}: {}",
                        sobj_preview_indent(indent + 1),
                        key,
                        value
                    ));
                } else {
                    lines.push(format!("{}{}:", sobj_preview_indent(indent + 1), key));
                    write_sobj_element_preview(lines, &field.value, indent + 2, depth + 1);
                }
            }
            if object.fields.len() > SOBJ_PREVIEW_MAX_CONTAINER_ITEMS {
                lines.push(format!(
                    "{}... {} more fields",
                    sobj_preview_indent(indent + 1),
                    object.fields.len() - SOBJ_PREVIEW_MAX_CONTAINER_ITEMS
                ));
            }
            lines.push(format!("{}}}", sobj_preview_indent(indent)));
        }
        ElementView::List(list) => {
            lines.push(format!(
                "{}list<{}>[{}]",
                sobj_preview_indent(indent),
                format_sobj_element_type(&list.element_type),
                list.values.len()
            ));
            for (index, value) in list
                .values
                .iter()
                .take(SOBJ_PREVIEW_MAX_CONTAINER_ITEMS)
                .enumerate()
            {
                if lines.is_full() {
                    lines.mark_truncated();
                    return;
                }
                if let Some(value) = format_sobj_inline_element(value) {
                    lines.push(format!(
                        "{}[{index}]: {value}",
                        sobj_preview_indent(indent + 1)
                    ));
                } else {
                    lines.push(format!("{}[{index}]:", sobj_preview_indent(indent + 1)));
                    write_sobj_element_preview(lines, value, indent + 2, depth + 1);
                }
            }
            if list.values.len() > SOBJ_PREVIEW_MAX_CONTAINER_ITEMS {
                lines.push(format!(
                    "{}... {} more values",
                    sobj_preview_indent(indent + 1),
                    list.values.len() - SOBJ_PREVIEW_MAX_CONTAINER_ITEMS
                ));
            }
        }
        ElementView::Optional(optional) => {
            lines.push(format!(
                "{}optional<{}>:",
                sobj_preview_indent(indent),
                format_sobj_element_type(&optional.element_type)
            ));
            match optional.value.as_deref() {
                Some(value) => write_sobj_element_preview(lines, value, indent + 1, depth + 1),
                None => lines.push(format!("{}none", sobj_preview_indent(indent + 1))),
            }
        }
        _ => lines.push(format!("{}{:?}", sobj_preview_indent(indent), element)),
    }
}

fn format_sobj_inline_element(element: &ElementView<'_>) -> Option<String> {
    match element {
        ElementView::Boolean(value) => Some(value.to_string()),
        ElementView::Uint8(value) => Some(value.to_string()),
        ElementView::Int8(value) => Some(value.to_string()),
        ElementView::Uint16(value) => Some(value.to_string()),
        ElementView::Int16(value) => Some(value.to_string()),
        ElementView::Uint32(value) => Some(value.to_string()),
        ElementView::Int32(value) => Some(value.to_string()),
        ElementView::Uint64(value) => Some(value.to_string()),
        ElementView::Int64(value) => Some(value.to_string()),
        ElementView::Float(value) => Some(value.to_string()),
        ElementView::Double(value) => Some(value.to_string()),
        ElementView::String(value) => Some(format_sobj_string_preview(value)),
        ElementView::Bytes(value) => Some(format_sobj_bytes_summary(value)),
        ElementView::Array(array) => Some(format!(
            "array<{}> dims={} element_size={} data={}",
            format_sobj_element_type(&array.element_type),
            format_sobj_dimensions(&array.dimensions),
            format_file_size(array.element_size),
            format_sobj_bytes_summary(array.data)
        )),
        ElementView::Optional(optional) => match optional.value.as_deref() {
            None => Some(format!(
                "none<{}>",
                format_sobj_element_type(&optional.element_type)
            )),
            Some(value) => format_sobj_inline_element(value).map(|value| {
                format!(
                    "some<{}>({value})",
                    format_sobj_element_type(&optional.element_type)
                )
            }),
        },
        ElementView::Object(_) | ElementView::List(_) => None,
    }
}

fn format_sobj_element_type(element_type: &ElementType) -> String {
    match element_type {
        ElementType::Builtin(type_id) => format!("{type_id:?}"),
        ElementType::SchemaRef(id) => format!("schema#{id}"),
        ElementType::List(inner) => format!("list<{}>", format_sobj_element_type(inner)),
        ElementType::Array(inner) => format!("array<{}>", format_sobj_element_type(inner)),
        ElementType::Object => "object".to_string(),
        ElementType::Optional(inner) => format!("optional<{}>", format_sobj_element_type(inner)),
        ElementType::Extension(extension) => format!(
            "extension#{}({})",
            extension.id,
            format_file_size(extension.data.len() as u64)
        ),
    }
}

fn format_sobj_string_preview(value: &str) -> String {
    let char_count = value.chars().count();
    let truncated = char_count > SOBJ_PREVIEW_MAX_STRING_CHARS;
    let snippet = value
        .chars()
        .take(SOBJ_PREVIEW_MAX_STRING_CHARS)
        .collect::<String>();
    let suffix = if truncated { " ..." } else { "" };
    format!("string({char_count} chars): {snippet:?}{suffix}")
}

fn format_sobj_bytes_summary(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "bytes(0 B)".to_string();
    }

    let mut hex = String::new();
    for (index, byte) in bytes.iter().take(SOBJ_PREVIEW_MAX_INLINE_BYTES).enumerate() {
        if index > 0 {
            hex.push(' ');
        }
        let _ = write!(&mut hex, "{byte:02x}");
    }
    if bytes.len() > SOBJ_PREVIEW_MAX_INLINE_BYTES {
        hex.push_str(" ...");
    }
    format!("bytes({}): {hex}", format_file_size(bytes.len() as u64))
}

fn format_sobj_dimensions(dimensions: &[u64]) -> String {
    if dimensions.is_empty() {
        "[]".to_string()
    } else {
        dimensions
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join("x")
    }
}

fn sobj_preview_indent(indent: usize) -> String {
    "  ".repeat(indent)
}

#[derive(Default)]
struct SobjPreviewLines {
    lines: Vec<String>,
    truncated: bool,
}

impl SobjPreviewLines {
    fn push(&mut self, line: impl Into<String>) {
        if self.lines.len() < FILE_MANAGER_PREVIEW_MAX_LINES {
            self.lines.push(line.into());
        } else {
            self.truncated = true;
        }
    }

    fn is_full(&self) -> bool {
        self.lines.len() >= FILE_MANAGER_PREVIEW_MAX_LINES
    }

    fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    fn finish(mut self) -> Vec<String> {
        if self.truncated {
            if self.lines.len() >= FILE_MANAGER_PREVIEW_MAX_LINES {
                self.lines.pop();
            }
            self.lines.push(format!(
                "... truncated after {FILE_MANAGER_PREVIEW_MAX_LINES} lines"
            ));
        }
        self.lines
    }
}
