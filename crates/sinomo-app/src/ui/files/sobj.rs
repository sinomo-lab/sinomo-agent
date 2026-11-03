//! Structured SOBJ editing. Field drafts address indices in the exact hex buffer
//! they were opened against; navigation applies a valid draft before moving.
use super::*;
use sinomo_runtime::object::{
    ArrayView, DecodeLimits, DecodeOptions, DocumentCursor, ElementCursor, ElementType,
    PayloadCursor, Schema, SchemaType, TypeId, Value,
};
use sui::Signal;

const PAGE: usize = 32;
const FIELD_BYTES: usize = 64 * 1024;
#[derive(Clone, Default, PartialEq)]
struct TreeState {
    // Expanded containers keep independent pages, including while an ancestor is collapsed.
    branches: BTreeMap<Vec<usize>, usize>,
    selected: Option<Vec<usize>>,
    editing: bool,
}

enum RowAction {
    Field(Vec<usize>, bool),
    Page(Vec<usize>, usize),
}

pub(super) fn field_row(
    reader: &ThemeReader,
    indent: f32,
    name: &str,
    glyph: &str,
    value: Option<String>,
    detail: String,
) -> ListItem {
    let mut line = Flex::horizontal()
        .align_items(Alignment::Center)
        .gap(6.)
        .with_child(Label::new(glyph).theme_when(clone_reader(reader)))
        .with_child(
            Label::new(middle_short(name, 48))
                .single_line(true)
                .theme_when(clone_reader(reader)),
        );
    if let Some(value) = value {
        let value = middle_short(&value.replace('\n', " "), 60);
        line = line.with_item(
            Label::new(value)
                .single_line(true)
                .theme_when(clone_reader(reader)),
            FlexItem::fill(),
        );
    } else {
        line = line.spacer();
    }
    line = line.with_child(
        Label::new(detail.clone()).text_style_when(live_text_style(reader, |theme| {
            theme.text_style(theme.palette.text_muted)
        })),
    );
    ListItem::new(name)
        .description(detail)
        .activate_with_child(true)
        .child(
            WidgetPadding::new(
                Insets {
                    left: indent,
                    ..Insets::ZERO
                },
                line,
            )
            .fill_child_width(true),
        )
}

fn tree_rows(
    node: &Node<'_>,
    key: &str,
    path: &mut Vec<usize>,
    schema: &Schema,
    state: &TreeState,
    rows: &mut Vec<(ListItem, RowAction)>,
    editor: &mut Option<ListItem>,
    reader: &ThemeReader,
) -> Result<(), String> {
    let count = node.len(schema);
    let branch = count.is_some_and(|n| n > 0) && path.len() < 48;
    let page = state.branches.get(path);
    let value = node.scalar().and_then(|(id, data)| match id {
        TypeId::Bytes => Some(formats::encode_hex(&data[..data.len().min(16)])),
        TypeId::String => Some(std::str::from_utf8(data).ok()?.chars().take(60).collect()),
        _ => display(id, data),
    });
    let indent = reader().metrics.tree_indent * path.len() as f32;
    let selected = state.selected.as_deref() == Some(path.as_slice());
    let item = |name: &str, glyph: &str, value: Option<String>, detail: String| {
        field_row(reader, indent, name, glyph, value, detail)
    };
    let glyph = if branch {
        if page.is_some() { "▾" } else { "▸" }
    } else {
        "·"
    };
    rows.push((
        if selected && state.editing && editor.is_some() {
            editor.take().unwrap()
        } else {
            item(key, glyph, value, node.describe())
        },
        RowAction::Field(path.clone(), branch),
    ));
    if let (true, Some(&page), Some(count)) = (branch, page, count) {
        let start = page * PAGE;
        if page > 0 {
            rows.push((
                item("Previous items", "↑", None, String::new()),
                RowAction::Page(path.clone(), page - 1),
            ));
        }
        for (index, (key, child)) in node.page(start, schema)?.into_iter().enumerate() {
            path.push(start + index);
            tree_rows(&child, &key, path, schema, state, rows, editor, reader)?;
            path.pop();
        }
        if start + PAGE < count {
            rows.push((
                item(
                    "Next items",
                    "↓",
                    None,
                    format!("{}–{} of {count}", start + 1, start + PAGE),
                ),
                RowAction::Page(path.clone(), page + 1),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct FieldDraft {
    path: Vec<usize>,
    original: String,
    input: String,
    #[serde(default)]
    key: Option<(String, String)>,
}
impl FieldDraft {
    pub(super) fn modified(&self) -> bool {
        self.original != self.input || self.key.as_ref().is_some_and(|(a, b)| a != b)
    }
}

fn parse(bytes: &[u8]) -> Result<DocumentCursor<'_>, String> {
    DocumentCursor::parse_with_options(
        bytes,
        DecodeOptions::new(DecodeLimits {
            max_document_bytes: FILE_DOCUMENT_MAX_BYTES,
            max_nesting_depth: 48,
            max_container_items: 65536,
            max_schema_types: 256,
            max_schema_fields_per_type: 1024,
            ..DecodeLimits::standard()
        }),
    )
    .map_err(|e| e.to_string())
}

pub(super) fn can_inspect(bytes: &[u8]) -> bool {
    parse(bytes).is_ok()
}

enum Node<'a> {
    Dynamic(ElementCursor<'a>),
    Array(ArrayView<'a>),
    Records(&'a SchemaType, usize, &'a [u8]),
    Record(&'a SchemaType, &'a [u8]),
    Fixed(TypeId, &'a [u8]),
    Opaque(&'a [u8]),
}

fn fixed_width(id: TypeId) -> Option<usize> {
    Some(match id {
        TypeId::Boolean | TypeId::Int8 | TypeId::Uint8 => 1,
        TypeId::Int16 | TypeId::Uint16 => 2,
        TypeId::Int32 | TypeId::Uint32 | TypeId::Float => 4,
        TypeId::Int64 | TypeId::Uint64 | TypeId::Double => 8,
        _ => return None,
    })
}
fn schema_type(schema: &Schema, id: u16) -> Result<&SchemaType, String> {
    schema
        .types
        .iter()
        .find(|s| s.id == id)
        .ok_or_else(|| format!("Unknown schema #{id}"))
}
fn width(ty: &ElementType, schema: &Schema) -> Option<usize> {
    match ty {
        ElementType::Builtin(id) => fixed_width(*id),
        ElementType::SchemaRef(id) => usize::try_from(schema_type(schema, *id).ok()?.payload_size)
            .ok()
            .filter(|n| *n > 0),
        _ => None,
    }
}
// Fixed-layout schemas carry a record stride, but no explicit field offsets.
// Only expose typed offsets if the declared widths account for every byte.
fn packed(record: &SchemaType, schema: &Schema) -> bool {
    record.fields.iter().try_fold(0usize, |n, f| {
        n.checked_add(width(&f.element_type, schema)?)
    }) == usize::try_from(record.payload_size).ok()
}
fn fixed_node<'a>(ty: &ElementType, data: &'a [u8], schema: &'a Schema) -> Node<'a> {
    match ty {
        ElementType::Builtin(id) if fixed_width(*id) == Some(data.len()) => Node::Fixed(*id, data),
        ElementType::SchemaRef(id)
            if let Ok(s) = schema_type(schema, *id)
                && s.payload_size == data.len() as u64 =>
        {
            Node::Record(s, data)
        }
        _ => Node::Opaque(data),
    }
}
impl<'a> Node<'a> {
    fn dynamic(c: ElementCursor<'a>) -> Result<Self, String> {
        if c.type_id() == TypeId::Array {
            Ok(Self::Array(c.as_array().map_err(|e| e.to_string())?))
        } else {
            Ok(Self::Dynamic(c))
        }
    }
    fn root(doc: &'a DocumentCursor<'a>) -> Result<Self, String> {
        match &doc.payload {
            PayloadCursor::Dynamic(c) => Self::dynamic(c.clone()),
            PayloadCursor::Schema {
                root_schema_type_id,
                root_count,
                data,
            } => {
                let ty = schema_type(&doc.schema, *root_schema_type_id)?;
                if ty.payload_size == 0 {
                    Ok(Self::Opaque(data))
                } else {
                    Ok(Self::Records(ty, *root_count as usize, data))
                }
            }
        }
    }
    fn len(&self, schema: &Schema) -> Option<usize> {
        match self {
            Self::Dynamic(c) => match c.type_id() {
                TypeId::Object => Some(c.as_object().ok()?.field_count()),
                TypeId::List => Some(c.as_list().ok()?.len()),
                TypeId::Optional => Some(usize::from(c.optional_child().ok()?.is_some())),
                _ => None,
            },
            Self::Array(a) => Some(a.data.len() / usize::try_from(a.element_size).ok()?.max(1)),
            Self::Records(_, n, _) => Some(*n),
            Self::Record(ty, _) if packed(ty, schema) => Some(ty.fields.len()),
            _ => None,
        }
    }
    fn page(&self, start: usize, schema: &'a Schema) -> Result<Vec<(String, Self)>, String> {
        // A cursor walks variable-width fields sequentially. Scan to the page once.
        if let Self::Dynamic(c) = self {
            match c.type_id() {
                TypeId::Object => {
                    return c
                        .as_object()
                        .map_err(|e| e.to_string())?
                        .iter()
                        .skip(start)
                        .take(PAGE)
                        .map(|field| {
                            let field = field.map_err(|e| e.to_string())?;
                            Ok((field.key.into(), Self::dynamic(field.value)?))
                        })
                        .collect();
                }
                TypeId::List => {
                    return c
                        .as_list()
                        .map_err(|e| e.to_string())?
                        .iter()
                        .skip(start)
                        .take(PAGE)
                        .enumerate()
                        .map(|(i, child)| {
                            Ok((
                                format!("[{}]", start + i),
                                Self::dynamic(child.map_err(|e| e.to_string())?)?,
                            ))
                        })
                        .collect();
                }
                _ => {}
            }
        }
        (start..self.len(schema).unwrap_or(0).min(start + PAGE))
            .map(|i| self.child(i, schema))
            .collect()
    }
    fn child(&self, i: usize, schema: &'a Schema) -> Result<(String, Self), String> {
        if i >= self.len(schema).unwrap_or(0) {
            return Err("Field is no longer available".into());
        }
        Ok(match self {
            Self::Dynamic(c) => match c.type_id() {
                TypeId::Object => {
                    let f = c
                        .as_object()
                        .map_err(|e| e.to_string())?
                        .iter()
                        .nth(i)
                        .ok_or("Missing field")?
                        .map_err(|e| e.to_string())?;
                    (f.key.into(), Self::dynamic(f.value)?)
                }
                TypeId::List => {
                    let item = c
                        .as_list()
                        .map_err(|e| e.to_string())?
                        .iter()
                        .nth(i)
                        .ok_or("Missing item")?
                        .map_err(|e| e.to_string())?;
                    (format!("[{i}]"), Self::dynamic(item)?)
                }
                TypeId::Optional => (
                    "Value".into(),
                    Self::dynamic(
                        c.optional_child()
                            .map_err(|e| e.to_string())?
                            .ok_or("Absent optional")?,
                    )?,
                ),
                _ => return Err("Not a container".into()),
            },
            Self::Array(a) => {
                let w = a.element_size as usize;
                (
                    format!("[{i}]"),
                    fixed_node(&a.element_type, &a.data[i * w..(i + 1) * w], schema),
                )
            }
            Self::Records(ty, _, data) => {
                let w = ty.payload_size as usize;
                (
                    format!("[{i}]"),
                    Self::Record(ty, &data[i * w..(i + 1) * w]),
                )
            }
            Self::Record(ty, data) => {
                let f = &ty.fields[i];
                let start = ty.fields[..i]
                    .iter()
                    .map(|f| width(&f.element_type, schema).unwrap())
                    .sum::<usize>();
                (
                    f.name.clone(),
                    fixed_node(
                        &f.element_type,
                        &data[start..start + width(&f.element_type, schema).unwrap()],
                        schema,
                    ),
                )
            }
            _ => return Err("Not a container".into()),
        })
    }
    fn describe(&self) -> String {
        match self {
            Self::Dynamic(c) => format!("{:?} · {} B", c.type_id(), c.encoded_value().len()),
            Self::Array(a) => format!("Array {:?} · {} B", a.dimensions, a.data.len()),
            Self::Records(ty, n, data) => format!("{} × {n} · {} B", ty.name, data.len()),
            Self::Record(ty, data) => format!("{} · {} B", ty.name, data.len()),
            Self::Fixed(id, data) => format!("{id:?} · {} B", data.len()),
            Self::Opaque(data) => format!("Raw payload · {} B", data.len()),
        }
    }
    fn scalar(&self) -> Option<(TypeId, &[u8])> {
        match self {
            Self::Dynamic(c) => match c.type_id() {
                TypeId::String => Some((TypeId::String, c.as_str().ok()?.as_bytes())),
                TypeId::Bytes => Some((TypeId::Bytes, c.as_bytes().ok()?)),
                id if fixed_width(id).is_some() => Some((id, c.encoded_value())),
                _ => None,
            },
            Self::Fixed(id, data) => Some((*id, data)),
            _ => None,
        }
    }
}
fn locate<'a>(doc: &'a DocumentCursor<'a>, path: &[usize]) -> Result<(String, Node<'a>), String> {
    let mut node = Node::root(doc)?;
    let mut names = vec!["Payload".to_owned()];
    for &index in path {
        let (name, child) = node.child(index, &doc.schema)?;
        names.push(name);
        node = child;
    }
    Ok((names.join(" / "), node))
}
pub(super) fn display(id: TypeId, data: &[u8]) -> Option<String> {
    macro_rules! number {
        ($t:ty) => {
            <$t>::from_le_bytes(data.try_into().ok()?).to_string()
        };
    }
    Some(match id {
        TypeId::String => std::str::from_utf8(data).ok()?.into(),
        TypeId::Bytes => formats::encode_hex(data),
        TypeId::Boolean => match data {
            [0] => "false".into(),
            [1] => "true".into(),
            _ => return None,
        },
        TypeId::Uint8 => number!(u8),
        TypeId::Int8 => number!(i8),
        TypeId::Uint16 => number!(u16),
        TypeId::Int16 => number!(i16),
        TypeId::Uint32 => number!(u32),
        TypeId::Int32 => number!(i32),
        TypeId::Uint64 => number!(u64),
        TypeId::Int64 => number!(i64),
        TypeId::Float => number!(f32),
        TypeId::Double => number!(f64),
        _ => return None,
    })
}
fn replacement(id: TypeId, input: &str) -> Result<Value, String> {
    macro_rules! number {
        ($t:ident) => {
            Value::$t(
                input
                    .trim()
                    .parse()
                    .map_err(|_| format!("Enter a valid {id:?} value"))?,
            )
        };
    }
    Ok(match id {
        TypeId::String => Value::String(input.into()),
        TypeId::Bytes => Value::Bytes(formats::document_bytes(DocumentFormat::Hex, input)?),
        TypeId::Boolean => number!(Boolean),
        TypeId::Uint8 => number!(Uint8),
        TypeId::Int8 => number!(Int8),
        TypeId::Uint16 => number!(Uint16),
        TypeId::Int16 => number!(Int16),
        TypeId::Uint32 => number!(Uint32),
        TypeId::Int32 => number!(Int32),
        TypeId::Uint64 => number!(Uint64),
        TypeId::Int64 => number!(Int64),
        TypeId::Float => number!(Float),
        TypeId::Double => number!(Double),
        _ => return Err("Use Hex editor for this field".into()),
    })
}

pub(super) fn draft_bytes(doc: &FilesDocument) -> Result<Vec<u8>, String> {
    let bytes = formats::document_bytes(doc.format, &doc.text)?;
    let Some(edit) = doc.sobj_field.as_ref().filter(|e| e.modified()) else {
        return Ok(bytes);
    };
    let bytes = if let Some((old, new)) = &edit.key
        && old != new
    {
        let parsed = parse(&bytes)?;
        let (index, parent) = edit
            .path
            .split_last()
            .ok_or("The payload root has no key")?;
        let (_, Node::Dynamic(object)) = locate(&parsed, parent)? else {
            return Err("Schema fields cannot be renamed".into());
        };
        object
            .rename_field(*index, old, new)
            .map_err(|e| e.to_string())?
    } else {
        bytes
    };
    if edit.input == edit.original {
        return Ok(bytes);
    }
    let parsed = parse(&bytes)?;
    let (_, node) = locate(&parsed, &edit.path)?;
    let (id, original) = node
        .scalar()
        .ok_or("The selected field is no longer editable")?;
    if edit.input.len() > 3 * FIELD_BYTES
        || (id == TypeId::String && edit.input.len() > FIELD_BYTES)
    {
        return Err("Field input is too large. Use Hex editor.".into());
    }
    if display(id, original).as_deref() != Some(&edit.original) {
        return Err("Field changed since editing began".into());
    }
    let value = replacement(id, &edit.input)?;
    match node {
        Node::Dynamic(c) => c.replace_leaf(&value).map_err(|e| e.to_string()),
        Node::Fixed(_, slice) => {
            let offset = slice.as_ptr() as usize - bytes.as_ptr() as usize;
            let raw = value
                .to_value_bytes(parsed.metadata.length_mode)
                .map_err(|e| e.to_string())?;
            let len = slice.len();
            let mut result = bytes;
            result[offset..offset + len].copy_from_slice(&raw);
            Ok(result)
        }
        _ => Err("Select a scalar to edit".into()),
    }
}

pub(super) fn apply_field(doc: &mut FilesDocument) -> Result<(), String> {
    if doc.sobj_field.as_ref().is_some_and(FieldDraft::modified) {
        let bytes = draft_bytes(doc)?;
        let edit = doc.sobj_field.as_mut().unwrap();
        if edit.original != edit.input {
            let parsed = parse(&bytes)?;
            let (_, updated) = locate(&parsed, &edit.path)?;
            let (id, data) = updated
                .scalar()
                .ok_or("Edited field is no longer a scalar")?;
            edit.original = display(id, data).ok_or("Invalid edited field")?;
            edit.input = edit.original.clone();
        }
        if let Some((old, new)) = &mut edit.key {
            old.clone_from(new);
        }
        doc.text = formats::encode_hex(&bytes);
    }
    Ok(())
}

pub(super) fn summary(hex: &str) -> Result<String, String> {
    let bytes = formats::document_bytes(DocumentFormat::Sobj, hex)?;
    let doc = parse(&bytes)?;
    let root = Node::root(&doc)?;
    let mut lines = vec![format!(
        "SOBJ v{} · {}",
        doc.metadata.version_id,
        root.describe()
    )];
    if let Some(n) = root.len(&doc.schema) {
        for i in 0..n.min(8) {
            let (name, child) = root.child(i, &doc.schema)?;
            lines.push(format!(
                "{} · {}",
                middle_short(&name, 64),
                child.describe()
            ));
        }
        if n > 8 {
            lines.push(format!("{} more fields or items", n - 8));
        }
    }
    Ok(lines.join("\n"))
}

fn inline_row(
    doc: &FilesDocument,
    shared: &Shared,
    r: &ThemeReader,
    path: &[usize],
    parsed: &DocumentCursor<'_>,
    state: &Signal<TreeState>,
) -> Result<Option<ListItem>, String> {
    if !doc.editable() {
        return Ok(None);
    }
    let (_, node) = locate(parsed, path)?;
    let key = if let Some((&i, parent)) = path.split_last() {
        let (_, p) = locate(parsed, parent)?;
        if matches!(&p,Node::Dynamic(c) if c.type_id()==TypeId::Object) {
            Some(p.child(i, &parsed.schema)?.0)
        } else {
            None
        }
    } else {
        None
    };
    let scalar = node
        .scalar()
        .filter(|(_, data)| data.len() <= FIELD_BYTES)
        .and_then(|(id, data)| display(id, data));
    if key.is_none() && scalar.is_none() {
        return Ok(None);
    }
    let value = scalar.clone().unwrap_or_default();
    let baseline = FieldDraft {
        path: path.to_vec(),
        original: value.clone(),
        input: value,
        key: key.clone().map(|k| (k.clone(), k)),
    };
    let draft = doc
        .sobj_field
        .as_ref()
        .filter(|d| d.path == path)
        .unwrap_or(&baseline);
    let input = |is_key: bool, initial: String| {
        let shared = shared.clone();
        let baseline = baseline.clone();
        let mut surface = TextSurface::new(if is_key {
            "SOBJ field key"
        } else {
            "SOBJ field value"
        })
        .value(initial)
        .wrap(TextWrap::NoWrap)
        .theme_when(clone_reader(r))
        .text_style_when(|t| source_editor::mono(&t))
        .on_change(move |input| {
            let mut files = shared.files_mut();
            if let Some(doc) = files.document.as_mut()
                && doc.editable()
            {
                let draft = doc.sobj_field.get_or_insert_with(|| baseline.clone());
                if is_key {
                    let key = draft
                        .key
                        .get_or_insert_with(|| baseline.key.clone().unwrap());
                    key.1 = input;
                } else {
                    draft.input = input;
                }
                doc.save_error = None;
            }
            files.attachment_error = None;
        });
        surface.set_selection(0, 0);
        let scope = sui::FocusScopeState::new();
        if !is_key || scalar.is_none() {
            scope.request_focus();
        }
        sui::FocusScope::new(editor_surface::EditorSurface::new(
            surface,
            r,
            Signal::new(Default::default()),
            Insets::all(5.),
        ))
        .state(scope)
        .restore_policy(sui::FocusRestorePolicy::FirstFocusable)
    };
    let mut cells = Flex::horizontal().align_items(Alignment::Stretch).gap(4.);
    if let Some(key) = key {
        cells = cells.with_item(
            input(true, draft.key.as_ref().map(|p| p.1.clone()).unwrap_or(key)),
            FlexItem::fill(),
        );
    } else {
        let (name, _) = locate(parsed, path)?;
        cells = cells.with_child(
            Label::new(name.rsplit(" / ").next().unwrap_or("Payload")).theme_when(clone_reader(r)),
        );
    }
    if scalar.is_some() {
        cells = cells.with_item(input(false, draft.input.clone()), FlexItem::fill());
    }
    let finish = |apply: bool| {
        let shared = shared.clone();
        let state = state.clone();
        IconButton::new(
            if apply {
                IconGlyph::Check
            } else {
                IconGlyph::Close
            },
            if apply {
                "Apply field edit"
            } else {
                "Cancel field edit"
            },
        )
        .theme_when(clone_reader(r))
        .on_press_with_ctx(move |ctx| {
            let mut files = shared.files_mut();
            if let Some(doc) = files.document.as_mut() {
                if apply && let Err(e) = apply_field(doc) {
                    doc.save_error = Some(e);
                } else {
                    doc.sobj_field = None;
                    doc.save_error = None;
                    state.update(|s| s.editing = false);
                }
            }
            request_files_overlay_rebuild(ctx);
        })
    };
    cells = cells.with_child(finish(true)).with_child(finish(false));
    let validation =
        shared
            .signals()
            .files_structure
            .select_named("SOBJ inline error", |s| match &s.dialog {
                FilesDialogStructure::Document { save_error, .. } => save_error.clone(),
                _ => None,
            });
    let theme = r.clone();
    let notice = RebuildOnChange::key_from(validation, move |e| match e {
        Some(e) => WidgetPod::new(Label::new(e).theme_when(clone_reader(&theme))),
        None => WidgetPod::new(SizedBox::new().height(0.)),
    });
    let height = if draft.input.contains('\n') { 72. } else { 36. };
    let content = Stack::vertical()
        .alignment(Alignment::Stretch)
        .with_child(
            SizedBox::new()
                .height(height)
                .child(Surface::panel(cells).theme_when(clone_reader(r)).fill()),
        )
        .with_child(notice);
    Ok(Some(
        ListItem::new("Editing field").child(
            WidgetPadding::new(
                Insets {
                    left: r().metrics.tree_indent * path.len().min(6) as f32,
                    ..Insets::ZERO
                },
                Surface::panel(content)
                    .theme_when(clone_reader(r))
                    .fill_width(true),
            )
            .fill_child_width(true),
        ),
    ))
}

pub(super) fn view(doc: &FilesDocument, shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let mut initial = TreeState::default();
    initial.branches.insert(Vec::new(), 0);
    if let Some(edit) = &doc.sobj_field {
        initial.selected = Some(edit.path.clone());
        initial.editing = true;
        for (depth, index) in edit.path.iter().enumerate() {
            initial
                .branches
                .insert(edit.path[..depth].to_vec(), index / PAGE);
        }
    }
    let selection = Signal::new(initial);
    let target = selection.clone();
    let scroll = ScrollState::new();
    let last_click = Rc::new(RefCell::new(None::<(Vec<usize>, Instant)>));
    let shared = shared.clone();
    let r = reader.clone();
    WidgetPod::new(RebuildOnChange::key_from(selection, move |state| {
        let files = shared.files();
        let Some(doc) = files.document.as_ref() else {
            return WidgetPod::new(SizedBox::new());
        };
        let render = || -> Result<WidgetPod, String> {
            let bytes = formats::document_bytes(DocumentFormat::Sobj, &doc.text)?;
            let parsed = parse(&bytes)?;
            let root = Node::root(&parsed)?;
            let mut rows = Vec::new();
            let mut editor = match state.selected.as_deref() {
                Some(path) if state.editing => {
                    inline_row(doc, &shared, &r, path, &parsed, &target)?
                }
                _ => None,
            };
            tree_rows(
                &root,
                "Payload",
                &mut vec![],
                &parsed.schema,
                state,
                &mut rows,
                &mut editor,
                &r,
            )?;
            let selected = rows.iter().position(|(_, action)| {
                matches!(action, RowAction::Field(path, _) if Some(path) == state.selected.as_ref())
            });
            let (items, actions): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
            let s = shared.clone();
            let next = target.clone();
            let clicks = last_click.clone();
            let list = ListView::new("SOBJ fields")
                .items(items)
                .selected(selected)
                .theme_when(clone_reader(&r))
                .on_change_with_ctx(move |ctx, i, _| {
                    let mut files = s.files_mut();
                    let Some(doc) = files.document.as_mut() else {
                        return;
                    };
                    if let Err(error) = apply_field(doc) {
                        doc.save_error = Some(error);
                        request_files_overlay_rebuild(ctx);
                        return;
                    }
                    doc.sobj_field = None;
                    doc.save_error = None;
                    next.update(|state| {
                        state.editing = false;
                        match &actions[i] {
                            RowAction::Field(path, branch) => {
                                let now = Instant::now();
                                let double = clicks.borrow_mut().take().is_some_and(|(p, t)| {
                                    p == *path
                                        && now.duration_since(t)
                                            <= FILE_ICON_GRID_DOUBLE_CLICK_WINDOW
                                });
                                if !double {
                                    *clicks.borrow_mut() = Some((path.clone(), now));
                                    if *branch && state.branches.remove(path).is_none() {
                                        state.branches.insert(path.clone(), 0);
                                    }
                                }
                                state.selected = Some(path.clone());
                                state.editing = double && doc.editable();
                            }
                            RowAction::Page(path, page) => {
                                clicks.borrow_mut().take();
                                state.branches.insert(path.clone(), *page);
                                state.selected = None;
                            }
                        }
                    });
                    request_files_overlay_rebuild(ctx);
                });
            let tree = SemanticRegion::new(
                "SOBJ field tree",
                ScrollView::vertical(list)
                    .state(scroll.clone())
                    .viewport_width_hint(true)
                    .theme_when(clone_gray_scrollbar_reader(&r)),
            )
            .role(SemanticsRole::Tree);
            Ok(WidgetPod::new(tree))
        };
        render().unwrap_or_else(|error| {
            WidgetPod::new(
                Label::new(format!(
                    "Fields unavailable. {error}. Open Hex editor to inspect or repair the bytes."
                ))
                .theme_when(clone_reader(&r)),
            )
        })
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::object::{Document, Field, LengthMode, List, Object, OptionalValue};
    const FIXTURE: &[u8] = include_bytes!(
        "../../../../sinomo-dfs-migration/tests/fixtures/original-carbon-mounted-latch-v1.sobj"
    );

    fn document(bytes: &[u8]) -> FilesDocument {
        let mut doc = FilesDocument::loading(
            FilesSourceKind::Local,
            "sample.sobj".into(),
            "sample.sobj".into(),
        );
        (doc.format, doc.text) = formats::document_from_bytes(&doc.path, bytes);
        doc.saved_text = doc.text.clone();
        doc.loading = false;
        doc.preview_open = true;
        doc
    }

    #[test]
    fn malformed_sobj_starts_in_hex_and_preserves_bytes() {
        let bytes = b"SOBJ damaged";
        let (format, text, _, fields) =
            decode_loaded_document("broken.SOBJ", Some(bytes.to_vec()), FILE_DOCUMENT_MAX_BYTES)
                .unwrap();
        assert_eq!(format, DocumentFormat::Sobj);
        assert!(!fields);
        assert_eq!(formats::document_bytes(format, &text).unwrap(), bytes);
    }

    #[test]
    fn sobj_nested_optional_edits_preserve_padding_and_original_types() {
        let ty = ElementType::Builtin(TypeId::String);
        let value = Value::Object(Object::new(vec![
            Field::new(
                "names",
                Value::list(List::new(
                    ElementType::Optional(Box::new(ty.clone())),
                    vec![Value::optional(OptionalValue::some(
                        ty,
                        Value::String("old".into()),
                    ))],
                )),
            ),
            Field::new("nan", Value::Double(f64::from_bits(0x7ff8000000000123))),
        ]));
        let mut bytes = Document::dynamic(99, LengthMode::U16, value)
            .to_bytes()
            .unwrap();
        bytes.push(0);
        let length = bytes.len() as u16;
        bytes[8..10].copy_from_slice(&length.to_le_bytes());
        let mut doc = document(&bytes);
        doc.sobj_field = Some(FieldDraft {
            key: None,
            path: vec![0, 0, 0],
            original: "old".into(),
            input: "longer λ".into(),
        });
        apply_field(&mut doc).unwrap();
        let edit = doc.sobj_field.as_mut().unwrap();
        edit.input = "old".into();
        assert_eq!(draft_bytes(&doc).unwrap(), bytes);
    }

    #[test]
    fn dynamic_keys_rename_by_parent_and_preserve_container_content() {
        let mut doc = document(FIXTURE);
        let rename = |path: Vec<usize>, old: &str, new: &str| FieldDraft {
            path,
            key: Some((old.into(), new.into())),
            original: String::new(),
            input: String::new(),
        };
        doc.sobj_field = Some(rename(vec![2], "committed_work", "generation"));
        assert!(draft_bytes(&doc).unwrap_err().contains("duplicate"));
        assert_eq!(
            formats::document_bytes(doc.format, &doc.text).unwrap(),
            FIXTURE
        );
        doc.sobj_field = Some(rename(vec![2], "committed_work", "pending λ"));
        apply_field(&mut doc).unwrap();
        let bytes = draft_bytes(&doc).unwrap();
        let parsed = parse(&bytes).unwrap();
        assert!(locate(&parsed, &[2, 0]).unwrap().0.contains("pending λ"));
        doc.sobj_field = Some(rename(vec![2], "pending λ", "committed_work"));
        assert_eq!(draft_bytes(&doc).unwrap(), FIXTURE);
    }

    #[test]
    fn sobj_packed_schema_and_array_edit_exact_elements() {
        use sinomo_runtime::object::{Array, Metadata, Payload, SchemaField};
        let ty = SchemaType {
            id: 4,
            name: "Pair".into(),
            payload_size: 6,
            fields: vec![
                SchemaField {
                    name: "x".into(),
                    element_type: ElementType::Builtin(TypeId::Uint16),
                },
                SchemaField {
                    name: "y".into(),
                    element_type: ElementType::Builtin(TypeId::Uint32),
                },
            ],
        };
        let record = Document {
            metadata: Metadata::new(1, LengthMode::U32),
            schema: Schema { types: vec![ty] },
            payload: Payload::Schema {
                root_schema_type_id: 4,
                root_count: 2,
                data: vec![0; 12],
            },
        };
        let array = Document::dynamic(
            1,
            LengthMode::U32,
            Value::array(
                Array::new(ElementType::Builtin(TypeId::Uint32), 4, vec![2], vec![0; 8]).unwrap(),
            ),
        );
        for (document_bytes, path) in [
            (record.to_bytes().unwrap(), vec![1, 1]),
            (array.to_bytes().unwrap(), vec![1]),
        ] {
            let mut doc = document(&document_bytes);
            doc.sobj_field = Some(FieldDraft {
                key: None,
                path,
                original: "0".into(),
                input: "4294967295".into(),
            });
            let bytes = draft_bytes(&doc).unwrap();
            assert_eq!(
                &bytes[..bytes.len() - 4],
                &document_bytes[..document_bytes.len() - 4]
            );
            assert_eq!(&bytes[bytes.len() - 4..], &[255; 4]);
            doc.sobj_field.as_mut().unwrap().input = "-1".into();
            assert!(draft_bytes(&doc).is_err());
            let edit = doc.sobj_field.as_mut().unwrap();
            edit.input = edit.original.clone();
            edit.key = Some(("y".into(), "renamed".into()));
            assert!(
                draft_bytes(&doc).is_err(),
                "schema fields and array indices are immutable"
            );
        }
    }

    #[test]
    fn sobj_sifs_save_uses_original_version_and_attach_uses_field_draft() {
        let backend = Backend::local_fallback();
        let browser = SifsBrowser::open(&backend.data_dir).unwrap();
        let path = "/test-editor.sobj";
        browser.write_file(path, FIXTURE).unwrap();
        let load = load_sifs_text_document(&browser, path, FILE_DOCUMENT_MAX_BYTES).unwrap();
        let mut doc = document(FIXTURE);
        doc.source = FilesSourceKind::Sifs;
        doc.path = path.into();
        doc.sifs_fingerprint = load.sifs_fingerprint;
        doc.sobj_field = Some(FieldDraft {
            key: None,
            path: vec![2, 0],
            original: "true".into(),
            input: "false".into(),
        });
        let expected = draft_bytes(&doc).unwrap();
        assert_eq!(
            expected.iter().zip(FIXTURE).filter(|(a, b)| a != b).count(),
            1
        );
        let mut files = FilesState::new(&backend);
        files.document = Some(doc);
        files.start_attach_document(None);
        let attach = files.take_ready_attachment_intent().unwrap();
        assert!(attach.is_unsaved_editor_buffer);
        assert_eq!(attach.bytes, expected);
        let (tx, rx) = mpsc::channel();
        files.worker_tx = Some(tx);
        files.save_document(None);
        let FilesWorkerCommand::SaveSifsDocument {
            bytes,
            path,
            expected: version,
            ..
        } = rx.try_recv().unwrap()
        else {
            panic!("missing SIFS save")
        };
        save_sifs_text_document(&browser, &path, &version, &bytes).unwrap();
        assert_eq!(browser.read_file(&path).unwrap(), expected);
        assert!(save_sifs_text_document(&browser, &path, &version, FIXTURE).is_err());
    }

    #[test]
    fn sobj_fields_hex_and_local_save_in_a_document_tab() -> sui::Result<()> {
        let tmp = sinomo_testdir::TestDir::new("sobj-editor");
        let path = tmp.join("latch.sobj");
        fs::write(&path, FIXTURE).unwrap();
        let path = path.to_str().unwrap();
        let load = load_local_text_document(path, FILE_DOCUMENT_MAX_BYTES).unwrap();
        let shared = Shared::new(crate::state::AppState::ready(Backend::local_fallback()));
        shared
            .open_document_tab(FilesSourceKind::Local, path.into(), "latch.sobj".into())
            .unwrap();
        let shared = shared.active_scope();
        let (tx, rx) = mpsc::channel();
        {
            let mut files = shared.files_mut();
            files.worker_tx = Some(tx);
            files.pending_document_request_id = Some(1);
            assert!(files.apply_worker_result(FilesWorkerResult::Document {
                request_id: 1,
                source: FilesSourceKind::Local,
                path: path.into(),
                result: Ok(load)
            }));
        }
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("SOBJ editor")
                    .root(document_view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let win = app.main_window()?;
        win.root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(560., 720.))))?;
        win.run_until_idle()?;
        let open = |name: &str| {
            win.get_by_role(SemanticsRole::ListItem)
                .with_name(name)
                .click()
        };
        let field = |name: &str| win.get_by_role(SemanticsRole::ListItem).with_name(name);
        assert_eq!(field("publish_metadata").count()?, 0);
        open("committed_work")?;
        assert_eq!(field("generation").count()?, 1);
        open("publish_metadata")?;
        assert_eq!(
            win.get_by_role(SemanticsRole::TextInput)
                .with_name("SOBJ field value")
                .count()?,
            0
        );
        open("publish_metadata")?;
        win.capture_screenshot()?
            .write_png(std::env::temp_dir().join("sinomo-sobj-inline.png"))?;
        let input = win
            .get_by_role(SemanticsRole::TextInput)
            .with_name("SOBJ field value");
        let overwrite = |value: &str| -> sui::Result<()> {
            input.focus()?;
            let mut key = sui::KeyboardEvent::new("a", KeyState::Pressed);
            key.modifiers.control = true;
            input.dispatch_event(Event::Keyboard(key))?;
            input.dispatch_event(Event::Ime(sui::ImeEvent::CompositionCommit {
                text: value.into(),
            }))?;
            win.run_until_idle()
        };
        overwrite("invalid")?;
        win.get_by_role(SemanticsRole::Button)
            .with_name("Save")
            .click()?;
        assert!(rx.try_recv().is_err());
        assert!(shared.files().document.as_ref().unwrap().dirty());
        win.get_by_role(SemanticsRole::RadioButton)
            .with_name("Hex editor")
            .click()?;
        assert!(shared.files().document.as_ref().unwrap().preview_open);
        assert_eq!(
            shared
                .files()
                .document
                .as_ref()
                .unwrap()
                .sobj_field
                .as_ref()
                .unwrap()
                .input,
            "invalid"
        );
        overwrite("false")?;
        let key_input = win
            .get_by_role(SemanticsRole::TextInput)
            .with_name("SOBJ field key");
        key_input.focus()?;
        let mut select_all = sui::KeyboardEvent::new("a", KeyState::Pressed);
        select_all.modifiers.control = true;
        key_input.dispatch_event(Event::Keyboard(select_all))?;
        key_input.dispatch_event(Event::Ime(sui::ImeEvent::CompositionCommit {
            text: "publish_override".into(),
        }))?;

        win.get_by_role(SemanticsRole::RadioButton)
            .with_name("Hex editor")
            .click()?;
        let doc = shared.files().document.as_ref().unwrap().clone();
        assert!(!doc.preview_open && doc.dirty());
        let hex = draft_bytes(&doc).unwrap();
        let parsed = parse(&hex).unwrap();
        let (name, field) = locate(&parsed, &[2, 0]).unwrap();
        assert!(name.ends_with("publish_override"));
        assert_eq!(field.scalar().unwrap().1, &[0]);
        // Make a second independent edit in raw bytes, then return to Fields.
        let mut raw_doc = document(&hex);
        raw_doc.sobj_field = Some(FieldDraft {
            key: None,
            path: vec![2, 1],
            original: "true".into(),
            input: "false".into(),
        });
        let hex = draft_bytes(&raw_doc).unwrap();
        let raw = win
            .get_by_role(SemanticsRole::TextInput)
            .with_name(&format!("files-document:{path}"));
        raw.focus()?;
        let mut all = sui::KeyboardEvent::new("a", KeyState::Pressed);
        all.modifiers.control = true;
        raw.dispatch_event(Event::Keyboard(all))?;
        raw.dispatch_event(Event::Ime(sui::ImeEvent::CompositionCommit {
            text: formats::encode_hex(&hex),
        }))?;
        win.get_by_role(SemanticsRole::RadioButton)
            .with_name("Fields")
            .click()?;
        win.get_by_role(SemanticsRole::Button)
            .with_name("Save")
            .click()?;
        let FilesWorkerCommand::SaveLocalDocument {
            bytes,
            expected,
            path,
            ..
        } = rx.try_recv().unwrap()
        else {
            panic!("missing local save")
        };
        save_local_text_document(&path, &expected, &bytes).unwrap();
        assert_eq!(fs::read(path).unwrap(), hex);
        Ok(())
    }
}
