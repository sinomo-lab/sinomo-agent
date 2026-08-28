//! Sinomo Object binary serialization.
//!
//! This crate implements the `SOBJ` document format used for Sinomo networking
//! and non-user-facing storage. The API deliberately has both owned values for
//! convenient construction and borrowed views for direct operation on existing
//! byte regions.

use std::collections::HashSet;

use thiserror::Error;

#[cfg(feature = "serde")]
pub mod serde_ext;
#[cfg(feature = "serde")]
mod stream_serde;
pub mod text;

pub const MAGIC: &[u8; 4] = b"SOBJ";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("invalid magic")]
    InvalidMagic,
    #[error("invalid length mode {0}")]
    InvalidLengthMode(u8),
    #[error("invalid alignment hint {0}")]
    InvalidAlignmentHint(u8),
    #[error("document length mismatch: expected {expected}, actual {actual}")]
    DocumentLengthMismatch { expected: u64, actual: u64 },
    #[error("unexpected version id: expected {expected}, actual {actual}")]
    UnexpectedVersionId { expected: u16, actual: u16 },
    #[error("decode limit `{limit}` exceeded: maximum {max}, actual {actual}")]
    DecodeLimitExceeded {
        limit: &'static str,
        max: u64,
        actual: u64,
    },
    #[error("invalid type id 0x{0:02X}")]
    InvalidTypeId(u8),
    #[error("unsupported type id 0x{0:02X}")]
    UnsupportedTypeId(u8),
    #[error("invalid utf-8 string")]
    InvalidUtf8,
    #[error("invalid boolean byte {0}")]
    InvalidBoolean(u8),
    #[error("length value {0} does not fit in selected length mode")]
    LengthOverflow(u64),
    #[error("length value does not fit in usize")]
    LengthTooLarge,
    #[error("duplicate object key `{0}`")]
    DuplicateKey(String),
    #[error("duplicate schema type id {0}")]
    DuplicateSchemaTypeId(u16),
    #[error("value does not match declared type")]
    TypeMismatch,
    #[error("array element size is zero")]
    ZeroArrayElementSize,
    #[error("array byte length mismatch")]
    ArrayByteLengthMismatch,
    #[error("fixed-layout payload is not aligned to {alignment}-byte boundary")]
    UnalignedFixedLayoutPayload { alignment: usize },
    #[error("fixed-layout payload requires a little-endian target")]
    UnsupportedFixedLayoutEndian,
    #[error("integer overflow while calculating size")]
    SizeOverflow,
    #[error("resource exhausted while allocating {requested_bytes} bytes for {context}")]
    ResourceExhausted {
        context: &'static str,
        requested_bytes: usize,
    },
    #[error("text format error: {0}")]
    Text(String),
    #[cfg(feature = "serde")]
    #[error("serde error: {0}")]
    Serde(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Resource limits applied while decoding an untrusted SOBJ document.
///
/// The default keeps historical size behavior while bounding recursive shape
/// depth. Network and storage boundaries should select tighter values for their
/// own trust and resource budgets through [`DecodeOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    pub max_document_bytes: u64,
    pub max_nesting_depth: usize,
    pub max_container_items: u64,
    pub max_string_bytes: u64,
    pub max_bytes_value_bytes: u64,
    pub max_schema_types: u64,
    pub max_schema_fields_per_type: u64,
}

impl DecodeLimits {
    pub const fn standard() -> Self {
        Self {
            max_document_bytes: u64::MAX,
            max_nesting_depth: 128,
            max_container_items: u64::MAX,
            max_string_bytes: u64::MAX,
            max_bytes_value_bytes: u64::MAX,
            // Schema ids are u16 and must be unique, so a larger value can
            // never describe a valid document.
            max_schema_types: u16::MAX as u64 + 1,
            max_schema_fields_per_type: u64::MAX,
        }
    }
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self::standard()
    }
}

/// Decode policy shared by borrowed views, cursors, and serde adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DecodeOptions {
    pub expected_version_id: Option<u16>,
    pub limits: DecodeLimits,
}

impl DecodeOptions {
    pub const fn new(limits: DecodeLimits) -> Self {
        Self {
            expected_version_id: None,
            limits,
        }
    }

    pub const fn with_expected_version_id(mut self, version_id: u16) -> Self {
        self.expected_version_id = Some(version_id);
        self
    }
}

fn ensure_decode_limit(limit: &'static str, actual: u64, max: u64) -> Result<()> {
    if actual > max {
        return Err(Error::DecodeLimitExceeded { limit, max, actual });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LengthMode {
    U16,
    U32,
    U64,
}

impl LengthMode {
    pub const fn width(self) -> usize {
        match self {
            Self::U16 => 2,
            Self::U32 => 4,
            Self::U64 => 8,
        }
    }

    pub const fn code(self) -> u8 {
        match self {
            Self::U16 => 2,
            Self::U32 => 4,
            Self::U64 => 8,
        }
    }

    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            2 => Ok(Self::U16),
            4 => Ok(Self::U32),
            8 => Ok(Self::U64),
            other => Err(Error::InvalidLengthMode(other)),
        }
    }

    fn validate(self, value: u64) -> Result<()> {
        match self {
            Self::U16 if value > u16::MAX as u64 => Err(Error::LengthOverflow(value)),
            Self::U32 if value > u32::MAX as u64 => Err(Error::LengthOverflow(value)),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlignmentHint {
    NotAligned,
    Aligned,
}

impl AlignmentHint {
    pub const fn code(self) -> u8 {
        match self {
            Self::NotAligned => 0,
            Self::Aligned => 1,
        }
    }

    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            0 => Ok(Self::NotAligned),
            1 => Ok(Self::Aligned),
            other => Err(Error::InvalidAlignmentHint(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    pub version_id: u16,
    pub length_mode: LengthMode,
    pub alignment_hint: AlignmentHint,
    pub document_length: u64,
}

impl Metadata {
    pub const fn new(version_id: u16, length_mode: LengthMode) -> Self {
        Self {
            version_id,
            length_mode,
            alignment_hint: AlignmentHint::NotAligned,
            document_length: 0,
        }
    }

    pub const fn aligned(mut self) -> Self {
        self.alignment_hint = AlignmentHint::Aligned;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TypeId {
    Padding = 0x00,
    Boolean = 0x01,
    Uint8 = 0x02,
    Int8 = 0x03,
    Uint16 = 0x04,
    Int16 = 0x05,
    Uint32 = 0x06,
    Int32 = 0x07,
    Uint64 = 0x08,
    Int64 = 0x09,
    Float = 0x0A,
    Double = 0x0B,
    Document = 0x20,
    Object = 0x21,
    List = 0x22,
    Array = 0x23,
    String = 0x24,
    Bytes = 0x25,
    Optional = 0x26,
}

impl TypeId {
    pub fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0x00 => Ok(Self::Padding),
            0x01 => Ok(Self::Boolean),
            0x02 => Ok(Self::Uint8),
            0x03 => Ok(Self::Int8),
            0x04 => Ok(Self::Uint16),
            0x05 => Ok(Self::Int16),
            0x06 => Ok(Self::Uint32),
            0x07 => Ok(Self::Int32),
            0x08 => Ok(Self::Uint64),
            0x09 => Ok(Self::Int64),
            0x0A => Ok(Self::Float),
            0x0B => Ok(Self::Double),
            0x20 => Ok(Self::Document),
            0x21 => Ok(Self::Object),
            0x22 => Ok(Self::List),
            0x23 => Ok(Self::Array),
            0x24 => Ok(Self::String),
            0x25 => Ok(Self::Bytes),
            0x26 => Ok(Self::Optional),
            0x80..=0xFF => Err(Error::UnsupportedTypeId(byte)),
            other => Err(Error::InvalidTypeId(other)),
        }
    }

    pub const fn byte(self) -> u8 {
        self as u8
    }

    pub const fn fixed_size(self) -> Option<u64> {
        match self {
            Self::Boolean | Self::Uint8 | Self::Int8 => Some(1),
            Self::Uint16 | Self::Int16 => Some(2),
            Self::Uint32 | Self::Int32 | Self::Float => Some(4),
            Self::Uint64 | Self::Int64 | Self::Double => Some(8),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElementType {
    Builtin(TypeId),
    SchemaRef(u16),
    List(Box<ElementType>),
    Array(Box<ElementType>),
    Object,
    Optional(Box<ElementType>),
    Extension(Box<ExtensionType>),
}

impl ElementType {
    /// Constructs an extension element type. The payload is boxed so the rare
    /// extension variant does not inflate `ElementType` — and every type that
    /// embeds it — with an inline `Vec`.
    pub fn extension(id: u8, data: Vec<u8>) -> Self {
        Self::Extension(Box::new(ExtensionType { id, data }))
    }

    pub fn try_extension(id: u8, data: Vec<u8>) -> Result<Self> {
        Ok(Self::Extension(try_box(
            ExtensionType { id, data },
            "extension element type",
        )?))
    }
}

/// Payload of an [`ElementType::Extension`]: a length-delimited extension id and
/// its bytes. Boxed inside `ElementType` to keep that enum small.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionType {
    pub id: u8,
    pub data: Vec<u8>,
}

fn allocation_bytes<T>(items: usize) -> Result<usize> {
    items
        .checked_mul(std::mem::size_of::<T>().max(1))
        .ok_or(Error::SizeOverflow)
}

fn try_reserve_vec<T>(vec: &mut Vec<T>, additional: usize, context: &'static str) -> Result<()> {
    let requested_items = vec
        .len()
        .checked_add(additional)
        .ok_or(Error::SizeOverflow)?;
    let requested_bytes = allocation_bytes::<T>(requested_items)?;
    vec.try_reserve(additional)
        .map_err(|_| Error::ResourceExhausted {
            context,
            requested_bytes,
        })
}

fn try_vec_with_capacity<T>(capacity: usize, context: &'static str) -> Result<Vec<T>> {
    let mut vec = Vec::new();
    let requested_bytes = allocation_bytes::<T>(capacity)?;
    vec.try_reserve(capacity)
        .map_err(|_| Error::ResourceExhausted {
            context,
            requested_bytes,
        })?;
    Ok(vec)
}

fn try_hashset_with_capacity<T>(capacity: usize, context: &'static str) -> Result<HashSet<T>>
where
    T: Eq + std::hash::Hash,
{
    let mut set = HashSet::new();
    let requested_bytes = allocation_bytes::<T>(capacity)?;
    set.try_reserve(capacity)
        .map_err(|_| Error::ResourceExhausted {
            context,
            requested_bytes,
        })?;
    Ok(set)
}

#[cfg(feature = "serde")]
fn try_reserve_hashset<T>(
    set: &mut HashSet<T>,
    additional: usize,
    context: &'static str,
) -> Result<()>
where
    T: Eq + std::hash::Hash,
{
    let requested_items = set
        .len()
        .checked_add(additional)
        .ok_or(Error::SizeOverflow)?;
    let requested_bytes = allocation_bytes::<T>(requested_items)?;
    set.try_reserve(additional)
        .map_err(|_| Error::ResourceExhausted {
            context,
            requested_bytes,
        })
}

fn try_push_vec<T>(vec: &mut Vec<T>, value: T, context: &'static str) -> Result<()> {
    try_reserve_vec(vec, 1, context)?;
    vec.push(value);
    Ok(())
}

fn try_extend_from_slice(vec: &mut Vec<u8>, value: &[u8], context: &'static str) -> Result<()> {
    try_reserve_vec(vec, value.len(), context)?;
    vec.extend_from_slice(value);
    Ok(())
}

fn try_string_from_str(value: &str, context: &'static str) -> Result<String> {
    let mut out = String::new();
    out.try_reserve(value.len())
        .map_err(|_| Error::ResourceExhausted {
            context,
            requested_bytes: value.len(),
        })?;
    out.push_str(value);
    Ok(out)
}

pub(crate) fn try_box<T>(value: T, context: &'static str) -> Result<Box<T>> {
    let requested_bytes = std::mem::size_of::<T>().max(1);
    if std::mem::size_of::<T>() == 0 {
        return Ok(Box::new(value));
    }
    let layout = std::alloc::Layout::new::<T>();
    // SAFETY: `alloc` returns a pointer suitable for `T` or null. On success we
    // initialize it exactly once and hand ownership to `Box`, which will use the
    // same global allocator to deallocate it.
    let ptr = unsafe { std::alloc::alloc(layout) as *mut T };
    if ptr.is_null() {
        return Err(Error::ResourceExhausted {
            context,
            requested_bytes,
        });
    }
    // SAFETY: `ptr` points to valid, writable memory for one `T`.
    unsafe {
        ptr.write(value);
        Ok(Box::from_raw(ptr))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    pub metadata: Metadata,
    pub schema: Schema,
    pub payload: Payload,
}

impl Document {
    pub fn dynamic(version_id: u16, length_mode: LengthMode, value: Value) -> Self {
        Self {
            metadata: Metadata::new(version_id, length_mode),
            schema: Schema::default(),
            payload: Payload::Dynamic(value),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        try_extend_from_slice(&mut out, MAGIC, "document magic")?;
        write_u16(&mut out, self.metadata.version_id)?;
        try_push_vec(
            &mut out,
            self.metadata.length_mode.code(),
            "document length mode",
        )?;
        try_push_vec(
            &mut out,
            self.metadata.alignment_hint.code(),
            "document alignment hint",
        )?;
        let length_offset = out.len();
        write_length(&mut out, self.metadata.length_mode, 0)?;
        self.schema.encode(&mut out, self.metadata.length_mode)?;
        self.payload.encode(&mut out, self.metadata.length_mode)?;
        let document_length = u64::try_from(out.len()).map_err(|_| Error::LengthTooLarge)?;
        self.metadata.length_mode.validate(document_length)?;
        patch_length(
            &mut out[length_offset..length_offset + self.metadata.length_mode.width()],
            self.metadata.length_mode,
            document_length,
        )?;
        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BorrowedDocumentBuilder<'a> {
    pub metadata: Metadata,
    pub schema: Schema,
    pub payload: BorrowedPayload<'a>,
}

impl<'a> BorrowedDocumentBuilder<'a> {
    pub fn dynamic_bytes(version_id: u16, length_mode: LengthMode, data: &'a [u8]) -> Self {
        Self {
            metadata: Metadata::new(version_id, length_mode),
            schema: Schema::default(),
            payload: BorrowedPayload::Dynamic(BorrowedValue::Bytes(data)),
        }
    }

    pub fn dynamic_array(
        version_id: u16,
        length_mode: LengthMode,
        array: BorrowedArray<'a>,
    ) -> Result<Self> {
        array.validate()?;
        Ok(Self {
            metadata: Metadata::new(version_id, length_mode),
            schema: Schema::default(),
            payload: BorrowedPayload::Dynamic(BorrowedValue::try_array(array)?),
        })
    }

    pub fn schema_payload(
        version_id: u16,
        length_mode: LengthMode,
        schema: Schema,
        root_schema_type_id: u16,
        root_count: u64,
        data: &'a [u8],
    ) -> Result<Self> {
        validate_schema_payload_data(&schema, root_schema_type_id, root_count, data.len())?;
        Ok(Self {
            metadata: Metadata::new(version_id, length_mode),
            schema,
            payload: BorrowedPayload::Schema {
                root_schema_type_id,
                root_count,
                data,
            },
        })
    }

    pub fn fixed_layout_records<T: FixedLayoutRecord>(
        version_id: u16,
        length_mode: LengthMode,
        records: &'a [T],
    ) -> Result<Self> {
        let root_count = u64::try_from(records.len()).map_err(|_| Error::LengthTooLarge)?;
        Self::schema_payload(
            version_id,
            length_mode,
            T::schema(),
            T::SCHEMA_TYPE_ID,
            root_count,
            fixed_layout_record_bytes(records)?,
        )
    }

    pub fn aligned(mut self) -> Self {
        self.metadata = self.metadata.aligned();
        self
    }

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = schema;
        self
    }

    pub fn to_vec(&self) -> Result<Vec<u8>> {
        self.to_parts().and_then(BorrowedDocumentParts::to_vec)
    }

    pub fn to_parts(&self) -> Result<BorrowedDocumentParts<'a>> {
        let mut prefix = Vec::new();
        try_extend_from_slice(&mut prefix, MAGIC, "borrowed document magic")?;
        write_u16(&mut prefix, self.metadata.version_id)?;
        try_push_vec(
            &mut prefix,
            self.metadata.length_mode.code(),
            "borrowed document length mode",
        )?;
        try_push_vec(
            &mut prefix,
            self.metadata.alignment_hint.code(),
            "borrowed document alignment hint",
        )?;
        let length_offset = prefix.len();
        write_length(&mut prefix, self.metadata.length_mode, 0)?;
        self.schema.encode(&mut prefix, self.metadata.length_mode)?;
        let payload = self.encode_payload_prefix(&mut prefix)?;
        let document_length = prefix
            .len()
            .checked_add(payload.len())
            .ok_or(Error::SizeOverflow)
            .and_then(|length| u64::try_from(length).map_err(|_| Error::LengthTooLarge))?;
        self.metadata.length_mode.validate(document_length)?;
        patch_length(
            &mut prefix[length_offset..length_offset + self.metadata.length_mode.width()],
            self.metadata.length_mode,
            document_length,
        )?;
        Ok(BorrowedDocumentParts { prefix, payload })
    }

    fn encode_payload_prefix(&self, out: &mut Vec<u8>) -> Result<&'a [u8]> {
        match &self.payload {
            BorrowedPayload::Dynamic(value) => {
                value.encode_element_prefix(out, self.metadata.length_mode)
            }
            BorrowedPayload::Schema {
                root_schema_type_id,
                root_count,
                data,
            } => {
                validate_schema_payload_data(
                    &self.schema,
                    *root_schema_type_id,
                    *root_count,
                    data.len(),
                )?;
                write_u16(out, *root_schema_type_id)?;
                write_length(out, self.metadata.length_mode, *root_count)?;
                Ok(data)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BorrowedDocumentParts<'a> {
    pub prefix: Vec<u8>,
    pub payload: &'a [u8],
}

impl<'a> BorrowedDocumentParts<'a> {
    pub fn document_len(&self) -> Result<usize> {
        self.prefix
            .len()
            .checked_add(self.payload.len())
            .ok_or(Error::SizeOverflow)
    }

    pub fn as_slices(&self) -> [&[u8]; 2] {
        [self.prefix.as_slice(), self.payload]
    }

    pub fn to_vec(self) -> Result<Vec<u8>> {
        let total = self.document_len()?;
        let mut out = try_vec_with_capacity(total, "borrowed document")?;
        try_extend_from_slice(&mut out, &self.prefix, "borrowed document prefix")?;
        try_extend_from_slice(&mut out, self.payload, "borrowed document payload")?;
        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BorrowedPayload<'a> {
    Dynamic(BorrowedValue<'a>),
    Schema {
        root_schema_type_id: u16,
        root_count: u64,
        data: &'a [u8],
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BorrowedValue<'a> {
    Bytes(&'a [u8]),
    Array(Box<BorrowedArray<'a>>),
}

impl<'a> BorrowedValue<'a> {
    pub fn array(value: BorrowedArray<'a>) -> Self {
        Self::Array(Box::new(value))
    }

    pub fn try_array(value: BorrowedArray<'a>) -> Result<Self> {
        Ok(Self::Array(try_box(value, "borrowed array value")?))
    }

    pub const fn type_id(&self) -> TypeId {
        match self {
            Self::Bytes(_) => TypeId::Bytes,
            Self::Array(_) => TypeId::Array,
        }
    }

    fn encode_element_prefix(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<&'a [u8]> {
        try_push_vec(out, self.type_id().byte(), "borrowed value type id")?;
        self.encode_value_prefix(out, mode)
    }

    fn encode_value_prefix(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<&'a [u8]> {
        match self {
            Self::Bytes(value) => {
                write_length(out, mode, value.len() as u64)?;
                Ok(value)
            }
            Self::Array(value) => value.encode_prefix(out, mode),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BorrowedArray<'a> {
    pub element_type: ElementType,
    pub element_size: u64,
    pub dimensions: Vec<u64>,
    pub data: &'a [u8],
}

impl<'a> BorrowedArray<'a> {
    pub fn new(
        element_type: ElementType,
        element_size: u64,
        dimensions: Vec<u64>,
        data: &'a [u8],
    ) -> Result<Self> {
        let array = Self {
            element_type,
            element_size,
            dimensions,
            data,
        };
        array.validate()?;
        Ok(array)
    }

    fn encode_prefix(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<&'a [u8]> {
        self.validate()?;
        encode_element_type(out, mode, &self.element_type)?;
        write_length(out, mode, self.element_size)?;
        try_push_vec(
            out,
            u8::try_from(self.dimensions.len()).map_err(|_| Error::LengthTooLarge)?,
            "borrowed array rank",
        )?;
        for dimension in &self.dimensions {
            write_length(out, mode, *dimension)?;
        }
        Ok(self.data)
    }

    fn validate(&self) -> Result<()> {
        validate_array_data(
            &self.element_type,
            self.element_size,
            &self.dimensions,
            self.data.len(),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Payload {
    Dynamic(Value),
    Schema {
        root_schema_type_id: u16,
        root_count: u64,
        data: Vec<u8>,
    },
}

impl Payload {
    fn encode(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<()> {
        match self {
            Self::Dynamic(value) => value.encode_element(out, mode),
            Self::Schema {
                root_schema_type_id,
                root_count,
                data,
            } => {
                write_u16(out, *root_schema_type_id)?;
                write_length(out, mode, *root_count)?;
                try_extend_from_slice(out, data, "schema payload")?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Boolean(bool),
    Uint8(u8),
    Int8(i8),
    Uint16(u16),
    Int16(i16),
    Uint32(u32),
    Int32(i32),
    Uint64(u64),
    Int64(i64),
    Float(f32),
    Double(f64),
    Object(Object),
    List(Box<List>),
    Array(Box<Array>),
    String(String),
    Bytes(Vec<u8>),
    Optional(Box<OptionalValue>),
}

impl Value {
    /// Wraps a list value. The payload is boxed so the (comparatively rare)
    /// container variants do not inflate every `Value` to the size of the
    /// largest one.
    pub fn list(value: List) -> Self {
        Self::List(Box::new(value))
    }

    pub fn try_list(value: List) -> Result<Self> {
        Ok(Self::List(try_box(value, "list value")?))
    }

    /// Wraps an array value (boxed; see [`Value::list`]).
    pub fn array(value: Array) -> Self {
        Self::Array(Box::new(value))
    }

    pub fn try_array(value: Array) -> Result<Self> {
        Ok(Self::Array(try_box(value, "array value")?))
    }

    /// Wraps an optional value (boxed; see [`Value::list`]).
    pub fn optional(value: OptionalValue) -> Self {
        Self::Optional(Box::new(value))
    }

    pub fn try_optional(value: OptionalValue) -> Result<Self> {
        Ok(Self::Optional(try_box(value, "optional value")?))
    }

    pub const fn type_id(&self) -> TypeId {
        match self {
            Self::Boolean(_) => TypeId::Boolean,
            Self::Uint8(_) => TypeId::Uint8,
            Self::Int8(_) => TypeId::Int8,
            Self::Uint16(_) => TypeId::Uint16,
            Self::Int16(_) => TypeId::Int16,
            Self::Uint32(_) => TypeId::Uint32,
            Self::Int32(_) => TypeId::Int32,
            Self::Uint64(_) => TypeId::Uint64,
            Self::Int64(_) => TypeId::Int64,
            Self::Float(_) => TypeId::Float,
            Self::Double(_) => TypeId::Double,
            Self::Object(_) => TypeId::Object,
            Self::List(_) => TypeId::List,
            Self::Array(_) => TypeId::Array,
            Self::String(_) => TypeId::String,
            Self::Bytes(_) => TypeId::Bytes,
            Self::Optional(_) => TypeId::Optional,
        }
    }

    fn encode_element(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<()> {
        try_push_vec(out, self.type_id().byte(), "value type id")?;
        self.encode_value(out, mode)
    }

    fn encode_value(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<()> {
        match self {
            Self::Boolean(value) => try_push_vec(out, u8::from(*value), "bool")?,
            Self::Uint8(value) => try_push_vec(out, *value, "u8")?,
            Self::Int8(value) => try_push_vec(out, *value as u8, "i8")?,
            Self::Uint16(value) => write_u16(out, *value)?,
            Self::Int16(value) => try_extend_from_slice(out, &value.to_le_bytes(), "i16")?,
            Self::Uint32(value) => try_extend_from_slice(out, &value.to_le_bytes(), "u32")?,
            Self::Int32(value) => try_extend_from_slice(out, &value.to_le_bytes(), "i32")?,
            Self::Uint64(value) => try_extend_from_slice(out, &value.to_le_bytes(), "u64")?,
            Self::Int64(value) => try_extend_from_slice(out, &value.to_le_bytes(), "i64")?,
            Self::Float(value) => try_extend_from_slice(out, &value.to_le_bytes(), "f32")?,
            Self::Double(value) => try_extend_from_slice(out, &value.to_le_bytes(), "f64")?,
            Self::Object(value) => value.encode(out, mode)?,
            Self::List(value) => value.encode(out, mode)?,
            Self::Array(value) => value.encode(out, mode)?,
            Self::String(value) => write_string(out, mode, value)?,
            Self::Bytes(value) => write_bytes(out, mode, value)?,
            Self::Optional(value) => value.encode(out, mode)?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Object {
    pub fields: Vec<Field>,
}

impl Object {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    fn encode(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<()> {
        validate_unique_keys(self.fields.iter().map(|field| field.key.as_str()))?;
        write_length(out, mode, self.fields.len() as u64)?;
        for field in &self.fields {
            write_string(out, mode, &field.key)?;
            field.value.encode_element(out, mode)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub key: String,
    pub value: Value,
}

impl Field {
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct List {
    pub element_type: ElementType,
    pub values: Vec<Value>,
}

impl List {
    pub fn new(element_type: ElementType, values: Vec<Value>) -> Self {
        Self {
            element_type,
            values,
        }
    }

    fn encode(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<()> {
        encode_element_type(out, mode, &self.element_type)?;
        write_length(out, mode, self.values.len() as u64)?;
        for value in &self.values {
            encode_typed_value(out, mode, &self.element_type, value)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Array {
    pub element_type: ElementType,
    pub element_size: u64,
    pub dimensions: Vec<u64>,
    pub data: Vec<u8>,
}

impl Array {
    pub fn new(
        element_type: ElementType,
        element_size: u64,
        dimensions: Vec<u64>,
        data: Vec<u8>,
    ) -> Result<Self> {
        let array = Self {
            element_type,
            element_size,
            dimensions,
            data,
        };
        array.validate()?;
        Ok(array)
    }

    fn encode(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<()> {
        self.validate()?;
        encode_element_type(out, mode, &self.element_type)?;
        write_length(out, mode, self.element_size)?;
        try_push_vec(
            out,
            u8::try_from(self.dimensions.len()).map_err(|_| Error::LengthTooLarge)?,
            "array rank",
        )?;
        for dimension in &self.dimensions {
            write_length(out, mode, *dimension)?;
        }
        try_extend_from_slice(out, &self.data, "array data")?;
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        validate_array_data(
            &self.element_type,
            self.element_size,
            &self.dimensions,
            self.data.len(),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OptionalValue {
    pub element_type: ElementType,
    pub value: Option<Box<Value>>,
}

impl OptionalValue {
    pub fn some(element_type: ElementType, value: Value) -> Self {
        Self {
            element_type,
            value: Some(Box::new(value)),
        }
    }

    pub fn try_some(element_type: ElementType, value: Value) -> Result<Self> {
        Ok(Self {
            element_type,
            value: Some(try_box(value, "optional inner value")?),
        })
    }

    pub fn none(element_type: ElementType) -> Self {
        Self {
            element_type,
            value: None,
        }
    }

    fn encode(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<()> {
        encode_element_type(out, mode, &self.element_type)?;
        match &self.value {
            Some(value) => {
                try_push_vec(out, 1, "optional presence")?;
                encode_typed_value(out, mode, &self.element_type, value)
            }
            None => {
                try_push_vec(out, 0, "optional presence")?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    pub types: Vec<SchemaType>,
}

impl Schema {
    fn encode(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<()> {
        write_length(out, mode, self.types.len() as u64)?;
        let mut ids = try_hashset_with_capacity(self.types.len(), "schema type ids")?;
        for schema_type in &self.types {
            if !ids.insert(schema_type.id) {
                return Err(Error::DuplicateSchemaTypeId(schema_type.id));
            }
            schema_type.encode(out, mode)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaType {
    pub id: u16,
    pub name: String,
    pub payload_size: u64,
    pub fields: Vec<SchemaField>,
}

impl SchemaType {
    fn encode(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<()> {
        write_u16(out, self.id)?;
        write_string(out, mode, &self.name)?;
        write_length(out, mode, self.payload_size)?;
        write_length(out, mode, self.fields.len() as u64)?;
        for field in &self.fields {
            write_string(out, mode, &field.name)?;
            encode_element_type(out, mode, &field.element_type)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaField {
    pub name: String,
    pub element_type: ElementType,
}

/// Marker for schema-payload records that can be safely exposed as a borrowed
/// typed slice.
///
/// # Safety
///
/// Implementors must have a stable byte layout, normally `#[repr(C)]`, and must
/// not contain pointers, references, heap-owned fields, or any other value whose
/// in-memory representation is not the SOBJ wire representation. Every byte of
/// the record, including padding, must be initialized and deterministic. Numeric
/// fields must already be stored in SOBJ little-endian order on the target.
pub unsafe trait FixedLayoutRecord: Copy + Sized + 'static {
    const SCHEMA_TYPE_ID: u16;
    const SCHEMA_TYPE_NAME: &'static str;

    fn schema_fields() -> Vec<SchemaField>;

    fn schema_type() -> SchemaType {
        SchemaType {
            id: Self::SCHEMA_TYPE_ID,
            name: Self::SCHEMA_TYPE_NAME.to_owned(),
            payload_size: std::mem::size_of::<Self>() as u64,
            fields: Self::schema_fields(),
        }
    }

    fn schema() -> Schema {
        Schema {
            types: vec![Self::schema_type()],
        }
    }
}

pub fn fixed_layout_record_bytes<T: FixedLayoutRecord>(records: &[T]) -> Result<&[u8]> {
    ensure_fixed_layout_supported::<T>()?;
    let byte_len = records
        .len()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(Error::SizeOverflow)?;
    Ok(unsafe { std::slice::from_raw_parts(records.as_ptr().cast::<u8>(), byte_len) })
}

pub fn fixed_layout_records_from_bytes<T: FixedLayoutRecord>(bytes: &[u8]) -> Result<&[T]> {
    ensure_fixed_layout_supported::<T>()?;
    let record_size = std::mem::size_of::<T>();
    if !bytes.len().is_multiple_of(record_size) {
        return Err(Error::ArrayByteLengthMismatch);
    }
    let alignment = std::mem::align_of::<T>();
    if alignment > 1 && !(bytes.as_ptr() as usize).is_multiple_of(alignment) {
        return Err(Error::UnalignedFixedLayoutPayload { alignment });
    }
    Ok(
        unsafe {
            std::slice::from_raw_parts(bytes.as_ptr().cast::<T>(), bytes.len() / record_size)
        },
    )
}

#[derive(Debug, Clone, PartialEq)]
pub struct DocumentView<'a> {
    pub metadata: Metadata,
    pub schema: Schema,
    pub payload: PayloadView<'a>,
    bytes: &'a [u8],
}

impl<'a> DocumentView<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self> {
        Self::parse_with_options(bytes, DecodeOptions::default())
    }

    pub fn parse_with_options(bytes: &'a [u8], options: DecodeOptions) -> Result<Self> {
        let (mut reader, metadata) = read_document_header(bytes, options)?;
        let schema = read_schema(&mut reader)?;
        let payload = if schema.types.is_empty() {
            PayloadView::Dynamic(reader.read_element()?)
        } else {
            let root_schema_type_id = reader.read_u16()?;
            let root_count = reader.read_length()?;
            ensure_decode_limit(
                "container items",
                root_count,
                reader.limits.max_container_items,
            )?;
            let data = reader.remaining_bytes();
            ensure_decode_limit(
                "bytes value bytes",
                u64::try_from(data.len()).map_err(|_| Error::LengthTooLarge)?,
                reader.limits.max_bytes_value_bytes,
            )?;
            validate_schema_payload_data(&schema, root_schema_type_id, root_count, data.len())?;
            reader.pos = bytes.len();
            PayloadView::Schema {
                root_schema_type_id,
                root_count,
                data,
            }
        };
        reader.skip_padding();
        if reader.remaining() != 0 {
            return Err(Error::UnexpectedEof);
        }
        Ok(Self {
            metadata,
            schema,
            payload,
            bytes,
        })
    }

    pub const fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    pub fn fixed_layout_records<T: FixedLayoutRecord>(&self) -> Result<&'a [T]> {
        ensure_fixed_layout_supported::<T>()?;
        let PayloadView::Schema {
            root_schema_type_id,
            root_count,
            data,
        } = self.payload
        else {
            return Err(Error::TypeMismatch);
        };
        if root_schema_type_id != T::SCHEMA_TYPE_ID {
            return Err(Error::TypeMismatch);
        }
        let Some(schema_type) = self
            .schema
            .types
            .iter()
            .find(|schema_type| schema_type.id == root_schema_type_id)
        else {
            return Err(Error::TypeMismatch);
        };
        if schema_type.payload_size != std::mem::size_of::<T>() as u64 {
            return Err(Error::TypeMismatch);
        }
        validate_schema_payload_data(&self.schema, root_schema_type_id, root_count, data.len())?;
        let records = fixed_layout_records_from_bytes::<T>(data)?;
        if u64::try_from(records.len()).map_err(|_| Error::LengthTooLarge)? != root_count {
            return Err(Error::ArrayByteLengthMismatch);
        }
        Ok(records)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PayloadView<'a> {
    Dynamic(ElementView<'a>),
    Schema {
        root_schema_type_id: u16,
        root_count: u64,
        data: &'a [u8],
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DocumentCursor<'a> {
    pub metadata: Metadata,
    pub schema: Schema,
    pub payload: PayloadCursor<'a>,
    bytes: &'a [u8],
}

impl<'a> DocumentCursor<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self> {
        Self::parse_with_options(bytes, DecodeOptions::default())
    }

    pub fn parse_with_options(bytes: &'a [u8], options: DecodeOptions) -> Result<Self> {
        let (mut reader, metadata) = read_document_header(bytes, options)?;
        let schema = read_schema(&mut reader)?;
        let payload = if schema.types.is_empty() {
            PayloadCursor::Dynamic(read_element_cursor(&mut reader)?)
        } else {
            let root_schema_type_id = reader.read_u16()?;
            let root_count = reader.read_length()?;
            ensure_decode_limit(
                "container items",
                root_count,
                reader.limits.max_container_items,
            )?;
            let data = reader.remaining_bytes();
            ensure_decode_limit(
                "bytes value bytes",
                u64::try_from(data.len()).map_err(|_| Error::LengthTooLarge)?,
                reader.limits.max_bytes_value_bytes,
            )?;
            validate_schema_payload_data(&schema, root_schema_type_id, root_count, data.len())?;
            reader.pos = bytes.len();
            PayloadCursor::Schema {
                root_schema_type_id,
                root_count,
                data,
            }
        };
        reader.skip_padding();
        if reader.remaining() != 0 {
            return Err(Error::UnexpectedEof);
        }
        Ok(Self {
            metadata,
            schema,
            payload,
            bytes,
        })
    }

    pub const fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PayloadCursor<'a> {
    Dynamic(ElementCursor<'a>),
    Schema {
        root_schema_type_id: u16,
        root_count: u64,
        data: &'a [u8],
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ElementCursor<'a> {
    type_id: TypeId,
    value_offset: usize,
    next_offset: usize,
    bytes: &'a [u8],
    mode: LengthMode,
    limits: DecodeLimits,
}

impl<'a> ElementCursor<'a> {
    pub const fn type_id(&self) -> TypeId {
        self.type_id
    }

    pub fn as_u32(&self) -> Result<u32> {
        if self.type_id != TypeId::Uint32 {
            return Err(Error::TypeMismatch);
        }
        let mut reader = self.value_reader();
        let value = reader.read_u32()?;
        self.finish_reader(&reader)?;
        Ok(value)
    }

    pub fn as_str(&self) -> Result<&'a str> {
        if self.type_id != TypeId::String {
            return Err(Error::TypeMismatch);
        }
        let mut reader = self.value_reader();
        let value = reader.read_string()?;
        self.finish_reader(&reader)?;
        Ok(value)
    }

    pub fn as_bytes(&self) -> Result<&'a [u8]> {
        if self.type_id != TypeId::Bytes {
            return Err(Error::TypeMismatch);
        }
        let mut reader = self.value_reader();
        let value = reader.read_bytes()?;
        self.finish_reader(&reader)?;
        Ok(value)
    }

    pub fn as_object(&self) -> Result<ObjectCursor<'a>> {
        if self.type_id != TypeId::Object {
            return Err(Error::TypeMismatch);
        }
        let mut reader = self.value_reader();
        let field_count = reader.read_container_len()?;
        Ok(ObjectCursor {
            bytes: self.bytes,
            mode: self.mode,
            limits: self.limits,
            field_count,
            fields_offset: reader.pos,
            end_offset: self.next_offset,
        })
    }

    pub fn as_list(&self) -> Result<ListCursor<'a>> {
        if self.type_id != TypeId::List {
            return Err(Error::TypeMismatch);
        }
        let mut reader = self.value_reader();
        let element_type = read_element_type(&mut reader)?;
        let len = reader.read_container_len()?;
        Ok(ListCursor {
            bytes: self.bytes,
            mode: self.mode,
            limits: self.limits,
            element_type,
            len,
            values_offset: reader.pos,
            end_offset: self.next_offset,
        })
    }

    pub fn as_array(&self) -> Result<ArrayView<'a>> {
        if self.type_id != TypeId::Array {
            return Err(Error::TypeMismatch);
        }
        let mut reader = self.value_reader();
        let value = reader.read_array()?;
        self.finish_reader(&reader)?;
        Ok(value)
    }

    fn value_reader(&self) -> Reader<'a> {
        Reader {
            bytes: self.bytes,
            pos: self.value_offset,
            mode: self.mode,
            limits: self.limits,
            nesting_depth: 0,
        }
    }

    fn finish_reader(&self, reader: &Reader<'_>) -> Result<()> {
        if reader.pos == self.next_offset {
            Ok(())
        } else {
            Err(Error::UnexpectedEof)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectCursor<'a> {
    bytes: &'a [u8],
    mode: LengthMode,
    limits: DecodeLimits,
    field_count: usize,
    fields_offset: usize,
    end_offset: usize,
}

impl<'a> ObjectCursor<'a> {
    pub const fn field_count(&self) -> usize {
        self.field_count
    }

    pub fn iter(&self) -> ObjectCursorIter<'a> {
        ObjectCursorIter {
            reader: Reader {
                bytes: self.bytes,
                pos: self.fields_offset,
                mode: self.mode,
                limits: self.limits,
                nesting_depth: 0,
            },
            remaining: self.field_count,
            end_offset: self.end_offset,
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<ElementCursor<'a>>> {
        for field in self.iter() {
            let field = field?;
            if field.key == key {
                return Ok(Some(field.value));
            }
        }
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectFieldCursor<'a> {
    pub key: &'a str,
    pub value: ElementCursor<'a>,
}

pub struct ObjectCursorIter<'a> {
    reader: Reader<'a>,
    remaining: usize,
    end_offset: usize,
}

impl<'a> Iterator for ObjectCursorIter<'a> {
    type Item = Result<ObjectFieldCursor<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let key = match self.reader.read_string() {
            Ok(key) => key,
            Err(error) => return Some(Err(error)),
        };
        let value = match read_element_cursor(&mut self.reader) {
            Ok(value) => value,
            Err(error) => return Some(Err(error)),
        };
        if self.remaining == 0 && self.reader.pos != self.end_offset {
            return Some(Err(Error::UnexpectedEof));
        }
        Some(Ok(ObjectFieldCursor { key, value }))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListCursor<'a> {
    bytes: &'a [u8],
    mode: LengthMode,
    limits: DecodeLimits,
    element_type: ElementType,
    len: usize,
    values_offset: usize,
    end_offset: usize,
}

impl<'a> ListCursor<'a> {
    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub const fn element_type(&self) -> &ElementType {
        &self.element_type
    }

    pub fn iter(&self) -> ListCursorIter<'a> {
        ListCursorIter {
            reader: Reader {
                bytes: self.bytes,
                pos: self.values_offset,
                mode: self.mode,
                limits: self.limits,
                nesting_depth: 0,
            },
            element_type: self.element_type.clone(),
            remaining: self.len,
            end_offset: self.end_offset,
        }
    }
}

pub struct ListCursorIter<'a> {
    reader: Reader<'a>,
    element_type: ElementType,
    remaining: usize,
    end_offset: usize,
}

impl<'a> Iterator for ListCursorIter<'a> {
    type Item = Result<ElementCursor<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let value = match read_typed_value_cursor(&mut self.reader, &self.element_type) {
            Ok(value) => value,
            Err(error) => return Some(Err(error)),
        };
        if self.remaining == 0 && self.reader.pos != self.end_offset {
            return Some(Err(Error::UnexpectedEof));
        }
        Some(Ok(value))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ElementView<'a> {
    Boolean(bool),
    Uint8(u8),
    Int8(i8),
    Uint16(u16),
    Int16(i16),
    Uint32(u32),
    Int32(i32),
    Uint64(u64),
    Int64(i64),
    Float(f32),
    Double(f64),
    Object(ObjectView<'a>),
    List(Box<ListView<'a>>),
    Array(Box<ArrayView<'a>>),
    String(&'a str),
    Bytes(&'a [u8]),
    Optional(Box<OptionalView<'a>>),
}

impl<'a> ElementView<'a> {
    /// Wraps a list view (boxed to keep `ElementView` small; mirrors
    /// [`Value::list`]).
    fn list(value: ListView<'a>) -> Result<Self> {
        Ok(Self::List(try_box(value, "list view")?))
    }

    /// Wraps an array view (boxed; see [`ElementView::list`]).
    fn array(value: ArrayView<'a>) -> Result<Self> {
        Ok(Self::Array(try_box(value, "array view")?))
    }

    /// Wraps an optional view (boxed; see [`ElementView::list`]).
    fn optional(value: OptionalView<'a>) -> Result<Self> {
        Ok(Self::Optional(try_box(value, "optional view")?))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectView<'a> {
    pub fields: Vec<FieldView<'a>>,
}

impl<'a> ObjectView<'a> {
    pub fn get(&self, key: &str) -> Option<&ElementView<'a>> {
        self.fields
            .iter()
            .find(|field| field.key == key)
            .map(|field| &field.value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldView<'a> {
    pub key: &'a str,
    pub value: ElementView<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListView<'a> {
    pub element_type: ElementType,
    pub values: Vec<ElementView<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayView<'a> {
    pub element_type: ElementType,
    pub element_size: u64,
    pub dimensions: Vec<u64>,
    pub data: &'a [u8],
}

#[derive(Debug, Clone, PartialEq)]
pub struct OptionalView<'a> {
    pub element_type: ElementType,
    pub value: Option<Box<ElementView<'a>>>,
}

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
    mode: LengthMode,
    limits: DecodeLimits,
    nesting_depth: usize,
}

impl<'a> Reader<'a> {
    const fn with_limits(bytes: &'a [u8], mode: LengthMode, limits: DecodeLimits) -> Self {
        Self {
            bytes,
            pos: 0,
            mode,
            limits,
            nesting_depth: 0,
        }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(len).ok_or(Error::SizeOverflow)?;
        if end > self.bytes.len() {
            return Err(Error::UnexpectedEof);
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(*self.take(1)?.first().ok_or(Error::UnexpectedEof)?)
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let bytes = self.take(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.take(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_length(&mut self) -> Result<u64> {
        match self.mode {
            LengthMode::U16 => Ok(self.read_u16()? as u64),
            LengthMode::U32 => Ok(self.read_u32()? as u64),
            LengthMode::U64 => self.read_u64(),
        }
    }

    fn read_limited_len_usize(&mut self, limit: &'static str, max: u64) -> Result<usize> {
        let len = self.read_length()?;
        ensure_decode_limit(limit, len, max)?;
        usize::try_from(len).map_err(|_| Error::LengthTooLarge)
    }

    fn read_container_len(&mut self) -> Result<usize> {
        let len =
            self.read_limited_len_usize("container items", self.limits.max_container_items)?;
        // Every dynamic object/list item consumes at least one byte. Reject an
        // impossible count before serde or a view attempts capacity allocation.
        if len > self.remaining() {
            return Err(Error::LengthTooLarge);
        }
        Ok(len)
    }

    fn ensure_count_fits_remaining(
        &self,
        limit: &'static str,
        count: usize,
        minimum_item_bytes: usize,
    ) -> Result<()> {
        let maximum = self.remaining() / minimum_item_bytes.max(1);
        ensure_decode_limit(limit, count as u64, maximum as u64)
    }

    fn read_string(&mut self) -> Result<&'a str> {
        let len = self.read_limited_len_usize("string bytes", self.limits.max_string_bytes)?;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)
    }

    fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let len =
            self.read_limited_len_usize("bytes value bytes", self.limits.max_bytes_value_bytes)?;
        self.take(len)
    }

    fn with_nesting<T>(&mut self, decode: impl FnOnce(&mut Reader<'a>) -> Result<T>) -> Result<T> {
        let next_depth = self
            .nesting_depth
            .checked_add(1)
            .ok_or(Error::SizeOverflow)?;
        ensure_decode_limit(
            "nesting depth",
            u64::try_from(next_depth).unwrap_or(u64::MAX),
            u64::try_from(self.limits.max_nesting_depth).unwrap_or(u64::MAX),
        )?;
        let previous_depth = self.nesting_depth;
        self.nesting_depth = next_depth;
        let result = decode(self);
        self.nesting_depth = previous_depth;
        result
    }

    fn skip_padding(&mut self) {
        while self.pos < self.bytes.len() && self.bytes[self.pos] == TypeId::Padding.byte() {
            self.pos += 1;
        }
    }

    fn read_element(&mut self) -> Result<ElementView<'a>> {
        self.skip_padding();
        let type_id = TypeId::from_byte(self.read_u8()?)?;
        if type_id == TypeId::Padding {
            return Err(Error::InvalidTypeId(TypeId::Padding.byte()));
        }
        self.read_value(type_id)
    }

    fn read_value(&mut self, type_id: TypeId) -> Result<ElementView<'a>> {
        match type_id {
            TypeId::Boolean => match self.read_u8()? {
                0 => Ok(ElementView::Boolean(false)),
                1 => Ok(ElementView::Boolean(true)),
                other => Err(Error::InvalidBoolean(other)),
            },
            TypeId::Uint8 => Ok(ElementView::Uint8(self.read_u8()?)),
            TypeId::Int8 => Ok(ElementView::Int8(self.read_u8()? as i8)),
            TypeId::Uint16 => Ok(ElementView::Uint16(self.read_u16()?)),
            TypeId::Int16 => {
                let bytes = self.take(2)?;
                Ok(ElementView::Int16(i16::from_le_bytes([bytes[0], bytes[1]])))
            }
            TypeId::Uint32 => Ok(ElementView::Uint32(self.read_u32()?)),
            TypeId::Int32 => Ok(ElementView::Int32(self.read_i32()?)),
            TypeId::Uint64 => Ok(ElementView::Uint64(self.read_u64()?)),
            TypeId::Int64 => Ok(ElementView::Int64(self.read_i64()?)),
            TypeId::Float => Ok(ElementView::Float(f32::from_le_bytes(
                self.take(4)?.try_into().map_err(|_| Error::UnexpectedEof)?,
            ))),
            TypeId::Double => Ok(ElementView::Double(f64::from_le_bytes(
                self.take(8)?.try_into().map_err(|_| Error::UnexpectedEof)?,
            ))),
            TypeId::Object => self.read_object().map(ElementView::Object),
            TypeId::List => self.read_list().and_then(ElementView::list),
            TypeId::Array => self.read_array().and_then(ElementView::array),
            TypeId::String => self.read_string().map(ElementView::String),
            TypeId::Bytes => self.read_bytes().map(ElementView::Bytes),
            TypeId::Optional => self.read_optional().and_then(ElementView::optional),
            TypeId::Padding | TypeId::Document => Err(Error::UnsupportedTypeId(type_id.byte())),
        }
    }

    fn read_object(&mut self) -> Result<ObjectView<'a>> {
        self.with_nesting(Self::read_object_inner)
    }

    fn read_object_inner(&mut self) -> Result<ObjectView<'a>> {
        let len = self.read_container_len()?;
        let mut keys = try_hashset_with_capacity(len, "object keys")?;
        let mut fields = try_vec_with_capacity(len, "object fields")?;
        for _ in 0..len {
            let key = self.read_string()?;
            if !keys.insert(key) {
                return duplicate_key_error(key);
            }
            let value = self.read_element()?;
            try_push_vec(&mut fields, FieldView { key, value }, "object fields")?;
        }
        Ok(ObjectView { fields })
    }

    fn read_list(&mut self) -> Result<ListView<'a>> {
        self.with_nesting(Self::read_list_inner)
    }

    fn read_list_inner(&mut self) -> Result<ListView<'a>> {
        let element_type = read_element_type(self)?;
        let len = self.read_container_len()?;
        let mut values = try_vec_with_capacity(len, "list values")?;
        for _ in 0..len {
            let value = self.read_typed_value(&element_type)?;
            try_push_vec(&mut values, value, "list values")?;
        }
        Ok(ListView {
            element_type,
            values,
        })
    }

    fn read_array(&mut self) -> Result<ArrayView<'a>> {
        self.with_nesting(Self::read_array_inner)
    }

    fn read_array_inner(&mut self) -> Result<ArrayView<'a>> {
        let element_type = read_element_type(self)?;
        let element_size = self.read_length()?;
        if element_size == 0 {
            return Err(Error::ZeroArrayElementSize);
        }
        let rank = self.read_u8()? as usize;
        let mut dimensions = try_vec_with_capacity(rank, "array dimensions")?;
        for _ in 0..rank {
            let dimension = self.read_length()?;
            try_push_vec(&mut dimensions, dimension, "array dimensions")?;
        }
        let count = product(&dimensions)?;
        ensure_decode_limit("container items", count, self.limits.max_container_items)?;
        let byte_len_u64 = count.checked_mul(element_size).ok_or(Error::SizeOverflow)?;
        ensure_decode_limit(
            "bytes value bytes",
            byte_len_u64,
            self.limits.max_bytes_value_bytes,
        )?;
        let byte_len = usize::try_from(byte_len_u64).map_err(|_| Error::LengthTooLarge)?;
        if let ElementType::Builtin(type_id) = element_type
            && let Some(size) = type_id.fixed_size()
            && size != element_size
        {
            return Err(Error::ArrayByteLengthMismatch);
        }
        let data = self.take(byte_len)?;
        Ok(ArrayView {
            element_type,
            element_size,
            dimensions,
            data,
        })
    }

    fn read_optional(&mut self) -> Result<OptionalView<'a>> {
        self.with_nesting(Self::read_optional_inner)
    }

    fn read_optional_inner(&mut self) -> Result<OptionalView<'a>> {
        let element_type = read_element_type(self)?;
        let present = self.read_u8()?;
        let value = match present {
            0 => None,
            1 => Some(try_box(
                self.read_typed_value(&element_type)?,
                "optional view inner value",
            )?),
            other => return Err(Error::InvalidBoolean(other)),
        };
        Ok(OptionalView {
            element_type,
            value,
        })
    }

    fn read_typed_value(&mut self, element_type: &ElementType) -> Result<ElementView<'a>> {
        match element_type {
            ElementType::Builtin(type_id) => self.read_value(*type_id),
            ElementType::Object => self.read_object().map(ElementView::Object),
            ElementType::List(inner) => {
                let list = self.read_list()?;
                if list.element_type != **inner {
                    return Err(Error::TypeMismatch);
                }
                ElementView::list(list)
            }
            ElementType::Array(inner) => {
                let array = self.read_array()?;
                if array.element_type != **inner {
                    return Err(Error::TypeMismatch);
                }
                ElementView::array(array)
            }
            ElementType::Optional(inner) => self.with_nesting(|reader| {
                let present = reader.read_u8()?;
                let value = match present {
                    0 => None,
                    1 => Some(try_box(
                        reader.read_typed_value(inner)?,
                        "optional typed view inner value",
                    )?),
                    other => return Err(Error::InvalidBoolean(other)),
                };
                ElementView::optional(OptionalView {
                    element_type: (**inner).clone(),
                    value,
                })
            }),
            ElementType::SchemaRef(_) | ElementType::Extension(_) => {
                Err(Error::UnsupportedTypeId(0x40))
            }
        }
    }

    fn remaining_bytes(&self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }
}

pub(crate) fn read_document_header(
    bytes: &[u8],
    options: DecodeOptions,
) -> Result<(Reader<'_>, Metadata)> {
    let actual = u64::try_from(bytes.len()).map_err(|_| Error::LengthTooLarge)?;
    ensure_decode_limit("document bytes", actual, options.limits.max_document_bytes)?;

    let mut reader = Reader::with_limits(bytes, LengthMode::U64, options.limits);
    if reader.take(4)? != MAGIC {
        return Err(Error::InvalidMagic);
    }
    let version_id = reader.read_u16()?;
    if let Some(expected) = options.expected_version_id
        && version_id != expected
    {
        return Err(Error::UnexpectedVersionId {
            expected,
            actual: version_id,
        });
    }
    let length_mode = LengthMode::from_code(reader.read_u8()?)?;
    reader.mode = length_mode;
    let alignment_hint = AlignmentHint::from_code(reader.read_u8()?)?;
    let document_length = reader.read_length()?;
    if document_length != actual {
        return Err(Error::DocumentLengthMismatch {
            expected: document_length,
            actual,
        });
    }

    Ok((
        reader,
        Metadata {
            version_id,
            length_mode,
            alignment_hint,
            document_length,
        },
    ))
}

fn read_element_cursor<'a>(reader: &mut Reader<'a>) -> Result<ElementCursor<'a>> {
    reader.skip_padding();
    let type_id = TypeId::from_byte(reader.read_u8()?)?;
    if type_id == TypeId::Padding {
        return Err(Error::InvalidTypeId(TypeId::Padding.byte()));
    }
    let value_offset = reader.pos;
    skip_value(reader, type_id)?;
    Ok(ElementCursor {
        type_id,
        value_offset,
        next_offset: reader.pos,
        bytes: reader.bytes,
        mode: reader.mode,
        limits: reader.limits,
    })
}

fn read_typed_value_cursor<'a>(
    reader: &mut Reader<'a>,
    element_type: &ElementType,
) -> Result<ElementCursor<'a>> {
    let type_id = cursor_type_id_for_element_type(element_type)?;
    let value_offset = reader.pos;
    skip_typed_value(reader, element_type)?;
    Ok(ElementCursor {
        type_id,
        value_offset,
        next_offset: reader.pos,
        bytes: reader.bytes,
        mode: reader.mode,
        limits: reader.limits,
    })
}

fn cursor_type_id_for_element_type(element_type: &ElementType) -> Result<TypeId> {
    match element_type {
        ElementType::Builtin(type_id) => Ok(*type_id),
        ElementType::Object => Ok(TypeId::Object),
        ElementType::List(_) => Ok(TypeId::List),
        ElementType::Array(_) => Ok(TypeId::Array),
        ElementType::Optional(_) => Ok(TypeId::Optional),
        ElementType::SchemaRef(_) | ElementType::Extension(_) => {
            Err(Error::UnsupportedTypeId(0x40))
        }
    }
}

fn skip_value(reader: &mut Reader<'_>, type_id: TypeId) -> Result<()> {
    match type_id {
        TypeId::Boolean => {
            match reader.read_u8()? {
                0 | 1 => {}
                other => return Err(Error::InvalidBoolean(other)),
            }
            Ok(())
        }
        TypeId::Uint8 | TypeId::Int8 => {
            reader.take(1)?;
            Ok(())
        }
        TypeId::Uint16 | TypeId::Int16 => {
            reader.take(2)?;
            Ok(())
        }
        TypeId::Uint32 | TypeId::Int32 | TypeId::Float => {
            reader.take(4)?;
            Ok(())
        }
        TypeId::Uint64 | TypeId::Int64 | TypeId::Double => {
            reader.take(8)?;
            Ok(())
        }
        TypeId::Object => skip_object(reader),
        TypeId::List => skip_list(reader).map(|_| ()),
        TypeId::Array => skip_array(reader).map(|_| ()),
        TypeId::String => {
            reader.read_string()?;
            Ok(())
        }
        TypeId::Bytes => {
            reader.read_bytes()?;
            Ok(())
        }
        TypeId::Optional => reader.with_nesting(|reader| {
            let element_type = read_element_type(reader)?;
            let present = reader.read_u8()?;
            match present {
                0 => Ok(()),
                1 => skip_typed_value(reader, &element_type),
                other => Err(Error::InvalidBoolean(other)),
            }
        }),
        TypeId::Padding => Err(Error::InvalidTypeId(TypeId::Padding.byte())),
        TypeId::Document => Err(Error::UnsupportedTypeId(type_id.byte())),
    }
}

fn skip_typed_value(reader: &mut Reader<'_>, element_type: &ElementType) -> Result<()> {
    match element_type {
        ElementType::Builtin(type_id) => skip_value(reader, *type_id),
        ElementType::Object => skip_object(reader),
        ElementType::List(inner) => {
            let actual = skip_list(reader)?;
            if actual == **inner {
                Ok(())
            } else {
                Err(Error::TypeMismatch)
            }
        }
        ElementType::Array(inner) => {
            let actual = skip_array(reader)?;
            if actual == **inner {
                Ok(())
            } else {
                Err(Error::TypeMismatch)
            }
        }
        ElementType::Optional(inner) => reader.with_nesting(|reader| {
            let present = reader.read_u8()?;
            match present {
                0 => Ok(()),
                1 => skip_typed_value(reader, inner),
                other => Err(Error::InvalidBoolean(other)),
            }
        }),
        ElementType::SchemaRef(_) | ElementType::Extension(_) => {
            Err(Error::UnsupportedTypeId(0x40))
        }
    }
}

fn skip_object(reader: &mut Reader<'_>) -> Result<()> {
    reader.with_nesting(skip_object_inner)
}

fn skip_object_inner(reader: &mut Reader<'_>) -> Result<()> {
    let len = reader.read_container_len()?;
    let mut keys = try_hashset_with_capacity(len, "object keys")?;
    for _ in 0..len {
        let key = reader.read_string()?;
        if !keys.insert(key) {
            return duplicate_key_error(key);
        }
        read_element_cursor(reader)?;
    }
    Ok(())
}

fn skip_list(reader: &mut Reader<'_>) -> Result<ElementType> {
    reader.with_nesting(skip_list_inner)
}

fn skip_list_inner(reader: &mut Reader<'_>) -> Result<ElementType> {
    let element_type = read_element_type(reader)?;
    let len = reader.read_container_len()?;
    for _ in 0..len {
        skip_typed_value(reader, &element_type)?;
    }
    Ok(element_type)
}

fn skip_array(reader: &mut Reader<'_>) -> Result<ElementType> {
    reader.with_nesting(skip_array_inner)
}

fn skip_array_inner(reader: &mut Reader<'_>) -> Result<ElementType> {
    let element_type = read_element_type(reader)?;
    let element_size = reader.read_length()?;
    if element_size == 0 {
        return Err(Error::ZeroArrayElementSize);
    }
    let rank = reader.read_u8()? as usize;
    let mut count = 1u64;
    for _ in 0..rank {
        count = count
            .checked_mul(reader.read_length()?)
            .ok_or(Error::SizeOverflow)?;
    }
    ensure_decode_limit("container items", count, reader.limits.max_container_items)?;
    let byte_len_u64 = count.checked_mul(element_size).ok_or(Error::SizeOverflow)?;
    ensure_decode_limit(
        "bytes value bytes",
        byte_len_u64,
        reader.limits.max_bytes_value_bytes,
    )?;
    let byte_len = usize::try_from(byte_len_u64).map_err(|_| Error::LengthTooLarge)?;
    if let ElementType::Builtin(type_id) = &element_type
        && let Some(size) = type_id.fixed_size()
        && size != element_size
    {
        return Err(Error::ArrayByteLengthMismatch);
    }
    reader.take(byte_len)?;
    Ok(element_type)
}

fn encode_typed_value(
    out: &mut Vec<u8>,
    mode: LengthMode,
    element_type: &ElementType,
    value: &Value,
) -> Result<()> {
    match element_type {
        ElementType::Builtin(type_id) if value.type_id() == *type_id => {
            value.encode_value(out, mode)
        }
        ElementType::Object if matches!(value, Value::Object(_)) => value.encode_value(out, mode),
        ElementType::List(inner) => {
            let Value::List(list) = value else {
                return Err(Error::TypeMismatch);
            };
            if list.element_type != **inner {
                return Err(Error::TypeMismatch);
            }
            value.encode_value(out, mode)
        }
        ElementType::Array(inner) => {
            let Value::Array(array) = value else {
                return Err(Error::TypeMismatch);
            };
            if array.element_type != **inner {
                return Err(Error::TypeMismatch);
            }
            value.encode_value(out, mode)
        }
        ElementType::Optional(_) if matches!(value, Value::Optional(_)) => {
            if let Value::Optional(optional) = value {
                match (&optional.element_type, element_type) {
                    (actual, ElementType::Optional(expected)) if actual == expected.as_ref() => {
                        match &optional.value {
                            Some(value) => {
                                try_push_vec(out, 1, "optional presence")?;
                                encode_typed_value(out, mode, expected, value)
                            }
                            None => {
                                try_push_vec(out, 0, "optional presence")?;
                                Ok(())
                            }
                        }
                    }
                    _ => Err(Error::TypeMismatch),
                }
            } else {
                Err(Error::TypeMismatch)
            }
        }
        _ => Err(Error::TypeMismatch),
    }
}

fn read_schema(reader: &mut Reader<'_>) -> Result<Schema> {
    let type_count =
        reader.read_limited_len_usize("schema types", reader.limits.max_schema_types)?;
    reader.ensure_count_fits_remaining(
        "schema types fitting document",
        type_count,
        2 + reader.mode.width() * 3,
    )?;
    let mut types = try_vec_with_capacity(type_count, "schema types")?;
    let mut ids = try_hashset_with_capacity(type_count, "schema type ids")?;
    for _ in 0..type_count {
        let id = reader.read_u16()?;
        if !ids.insert(id) {
            return Err(Error::DuplicateSchemaTypeId(id));
        }
        let name = try_string_from_str(reader.read_string()?, "schema type name")?;
        let payload_size = reader.read_length()?;
        let field_count = reader.read_limited_len_usize(
            "schema fields per type",
            reader.limits.max_schema_fields_per_type,
        )?;
        reader.ensure_count_fits_remaining(
            "schema fields fitting document",
            field_count,
            reader.mode.width() + 1,
        )?;
        let mut fields = try_vec_with_capacity(field_count, "schema fields")?;
        for _ in 0..field_count {
            let name = try_string_from_str(reader.read_string()?, "schema field name")?;
            let element_type = read_element_type(reader)?;
            try_push_vec(
                &mut fields,
                SchemaField { name, element_type },
                "schema fields",
            )?;
        }
        try_push_vec(
            &mut types,
            SchemaType {
                id,
                name,
                payload_size,
                fields,
            },
            "schema types",
        )?;
    }
    for schema_type in &types {
        for field in &schema_type.fields {
            validate_schema_element_type_references(&field.element_type, &ids)?;
        }
    }
    Ok(Schema { types })
}

fn validate_schema_element_type_references(
    element_type: &ElementType,
    schema_type_ids: &HashSet<u16>,
) -> Result<()> {
    match element_type {
        ElementType::SchemaRef(id) if !schema_type_ids.contains(id) => Err(Error::TypeMismatch),
        ElementType::List(inner) | ElementType::Array(inner) | ElementType::Optional(inner) => {
            validate_schema_element_type_references(inner, schema_type_ids)
        }
        _ => Ok(()),
    }
}

fn encode_element_type(
    out: &mut Vec<u8>,
    mode: LengthMode,
    element_type: &ElementType,
) -> Result<()> {
    match element_type {
        ElementType::Builtin(type_id) => try_push_vec(out, type_id.byte(), "element type")?,
        ElementType::SchemaRef(id) => {
            try_push_vec(out, 0x40, "schema element type")?;
            write_u16(out, *id)?;
        }
        ElementType::List(inner) => {
            try_push_vec(out, 0x41, "list element type")?;
            encode_element_type(out, mode, inner)?;
        }
        ElementType::Array(inner) => {
            try_push_vec(out, 0x42, "array element type")?;
            encode_element_type(out, mode, inner)?;
        }
        ElementType::Object => try_push_vec(out, 0x43, "object element type")?,
        ElementType::Optional(inner) => {
            try_push_vec(out, 0x44, "optional element type")?;
            encode_element_type(out, mode, inner)?;
        }
        ElementType::Extension(ext) if ext.id >= 0x80 => {
            try_push_vec(out, ext.id, "extension element type")?;
            write_bytes(out, mode, &ext.data)?;
        }
        ElementType::Extension(ext) => return Err(Error::InvalidTypeId(ext.id)),
    }
    Ok(())
}

fn read_element_type(reader: &mut Reader<'_>) -> Result<ElementType> {
    read_element_type_at_depth(reader, 1)
}

fn read_element_type_at_depth(
    reader: &mut Reader<'_>,
    element_type_depth: usize,
) -> Result<ElementType> {
    let total_depth = reader
        .nesting_depth
        .checked_add(element_type_depth)
        .ok_or(Error::SizeOverflow)?;
    ensure_decode_limit(
        "nesting depth",
        u64::try_from(total_depth).unwrap_or(u64::MAX),
        u64::try_from(reader.limits.max_nesting_depth).unwrap_or(u64::MAX),
    )?;
    let byte = reader.read_u8()?;
    let next_depth = || element_type_depth.checked_add(1).ok_or(Error::SizeOverflow);
    match byte {
        0x01..=0x0B | 0x24 | 0x25 => Ok(ElementType::Builtin(TypeId::from_byte(byte)?)),
        0x40 => Ok(ElementType::SchemaRef(reader.read_u16()?)),
        0x41 => Ok(ElementType::List(try_box(
            read_element_type_at_depth(reader, next_depth()?)?,
            "list element type",
        )?)),
        0x42 => Ok(ElementType::Array(try_box(
            read_element_type_at_depth(reader, next_depth()?)?,
            "array element type",
        )?)),
        0x43 => Ok(ElementType::Object),
        0x44 => Ok(ElementType::Optional(try_box(
            read_element_type_at_depth(reader, next_depth()?)?,
            "optional element type",
        )?)),
        0x80..=0xFF => {
            let raw = reader.read_bytes()?;
            let mut data = try_vec_with_capacity(raw.len(), "extension element type data")?;
            try_extend_from_slice(&mut data, raw, "extension element type data")?;
            ElementType::try_extension(byte, data)
        }
        other => Err(Error::InvalidTypeId(other)),
    }
}

fn write_string(out: &mut Vec<u8>, mode: LengthMode, value: &str) -> Result<()> {
    write_bytes(out, mode, value.as_bytes())
}

fn write_bytes(out: &mut Vec<u8>, mode: LengthMode, value: &[u8]) -> Result<()> {
    write_length(out, mode, value.len() as u64)?;
    try_extend_from_slice(out, value, "bytes payload")?;
    Ok(())
}

fn write_u16(out: &mut Vec<u8>, value: u16) -> Result<()> {
    try_extend_from_slice(out, &value.to_le_bytes(), "u16")
}

fn write_length(out: &mut Vec<u8>, mode: LengthMode, value: u64) -> Result<()> {
    mode.validate(value)?;
    match mode {
        LengthMode::U16 => try_extend_from_slice(out, &(value as u16).to_le_bytes(), "length")?,
        LengthMode::U32 => try_extend_from_slice(out, &(value as u32).to_le_bytes(), "length")?,
        LengthMode::U64 => try_extend_from_slice(out, &value.to_le_bytes(), "length")?,
    }
    Ok(())
}

fn patch_length(out: &mut [u8], mode: LengthMode, value: u64) -> Result<()> {
    mode.validate(value)?;
    match mode {
        LengthMode::U16 => out.copy_from_slice(&(value as u16).to_le_bytes()),
        LengthMode::U32 => out.copy_from_slice(&(value as u32).to_le_bytes()),
        LengthMode::U64 => out.copy_from_slice(&value.to_le_bytes()),
    }
    Ok(())
}

fn product(values: &[u64]) -> Result<u64> {
    values.iter().try_fold(1u64, |acc, value| {
        acc.checked_mul(*value).ok_or(Error::SizeOverflow)
    })
}

fn validate_array_data(
    element_type: &ElementType,
    element_size: u64,
    dimensions: &[u64],
    data_len: usize,
) -> Result<()> {
    if element_size == 0 {
        return Err(Error::ZeroArrayElementSize);
    }
    let count = product(dimensions)?;
    let expected = count.checked_mul(element_size).ok_or(Error::SizeOverflow)?;
    let actual = u64::try_from(data_len).map_err(|_| Error::LengthTooLarge)?;
    if expected != actual {
        return Err(Error::ArrayByteLengthMismatch);
    }
    if let ElementType::Builtin(type_id) = element_type
        && let Some(size) = type_id.fixed_size()
        && size != element_size
    {
        return Err(Error::ArrayByteLengthMismatch);
    }
    Ok(())
}

fn ensure_fixed_layout_supported<T: FixedLayoutRecord>() -> Result<()> {
    if cfg!(target_endian = "big") {
        return Err(Error::UnsupportedFixedLayoutEndian);
    }
    if std::mem::size_of::<T>() == 0 {
        return Err(Error::ZeroArrayElementSize);
    }
    Ok(())
}

fn validate_schema_payload_data(
    schema: &Schema,
    root_schema_type_id: u16,
    root_count: u64,
    data_len: usize,
) -> Result<()> {
    let Some(schema_type) = schema
        .types
        .iter()
        .find(|schema_type| schema_type.id == root_schema_type_id)
    else {
        return Err(Error::TypeMismatch);
    };
    if schema_type.payload_size == 0 {
        return Ok(());
    }
    let expected = root_count
        .checked_mul(schema_type.payload_size)
        .ok_or(Error::SizeOverflow)?;
    let actual = u64::try_from(data_len).map_err(|_| Error::LengthTooLarge)?;
    if expected != actual {
        return Err(Error::ArrayByteLengthMismatch);
    }
    Ok(())
}

fn validate_unique_keys<'a>(keys: impl Iterator<Item = &'a str>) -> Result<()> {
    let mut seen = HashSet::new();
    for key in keys {
        let requested_items = seen.len().checked_add(1).ok_or(Error::SizeOverflow)?;
        let requested_bytes = requested_items
            .checked_mul(std::mem::size_of::<&str>().max(1))
            .ok_or(Error::SizeOverflow)?;
        seen.try_reserve(1).map_err(|_| Error::ResourceExhausted {
            context: "object keys",
            requested_bytes,
        })?;
        if !seen.insert(key) {
            return duplicate_key_error(key);
        }
    }
    Ok(())
}

fn duplicate_key_error<T>(key: &str) -> Result<T> {
    Err(Error::DuplicateKey(try_string_from_str(
        key,
        "duplicate object key",
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct PackedVec3 {
        x: f32,
        y: f32,
        z: f32,
    }

    unsafe impl FixedLayoutRecord for PackedVec3 {
        const SCHEMA_TYPE_ID: u16 = 7;
        const SCHEMA_TYPE_NAME: &'static str = "vector3";

        fn schema_fields() -> Vec<SchemaField> {
            vec![
                SchemaField {
                    name: "x".to_owned(),
                    element_type: ElementType::Builtin(TypeId::Float),
                },
                SchemaField {
                    name: "y".to_owned(),
                    element_type: ElementType::Builtin(TypeId::Float),
                },
                SchemaField {
                    name: "z".to_owned(),
                    element_type: ElementType::Builtin(TypeId::Float),
                },
            ]
        }
    }

    #[test]
    fn dynamic_document_round_trips_ordered_object() {
        let document = Document::dynamic(
            1,
            LengthMode::U32,
            Value::Object(Object::new(vec![
                Field::new("z", Value::Int32(-42)),
                Field::new("a", Value::String("hello".to_owned())),
                Field::new(
                    "present",
                    Value::optional(OptionalValue::some(
                        ElementType::Builtin(TypeId::Uint64),
                        Value::Uint64(99),
                    )),
                ),
            ])),
        );

        let bytes = document.to_bytes().unwrap();
        assert_eq!(&bytes[..4], MAGIC);

        let view = DocumentView::parse(&bytes).unwrap();
        assert_eq!(view.metadata.version_id, 1);
        assert_eq!(view.metadata.length_mode, LengthMode::U32);

        let PayloadView::Dynamic(ElementView::Object(object)) = view.payload else {
            panic!("expected object payload");
        };
        assert_eq!(object.fields[0].key, "z");
        assert_eq!(object.fields[1].key, "a");
        assert_eq!(object.get("z"), Some(&ElementView::Int32(-42)));
    }

    #[test]
    fn text_format_from_view_matches_owned_document() {
        let document = Document::dynamic(
            1,
            LengthMode::U32,
            Value::Object(Object::new(vec![
                Field::new("node", Value::String("aarch64".to_owned())),
                Field::new("port", Value::Uint16(21353)),
                Field::new(
                    "capabilities",
                    Value::list(List::new(
                        ElementType::Builtin(TypeId::String),
                        vec![
                            Value::String("rollout".to_owned()),
                            Value::String("kv".to_owned()),
                        ],
                    )),
                ),
            ])),
        );

        let bytes = document.to_bytes().unwrap();
        let view = DocumentView::parse(&bytes).unwrap();

        assert_eq!(
            crate::text::document_view_to_text(&view),
            crate::text::document_to_text(&document)
        );
    }

    #[test]
    fn bytes_and_array_views_borrow_payload() {
        let tensor_bytes: Vec<u8> = [1.0f32, 2.0, 3.0, 4.0]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect();
        let array = Array::new(
            ElementType::Builtin(TypeId::Float),
            4,
            vec![2, 2],
            tensor_bytes.clone(),
        )
        .unwrap();
        let document = Document::dynamic(
            1,
            LengthMode::U32,
            Value::Object(Object::new(vec![
                Field::new("file", Value::Bytes(vec![1, 2, 3, 4, 5])),
                Field::new("tensor", Value::array(array)),
            ])),
        );

        let bytes = document.to_bytes().unwrap();
        let view = DocumentView::parse(&bytes).unwrap();
        let PayloadView::Dynamic(ElementView::Object(object)) = view.payload else {
            panic!("expected object payload");
        };

        let Some(ElementView::Bytes(file)) = object.get("file") else {
            panic!("expected bytes");
        };
        assert_eq!(*file, &[1, 2, 3, 4, 5]);
        assert!(file.as_ptr() >= bytes.as_ptr());

        let Some(ElementView::Array(tensor)) = object.get("tensor") else {
            panic!("expected array");
        };
        assert_eq!(tensor.element_size, 4);
        assert_eq!(tensor.dimensions, &[2, 2]);
        assert_eq!(tensor.data, tensor_bytes.as_slice());
        assert!(tensor.data.as_ptr() >= bytes.as_ptr());
    }

    #[test]
    fn duplicate_object_keys_are_rejected() {
        let document = Document::dynamic(
            1,
            LengthMode::U32,
            Value::Object(Object::new(vec![
                Field::new("same", Value::Uint8(1)),
                Field::new("same", Value::Uint8(2)),
            ])),
        );

        assert!(matches!(
            document.to_bytes(),
            Err(Error::DuplicateKey(key)) if key == "same"
        ));
    }

    #[test]
    fn decode_options_enforce_expected_version_for_views_and_cursors() {
        let bytes = Document::dynamic(7, LengthMode::U32, Value::Uint8(1))
            .to_bytes()
            .unwrap();
        let options = DecodeOptions::default().with_expected_version_id(8);

        for error in [
            DocumentView::parse_with_options(&bytes, options).unwrap_err(),
            DocumentCursor::parse_with_options(&bytes, options).unwrap_err(),
        ] {
            assert_eq!(
                error,
                Error::UnexpectedVersionId {
                    expected: 8,
                    actual: 7,
                }
            );
        }

        DocumentView::parse_with_options(
            &bytes,
            DecodeOptions::default().with_expected_version_id(7),
        )
        .unwrap();
    }

    #[test]
    fn decode_limits_bound_document_and_payload_sizes() {
        let string_bytes = Document::dynamic(1, LengthMode::U32, Value::String("four".to_owned()))
            .to_bytes()
            .unwrap();
        let string_options = DecodeOptions::new(DecodeLimits {
            max_string_bytes: 3,
            ..DecodeLimits::default()
        });
        assert!(matches!(
            DocumentView::parse_with_options(&string_bytes, string_options),
            Err(Error::DecodeLimitExceeded {
                limit: "string bytes",
                max: 3,
                actual: 4,
            })
        ));

        let payload_bytes = Document::dynamic(1, LengthMode::U32, Value::Bytes(vec![1, 2, 3, 4]))
            .to_bytes()
            .unwrap();
        let payload_options = DecodeOptions::new(DecodeLimits {
            max_bytes_value_bytes: 3,
            ..DecodeLimits::default()
        });
        assert!(matches!(
            DocumentCursor::parse_with_options(&payload_bytes, payload_options),
            Err(Error::DecodeLimitExceeded {
                limit: "bytes value bytes",
                max: 3,
                actual: 4,
            })
        ));

        let document_options = DecodeOptions::new(DecodeLimits {
            max_document_bytes: payload_bytes.len() as u64 - 1,
            ..DecodeLimits::default()
        });
        assert!(matches!(
            DocumentView::parse_with_options(&payload_bytes, document_options),
            Err(Error::DecodeLimitExceeded {
                limit: "document bytes",
                ..
            })
        ));
    }

    #[test]
    fn decode_limits_bound_container_items_and_nesting_depth() {
        let list = Document::dynamic(
            1,
            LengthMode::U32,
            Value::list(List::new(
                ElementType::Builtin(TypeId::Uint8),
                vec![
                    Value::Uint8(1),
                    Value::Uint8(2),
                    Value::Uint8(3),
                    Value::Uint8(4),
                ],
            )),
        )
        .to_bytes()
        .unwrap();
        let list_options = DecodeOptions::new(DecodeLimits {
            max_container_items: 3,
            ..DecodeLimits::default()
        });
        assert!(matches!(
            DocumentView::parse_with_options(&list, list_options),
            Err(Error::DecodeLimitExceeded {
                limit: "container items",
                max: 3,
                actual: 4,
            })
        ));

        let mut nested = Value::Uint8(1);
        for _ in 0..4 {
            nested = Value::Object(Object::new(vec![Field::new("next", nested)]));
        }
        let nested = Document::dynamic(1, LengthMode::U32, nested)
            .to_bytes()
            .unwrap();
        let nesting_options = DecodeOptions::new(DecodeLimits {
            max_nesting_depth: 3,
            ..DecodeLimits::default()
        });
        assert!(matches!(
            DocumentCursor::parse_with_options(&nested, nesting_options),
            Err(Error::DecodeLimitExceeded {
                limit: "nesting depth",
                max: 3,
                actual: 4,
            })
        ));
    }

    #[test]
    fn decode_limits_bound_schema_shape() {
        let schema = Schema {
            types: vec![SchemaType {
                id: 7,
                name: "byte".to_owned(),
                payload_size: 1,
                fields: vec![SchemaField {
                    name: "value".to_owned(),
                    element_type: ElementType::Builtin(TypeId::Uint8),
                }],
            }],
        };
        let bytes = BorrowedDocumentBuilder::schema_payload(1, LengthMode::U32, schema, 7, 1, &[9])
            .unwrap()
            .to_vec()
            .unwrap();
        let options = DecodeOptions::new(DecodeLimits {
            max_schema_fields_per_type: 0,
            ..DecodeLimits::default()
        });

        assert!(matches!(
            DocumentView::parse_with_options(&bytes, options),
            Err(Error::DecodeLimitExceeded {
                limit: "schema fields per type",
                max: 0,
                actual: 1,
            })
        ));
    }

    #[test]
    fn dynamic_object_with_impossible_field_count_fails_before_allocation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.push(LengthMode::U64.code());
        bytes.push(AlignmentHint::NotAligned.code());
        bytes.extend_from_slice(&33_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.push(TypeId::Object.byte());
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());

        let error = DocumentView::parse(&bytes).expect_err("impossible field count");
        assert!(
            matches!(
                error,
                Error::LengthTooLarge | Error::SizeOverflow | Error::ResourceExhausted { .. }
            ),
            "unexpected error for impossible object length: {error:?}"
        );
    }

    #[test]
    fn impossible_schema_count_fails_before_allocation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.push(LengthMode::U64.code());
        bytes.push(AlignmentHint::NotAligned.code());
        bytes.extend_from_slice(&24_u64.to_le_bytes());
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());

        let options = DecodeOptions::new(DecodeLimits {
            max_schema_types: u64::MAX,
            ..DecodeLimits::default()
        });
        assert!(matches!(
            DocumentView::parse_with_options(&bytes, options),
            Err(Error::DecodeLimitExceeded {
                limit: "schema types fitting document",
                max: 0,
                actual: u64::MAX,
            })
        ));
    }

    #[test]
    fn document_cursor_rejects_invalid_boolean_payloads() {
        let mut bytes = Document::dynamic(1, LengthMode::U32, Value::Boolean(false))
            .to_bytes()
            .unwrap();
        *bytes.last_mut().expect("boolean byte") = 2;

        assert!(matches!(
            DocumentCursor::parse(&bytes),
            Err(Error::InvalidBoolean(2))
        ));
    }

    #[test]
    fn schema_payload_is_exposed_as_raw_bytes() {
        let document = Document {
            metadata: Metadata::new(1, LengthMode::U32),
            schema: Schema {
                types: vec![SchemaType {
                    id: 7,
                    name: "vector3".to_owned(),
                    payload_size: 12,
                    fields: vec![
                        SchemaField {
                            name: "x".to_owned(),
                            element_type: ElementType::Builtin(TypeId::Float),
                        },
                        SchemaField {
                            name: "y".to_owned(),
                            element_type: ElementType::Builtin(TypeId::Float),
                        },
                        SchemaField {
                            name: "z".to_owned(),
                            element_type: ElementType::Builtin(TypeId::Float),
                        },
                    ],
                }],
            },
            payload: Payload::Schema {
                root_schema_type_id: 7,
                root_count: 1,
                data: [1.0f32, 2.0, 3.0]
                    .into_iter()
                    .flat_map(f32::to_le_bytes)
                    .collect(),
            },
        };

        let bytes = document.to_bytes().unwrap();
        let view = DocumentView::parse(&bytes).unwrap();
        let PayloadView::Schema {
            root_schema_type_id,
            root_count,
            data,
        } = view.payload
        else {
            panic!("expected schema payload");
        };
        assert_eq!(root_schema_type_id, 7);
        assert_eq!(root_count, 1);
        assert_eq!(data.len(), 12);
    }

    #[test]
    fn borrowed_builder_writes_dynamic_bytes_like_owned_document() {
        let payload: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let borrowed = BorrowedDocumentBuilder::dynamic_bytes(1, LengthMode::U32, &payload)
            .to_vec()
            .unwrap();
        let owned = Document::dynamic(1, LengthMode::U32, Value::Bytes(payload.clone()))
            .to_bytes()
            .unwrap();

        assert_eq!(borrowed, owned);
        let view = DocumentView::parse(&borrowed).unwrap();
        let PayloadView::Dynamic(ElementView::Bytes(data)) = view.payload else {
            panic!("expected bytes payload");
        };
        assert_eq!(data, payload.as_slice());
        assert!(slice_is_borrowed_from(data, &borrowed));
    }

    #[test]
    fn borrowed_builder_writes_dynamic_array_like_owned_document() {
        let tensor_bytes: Vec<u8> = [1.0f32, 2.0, 3.0, 4.0]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect();
        let borrowed_array = BorrowedArray::new(
            ElementType::Builtin(TypeId::Float),
            4,
            vec![2, 2],
            &tensor_bytes,
        )
        .unwrap();
        let borrowed = BorrowedDocumentBuilder::dynamic_array(1, LengthMode::U32, borrowed_array)
            .unwrap()
            .to_vec()
            .unwrap();
        let owned = Document::dynamic(
            1,
            LengthMode::U32,
            Value::array(
                Array::new(
                    ElementType::Builtin(TypeId::Float),
                    4,
                    vec![2, 2],
                    tensor_bytes.clone(),
                )
                .unwrap(),
            ),
        )
        .to_bytes()
        .unwrap();

        assert_eq!(borrowed, owned);
        let view = DocumentView::parse(&borrowed).unwrap();
        let PayloadView::Dynamic(ElementView::Array(array)) = view.payload else {
            panic!("expected array payload");
        };
        assert_eq!(array.element_size, 4);
        assert_eq!(array.dimensions, &[2, 2]);
        assert_eq!(array.data, tensor_bytes.as_slice());
        assert!(slice_is_borrowed_from(array.data, &borrowed));
    }

    #[test]
    fn borrowed_builder_writes_raw_schema_payload() {
        let schema = Schema {
            types: vec![SchemaType {
                id: 7,
                name: "vector3".to_owned(),
                payload_size: 12,
                fields: vec![
                    SchemaField {
                        name: "x".to_owned(),
                        element_type: ElementType::Builtin(TypeId::Float),
                    },
                    SchemaField {
                        name: "y".to_owned(),
                        element_type: ElementType::Builtin(TypeId::Float),
                    },
                    SchemaField {
                        name: "z".to_owned(),
                        element_type: ElementType::Builtin(TypeId::Float),
                    },
                ],
            }],
        };
        let payload: Vec<u8> = [1.0f32, 2.0, 3.0]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect();

        let bytes =
            BorrowedDocumentBuilder::schema_payload(1, LengthMode::U32, schema, 7, 1, &payload)
                .unwrap()
                .to_vec()
                .unwrap();

        let view = DocumentView::parse(&bytes).unwrap();
        let PayloadView::Schema {
            root_schema_type_id,
            root_count,
            data,
        } = view.payload
        else {
            panic!("expected schema payload");
        };
        assert_eq!(root_schema_type_id, 7);
        assert_eq!(root_count, 1);
        assert_eq!(data, payload.as_slice());
        assert!(slice_is_borrowed_from(data, &bytes));
    }

    #[test]
    fn fixed_layout_records_borrow_original_slice() {
        assert_eq!(std::mem::size_of::<PackedVec3>(), 12);
        let records = [
            PackedVec3 {
                x: 1.0,
                y: 2.0,
                z: 3.0,
            },
            PackedVec3 {
                x: 4.0,
                y: 5.0,
                z: 6.0,
            },
        ];

        let record_bytes = fixed_layout_record_bytes(&records).unwrap();
        assert_eq!(record_bytes.as_ptr(), records.as_ptr().cast::<u8>());
        let typed_records = fixed_layout_records_from_bytes::<PackedVec3>(record_bytes).unwrap();
        assert_eq!(typed_records, &records);

        let view = DocumentView {
            metadata: Metadata::new(1, LengthMode::U32),
            schema: PackedVec3::schema(),
            payload: PayloadView::Schema {
                root_schema_type_id: PackedVec3::SCHEMA_TYPE_ID,
                root_count: records.len() as u64,
                data: record_bytes,
            },
            bytes: record_bytes,
        };
        assert_eq!(view.fixed_layout_records::<PackedVec3>().unwrap(), &records);
    }

    #[test]
    fn borrowed_builder_writes_fixed_layout_records_as_schema_payload() {
        let records = [
            PackedVec3 {
                x: 1.0,
                y: 2.0,
                z: 3.0,
            },
            PackedVec3 {
                x: 4.0,
                y: 5.0,
                z: 6.0,
            },
        ];
        let builder =
            BorrowedDocumentBuilder::fixed_layout_records(1, LengthMode::U32, &records).unwrap();
        let parts = builder.to_parts().unwrap();
        assert_eq!(parts.payload.as_ptr(), records.as_ptr().cast::<u8>());

        let bytes = parts.to_vec().unwrap();
        let view = DocumentView::parse(&bytes).unwrap();
        let PayloadView::Schema {
            root_schema_type_id,
            root_count,
            data,
        } = view.payload
        else {
            panic!("expected schema payload");
        };
        assert_eq!(root_schema_type_id, PackedVec3::SCHEMA_TYPE_ID);
        assert_eq!(root_count, records.len() as u64);
        assert_eq!(
            data.len(),
            records.len() * std::mem::size_of::<PackedVec3>()
        );
        assert_eq!(data, fixed_layout_record_bytes(&records).unwrap());
    }

    #[test]
    fn document_cursor_scans_large_object_without_view_vectors() {
        let fields = (0..1000u32)
            .map(|value| Field::new(format!("k{value:04}"), Value::Uint32(value)))
            .collect();
        let document = Document::dynamic(1, LengthMode::U32, Value::Object(Object::new(fields)));
        let bytes = document.to_bytes().unwrap();

        let cursor = DocumentCursor::parse(&bytes).unwrap();
        let PayloadCursor::Dynamic(root) = cursor.payload else {
            panic!("expected dynamic payload");
        };
        let object = root.as_object().unwrap();
        assert_eq!(object.field_count(), 1000);
        assert_eq!(
            object
                .get("k0420")
                .unwrap()
                .expect("field")
                .as_u32()
                .unwrap(),
            420
        );

        let mut count = 0usize;
        let mut sum = 0u64;
        for field in object.iter() {
            let field = field.unwrap();
            assert_eq!(field.value.type_id(), TypeId::Uint32);
            count += 1;
            sum += field.value.as_u32().unwrap() as u64;
        }
        assert_eq!(count, 1000);
        assert_eq!(sum, (0..1000u64).sum());
    }

    #[test]
    fn document_cursor_scans_large_list_without_view_vectors() {
        let values = (0..1000u32).map(Value::Uint32).collect();
        let document = Document::dynamic(
            1,
            LengthMode::U32,
            Value::list(List::new(ElementType::Builtin(TypeId::Uint32), values)),
        );
        let bytes = document.to_bytes().unwrap();

        let cursor = DocumentCursor::parse(&bytes).unwrap();
        let PayloadCursor::Dynamic(root) = cursor.payload else {
            panic!("expected dynamic payload");
        };
        let list = root.as_list().unwrap();
        assert_eq!(list.len(), 1000);
        assert_eq!(list.element_type(), &ElementType::Builtin(TypeId::Uint32));

        let sum = list
            .iter()
            .map(|value| value.unwrap().as_u32().unwrap() as u64)
            .sum::<u64>();
        assert_eq!(sum, (0..1000u64).sum());
    }

    #[test]
    fn document_cursor_reads_requested_borrowed_values() {
        let tensor_bytes: Vec<u8> = [1.0f32, 2.0, 3.0, 4.0]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect();
        let document = Document::dynamic(
            1,
            LengthMode::U32,
            Value::Object(Object::new(vec![
                Field::new("name", Value::String("sensor-a".to_owned())),
                Field::new("blob", Value::Bytes(vec![1, 2, 3, 4])),
                Field::new(
                    "tensor",
                    Value::array(
                        Array::new(
                            ElementType::Builtin(TypeId::Float),
                            4,
                            vec![2, 2],
                            tensor_bytes.clone(),
                        )
                        .unwrap(),
                    ),
                ),
            ])),
        );
        let bytes = document.to_bytes().unwrap();

        let cursor = DocumentCursor::parse(&bytes).unwrap();
        let PayloadCursor::Dynamic(root) = cursor.payload else {
            panic!("expected dynamic payload");
        };
        let object = root.as_object().unwrap();
        assert_eq!(
            object.get("name").unwrap().expect("name").as_str().unwrap(),
            "sensor-a"
        );
        assert_eq!(
            object
                .get("blob")
                .unwrap()
                .expect("blob")
                .as_bytes()
                .unwrap(),
            &[1, 2, 3, 4]
        );
        let array = object
            .get("tensor")
            .unwrap()
            .expect("tensor")
            .as_array()
            .unwrap();
        assert_eq!(array.dimensions, &[2, 2]);
        assert_eq!(array.data, tensor_bytes.as_slice());
        assert!(slice_is_borrowed_from(array.data, &bytes));
    }

    // Golden byte-exact conformance tests. Round-trip tests only prove the encoder
    // and decoder agree with each other; these pin the exact on-wire bytes to
    // docs/sinomo-object-format.md so a systematic deviation (endianness, type-id
    // values, field order, length width) is caught.

    #[test]
    fn golden_object_document_matches_spec_byte_layout() {
        let document = Document::dynamic(
            1,
            LengthMode::U32,
            Value::Object(Object::new(vec![
                Field::new("n", Value::Uint8(5)),
                Field::new("s", Value::String("hi".to_owned())),
            ])),
        );
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x53, 0x4F, 0x42, 0x4A,       // magic "SOBJ"
            0x01, 0x00,                   // version_id = 1 (u16 LE)
            0x04,                         // length_mode = 4 (U32)
            0x00,                         // alignment_hint = 0 (not aligned)
            0x28, 0x00, 0x00, 0x00,       // document_length = 40 (u32 LE)
            0x00, 0x00, 0x00, 0x00,       // schema type_count = 0
            0x21,                         // payload element type id = object
            0x02, 0x00, 0x00, 0x00,       // object length = 2 pairs
            0x01, 0x00, 0x00, 0x00, 0x6E, // key "n"
            0x02, 0x05,                   // value element: uint8(5)
            0x01, 0x00, 0x00, 0x00, 0x73, // key "s"
            0x24,                         // value element type id = string
            0x02, 0x00, 0x00, 0x00, 0x68, 0x69, // string "hi"
        ];
        assert_eq!(document.to_bytes().unwrap(), expected);
    }

    #[test]
    fn golden_int32_is_little_endian_twos_complement() {
        let document = Document::dynamic(1, LengthMode::U32, Value::Int32(-2));
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x53, 0x4F, 0x42, 0x4A,
            0x01, 0x00,
            0x04,
            0x00,
            0x15, 0x00, 0x00, 0x00,   // document_length = 21
            0x00, 0x00, 0x00, 0x00,   // schema type_count = 0
            0x07,                     // int32 type id
            0xFE, 0xFF, 0xFF, 0xFF,   // -2, little-endian two's complement
        ];
        assert_eq!(document.to_bytes().unwrap(), expected);
    }

    #[test]
    fn golden_list_writes_element_type_once_then_raw_values() {
        let document = Document::dynamic(
            1,
            LengthMode::U32,
            Value::list(List::new(
                ElementType::Builtin(TypeId::Uint8),
                vec![Value::Uint8(1), Value::Uint8(2), Value::Uint8(3)],
            )),
        );
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x53, 0x4F, 0x42, 0x4A,
            0x01, 0x00,
            0x04,
            0x00,
            0x19, 0x00, 0x00, 0x00,   // document_length = 25
            0x00, 0x00, 0x00, 0x00,   // schema type_count = 0
            0x22,                     // list type id
            0x02,                     // element type = uint8 (written once)
            0x03, 0x00, 0x00, 0x00,   // list length = 3
            0x01, 0x02, 0x03,         // raw values, no per-element type id
        ];
        assert_eq!(document.to_bytes().unwrap(), expected);
    }

    #[test]
    fn golden_spec_bytes_decode_to_expected_object() {
        // Independently verify the decoder reads canonical spec bytes, not just its
        // own output (a round-trip could not catch a shared encoder/decoder bug).
        #[rustfmt::skip]
        let bytes: &[u8] = &[
            0x53, 0x4F, 0x42, 0x4A, 0x01, 0x00, 0x04, 0x00, 0x28, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x21, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x6E, 0x02, 0x05, 0x01, 0x00, 0x00, 0x00, 0x73, 0x24, 0x02, 0x00,
            0x00, 0x00, 0x68, 0x69,
        ];
        let view = DocumentView::parse(bytes).unwrap();
        assert_eq!(view.metadata.version_id, 1);
        assert_eq!(view.metadata.length_mode, LengthMode::U32);
        let PayloadView::Dynamic(ElementView::Object(object)) = view.payload else {
            panic!("expected object payload");
        };
        assert_eq!(object.fields[0].key, "n");
        assert_eq!(object.get("n"), Some(&ElementView::Uint8(5)));
        assert_eq!(object.fields[1].key, "s");
        assert_eq!(object.get("s"), Some(&ElementView::String("hi")));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_streaming_path_matches_value_tree_bytes() {
        // The streaming serde codec must emit bytes identical to the Value-tree
        // path (which the golden tests pin to the spec).
        #[derive(serde::Serialize)]
        struct Rec {
            n: u8,
            s: String,
        }
        let streamed = crate::serde_ext::to_bytes(
            &Rec {
                n: 5,
                s: "hi".to_owned(),
            },
            1,
            LengthMode::U32,
        )
        .unwrap();
        let value_path = Document::dynamic(
            1,
            LengthMode::U32,
            Value::Object(Object::new(vec![
                Field::new("n", Value::Uint8(5)),
                Field::new("s", Value::String("hi".to_owned())),
            ])),
        )
        .to_bytes()
        .unwrap();
        assert_eq!(streamed, value_path);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_round_trips_derived_structures() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Packet {
            id: u64,
            label: String,
            samples: Vec<i32>,
            note: Option<String>,
            missing: Option<u16>,
            state: PacketState,
        }

        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        enum PacketState {
            Ready,
            Failed { code: u16, message: String },
        }

        let packet = Packet {
            id: 42,
            label: "sensor-a".to_owned(),
            samples: vec![1, 2, 3],
            note: Some("warm".to_owned()),
            missing: None,
            state: PacketState::Failed {
                code: 7,
                message: "offline".to_owned(),
            },
        };

        let bytes = crate::serde_ext::to_bytes(&packet, 1, LengthMode::U32).unwrap();
        let decoded: Packet = crate::serde_ext::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, packet);

        let value = crate::serde_ext::to_value(&PacketState::Ready).unwrap();
        let decoded: PacketState = crate::serde_ext::from_value(&value).unwrap();
        assert_eq!(decoded, PacketState::Ready);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_preserves_struct_field_order() {
        #[derive(serde::Serialize)]
        struct Ordered {
            z: u8,
            a: u8,
            middle: u8,
        }

        let value = crate::serde_ext::to_value(&Ordered {
            z: 1,
            a: 2,
            middle: 3,
        })
        .unwrap();

        let Value::Object(object) = value else {
            panic!("expected object");
        };
        let keys = object
            .fields
            .iter()
            .map(|field| field.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, ["z", "a", "middle"]);
    }

    fn slice_is_borrowed_from(slice: &[u8], source: &[u8]) -> bool {
        let source_start = source.as_ptr() as usize;
        let source_end = source_start + source.len();
        let slice_start = slice.as_ptr() as usize;
        let slice_end = slice_start + slice.len();
        slice_start >= source_start && slice_end <= source_end
    }
}
