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
    #[error("value does not match declared type")]
    TypeMismatch,
    #[error("array element size is zero")]
    ZeroArrayElementSize,
    #[error("array byte length mismatch")]
    ArrayByteLengthMismatch,
    #[error("integer overflow while calculating size")]
    SizeOverflow,
    #[error("text format error: {0}")]
    Text(String),
    #[cfg(feature = "serde")]
    #[error("serde error: {0}")]
    Serde(String),
}

pub type Result<T> = std::result::Result<T, Error>;

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
}

/// Payload of an [`ElementType::Extension`]: a length-delimited extension id and
/// its bytes. Boxed inside `ElementType` to keep that enum small.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionType {
    pub id: u8,
    pub data: Vec<u8>,
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
        out.extend_from_slice(MAGIC);
        write_u16(&mut out, self.metadata.version_id);
        out.push(self.metadata.length_mode.code());
        out.push(self.metadata.alignment_hint.code());
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
                write_u16(out, *root_schema_type_id);
                write_length(out, mode, *root_count)?;
                out.extend_from_slice(data);
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

    /// Wraps an array value (boxed; see [`Value::list`]).
    pub fn array(value: Array) -> Self {
        Self::Array(Box::new(value))
    }

    /// Wraps an optional value (boxed; see [`Value::list`]).
    pub fn optional(value: OptionalValue) -> Self {
        Self::Optional(Box::new(value))
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
        out.push(self.type_id().byte());
        self.encode_value(out, mode)
    }

    fn encode_value(&self, out: &mut Vec<u8>, mode: LengthMode) -> Result<()> {
        match self {
            Self::Boolean(value) => out.push(u8::from(*value)),
            Self::Uint8(value) => out.push(*value),
            Self::Int8(value) => out.push(*value as u8),
            Self::Uint16(value) => write_u16(out, *value),
            Self::Int16(value) => out.extend_from_slice(&value.to_le_bytes()),
            Self::Uint32(value) => out.extend_from_slice(&value.to_le_bytes()),
            Self::Int32(value) => out.extend_from_slice(&value.to_le_bytes()),
            Self::Uint64(value) => out.extend_from_slice(&value.to_le_bytes()),
            Self::Int64(value) => out.extend_from_slice(&value.to_le_bytes()),
            Self::Float(value) => out.extend_from_slice(&value.to_le_bytes()),
            Self::Double(value) => out.extend_from_slice(&value.to_le_bytes()),
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
        out.push(u8::try_from(self.dimensions.len()).map_err(|_| Error::LengthTooLarge)?);
        for dimension in &self.dimensions {
            write_length(out, mode, *dimension)?;
        }
        out.extend_from_slice(&self.data);
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.element_size == 0 {
            return Err(Error::ZeroArrayElementSize);
        }
        let count = product(&self.dimensions)?;
        let expected = count
            .checked_mul(self.element_size)
            .ok_or(Error::SizeOverflow)?;
        let actual = u64::try_from(self.data.len()).map_err(|_| Error::LengthTooLarge)?;
        if expected != actual {
            return Err(Error::ArrayByteLengthMismatch);
        }
        if let ElementType::Builtin(type_id) = self.element_type {
            if let Some(size) = type_id.fixed_size() {
                if size != self.element_size {
                    return Err(Error::ArrayByteLengthMismatch);
                }
            }
        }
        Ok(())
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
                out.push(1);
                encode_typed_value(out, mode, &self.element_type, value)
            }
            None => {
                out.push(0);
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
        let mut ids = HashSet::new();
        for schema_type in &self.types {
            if !ids.insert(schema_type.id) {
                return Err(Error::DuplicateKey(schema_type.id.to_string()));
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
        write_u16(out, self.id);
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

#[derive(Debug, Clone, PartialEq)]
pub struct DocumentView<'a> {
    pub metadata: Metadata,
    pub schema: Schema,
    pub payload: PayloadView<'a>,
    bytes: &'a [u8],
}

impl<'a> DocumentView<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes, LengthMode::U64);
        let magic = reader.take(4)?;
        if magic != MAGIC {
            return Err(Error::InvalidMagic);
        }
        let version_id = reader.read_u16()?;
        let length_mode = LengthMode::from_code(reader.read_u8()?)?;
        reader.mode = length_mode;
        let alignment_hint = AlignmentHint::from_code(reader.read_u8()?)?;
        let document_length = reader.read_length()?;
        let actual = u64::try_from(bytes.len()).map_err(|_| Error::LengthTooLarge)?;
        if document_length != actual {
            return Err(Error::DocumentLengthMismatch {
                expected: document_length,
                actual,
            });
        }
        let metadata = Metadata {
            version_id,
            length_mode,
            alignment_hint,
            document_length,
        };
        let schema = read_schema(&mut reader)?;
        let payload = if schema.types.is_empty() {
            PayloadView::Dynamic(reader.read_element()?)
        } else {
            let root_schema_type_id = reader.read_u16()?;
            let root_count = reader.read_length()?;
            let data = reader.remaining_bytes();
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
    fn list(value: ListView<'a>) -> Self {
        Self::List(Box::new(value))
    }

    /// Wraps an array view (boxed; see [`ElementView::list`]).
    fn array(value: ArrayView<'a>) -> Self {
        Self::Array(Box::new(value))
    }

    /// Wraps an optional view (boxed; see [`ElementView::list`]).
    fn optional(value: OptionalView<'a>) -> Self {
        Self::Optional(Box::new(value))
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

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
    mode: LengthMode,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8], mode: LengthMode) -> Self {
        Self {
            bytes,
            pos: 0,
            mode,
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

    fn read_len_usize(&mut self) -> Result<usize> {
        usize::try_from(self.read_length()?).map_err(|_| Error::LengthTooLarge)
    }

    fn read_string(&mut self) -> Result<&'a str> {
        let len = self.read_len_usize()?;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)
    }

    fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_len_usize()?;
        self.take(len)
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
            TypeId::List => self.read_list().map(ElementView::list),
            TypeId::Array => self.read_array().map(ElementView::array),
            TypeId::String => self.read_string().map(ElementView::String),
            TypeId::Bytes => self.read_bytes().map(ElementView::Bytes),
            TypeId::Optional => self.read_optional().map(ElementView::optional),
            TypeId::Padding | TypeId::Document => Err(Error::UnsupportedTypeId(type_id.byte())),
        }
    }

    fn read_object(&mut self) -> Result<ObjectView<'a>> {
        let len = self.read_len_usize()?;
        let mut keys = HashSet::with_capacity(len);
        let mut fields = Vec::with_capacity(len);
        for _ in 0..len {
            let key = self.read_string()?;
            if !keys.insert(key) {
                return Err(Error::DuplicateKey(key.to_owned()));
            }
            let value = self.read_element()?;
            fields.push(FieldView { key, value });
        }
        Ok(ObjectView { fields })
    }

    fn read_list(&mut self) -> Result<ListView<'a>> {
        let element_type = read_element_type(self)?;
        let len = self.read_len_usize()?;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(self.read_typed_value(&element_type)?);
        }
        Ok(ListView {
            element_type,
            values,
        })
    }

    fn read_array(&mut self) -> Result<ArrayView<'a>> {
        let element_type = read_element_type(self)?;
        let element_size = self.read_length()?;
        if element_size == 0 {
            return Err(Error::ZeroArrayElementSize);
        }
        let rank = self.read_u8()? as usize;
        let mut dimensions = Vec::with_capacity(rank);
        for _ in 0..rank {
            dimensions.push(self.read_length()?);
        }
        let count = product(&dimensions)?;
        let byte_len_u64 = count.checked_mul(element_size).ok_or(Error::SizeOverflow)?;
        let byte_len = usize::try_from(byte_len_u64).map_err(|_| Error::LengthTooLarge)?;
        if let ElementType::Builtin(type_id) = element_type {
            if let Some(size) = type_id.fixed_size() {
                if size != element_size {
                    return Err(Error::ArrayByteLengthMismatch);
                }
            }
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
        let element_type = read_element_type(self)?;
        let present = self.read_u8()?;
        let value = match present {
            0 => None,
            1 => Some(Box::new(self.read_typed_value(&element_type)?)),
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
                Ok(ElementView::list(list))
            }
            ElementType::Array(inner) => {
                let array = self.read_array()?;
                if array.element_type != **inner {
                    return Err(Error::TypeMismatch);
                }
                Ok(ElementView::array(array))
            }
            ElementType::Optional(inner) => {
                let present = self.read_u8()?;
                let value = match present {
                    0 => None,
                    1 => Some(Box::new(self.read_typed_value(inner)?)),
                    other => return Err(Error::InvalidBoolean(other)),
                };
                Ok(ElementView::optional(OptionalView {
                    element_type: (**inner).clone(),
                    value,
                }))
            }
            ElementType::SchemaRef(_) | ElementType::Extension(_) => {
                Err(Error::UnsupportedTypeId(0x40))
            }
        }
    }

    fn remaining_bytes(&self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }
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
                                out.push(1);
                                encode_typed_value(out, mode, expected, value)
                            }
                            None => {
                                out.push(0);
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
    let type_count = reader.read_len_usize()?;
    let mut types = Vec::with_capacity(type_count);
    let mut ids = HashSet::with_capacity(type_count);
    for _ in 0..type_count {
        let id = reader.read_u16()?;
        if !ids.insert(id) {
            return Err(Error::DuplicateKey(id.to_string()));
        }
        let name = reader.read_string()?.to_owned();
        let payload_size = reader.read_length()?;
        let field_count = reader.read_len_usize()?;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let name = reader.read_string()?.to_owned();
            let element_type = read_element_type(reader)?;
            fields.push(SchemaField { name, element_type });
        }
        types.push(SchemaType {
            id,
            name,
            payload_size,
            fields,
        });
    }
    Ok(Schema { types })
}

fn encode_element_type(
    out: &mut Vec<u8>,
    mode: LengthMode,
    element_type: &ElementType,
) -> Result<()> {
    match element_type {
        ElementType::Builtin(type_id) => out.push(type_id.byte()),
        ElementType::SchemaRef(id) => {
            out.push(0x40);
            write_u16(out, *id);
        }
        ElementType::List(inner) => {
            out.push(0x41);
            encode_element_type(out, mode, inner)?;
        }
        ElementType::Array(inner) => {
            out.push(0x42);
            encode_element_type(out, mode, inner)?;
        }
        ElementType::Object => out.push(0x43),
        ElementType::Optional(inner) => {
            out.push(0x44);
            encode_element_type(out, mode, inner)?;
        }
        ElementType::Extension(ext) if ext.id >= 0x80 => {
            out.push(ext.id);
            write_bytes(out, mode, &ext.data)?;
        }
        ElementType::Extension(ext) => return Err(Error::InvalidTypeId(ext.id)),
    }
    Ok(())
}

fn read_element_type(reader: &mut Reader<'_>) -> Result<ElementType> {
    let byte = reader.read_u8()?;
    match byte {
        0x01..=0x0B | 0x24 | 0x25 => Ok(ElementType::Builtin(TypeId::from_byte(byte)?)),
        0x40 => Ok(ElementType::SchemaRef(reader.read_u16()?)),
        0x41 => Ok(ElementType::List(Box::new(read_element_type(reader)?))),
        0x42 => Ok(ElementType::Array(Box::new(read_element_type(reader)?))),
        0x43 => Ok(ElementType::Object),
        0x44 => Ok(ElementType::Optional(Box::new(read_element_type(reader)?))),
        0x80..=0xFF => {
            let data = reader.read_bytes()?.to_vec();
            Ok(ElementType::extension(byte, data))
        }
        other => Err(Error::InvalidTypeId(other)),
    }
}

fn write_string(out: &mut Vec<u8>, mode: LengthMode, value: &str) -> Result<()> {
    write_bytes(out, mode, value.as_bytes())
}

fn write_bytes(out: &mut Vec<u8>, mode: LengthMode, value: &[u8]) -> Result<()> {
    write_length(out, mode, value.len() as u64)?;
    out.extend_from_slice(value);
    Ok(())
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_length(out: &mut Vec<u8>, mode: LengthMode, value: u64) -> Result<()> {
    mode.validate(value)?;
    match mode {
        LengthMode::U16 => out.extend_from_slice(&(value as u16).to_le_bytes()),
        LengthMode::U32 => out.extend_from_slice(&(value as u32).to_le_bytes()),
        LengthMode::U64 => out.extend_from_slice(&value.to_le_bytes()),
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

fn validate_unique_keys<'a>(keys: impl Iterator<Item = &'a str>) -> Result<()> {
    let mut seen = HashSet::new();
    for key in keys {
        if !seen.insert(key) {
            return Err(Error::DuplicateKey(key.to_owned()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let streamed =
            crate::serde_ext::to_bytes(&Rec { n: 5, s: "hi".to_owned() }, 1, LengthMode::U32)
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
}
