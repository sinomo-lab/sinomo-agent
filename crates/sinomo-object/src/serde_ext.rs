use serde::Serialize;
use serde::de::Deserializer as _;
use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};
use serde::ser::{
    self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};

use crate::{
    AlignmentHint, Array, DecodeOptions, Document, ElementType, Error, Field, LengthMode, List,
    MAGIC, Object, OptionalValue, Reader, Result, TypeId, Value, duplicate_key_error,
    read_document_header, read_element_type, read_schema, try_box, try_extend_from_slice,
    try_hashset_with_capacity, try_push_vec, try_reserve_hashset, try_string_from_str,
    try_vec_with_capacity,
};
use std::collections::HashSet;
use std::ops::Range;

pub use crate::stream_serde::from_chunk_iter_with_options;

/// Patchable output used by the SOBJ serde encoder.
///
/// SOBJ writes a small number of lengths and element tags before their values
/// are known. A contiguous `Vec<u8>` can patch those bytes by indexing; an
/// ownership-native transport can implement this trait over fixed-size chunks
/// and patch the same logical offsets without ever joining the payload into one
/// allocation.
pub trait SobjOutput {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn append(&mut self, bytes: &[u8], context: &'static str) -> Result<()>;

    fn patch(&mut self, offset: usize, bytes: &[u8]) -> Result<()>;

    fn truncate(&mut self, len: usize);
}

impl SobjOutput for Vec<u8> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn append(&mut self, bytes: &[u8], context: &'static str) -> Result<()> {
        try_extend_from_slice(self, bytes, context)
    }

    fn patch(&mut self, offset: usize, bytes: &[u8]) -> Result<()> {
        let end = offset.checked_add(bytes.len()).ok_or(Error::SizeOverflow)?;
        let destination = self.get_mut(offset..end).ok_or(Error::UnexpectedEof)?;
        destination.copy_from_slice(bytes);
        Ok(())
    }

    fn truncate(&mut self, len: usize) {
        Vec::truncate(self, len);
    }
}

pub fn to_value<T: Serialize + ?Sized>(value: &T) -> Result<Value> {
    value.serialize(ValueSerializer)
}

pub fn from_value<T: DeserializeOwned>(value: &Value) -> Result<T> {
    T::deserialize(ValueDeserializer { value })
}

pub fn to_document<T: Serialize + ?Sized>(
    value: &T,
    version_id: u16,
    length_mode: LengthMode,
) -> Result<Document> {
    Ok(Document::dynamic(version_id, length_mode, to_value(value)?))
}

pub fn to_bytes<T: Serialize + ?Sized>(
    value: &T,
    version_id: u16,
    length_mode: LengthMode,
) -> Result<Vec<u8>> {
    // Stream the value straight into the document buffer rather than building a
    // `Value` tree first; see the "Streaming serde codec" section below.
    let mut out = Vec::new();
    append_bytes(&mut out, value, version_id, length_mode)?;
    Ok(out)
}

pub fn append_bytes<T: Serialize + ?Sized>(
    out: &mut Vec<u8>,
    value: &T,
    version_id: u16,
    length_mode: LengthMode,
) -> Result<Range<usize>> {
    append_bytes_to_output(out, value, version_id, length_mode)
}

/// Appends one canonical SOBJ document to a patchable output.
///
/// The output may be contiguous or segmented. The returned range uses logical
/// output offsets and covers only the newly encoded document.
pub fn append_bytes_to_output<T, O>(
    out: &mut O,
    value: &T,
    version_id: u16,
    length_mode: LengthMode,
) -> Result<Range<usize>>
where
    T: Serialize + ?Sized,
    O: SobjOutput + ?Sized,
{
    let start = out.len();
    if let Err(error) = append_bytes_inner(out, value, version_id, length_mode) {
        out.truncate(start);
        return Err(error);
    }
    Ok(start..out.len())
}

fn append_bytes_inner<T, O>(
    out: &mut O,
    value: &T,
    version_id: u16,
    length_mode: LengthMode,
) -> Result<()>
where
    T: Serialize + ?Sized,
    O: SobjOutput + ?Sized,
{
    let start = out.len();
    out.append(MAGIC, "serde document magic")?;
    output_write_u16(out, version_id, "serde document version")?;
    output_write_u8(out, length_mode.code(), "serde document length mode")?;
    output_write_u8(
        out,
        AlignmentHint::NotAligned.code(),
        "serde document alignment hint",
    )?;
    let length_offset = out.len();
    output_write_length(out, length_mode, 0)?; // document length, patched below
    output_write_length(out, length_mode, 0)?; // empty dynamic schema (0 types)
    serialize_element_into(out, length_mode, value)?;
    let document_length = u64::try_from(out.len() - start).map_err(|_| Error::LengthTooLarge)?;
    length_mode.validate(document_length)?;
    output_patch_length(out, length_offset, length_mode, document_length)?;
    Ok(())
}

fn output_write_u8<O: SobjOutput + ?Sized>(
    out: &mut O,
    value: u8,
    context: &'static str,
) -> Result<()> {
    out.append(&[value], context)
}

fn output_write_u16<O: SobjOutput + ?Sized>(
    out: &mut O,
    value: u16,
    context: &'static str,
) -> Result<()> {
    out.append(&value.to_le_bytes(), context)
}

fn output_length_bytes(mode: LengthMode, value: u64) -> Result<([u8; 8], usize)> {
    mode.validate(value)?;
    let mut bytes = [0_u8; 8];
    let width = mode.width();
    match mode {
        LengthMode::U16 => bytes[..width].copy_from_slice(&(value as u16).to_le_bytes()),
        LengthMode::U32 => bytes[..width].copy_from_slice(&(value as u32).to_le_bytes()),
        LengthMode::U64 => bytes.copy_from_slice(&value.to_le_bytes()),
    }
    Ok((bytes, width))
}

fn output_write_length<O: SobjOutput + ?Sized>(
    out: &mut O,
    mode: LengthMode,
    value: u64,
) -> Result<()> {
    let (bytes, width) = output_length_bytes(mode, value)?;
    out.append(&bytes[..width], "length")
}

fn output_patch_length<O: SobjOutput + ?Sized>(
    out: &mut O,
    offset: usize,
    mode: LengthMode,
    value: u64,
) -> Result<()> {
    let (bytes, width) = output_length_bytes(mode, value)?;
    out.patch(offset, &bytes[..width])
}

fn output_write_bytes<O: SobjOutput + ?Sized>(
    out: &mut O,
    mode: LengthMode,
    value: &[u8],
    context: &'static str,
) -> Result<()> {
    output_write_length(
        out,
        mode,
        u64::try_from(value.len()).map_err(|_| Error::LengthTooLarge)?,
    )?;
    out.append(value, context)
}

fn output_write_string<O: SobjOutput + ?Sized>(
    out: &mut O,
    mode: LengthMode,
    value: &str,
) -> Result<()> {
    output_write_bytes(out, mode, value.as_bytes(), "string payload")
}

fn output_encode_element_type<O: SobjOutput + ?Sized>(
    out: &mut O,
    mode: LengthMode,
    element_type: &ElementType,
) -> Result<()> {
    match element_type {
        ElementType::Builtin(type_id) => {
            output_write_u8(out, type_id.byte(), "element type")?;
        }
        ElementType::SchemaRef(id) => {
            output_write_u8(out, 0x40, "schema element type")?;
            output_write_u16(out, *id, "schema element type id")?;
        }
        ElementType::List(inner) => {
            output_write_u8(out, 0x41, "list element type")?;
            output_encode_element_type(out, mode, inner)?;
        }
        ElementType::Array(inner) => {
            output_write_u8(out, 0x42, "array element type")?;
            output_encode_element_type(out, mode, inner)?;
        }
        ElementType::Object => output_write_u8(out, 0x43, "object element type")?,
        ElementType::Optional(inner) => {
            output_write_u8(out, 0x44, "optional element type")?;
            output_encode_element_type(out, mode, inner)?;
        }
        ElementType::Extension(extension) if extension.id >= 0x80 => {
            output_write_u8(out, extension.id, "extension element type")?;
            output_write_bytes(out, mode, &extension.data, "extension element type payload")?;
        }
        ElementType::Extension(extension) => return Err(Error::InvalidTypeId(extension.id)),
    }
    Ok(())
}

pub fn from_bytes<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    from_bytes_with_options(bytes, DecodeOptions::default())
}

pub fn from_bytes_with_options<T: DeserializeOwned>(
    bytes: &[u8],
    options: DecodeOptions,
) -> Result<T> {
    from_borrowed_bytes_with_options(bytes, options)
}

pub fn from_borrowed_bytes<'de, T>(bytes: &'de [u8]) -> Result<T>
where
    T: serde::Deserialize<'de>,
{
    from_borrowed_bytes_with_options(bytes, DecodeOptions::default())
}

pub fn from_borrowed_bytes_with_options<'de, T>(
    bytes: &'de [u8],
    options: DecodeOptions,
) -> Result<T>
where
    T: serde::Deserialize<'de>,
{
    // Stream the value straight out of the document buffer rather than parsing it
    // into an `ElementView`/`Value` tree first.
    let (mut reader, _) = read_document_header(bytes, options)?;
    let schema = read_schema(&mut reader)?;
    if !schema.types.is_empty() {
        return Err(Error::Serde(
            "serde deserialization from raw schema payload is not implemented".to_owned(),
        ));
    }
    let value = T::deserialize(SobjDeserializer {
        reader: &mut reader,
        shape: Shape::Element,
    })?;
    reader.skip_padding();
    if reader.remaining() != 0 {
        return Err(Error::UnexpectedEof);
    }
    Ok(value)
}

impl ser::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self::Serde(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self::Serde(msg.to_string())
    }
}

// ---------------------------------------------------------------------------
// Streaming serde codec
//
// `to_bytes`/`from_bytes` encode/decode directly against the byte buffer instead
// of materializing a `Value`/`ElementView` tree. The tree representation costs one
// ~80-byte `Value` per element, so *any* sequence (`list<uint8>`, `list<int32>`,
// `list<(i32,i32)>`, …) paid an enormous per-element memory overhead. Streaming
// keeps memory at O(serialized size) for all element types. The produced bytes are
// identical to the `Value`-tree path, so the document format is unchanged; the
// dynamic `Value`/`ElementView` API (`to_value`/`from_value`/`text`) is untouched.
// ---------------------------------------------------------------------------

/// The 1-byte element tag (`type_id`) for a value of the given element type, used
/// when writing a standalone element (`type_id` + value) for the document payload
/// and object fields.
fn element_type_id(element_type: &ElementType) -> Result<u8> {
    Ok(match element_type {
        ElementType::Builtin(type_id) => type_id.byte(),
        ElementType::Object => TypeId::Object.byte(),
        ElementType::List(_) => TypeId::List.byte(),
        ElementType::Array(_) => TypeId::Array.byte(),
        ElementType::Optional(_) => TypeId::Optional.byte(),
        ElementType::SchemaRef(_) | ElementType::Extension(_) => {
            return Err(Error::Serde(
                "serde serialization does not produce schema or extension types".to_owned(),
            ));
        }
    })
}

/// Writes a standalone element (`type_id` byte + value bytes) for `value` into `out`.
fn serialize_element_into<T, O>(out: &mut O, mode: LengthMode, value: &T) -> Result<()>
where
    T: Serialize + ?Sized,
    O: SobjOutput + ?Sized,
{
    let type_id_offset = out.len();
    output_write_u8(out, 0, "serde element type id")?; // patched once the type is known
    let element_type = value.serialize(ByteSerializer { out, mode })?;
    out.patch(type_id_offset, &[element_type_id(&element_type)?])?;
    Ok(())
}

/// serde `Serializer` that writes a value's bytes straight into `out` and returns
/// the value's `ElementType` (so a containing list/optional/element can frame it).
struct ByteSerializer<'a, O: SobjOutput + ?Sized> {
    out: &'a mut O,
    mode: LengthMode,
}

impl<'a, O: SobjOutput + ?Sized> ByteSerializer<'a, O> {
    fn builtin(
        self,
        type_id: TypeId,
        write: impl FnOnce(&mut O) -> Result<()>,
    ) -> Result<ElementType> {
        write(self.out)?;
        Ok(ElementType::Builtin(type_id))
    }
}

/// Inspect only the shape needed before a list or optional payload. Object
/// fields and subsequent list items cannot change that descriptor; the actual
/// encoder visits them once and retains all value and container validation.
struct TypeProbeSerializer;

macro_rules! probe_constant_methods {
    (
        builtin { $($method:ident($input:ty) => $type_id:ident),* $(,)? }
        containers {
            $($container:ident($($argument:ty),*) -> $output:ty => $value:expr),* $(,)?
        }
    ) => {
        $(fn $method(self, _: $input) -> Result<ElementType> {
            Ok(ElementType::Builtin(TypeId::$type_id))
        })*
        $(fn $container(self, $(_: $argument),*) -> Result<$output> {
            Ok($value)
        })*
    };
}

impl ser::Serializer for TypeProbeSerializer {
    type Ok = ElementType;
    type Error = Error;
    type SerializeSeq = TypeProbeSeq;
    type SerializeTuple = TypeProbeSeq;
    type SerializeTupleStruct = TypeProbeSeq;
    type SerializeTupleVariant = TypeProbeObject;
    type SerializeMap = TypeProbeObject;
    type SerializeStruct = TypeProbeObject;
    type SerializeStructVariant = TypeProbeObject;

    probe_constant_methods! {
        builtin {
            serialize_bool(bool) => Boolean,
            serialize_i8(i8) => Int8,
            serialize_i16(i16) => Int16,
            serialize_i32(i32) => Int32,
            serialize_i64(i64) => Int64,
            serialize_u8(u8) => Uint8,
            serialize_u16(u16) => Uint16,
            serialize_u32(u32) => Uint32,
            serialize_u64(u64) => Uint64,
            serialize_f32(f32) => Float,
            serialize_f64(f64) => Double,
            serialize_char(char) => String,
            serialize_str(&str) => String,
            serialize_bytes(&[u8]) => Bytes,
        }
        containers {
            serialize_unit_variant(&'static str, u32, &'static str) -> ElementType
                => ElementType::Builtin(TypeId::String),
            serialize_seq(Option<usize>) -> TypeProbeSeq => TypeProbeSeq::default(),
            serialize_tuple(usize) -> TypeProbeSeq => TypeProbeSeq::default(),
            serialize_tuple_struct(&'static str, usize) -> TypeProbeSeq => TypeProbeSeq::default(),
            serialize_tuple_variant(&'static str, u32, &'static str, usize) -> TypeProbeObject
                => TypeProbeObject,
            serialize_map(Option<usize>) -> TypeProbeObject => TypeProbeObject,
            serialize_struct(&'static str, usize) -> TypeProbeObject => TypeProbeObject,
            serialize_struct_variant(&'static str, u32, &'static str, usize) -> TypeProbeObject
                => TypeProbeObject,
        }
    }

    fn serialize_none(self) -> Result<ElementType> {
        Ok(ElementType::Optional(try_box(
            ElementType::Builtin(TypeId::Bytes),
            "optional element type",
        )?))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<ElementType> {
        Ok(ElementType::Optional(try_box(
            probe_element_type(value)?,
            "optional element type",
        )?))
    }

    fn serialize_unit(self) -> Result<ElementType> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<ElementType> {
        self.serialize_none()
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<ElementType> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<ElementType> {
        Ok(ElementType::Object)
    }

    fn collect_str<T: std::fmt::Display + ?Sized>(self, _value: &T) -> Result<ElementType> {
        Ok(ElementType::Builtin(TypeId::String))
    }

    fn collect_seq<I>(self, iter: I) -> Result<ElementType>
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        let mut sequence = TypeProbeSeq::default();
        if let Some(first) = iter.into_iter().next() {
            sequence.push(&first)?;
        }
        sequence.finish()
    }
}

#[derive(Default)]
struct TypeProbeSeq {
    first: Option<ElementType>,
}

impl TypeProbeSeq {
    fn push<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        if self.first.is_none() {
            self.first = Some(probe_element_type(value)?);
        }
        Ok(())
    }

    fn finish(self) -> Result<ElementType> {
        Ok(ElementType::List(try_box(
            self.first.unwrap_or(ElementType::Builtin(TypeId::Bytes)),
            "list element type",
        )?))
    }
}

macro_rules! impl_probe_sequence {
    ($trait:ident, $method:ident) => {
        impl $trait for TypeProbeSeq {
            type Ok = ElementType;
            type Error = Error;
            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
                self.push(value)
            }
            fn end(self) -> Result<ElementType> {
                self.finish()
            }
        }
    };
}

impl_probe_sequence!(SerializeSeq, serialize_element);
impl_probe_sequence!(SerializeTuple, serialize_element);
impl_probe_sequence!(SerializeTupleStruct, serialize_field);

struct TypeProbeObject;

macro_rules! impl_probe_object {
    ($trait:ident, $($method:ident $($key:ident)?),+ $(,)?) => {
        impl $trait for TypeProbeObject {
            type Ok = ElementType;
            type Error = Error;
            $(fn $method<T: Serialize + ?Sized>(
                &mut self, $($key: &'static str,)? _value: &T,
            ) -> Result<()> {
                Ok(())
            })+
            fn end(self) -> Result<ElementType> {
                Ok(ElementType::Object)
            }
        }
    };
}

impl_probe_object!(SerializeMap, serialize_key, serialize_value);
impl_probe_object!(SerializeTupleVariant, serialize_field);
impl_probe_object!(SerializeStruct, serialize_field _key);
impl_probe_object!(SerializeStructVariant, serialize_field _key);

fn probe_element_type<T: Serialize + ?Sized>(value: &T) -> Result<ElementType> {
    value.serialize(TypeProbeSerializer)
}

impl<'a, O: SobjOutput + ?Sized> ser::Serializer for ByteSerializer<'a, O> {
    type Ok = ElementType;
    type Error = Error;
    type SerializeSeq = ByteSeqSerializer<'a, O>;
    type SerializeTuple = ByteSeqSerializer<'a, O>;
    type SerializeTupleStruct = ByteSeqSerializer<'a, O>;
    type SerializeTupleVariant = ByteVariantSeqSerializer<'a, O>;
    type SerializeMap = ByteMapSerializer<'a, O>;
    type SerializeStruct = ByteStructSerializer<'a, O>;
    type SerializeStructVariant = ByteVariantStructSerializer<'a, O>;

    fn serialize_bool(self, v: bool) -> Result<ElementType> {
        self.builtin(TypeId::Boolean, |out| {
            output_write_u8(out, u8::from(v), "bool")
        })
    }

    fn serialize_i8(self, v: i8) -> Result<ElementType> {
        self.builtin(TypeId::Int8, |out| output_write_u8(out, v as u8, "i8"))
    }

    fn serialize_i16(self, v: i16) -> Result<ElementType> {
        self.builtin(TypeId::Int16, |out| out.append(&v.to_le_bytes(), "i16"))
    }

    fn serialize_i32(self, v: i32) -> Result<ElementType> {
        self.builtin(TypeId::Int32, |out| out.append(&v.to_le_bytes(), "i32"))
    }

    fn serialize_i64(self, v: i64) -> Result<ElementType> {
        self.builtin(TypeId::Int64, |out| out.append(&v.to_le_bytes(), "i64"))
    }

    fn serialize_u8(self, v: u8) -> Result<ElementType> {
        self.builtin(TypeId::Uint8, |out| output_write_u8(out, v, "u8"))
    }

    fn serialize_u16(self, v: u16) -> Result<ElementType> {
        self.builtin(TypeId::Uint16, |out| out.append(&v.to_le_bytes(), "u16"))
    }

    fn serialize_u32(self, v: u32) -> Result<ElementType> {
        self.builtin(TypeId::Uint32, |out| out.append(&v.to_le_bytes(), "u32"))
    }

    fn serialize_u64(self, v: u64) -> Result<ElementType> {
        self.builtin(TypeId::Uint64, |out| out.append(&v.to_le_bytes(), "u64"))
    }

    fn serialize_f32(self, v: f32) -> Result<ElementType> {
        self.builtin(TypeId::Float, |out| out.append(&v.to_le_bytes(), "f32"))
    }

    fn serialize_f64(self, v: f64) -> Result<ElementType> {
        self.builtin(TypeId::Double, |out| out.append(&v.to_le_bytes(), "f64"))
    }

    fn serialize_char(self, v: char) -> Result<ElementType> {
        self.serialize_str(v.encode_utf8(&mut [0_u8; 4]))
    }

    fn serialize_str(self, v: &str) -> Result<ElementType> {
        output_write_string(self.out, self.mode, v)?;
        Ok(ElementType::Builtin(TypeId::String))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<ElementType> {
        output_write_bytes(self.out, self.mode, v, "bytes payload")?;
        Ok(ElementType::Builtin(TypeId::Bytes))
    }

    fn serialize_none(self) -> Result<ElementType> {
        // Absent optional: element type defaults to `bytes`, presence byte 0.
        let inner = ElementType::Builtin(TypeId::Bytes);
        output_encode_element_type(self.out, self.mode, &inner)?;
        output_write_u8(self.out, 0, "optional presence")?;
        Ok(ElementType::Optional(try_box(
            inner,
            "optional element type",
        )?))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<ElementType> {
        // Probe the type without retaining the encoded payload, then serialize
        // directly into the destination. This keeps even a large optional
        // value out of a temporary contiguous allocation.
        let inner = probe_element_type(value)?;
        output_encode_element_type(self.out, self.mode, &inner)?;
        output_write_u8(self.out, 1, "optional presence")?;
        let actual = value.serialize(ByteSerializer {
            out: self.out,
            mode: self.mode,
        })?;
        if actual != inner {
            return Err(Error::TypeMismatch);
        }
        Ok(ElementType::Optional(try_box(
            inner,
            "optional element type",
        )?))
    }

    fn serialize_unit(self) -> Result<ElementType> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<ElementType> {
        self.serialize_none()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<ElementType> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<ElementType> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<ElementType> {
        // Encoded as a single-field object `{ variant: value }`.
        output_write_length(self.out, self.mode, 1)?;
        output_write_string(self.out, self.mode, variant)?;
        serialize_element_into(self.out, self.mode, value)?;
        Ok(ElementType::Object)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<ByteSeqSerializer<'a, O>> {
        Ok(ByteSeqSerializer::new(self.out, self.mode, len))
    }

    fn serialize_tuple(self, len: usize) -> Result<ByteSeqSerializer<'a, O>> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<ByteSeqSerializer<'a, O>> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<ByteVariantSeqSerializer<'a, O>> {
        // `{ variant: [ .. ] }`: outer object header, then the list value.
        output_write_length(self.out, self.mode, 1)?;
        output_write_string(self.out, self.mode, variant)?;
        output_write_u8(self.out, TypeId::List.byte(), "tuple variant list type")?;
        Ok(ByteVariantSeqSerializer {
            inner: ByteSeqSerializer::new(self.out, self.mode, Some(_len)),
        })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<ByteMapSerializer<'a, O>> {
        let length_offset = self.out.len();
        output_write_length(self.out, self.mode, 0)?; // patched in `end`
        let keys = try_hashset_with_capacity(_len.unwrap_or(0), "serde map keys")?;
        Ok(ByteMapSerializer {
            out: self.out,
            mode: self.mode,
            length_offset,
            count: 0,
            pending_key: None,
            keys,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<ByteStructSerializer<'a, O>> {
        output_write_length(self.out, self.mode, len as u64)?;
        Ok(ByteStructSerializer {
            out: self.out,
            mode: self.mode,
            expected_fields: len,
            written_fields: 0,
            keys: InlineKeySet::default(),
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<ByteVariantStructSerializer<'a, O>> {
        // `{ variant: { .. } }`: outer object header, then the inner object value.
        output_write_length(self.out, self.mode, 1)?;
        output_write_string(self.out, self.mode, variant)?;
        output_write_u8(
            self.out,
            TypeId::Object.byte(),
            "struct variant object type",
        )?;
        output_write_length(self.out, self.mode, len as u64)?;
        Ok(ByteVariantStructSerializer {
            inner: ByteStructSerializer {
                out: self.out,
                mode: self.mode,
                expected_fields: len,
                written_fields: 0,
                keys: InlineKeySet::default(),
            },
        })
    }
}

/// Streams a homogeneous sequence into `out` as `list<element_type>`. Inspect
/// the first element's descriptor, then write every value directly into `out`.
struct ByteSeqSerializer<'a, O: SobjOutput + ?Sized> {
    out: &'a mut O,
    mode: LengthMode,
    element_type: Option<ElementType>,
    length_offset: usize,
    count: u64,
}

impl<'a, O: SobjOutput + ?Sized> ByteSeqSerializer<'a, O> {
    fn new(out: &'a mut O, mode: LengthMode, _len: Option<usize>) -> Self {
        Self {
            out,
            mode,
            element_type: None,
            length_offset: 0,
            count: 0,
        }
    }

    fn push<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        match &self.element_type {
            None => {
                let element_type = probe_element_type(value)?;
                output_encode_element_type(self.out, self.mode, &element_type)?;
                self.length_offset = self.out.len();
                output_write_length(self.out, self.mode, 0)?; // patched in `finish`
                let actual = value.serialize(ByteSerializer {
                    out: &mut *self.out,
                    mode: self.mode,
                })?;
                if actual != element_type {
                    return Err(Error::TypeMismatch);
                }
                self.element_type = Some(element_type);
                self.count = 1;
            }
            Some(expected) => {
                let element_type = value.serialize(ByteSerializer {
                    out: &mut *self.out,
                    mode: self.mode,
                })?;
                if &element_type != expected {
                    return Err(Error::TypeMismatch);
                }
                self.count += 1;
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<ElementType> {
        let element_type = match self.element_type {
            Some(element_type) => {
                output_patch_length(self.out, self.length_offset, self.mode, self.count)?;
                element_type
            }
            None => {
                // Empty sequence: element type defaults to `bytes`, length 0.
                let element_type = ElementType::Builtin(TypeId::Bytes);
                output_encode_element_type(self.out, self.mode, &element_type)?;
                output_write_length(self.out, self.mode, 0)?;
                element_type
            }
        };
        Ok(ElementType::List(try_box(
            element_type,
            "list element type",
        )?))
    }
}

impl<'a, O: SobjOutput + ?Sized> SerializeSeq for ByteSeqSerializer<'a, O> {
    type Ok = ElementType;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.push(value)
    }

    fn end(self) -> Result<ElementType> {
        self.finish()
    }
}

impl<'a, O: SobjOutput + ?Sized> SerializeTuple for ByteSeqSerializer<'a, O> {
    type Ok = ElementType;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.push(value)
    }

    fn end(self) -> Result<ElementType> {
        self.finish()
    }
}

impl<'a, O: SobjOutput + ?Sized> SerializeTupleStruct for ByteSeqSerializer<'a, O> {
    type Ok = ElementType;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.push(value)
    }

    fn end(self) -> Result<ElementType> {
        self.finish()
    }
}

/// Tuple-enum-variant: a list value wrapped in a single-field object.
struct ByteVariantSeqSerializer<'a, O: SobjOutput + ?Sized> {
    inner: ByteSeqSerializer<'a, O>,
}

impl<'a, O: SobjOutput + ?Sized> SerializeTupleVariant for ByteVariantSeqSerializer<'a, O> {
    type Ok = ElementType;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.inner.push(value)
    }

    fn end(self) -> Result<ElementType> {
        self.inner.finish()?;
        Ok(ElementType::Object)
    }
}

/// Streams a map into `out` as an object (`length` + `key + element` pairs).
struct ByteMapSerializer<'a, O: SobjOutput + ?Sized> {
    out: &'a mut O,
    mode: LengthMode,
    length_offset: usize,
    count: u64,
    pending_key: Option<String>,
    keys: HashSet<String>,
}

impl<'a, O: SobjOutput + ?Sized> SerializeMap for ByteMapSerializer<'a, O> {
    type Ok = ElementType;
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        self.pending_key = Some(key.serialize(KeySerializer)?);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let key = self
            .pending_key
            .take()
            .ok_or_else(|| Error::Serde("map value serialized before key".to_owned()))?;
        if self.keys.contains(&key) {
            return Err(Error::DuplicateKey(key));
        }
        try_reserve_hashset(&mut self.keys, 1, "serde map keys")?;
        output_write_string(self.out, self.mode, &key)?;
        self.keys.insert(key);
        serialize_element_into(self.out, self.mode, value)?;
        self.count = self.count.checked_add(1).ok_or(Error::SizeOverflow)?;
        Ok(())
    }

    fn end(self) -> Result<ElementType> {
        output_patch_length(self.out, self.length_offset, self.mode, self.count)?;
        Ok(ElementType::Object)
    }
}

const INLINE_OBJECT_KEY_CAPACITY: usize = 16;

/// Avoids a heap allocation for the common RPC object while retaining
/// canonical duplicate-key validation. Larger objects spill into a HashSet.
struct InlineKeySet<'a> {
    inline: [Option<&'a str>; INLINE_OBJECT_KEY_CAPACITY],
    len: usize,
    overflow: Option<HashSet<&'a str>>,
}

impl Default for InlineKeySet<'_> {
    fn default() -> Self {
        Self {
            inline: [None; INLINE_OBJECT_KEY_CAPACITY],
            len: 0,
            overflow: None,
        }
    }
}

impl<'a> InlineKeySet<'a> {
    fn insert(&mut self, key: &'a str, context: &'static str) -> Result<bool> {
        if let Some(keys) = &mut self.overflow {
            if keys.contains(key) {
                return Ok(false);
            }
            try_reserve_hashset(keys, 1, context)?;
            keys.insert(key);
            return Ok(true);
        }
        if self.inline[..self.len]
            .iter()
            .flatten()
            .any(|existing| *existing == key)
        {
            return Ok(false);
        }
        if self.len < INLINE_OBJECT_KEY_CAPACITY {
            self.inline[self.len] = Some(key);
            self.len += 1;
            return Ok(true);
        }

        let mut keys = try_hashset_with_capacity(self.len + 1, context)?;
        keys.extend(self.inline.iter().flatten().copied());
        keys.insert(key);
        self.overflow = Some(keys);
        Ok(true)
    }
}

/// Streams a struct into `out` as an object. The field count is known up front, so
/// the object length is written immediately and each field is appended in place.
struct ByteStructSerializer<'a, O: SobjOutput + ?Sized> {
    out: &'a mut O,
    mode: LengthMode,
    expected_fields: usize,
    written_fields: usize,
    keys: InlineKeySet<'static>,
}

impl<'a, O: SobjOutput + ?Sized> ByteStructSerializer<'a, O> {
    fn field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> Result<()> {
        if self.written_fields >= self.expected_fields {
            return Err(Error::Serde(
                "struct serialized more fields than declared".to_string(),
            ));
        }
        if !self.keys.insert(key, "serde struct keys")? {
            return duplicate_key_error(key);
        }
        output_write_string(self.out, self.mode, key)?;
        serialize_element_into(self.out, self.mode, value)?;
        self.written_fields += 1;
        Ok(())
    }

    fn finish(self) -> Result<ElementType> {
        if self.written_fields != self.expected_fields {
            return Err(Error::Serde(format!(
                "struct declared {} fields but serialized {}",
                self.expected_fields, self.written_fields
            )));
        }
        Ok(ElementType::Object)
    }
}

impl<'a, O: SobjOutput + ?Sized> SerializeStruct for ByteStructSerializer<'a, O> {
    type Ok = ElementType;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.field(key, value)
    }

    fn end(self) -> Result<ElementType> {
        self.finish()
    }
}

/// Struct-enum-variant: an inner object wrapped in a single-field object.
struct ByteVariantStructSerializer<'a, O: SobjOutput + ?Sized> {
    inner: ByteStructSerializer<'a, O>,
}

impl<'a, O: SobjOutput + ?Sized> SerializeStructVariant for ByteVariantStructSerializer<'a, O> {
    type Ok = ElementType;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.inner.field(key, value)
    }

    fn end(self) -> Result<ElementType> {
        self.inner.finish()
    }
}

/// Whether the next value to read carries its own 1-byte `type_id` tag (a standalone
/// element: payload, object field, enum payload) or is a bare value whose type is
/// already known from the surrounding container (a list element / optional inner).
enum Shape {
    Element,
    Typed(ElementType),
}

/// The `TypeId` of the value the surrounding container declared, for a bare value.
fn element_type_type_id(element_type: &ElementType) -> Result<TypeId> {
    Ok(match element_type {
        ElementType::Builtin(type_id) => *type_id,
        ElementType::Object => TypeId::Object,
        ElementType::List(_) => TypeId::List,
        ElementType::Array(_) => TypeId::Array,
        ElementType::Optional(_) => TypeId::Optional,
        ElementType::SchemaRef(_) | ElementType::Extension(_) => {
            return Err(Error::Serde(
                "schema and extension element types are not supported by serde decoding".to_owned(),
            ));
        }
    })
}

/// serde `Deserializer` that reads a single value directly out of the byte stream.
struct SobjDeserializer<'r, 'a> {
    reader: &'r mut Reader<'a>,
    shape: Shape,
}

impl<'r, 'a> SobjDeserializer<'r, 'a> {
    /// Resolves the value's `TypeId`, consuming the tag byte for a standalone element.
    fn type_id(reader: &mut Reader<'a>, shape: &Shape) -> Result<TypeId> {
        match shape {
            Shape::Element => {
                reader.skip_padding();
                let byte = reader.read_u8()?;
                let type_id = TypeId::from_byte(byte)?;
                if type_id == TypeId::Padding {
                    return Err(Error::InvalidTypeId(byte));
                }
                Ok(type_id)
            }
            Shape::Typed(element_type) => element_type_type_id(element_type),
        }
    }
}

struct SobjStrDeserializer<'a> {
    value: &'a str,
    context: &'static str,
}

impl<'de, 'a> de::Deserializer<'de> for SobjStrDeserializer<'a>
where
    'a: 'de,
{
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_borrowed_str(self.value)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_borrowed_str(self.value)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_string(try_string_from_str(self.value, self.context)?)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_borrowed_str(self.value)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map struct
        enum ignored_any
    }
}

/// Reads a value of the resolved `type_id` from `reader`, driving the serde visitor.
fn deserialize_value<'de, 'a, V>(
    reader: &mut Reader<'a>,
    type_id: TypeId,
    visitor: V,
) -> Result<V::Value>
where
    'a: 'de,
    V: Visitor<'de>,
{
    match type_id {
        TypeId::Boolean => match reader.read_u8()? {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            other => Err(Error::InvalidBoolean(other)),
        },
        TypeId::Uint8 => visitor.visit_u8(reader.read_u8()?),
        TypeId::Int8 => visitor.visit_i8(reader.read_u8()? as i8),
        TypeId::Uint16 => visitor.visit_u16(reader.read_u16()?),
        TypeId::Int16 => {
            let bytes = reader.take(2)?;
            visitor.visit_i16(i16::from_le_bytes([bytes[0], bytes[1]]))
        }
        TypeId::Uint32 => visitor.visit_u32(reader.read_u32()?),
        TypeId::Int32 => visitor.visit_i32(reader.read_i32()?),
        TypeId::Uint64 => visitor.visit_u64(reader.read_u64()?),
        TypeId::Int64 => visitor.visit_i64(reader.read_i64()?),
        TypeId::Float => {
            let bytes = reader.take(4)?;
            visitor.visit_f32(f32::from_le_bytes(
                bytes.try_into().map_err(|_| Error::UnexpectedEof)?,
            ))
        }
        TypeId::Double => {
            let bytes = reader.take(8)?;
            visitor.visit_f64(f64::from_le_bytes(
                bytes.try_into().map_err(|_| Error::UnexpectedEof)?,
            ))
        }
        TypeId::String => visitor.visit_borrowed_str(reader.read_string()?),
        TypeId::Bytes => visitor.visit_borrowed_bytes(reader.read_bytes()?),
        TypeId::Object => reader.with_nesting(|reader| {
            let remaining = reader.read_container_len()?;
            visitor.visit_map(ObjectMapAccess {
                reader,
                remaining,
                keys: InlineKeySet::default(),
            })
        }),
        TypeId::List => reader.with_nesting(|reader| {
            let element_type = read_element_type(reader)?;
            let remaining = reader.read_container_len()?;
            visitor.visit_seq(ListSeqAccess {
                reader,
                element_type,
                remaining,
            })
        }),
        // serde never serializes a fixed-size `array` (only `list`s), so serde-encoded
        // data never contains one. Externally-built array payloads must be read
        // through the dynamic `Value`/`from_value` API instead.
        TypeId::Array => Err(Error::Serde(
            "array values are not supported by serde decoding; use the dynamic Value API"
                .to_owned(),
        )),
        TypeId::Optional => reader.with_nesting(|reader| {
            let element_type = read_element_type(reader)?;
            match reader.read_u8()? {
                0 => visitor.visit_none(),
                1 => visitor.visit_some(SobjDeserializer {
                    reader,
                    shape: Shape::Typed(element_type),
                }),
                other => Err(Error::InvalidBoolean(other)),
            }
        }),
        TypeId::Padding | TypeId::Document => Err(Error::UnsupportedTypeId(type_id.byte())),
    }
}

impl<'de, 'r, 'a> de::Deserializer<'de> for SobjDeserializer<'r, 'a>
where
    'a: 'de,
{
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let SobjDeserializer { reader, shape } = self;
        let type_id = Self::type_id(reader, &shape)?;
        deserialize_value(reader, type_id, visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let SobjDeserializer { reader, shape } = self;
        let type_id = Self::type_id(reader, &shape)?;
        match type_id {
            TypeId::String => visitor.visit_borrowed_str(reader.read_string()?),
            _ => Err(Error::Serde("expected string".to_owned())),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let SobjDeserializer { reader, shape } = self;
        let type_id = Self::type_id(reader, &shape)?;
        match type_id {
            TypeId::String => {
                visitor.visit_string(try_string_from_str(reader.read_string()?, "serde string")?)
            }
            _ => Err(Error::Serde("expected string".to_owned())),
        }
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let SobjDeserializer { reader, shape } = self;
        let type_id = Self::type_id(reader, &shape)?;
        match type_id {
            TypeId::Bytes => visitor.visit_borrowed_bytes(reader.read_bytes()?),
            _ => Err(Error::Serde("expected bytes".to_owned())),
        }
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let SobjDeserializer { reader, shape } = self;
        let type_id = Self::type_id(reader, &shape)?;
        match type_id {
            TypeId::Bytes => {
                let bytes = reader.read_bytes()?;
                let mut owned = try_vec_with_capacity(bytes.len(), "serde byte buffer")?;
                try_extend_from_slice(&mut owned, bytes, "serde byte buffer")?;
                visitor.visit_byte_buf(owned)
            }
            _ => Err(Error::Serde("expected bytes".to_owned())),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let SobjDeserializer { reader, shape } = self;
        let type_id = Self::type_id(reader, &shape)?;
        match type_id {
            TypeId::Optional => reader.with_nesting(|reader| {
                read_element_type(reader)?;
                match reader.read_u8()? {
                    0 => visitor.visit_unit(),
                    other => Err(Error::InvalidBoolean(other)),
                }
            }),
            TypeId::Object => reader.with_nesting(|reader| {
                if reader.read_container_len()? != 0 {
                    return Err(Error::Serde("expected unit".to_owned()));
                }
                visitor.visit_unit()
            }),
            _ => Err(Error::Serde("expected unit".to_owned())),
        }
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        let SobjDeserializer { reader, shape } = self;
        let type_id = Self::type_id(reader, &shape)?;
        match type_id {
            TypeId::String => {
                let variant = reader.read_string()?;
                visitor.visit_enum(EnumValueAccess {
                    variant,
                    value: None,
                })
            }
            TypeId::Object => reader.with_nesting(|reader| {
                if reader.read_container_len()? != 1 {
                    return Err(Error::Serde(
                        "expected single-field object for enum variant".to_owned(),
                    ));
                }
                let variant = reader.read_string()?;
                visitor.visit_enum(StreamEnumAccess { reader, variant })
            }),
            _ => Err(Error::Serde("expected enum representation".to_owned())),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char
        option seq tuple tuple_struct map struct identifier ignored_any
    }
}

struct ObjectMapAccess<'r, 'a> {
    reader: &'r mut Reader<'a>,
    remaining: usize,
    keys: InlineKeySet<'a>,
}

impl<'de, 'r, 'a> MapAccess<'de> for ObjectMapAccess<'r, 'a>
where
    'a: 'de,
{
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let key = self.reader.read_string()?;
        if !self.keys.insert(key, "serde object keys")? {
            return duplicate_key_error(key);
        }
        seed.deserialize(SobjStrDeserializer {
            value: key,
            context: "object key",
        })
        .map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        seed.deserialize(SobjDeserializer {
            reader: &mut *self.reader,
            shape: Shape::Element,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

struct ListSeqAccess<'r, 'a> {
    reader: &'r mut Reader<'a>,
    element_type: ElementType,
    remaining: usize,
}

impl<'de, 'r, 'a> SeqAccess<'de> for ListSeqAccess<'r, 'a>
where
    'a: 'de,
{
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(SobjDeserializer {
            reader: &mut *self.reader,
            shape: Shape::Typed(self.element_type.clone()),
        })
        .map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

/// Enum variant access for the single-field-object representation; the variant's
/// payload is the next element in the stream.
struct StreamEnumAccess<'r, 'a> {
    reader: &'r mut Reader<'a>,
    variant: &'a str,
}

impl<'de, 'r, 'a> EnumAccess<'de> for StreamEnumAccess<'r, 'a>
where
    'a: 'de,
{
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let variant = seed.deserialize(SobjStrDeserializer {
            value: self.variant,
            context: "enum variant",
        })?;
        Ok((variant, self))
    }
}

impl<'de, 'r, 'a> VariantAccess<'de> for StreamEnumAccess<'r, 'a>
where
    'a: 'de,
{
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        // Unit variants serialize as a bare string, so an object-wrapped variant
        // here should not occur; consume the payload defensively to stay aligned.
        de::Deserializer::deserialize_any(
            SobjDeserializer {
                reader: self.reader,
                shape: Shape::Element,
            },
            de::IgnoredAny,
        )
        .map(|_| ())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(SobjDeserializer {
            reader: self.reader,
            shape: Shape::Element,
        })
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        de::Deserializer::deserialize_any(
            SobjDeserializer {
                reader: self.reader,
                shape: Shape::Element,
            },
            visitor,
        )
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        de::Deserializer::deserialize_any(
            SobjDeserializer {
                reader: self.reader,
                shape: Shape::Element,
            },
            visitor,
        )
    }
}

struct ValueSerializer;

impl ser::Serializer for ValueSerializer {
    type Ok = Value;
    type Error = Error;
    type SerializeSeq = SeqSerializer;
    type SerializeTuple = SeqSerializer;
    type SerializeTupleStruct = SeqSerializer;
    type SerializeTupleVariant = TupleVariantSerializer;
    type SerializeMap = MapSerializer;
    type SerializeStruct = StructSerializer;
    type SerializeStructVariant = StructVariantSerializer;

    fn serialize_bool(self, v: bool) -> Result<Value> {
        Ok(Value::Boolean(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Value> {
        Ok(Value::Int8(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Value> {
        Ok(Value::Int16(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Value> {
        Ok(Value::Int32(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Value> {
        Ok(Value::Int64(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Value> {
        Ok(Value::Uint8(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Value> {
        Ok(Value::Uint16(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Value> {
        Ok(Value::Uint32(v))
    }

    fn serialize_u64(self, v: u64) -> Result<Value> {
        Ok(Value::Uint64(v))
    }

    fn serialize_f32(self, v: f32) -> Result<Value> {
        Ok(Value::Float(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Value> {
        Ok(Value::Double(v))
    }

    fn serialize_char(self, v: char) -> Result<Value> {
        let mut bytes = [0_u8; 4];
        Ok(Value::String(try_string_from_str(
            v.encode_utf8(&mut bytes),
            "value char",
        )?))
    }

    fn serialize_str(self, v: &str) -> Result<Value> {
        Ok(Value::String(try_string_from_str(v, "value string")?))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Value> {
        let mut bytes = try_vec_with_capacity(v.len(), "value bytes")?;
        try_extend_from_slice(&mut bytes, v, "value bytes")?;
        Ok(Value::Bytes(bytes))
    }

    fn serialize_none(self) -> Result<Value> {
        Ok(Value::optional(OptionalValue::none(ElementType::Builtin(
            TypeId::Bytes,
        ))))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Value> {
        let value = to_value(value)?;
        let element_type = element_type_for_value(&value)?;
        Value::try_optional(OptionalValue::try_some(element_type, value)?)
    }

    fn serialize_unit(self) -> Result<Value> {
        Ok(Value::optional(OptionalValue::none(ElementType::Builtin(
            TypeId::Bytes,
        ))))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Value> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Value> {
        Ok(Value::String(try_string_from_str(variant, "enum variant")?))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Value> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Value> {
        Ok(Value::Object(Object::new(vec![Field::new(
            variant,
            to_value(value)?,
        )])))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<SeqSerializer> {
        SeqSerializer::new(len)
    }

    fn serialize_tuple(self, len: usize) -> Result<SeqSerializer> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(self, _name: &'static str, len: usize) -> Result<SeqSerializer> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<TupleVariantSerializer> {
        Ok(TupleVariantSerializer {
            variant,
            inner: SeqSerializer::new(Some(len))?,
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<MapSerializer> {
        Ok(MapSerializer {
            fields: try_vec_with_capacity(len.unwrap_or(0), "value map fields")?,
            pending_key: None,
        })
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<StructSerializer> {
        Ok(StructSerializer {
            fields: try_vec_with_capacity(len, "value struct fields")?,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<StructVariantSerializer> {
        Ok(StructVariantSerializer {
            variant,
            fields: try_vec_with_capacity(len, "value struct variant fields")?,
        })
    }
}

struct SeqSerializer {
    values: Vec<Value>,
}

impl SeqSerializer {
    fn new(len: Option<usize>) -> Result<Self> {
        Ok(Self {
            values: try_vec_with_capacity(len.unwrap_or(0), "value sequence values")?,
        })
    }

    fn finish(self) -> Result<Value> {
        let element_type = common_element_type(&self.values)?;
        Value::try_list(List::new(element_type, self.values))
    }
}

impl SerializeSeq for SeqSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let value = to_value(value)?;
        try_push_vec(&mut self.values, value, "value sequence values")?;
        Ok(())
    }

    fn end(self) -> Result<Value> {
        self.finish()
    }
}

impl SerializeTuple for SeqSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Value> {
        self.finish()
    }
}

impl SerializeTupleStruct for SeqSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Value> {
        self.finish()
    }
}

struct TupleVariantSerializer {
    variant: &'static str,
    inner: SeqSerializer,
}

impl SerializeTupleVariant for TupleVariantSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        SerializeSeq::serialize_element(&mut self.inner, value)
    }

    fn end(self) -> Result<Value> {
        Ok(Value::Object(Object::new(vec![Field::new(
            self.variant,
            self.inner.finish()?,
        )])))
    }
}

struct MapSerializer {
    fields: Vec<Field>,
    pending_key: Option<String>,
}

impl SerializeMap for MapSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        self.pending_key = Some(key.serialize(KeySerializer)?);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let key = self
            .pending_key
            .take()
            .ok_or_else(|| Error::Serde("map value serialized before key".to_owned()))?;
        let value = Field::new(key, to_value(value)?);
        try_push_vec(&mut self.fields, value, "value map fields")?;
        Ok(())
    }

    fn end(self) -> Result<Value> {
        Ok(Value::Object(Object::new(self.fields)))
    }
}

struct StructSerializer {
    fields: Vec<Field>,
}

impl SerializeStruct for StructSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        let value = Field::new(key, to_value(value)?);
        try_push_vec(&mut self.fields, value, "value struct fields")?;
        Ok(())
    }

    fn end(self) -> Result<Value> {
        Ok(Value::Object(Object::new(self.fields)))
    }
}

struct StructVariantSerializer {
    variant: &'static str,
    fields: Vec<Field>,
}

impl SerializeStructVariant for StructVariantSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        let value = Field::new(key, to_value(value)?);
        try_push_vec(&mut self.fields, value, "value struct variant fields")?;
        Ok(())
    }

    fn end(self) -> Result<Value> {
        Ok(Value::Object(Object::new(vec![Field::new(
            self.variant,
            Value::Object(Object::new(self.fields)),
        )])))
    }
}

struct KeySerializer;

impl ser::Serializer for KeySerializer {
    type Ok = String;
    type Error = Error;
    type SerializeSeq = ser::Impossible<String, Error>;
    type SerializeTuple = ser::Impossible<String, Error>;
    type SerializeTupleStruct = ser::Impossible<String, Error>;
    type SerializeTupleVariant = ser::Impossible<String, Error>;
    type SerializeMap = ser::Impossible<String, Error>;
    type SerializeStruct = ser::Impossible<String, Error>;
    type SerializeStructVariant = ser::Impossible<String, Error>;

    fn serialize_bool(self, v: bool) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_i8(self, v: i8) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_i16(self, v: i16) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_i32(self, v: i32) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_i64(self, v: i64) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_u8(self, v: u8) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_u16(self, v: u16) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_u32(self, v: u32) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_u64(self, v: u64) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_f32(self, v: f32) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_f64(self, v: f64) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_char(self, v: char) -> Result<String> {
        Ok(v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<String> {
        Ok(v.to_owned())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<String> {
        Err(Error::Serde("bytes cannot be object keys".to_owned()))
    }

    fn serialize_none(self) -> Result<String> {
        Err(Error::Serde("none cannot be an object key".to_owned()))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<String> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<String> {
        Err(Error::Serde("unit cannot be an object key".to_owned()))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<String> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<String> {
        Ok(variant.to_owned())
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<String> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<String> {
        Err(Error::Serde(
            "newtype variants cannot be object keys".to_owned(),
        ))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(Error::Serde("sequences cannot be object keys".to_owned()))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(Error::Serde("tuples cannot be object keys".to_owned()))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(Error::Serde(
            "tuple structs cannot be object keys".to_owned(),
        ))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(Error::Serde(
            "tuple variants cannot be object keys".to_owned(),
        ))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(Error::Serde("maps cannot be object keys".to_owned()))
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(Error::Serde("structs cannot be object keys".to_owned()))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(Error::Serde(
            "struct variants cannot be object keys".to_owned(),
        ))
    }
}

struct ValueDeserializer<'a> {
    value: &'a Value,
}

impl<'de, 'a> de::Deserializer<'de> for ValueDeserializer<'a>
where
    'a: 'de,
{
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            Value::Boolean(value) => visitor.visit_bool(*value),
            Value::Uint8(value) => visitor.visit_u8(*value),
            Value::Int8(value) => visitor.visit_i8(*value),
            Value::Uint16(value) => visitor.visit_u16(*value),
            Value::Int16(value) => visitor.visit_i16(*value),
            Value::Uint32(value) => visitor.visit_u32(*value),
            Value::Int32(value) => visitor.visit_i32(*value),
            Value::Uint64(value) => visitor.visit_u64(*value),
            Value::Int64(value) => visitor.visit_i64(*value),
            Value::Float(value) => visitor.visit_f32(*value),
            Value::Double(value) => visitor.visit_f64(*value),
            Value::String(value) => visitor.visit_borrowed_str(value),
            Value::Bytes(value) => visitor.visit_borrowed_bytes(value),
            Value::Object(object) => visitor.visit_map(ObjectAccess::new(object)),
            Value::List(list) => visitor.visit_seq(ListAccess::new(list)),
            Value::Array(array) => visitor.visit_seq(ArrayAccess::new(array)?),
            Value::Optional(optional) => match optional.value.as_deref() {
                Some(value) => visitor.visit_some(ValueDeserializer { value }),
                None => visitor.visit_none(),
            },
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            Value::Optional(optional) => match optional.value.as_deref() {
                Some(value) => visitor.visit_some(ValueDeserializer { value }),
                None => visitor.visit_none(),
            },
            other => visitor.visit_some(ValueDeserializer { value: other }),
        }
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            Value::String(value) => visitor.visit_borrowed_str(value),
            _ => Err(Error::Serde("expected string".to_owned())),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            Value::String(value) => {
                visitor.visit_string(try_string_from_str(value, "value string")?)
            }
            _ => Err(Error::Serde("expected string".to_owned())),
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        match self.value {
            Value::String(variant) => visitor.visit_enum(EnumValueAccess {
                variant,
                value: None,
            }),
            Value::Object(object) if object.fields.len() == 1 => {
                visitor.visit_enum(EnumValueAccess {
                    variant: object.fields[0].key.as_str(),
                    value: Some(&object.fields[0].value),
                })
            }
            _ => Err(Error::Serde("expected enum representation".to_owned())),
        }
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            Value::Bytes(value) => visitor.visit_borrowed_bytes(value),
            Value::List(list) if list.element_type == ElementType::Builtin(TypeId::Uint8) => {
                let mut bytes = try_vec_with_capacity(list.values.len(), "value byte list")?;
                for value in &list.values {
                    let Value::Uint8(byte) = value else {
                        return Err(Error::TypeMismatch);
                    };
                    try_push_vec(&mut bytes, *byte, "value byte list")?;
                }
                visitor.visit_byte_buf(bytes)
            }
            _ => self.deserialize_any(visitor),
        }
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            Value::Bytes(value) => {
                let mut bytes = try_vec_with_capacity(value.len(), "value bytes")?;
                try_extend_from_slice(&mut bytes, value, "value bytes")?;
                visitor.visit_byte_buf(bytes)
            }
            Value::List(list) if list.element_type == ElementType::Builtin(TypeId::Uint8) => {
                let mut bytes = try_vec_with_capacity(list.values.len(), "value byte list")?;
                for value in &list.values {
                    let Value::Uint8(byte) = value else {
                        return Err(Error::TypeMismatch);
                    };
                    try_push_vec(&mut bytes, *byte, "value byte list")?;
                }
                visitor.visit_byte_buf(bytes)
            }
            _ => self.deserialize_bytes(visitor),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            Value::Object(object) if object.fields.is_empty() => visitor.visit_unit(),
            Value::Optional(optional) if optional.value.is_none() => visitor.visit_unit(),
            _ => Err(Error::Serde("expected unit".to_owned())),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        match self.value {
            Value::Object(object) => visitor.visit_map(ObjectAccess::new(object)),
            _ => Err(Error::Serde("expected object".to_owned())),
        }
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            Value::Object(object) => visitor.visit_map(ObjectAccess::new(object)),
            _ => Err(Error::Serde("expected object".to_owned())),
        }
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            Value::List(list) => visitor.visit_seq(ListAccess::new(list)),
            Value::Array(array) => visitor.visit_seq(ArrayAccess::new(array)?),
            _ => Err(Error::Serde("expected sequence".to_owned())),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char
        unit_struct tuple tuple_struct identifier ignored_any
    }
}

struct ObjectAccess<'a> {
    fields: &'a [Field],
    index: usize,
}

impl<'a> ObjectAccess<'a> {
    fn new(object: &'a Object) -> Self {
        Self {
            fields: &object.fields,
            index: 0,
        }
    }
}

impl<'de, 'a> MapAccess<'de> for ObjectAccess<'a>
where
    'a: 'de,
{
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        let Some(field) = self.fields.get(self.index) else {
            return Ok(None);
        };
        seed.deserialize(SobjStrDeserializer {
            value: field.key.as_str(),
            context: "object key",
        })
        .map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let field = self
            .fields
            .get(self.index)
            .ok_or_else(|| Error::Serde("map value requested after end".to_owned()))?;
        self.index += 1;
        seed.deserialize(ValueDeserializer {
            value: &field.value,
        })
    }
}

struct ListAccess<'a> {
    values: &'a [Value],
    index: usize,
}

impl<'a> ListAccess<'a> {
    fn new(list: &'a List) -> Self {
        Self {
            values: &list.values,
            index: 0,
        }
    }
}

impl<'de, 'a> SeqAccess<'de> for ListAccess<'a>
where
    'a: 'de,
{
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        let Some(value) = self.values.get(self.index) else {
            return Ok(None);
        };
        self.index += 1;
        seed.deserialize(ValueDeserializer { value }).map(Some)
    }
}

enum ArrayAccess<'a> {
    U8(std::slice::Iter<'a, u8>),
    I8(std::slice::Iter<'a, u8>),
    U16(std::slice::ChunksExact<'a, u8>),
    I16(std::slice::ChunksExact<'a, u8>),
    U32(std::slice::ChunksExact<'a, u8>),
    I32(std::slice::ChunksExact<'a, u8>),
    U64(std::slice::ChunksExact<'a, u8>),
    I64(std::slice::ChunksExact<'a, u8>),
    F32(std::slice::ChunksExact<'a, u8>),
    F64(std::slice::ChunksExact<'a, u8>),
}

impl<'a> ArrayAccess<'a> {
    fn new(array: &'a Array) -> Result<Self> {
        match array.element_type {
            ElementType::Builtin(TypeId::Uint8) => Ok(Self::U8(array.data.iter())),
            ElementType::Builtin(TypeId::Int8) => Ok(Self::I8(array.data.iter())),
            ElementType::Builtin(TypeId::Uint16) => Ok(Self::U16(array.data.chunks_exact(2))),
            ElementType::Builtin(TypeId::Int16) => Ok(Self::I16(array.data.chunks_exact(2))),
            ElementType::Builtin(TypeId::Uint32) => Ok(Self::U32(array.data.chunks_exact(4))),
            ElementType::Builtin(TypeId::Int32) => Ok(Self::I32(array.data.chunks_exact(4))),
            ElementType::Builtin(TypeId::Uint64) => Ok(Self::U64(array.data.chunks_exact(8))),
            ElementType::Builtin(TypeId::Int64) => Ok(Self::I64(array.data.chunks_exact(8))),
            ElementType::Builtin(TypeId::Float) => Ok(Self::F32(array.data.chunks_exact(4))),
            ElementType::Builtin(TypeId::Double) => Ok(Self::F64(array.data.chunks_exact(8))),
            _ => Err(Error::Serde(
                "only scalar numeric arrays deserialize as serde sequences".to_owned(),
            )),
        }
    }
}

impl<'de> SeqAccess<'de> for ArrayAccess<'_> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        let value = match self {
            Self::U8(iter) => iter.next().map(|value| Value::Uint8(*value)),
            Self::I8(iter) => iter.next().map(|value| Value::Int8(*value as i8)),
            Self::U16(iter) => iter
                .next()
                .map(|bytes| Value::Uint16(u16::from_le_bytes([bytes[0], bytes[1]]))),
            Self::I16(iter) => iter
                .next()
                .map(|bytes| Value::Int16(i16::from_le_bytes([bytes[0], bytes[1]]))),
            Self::U32(iter) => iter.next().map(|bytes| {
                Value::Uint32(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            }),
            Self::I32(iter) => iter.next().map(|bytes| {
                Value::Int32(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            }),
            Self::U64(iter) => iter.next().map(|bytes| {
                Value::Uint64(u64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]))
            }),
            Self::I64(iter) => iter.next().map(|bytes| {
                Value::Int64(i64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]))
            }),
            Self::F32(iter) => iter.next().map(|bytes| {
                Value::Float(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            }),
            Self::F64(iter) => iter.next().map(|bytes| {
                Value::Double(f64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]))
            }),
        };
        match value {
            Some(value) => seed.deserialize(ScalarDeserializer { value }).map(Some),
            None => Ok(None),
        }
    }
}

struct ScalarDeserializer {
    value: Value,
}

impl<'de> de::Deserializer<'de> for ScalarDeserializer {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            Value::Uint8(value) => visitor.visit_u8(value),
            Value::Int8(value) => visitor.visit_i8(value),
            Value::Uint16(value) => visitor.visit_u16(value),
            Value::Int16(value) => visitor.visit_i16(value),
            Value::Uint32(value) => visitor.visit_u32(value),
            Value::Int32(value) => visitor.visit_i32(value),
            Value::Uint64(value) => visitor.visit_u64(value),
            Value::Int64(value) => visitor.visit_i64(value),
            Value::Float(value) => visitor.visit_f32(value),
            Value::Double(value) => visitor.visit_f64(value),
            _ => Err(Error::TypeMismatch),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
        byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
        map struct enum identifier ignored_any
    }
}

struct EnumValueAccess<'a> {
    variant: &'a str,
    value: Option<&'a Value>,
}

impl<'de, 'a> EnumAccess<'de> for EnumValueAccess<'a>
where
    'a: 'de,
{
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let variant = seed.deserialize(SobjStrDeserializer {
            value: self.variant,
            context: "enum variant",
        })?;
        Ok((variant, self))
    }
}

impl<'de, 'a> VariantAccess<'de> for EnumValueAccess<'a>
where
    'a: 'de,
{
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        match self.value {
            None => Ok(()),
            Some(Value::Object(object)) if object.fields.is_empty() => Ok(()),
            Some(_) => Err(Error::Serde("expected unit variant".to_owned())),
        }
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        let value = self
            .value
            .ok_or_else(|| Error::Serde("expected newtype variant payload".to_owned()))?;
        seed.deserialize(ValueDeserializer { value })
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        let value = self
            .value
            .ok_or_else(|| Error::Serde("expected tuple variant payload".to_owned()))?;
        ValueDeserializer { value }.deserialize_seq(visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        let value = self
            .value
            .ok_or_else(|| Error::Serde("expected struct variant payload".to_owned()))?;
        ValueDeserializer { value }.deserialize_map(visitor)
    }
}

fn element_type_for_value(value: &Value) -> Result<ElementType> {
    Ok(match value {
        Value::Object(_) => ElementType::Object,
        Value::List(list) => {
            ElementType::List(try_box(list.element_type.clone(), "list element type")?)
        }
        Value::Array(array) => {
            ElementType::Array(try_box(array.element_type.clone(), "array element type")?)
        }
        Value::Optional(optional) => ElementType::Optional(try_box(
            optional.element_type.clone(),
            "optional element type",
        )?),
        other => ElementType::Builtin(other.type_id()),
    })
}

fn common_element_type(values: &[Value]) -> Result<ElementType> {
    let Some(first) = values.first() else {
        return Ok(ElementType::Builtin(TypeId::Bytes));
    };
    let element_type = element_type_for_value(first)?;
    let mut matches = true;
    for value in values {
        if element_type_for_value(value)? != element_type {
            matches = false;
            break;
        }
    }
    if matches {
        Ok(element_type)
    } else {
        Err(Error::TypeMismatch)
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::cell::Cell;
    use std::collections::BTreeMap;

    fn assert_same_wire(actual: &[u8], expected: &[u8], mode: LengthMode) {
        let first_difference = actual.iter().zip(expected).position(|(a, b)| a != b);
        assert!(
            actual == expected,
            "wire mismatch ({mode:?}): actual {} bytes, expected {} bytes, first differing byte {:?}",
            actual.len(),
            expected.len(),
            first_difference.or_else(
                || (actual.len() != expected.len()).then_some(actual.len().min(expected.len()))
            ),
        );
    }

    /// The streamed bytes must be byte-identical to the `Value`-tree path so the
    /// document format is unchanged and old/new peers interoperate.
    fn assert_wire_equivalent_and_round_trips<T>(value: &T)
    where
        T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        for mode in [LengthMode::U16, LengthMode::U32, LengthMode::U64] {
            let streamed = to_bytes(value, 1, mode).expect("stream encode");
            let tree = to_document(value, 1, mode)
                .expect("tree build")
                .to_bytes()
                .expect("tree encode");
            assert_same_wire(&streamed, &tree, mode);
            let decoded: T = from_bytes(&streamed).expect("decode");
            assert_eq!(&decoded, value, "round-trip mismatch (mode {mode:?})");
            let chunked: T = from_chunk_iter_with_options(
                streamed.chunks(3).map(Ok),
                streamed.len(),
                DecodeOptions::default().with_expected_version_id(1),
            )
            .expect("decode canonical value across chunk boundaries");
            assert_eq!(&chunked, value, "chunk round-trip mismatch (mode {mode:?})");
        }
    }

    fn assert_optional_encoding_error_and_rollback<T: Serialize>(value: &T, expected: Error) {
        for mode in [LengthMode::U16, LengthMode::U32, LengthMode::U64] {
            let mut output = b"prefix".to_vec();
            let error = append_bytes(&mut output, &Some(value), 1, mode)
                .expect_err("invalid optional payload");
            assert_eq!(error, expected, "length mode {mode:?}");
            assert_eq!(output, b"prefix");
        }
    }

    #[test]
    fn type_inspection_does_not_serialize_object_fields() {
        struct CountedBytes<'a> {
            calls: &'a Cell<usize>,
            bytes: &'a [u8],
        }
        impl Serialize for CountedBytes<'_> {
            fn serialize<S: ser::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                self.calls.set(self.calls.get() + 1);
                serializer.serialize_bytes(self.bytes)
            }
        }
        #[derive(Serialize)]
        struct Large<'a> {
            first: CountedBytes<'a>,
            second: CountedBytes<'a>,
        }

        let calls = Cell::new(0);
        let payload: Vec<u8> = (0..4096).map(|index| index as u8).collect();
        let large = || Large {
            first: CountedBytes {
                calls: &calls,
                bytes: &payload,
            },
            second: CountedBytes {
                calls: &calls,
                bytes: &payload,
            },
        };
        let nested = Some(vec![large(), large()]);
        for mode in [LengthMode::U16, LengthMode::U32, LengthMode::U64] {
            calls.set(0);
            let encoded = to_bytes(&nested, 1, mode).expect("encode nested payload");
            assert_eq!(
                calls.get(),
                4,
                "object fields visited again during inspection"
            );
            calls.set(0);
            let expected = to_document(&nested, 1, mode).unwrap().to_bytes().unwrap();
            assert_eq!(calls.get(), 4);
            assert_same_wire(&encoded, &expected, mode);
        }
    }

    #[test]
    fn collected_sequence_probe_skips_tail_and_emission_preserves_bytes() {
        struct CountedItem<'a> {
            value: u8,
            calls: &'a Cell<usize>,
        }

        impl Serialize for CountedItem<'_> {
            fn serialize<S: ser::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                self.calls.set(self.calls.get() + 1);
                serializer.serialize_u8(self.value)
            }
        }

        struct CountedIterator<'a> {
            remaining: &'a [u8],
            next_calls: &'a Cell<usize>,
            item_calls: &'a Cell<usize>,
        }

        impl<'a> Iterator for CountedIterator<'a> {
            type Item = CountedItem<'a>;

            fn next(&mut self) -> Option<Self::Item> {
                self.next_calls.set(self.next_calls.get() + 1);
                let (&value, tail) = self.remaining.split_first()?;
                self.remaining = tail;
                Some(CountedItem {
                    value,
                    calls: self.item_calls,
                })
            }
        }

        struct CollectedSequence<'a> {
            values: &'a [u8],
            next_calls: &'a Cell<usize>,
            item_calls: &'a Cell<usize>,
        }

        impl Serialize for CollectedSequence<'_> {
            fn serialize<S: ser::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                serializer.collect_seq(CountedIterator {
                    remaining: self.values,
                    next_calls: self.next_calls,
                    item_calls: self.item_calls,
                })
            }
        }

        let values: Vec<u8> = (0..64).collect();
        for values in [&[][..], values.as_slice()] {
            let next_calls = Cell::new(0);
            let item_calls = Cell::new(0);
            let collected = CollectedSequence {
                values,
                next_calls: &next_calls,
                item_calls: &item_calls,
            };
            let expected_inner = if values.is_empty() {
                TypeId::Bytes
            } else {
                TypeId::Uint8
            };
            assert_eq!(
                probe_element_type(&collected).unwrap(),
                ElementType::List(Box::new(ElementType::Builtin(expected_inner)))
            );
            assert_eq!(next_calls.get(), 1, "descriptor iterated an unused tail");
            assert_eq!(item_calls.get(), usize::from(!values.is_empty()));

            for mode in [LengthMode::U16, LengthMode::U32, LengthMode::U64] {
                next_calls.set(0);
                item_calls.set(0);
                let encoded = to_bytes(&collected, 1, mode).unwrap();
                assert_eq!(next_calls.get(), values.len() + 1);
                // Emission probes its first item's descriptor, then emits all items.
                assert_eq!(
                    item_calls.get(),
                    values.len() + usize::from(!values.is_empty())
                );
                assert_same_wire(
                    &encoded,
                    &to_bytes(&values.to_vec(), 1, mode).unwrap(),
                    mode,
                );

                next_calls.set(0);
                item_calls.set(0);
                let encoded = to_bytes(&Some(&collected), 1, mode).unwrap();
                assert_eq!(next_calls.get(), values.len() + 2);
                assert_eq!(
                    item_calls.get(),
                    values.len() + 2 * usize::from(!values.is_empty())
                );
                let expected = Some(values.to_vec());
                assert_same_wire(&encoded, &to_bytes(&expected, 1, mode).unwrap(), mode);
                assert_eq!(from_bytes::<Option<Vec<u8>>>(&encoded).unwrap(), expected);
                assert_eq!(
                    from_chunk_iter_with_options::<Option<Vec<u8>>, _, _>(
                        encoded.chunks(1).map(Ok),
                        encoded.len(),
                        DecodeOptions::default(),
                    )
                    .unwrap(),
                    expected
                );
            }
        }
    }

    #[test]
    fn streaming_accessors_expose_size_hints_for_fast_preallocation() {
        let mut map_reader = Reader::with_limits(&[], LengthMode::U64, Default::default());
        let map = ObjectMapAccess {
            reader: &mut map_reader,
            remaining: 7,
            keys: InlineKeySet::default(),
        };
        assert_eq!(map.size_hint(), Some(7));

        let mut seq_reader = Reader::with_limits(&[], LengthMode::U64, Default::default());
        let seq = ListSeqAccess {
            reader: &mut seq_reader,
            element_type: ElementType::Builtin(TypeId::Uint8),
            remaining: 11,
        };
        assert_eq!(seq.size_hint(), Some(11));
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Shape {
        Unit,
        Newtype(u32),
        // Homogeneous: serde encodes tuples as a `list<T>`, so a heterogeneous tuple
        // like `(u8, i32)` is unrepresentable in this format (rejected by both the
        // streaming and dynamic `Value` encoders). Use structs for heterogeneity.
        Tuple(i32, i32),
        Struct { x: i32, label: String },
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Everything {
        b: bool,
        i8: i8,
        i16: i16,
        i32: i32,
        i64: i64,
        u8: u8,
        u16: u16,
        u32: u32,
        u64: u64,
        f32: f32,
        f64: f64,
        c: char,
        s: String,
        unit: (),
        bytes_list: Vec<u8>,
        ints: Vec<i32>,
        pairs: Vec<(i32, i32)>,
        fixed: [u8; 4],
        opt_some: Option<String>,
        opt_none: Option<String>,
        nested: Vec<Vec<u8>>,
        map: BTreeMap<String, u32>,
        shapes: Vec<Shape>,
    }

    fn sample() -> Everything {
        let mut map = BTreeMap::new();
        map.insert("one".to_owned(), 1);
        map.insert("two".to_owned(), 2);
        Everything {
            b: true,
            i8: -5,
            i16: -300,
            i32: -70_000,
            i64: -5_000_000_000,
            u8: 250,
            u16: 60_000,
            u32: 4_000_000_000,
            u64: 18_000_000_000_000_000_000,
            f32: 1.5,
            f64: -2.25,
            c: 'λ',
            s: "hello".to_owned(),
            unit: (),
            bytes_list: vec![0, 1, 2, 250, 255],
            ints: vec![-1, 0, 7, 12345],
            pairs: vec![(1, 2), (3, 4)],
            fixed: [9, 8, 7, 6],
            opt_some: Some("present".to_owned()),
            opt_none: None,
            // Inner lists are non-empty: an empty inner `Vec<u8>` defaults its element
            // type to `bytes`, making the outer `list` heterogeneous with `list<uint8>`
            // siblings — a homogeneous-list-format limitation shared by the streaming
            // and dynamic `Value` encoders alike.
            nested: vec![vec![1, 2, 3], vec![4, 5], vec![255]],
            map,
            // All data-carrying variants (each encodes as an object), so the list is
            // homogeneous. A `Unit` variant encodes as a bare string, which would make
            // a `Vec<Shape>` heterogeneous (string vs object) and thus unrepresentable
            // — `Shape::Unit` is exercised on its own in the scalars test instead.
            shapes: vec![
                Shape::Newtype(42),
                Shape::Tuple(1, -2),
                Shape::Struct {
                    x: -9,
                    label: "s".to_owned(),
                },
            ],
        }
    }

    #[test]
    fn everything_is_wire_equivalent_and_round_trips() {
        assert_wire_equivalent_and_round_trips(&sample());
    }

    #[test]
    fn scalars_and_collections_are_wire_equivalent() {
        assert_wire_equivalent_and_round_trips(&());
        assert_wire_equivalent_and_round_trips(&true);
        assert_wire_equivalent_and_round_trips(&-12345_i32);
        assert_wire_equivalent_and_round_trips(&"a string".to_owned());
        assert_wire_equivalent_and_round_trips(&Vec::<u8>::new());
        assert_wire_equivalent_and_round_trips(&vec![1_u8, 2, 3]);
        assert_wire_equivalent_and_round_trips(&vec![vec![1_i32, 2], vec![3]]);
        assert_wire_equivalent_and_round_trips(&Some(vec![9_u8, 8, 7]));
        assert_wire_equivalent_and_round_trips(&Option::<u32>::None);
        assert_wire_equivalent_and_round_trips(&Shape::Unit);
        assert_wire_equivalent_and_round_trips(&Shape::Newtype(7));
        assert_wire_equivalent_and_round_trips(&Shape::Tuple(1, -2));
        assert_wire_equivalent_and_round_trips(&Some(vec![Vec::<u8>::new()]));
        assert_wire_equivalent_and_round_trips(&Some(Shape::Unit));
        assert_wire_equivalent_and_round_trips(&Some(Shape::Newtype(7)));
        assert_wire_equivalent_and_round_trips(&Some(Shape::Tuple(1, -2)));
        assert_wire_equivalent_and_round_trips(&Some(Shape::Struct {
            x: 3,
            label: "descriptor".to_owned(),
        }));
    }

    #[test]
    fn legacy_typed_optional_bytes_remain_compatible_with_paired_serde_readers() {
        // These pre-existing bytes repeat a typed optional's descriptor. The
        // canonical tree decoder rejects them, but runtime DTOs and stored KV
        // values use the paired serde codecs. Preserve that contract here;
        // correcting the format needs a separate versioned protocol change.
        fn assert_legacy<T>(value: &T, mode: LengthMode, payload: &[u8])
        where
            T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
        {
            let mut golden = vec![b'S', b'O', b'B', b'J', 1, 0, mode.code(), 0];
            let document_bytes = (8 + 2 * mode.width() + payload.len()) as u64;
            golden.extend_from_slice(&document_bytes.to_le_bytes()[..mode.width()]);
            golden.resize(golden.len() + mode.width(), 0); // empty schema
            golden.extend_from_slice(payload);
            assert_same_wire(&to_bytes(value, 1, mode).unwrap(), &golden, mode);
            let decoded: T = from_bytes(&golden).expect("decode legacy optional bytes");
            assert_eq!(&decoded, value);
            let decoded: T = from_chunk_iter_with_options(
                golden.chunks(1).map(Ok),
                golden.len(),
                DecodeOptions::default(),
            )
            .expect("decode segmented legacy optional bytes");
            assert_eq!(&decoded, value);
            assert!(
                crate::DocumentView::parse(&golden).is_err(),
                "fixture must expose the pre-existing noncanonical encoding"
            );
        }
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Item {
            a: u8,
        }
        for mode in [LengthMode::U16, LengthMode::U32, LengthMode::U64] {
            let absent = [0x26, 0x44, 0x25, 1, 0x25, 0];
            assert_legacy(&Some(()), mode, &absent);
            assert_legacy(&Some(Option::<u8>::None), mode, &absent);
            assert_legacy(
                &Some(Some(Some(1_u8))),
                mode,
                &[0x26, 0x44, 0x44, 0x02, 1, 0x44, 0x02, 1, 0x02, 1, 1],
            );
            let mut absent_list = vec![0x22, 0x44, 0x25];
            absent_list.extend_from_slice(&2_u64.to_le_bytes()[..mode.width()]);
            absent_list.extend_from_slice(&[0x25, 0, 0x25, 0]);
            assert_legacy(&vec![(), ()], mode, &absent_list);
            assert_legacy(&vec![Option::<u8>::None, None], mode, &absent_list);
            let mut present_list = vec![0x22, 0x44, 0x02];
            present_list.extend_from_slice(&2_u64.to_le_bytes()[..mode.width()]);
            present_list.extend_from_slice(&[0x02, 1, 1, 0x02, 1, 2]);
            assert_legacy(&vec![Some(1_u8), Some(2)], mode, &present_list);
            let mut objects = vec![0x22, 0x44, 0x43];
            objects.extend_from_slice(&2_u64.to_le_bytes()[..mode.width()]);
            for value in [1, 2] {
                objects.extend_from_slice(&[0x43, 1]);
                objects.extend_from_slice(&1_u64.to_le_bytes()[..mode.width()]);
                objects.extend_from_slice(&1_u64.to_le_bytes()[..mode.width()]);
                objects.extend_from_slice(&[b'a', 0x02, value]);
            }
            assert_legacy(
                &vec![Some(Item { a: 1 }), Some(Item { a: 2 })],
                mode,
                &objects,
            );
        }
    }

    #[test]
    fn borrowed_decode_returns_str_and_bytes_from_document_buffer() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct BorrowedPacket<'a> {
            label: &'a str,
            payload: &'a [u8],
            note: Option<&'a str>,
        }

        let document = Document::dynamic(
            1,
            LengthMode::U32,
            Value::Object(Object::new(vec![
                Field::new("label", Value::String("sensor-a".to_owned())),
                Field::new("payload", Value::Bytes(vec![0, 1, 2, 250, 255])),
                Field::new(
                    "note",
                    Value::optional(OptionalValue::some(
                        ElementType::Builtin(TypeId::String),
                        Value::String("warm".to_owned()),
                    )),
                ),
            ])),
        );
        let bytes = document.to_bytes().expect("encode");

        let decoded: BorrowedPacket<'_> = from_borrowed_bytes(&bytes).expect("borrowed decode");

        assert_eq!(
            decoded,
            BorrowedPacket {
                label: "sensor-a",
                payload: &[0, 1, 2, 250, 255],
                note: Some("warm"),
            }
        );
        assert!(slice_is_borrowed_from(decoded.label.as_bytes(), &bytes));
        assert!(slice_is_borrowed_from(decoded.payload, &bytes));
        assert!(slice_is_borrowed_from(
            decoded.note.expect("note").as_bytes(),
            &bytes
        ));
    }

    #[test]
    fn append_bytes_preserves_existing_buffer_and_rolls_back_on_error() {
        #[derive(Serialize)]
        struct Unsupported {
            values: (u8, u16),
        }

        let value = sample();
        let expected = to_bytes(&value, 1, LengthMode::U32).expect("standalone");
        let mut out = b"prefix".to_vec();
        let range = append_bytes(&mut out, &value, 1, LengthMode::U32).expect("append");

        assert_eq!(&out[..6], b"prefix");
        assert_eq!(&out[range.clone()], expected.as_slice());
        let decoded: Everything = from_bytes(&out[range]).expect("decode appended");
        assert_eq!(decoded, value);

        let before = out.clone();
        let error = append_bytes(
            &mut out,
            &Unsupported { values: (1, 2) },
            1,
            LengthMode::U32,
        )
        .expect_err("heterogeneous tuple should fail");
        assert_eq!(error, Error::TypeMismatch);
        assert_eq!(out, before);
        assert_optional_encoding_error_and_rollback(
            &Unsupported { values: (1, 2) },
            Error::TypeMismatch,
        );

        let invalid_keys = BTreeMap::from([(vec![1_u8], 2_u8)]);
        assert_optional_encoding_error_and_rollback(
            &invalid_keys,
            Error::Serde("sequences cannot be object keys".to_owned()),
        );
        assert_optional_encoding_error_and_rollback(
            &vec![Shape::Unit, Shape::Newtype(1)],
            Error::TypeMismatch,
        );
        for mixed in [vec![Some(1_u8), None], vec![None, Some(1_u8)]] {
            assert_eq!(
                to_document(&mixed, 1, LengthMode::U32),
                Err(Error::TypeMismatch)
            );
            assert_optional_encoding_error_and_rollback(&mixed, Error::TypeMismatch);
        }
    }

    #[test]
    fn serde_decode_options_enforce_version_and_limits() {
        let encoded = to_bytes(&vec![1_u8, 2, 3, 4], 7, LengthMode::U32).expect("encode");
        let version_error = from_bytes_with_options::<Vec<u8>>(
            &encoded,
            DecodeOptions::default().with_expected_version_id(8),
        )
        .expect_err("version mismatch");
        assert_eq!(
            version_error,
            Error::UnexpectedVersionId {
                expected: 8,
                actual: 7,
            }
        );

        let limit_error = from_bytes_with_options::<Vec<u8>>(
            &encoded,
            DecodeOptions::new(crate::DecodeLimits {
                max_container_items: 3,
                ..Default::default()
            }),
        )
        .expect_err("container limit");
        assert!(matches!(
            limit_error,
            Error::DecodeLimitExceeded {
                limit: "container items",
                max: 3,
                actual: 4,
            }
        ));
    }

    #[test]
    fn serde_map_encoder_rejects_duplicate_keys() {
        struct DuplicateKeyMap;

        impl Serialize for DuplicateKeyMap {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("same", &1_u8)?;
                map.serialize_entry("same", &2_u8)?;
                map.end()
            }
        }

        assert!(matches!(
            to_bytes(&DuplicateKeyMap, 1, LengthMode::U32),
            Err(Error::DuplicateKey(key)) if key == "same"
        ));
        assert_optional_encoding_error_and_rollback(
            &DuplicateKeyMap,
            Error::DuplicateKey("same".to_owned()),
        );
    }

    #[test]
    fn serde_struct_encoder_rejects_duplicate_and_incorrect_field_counts() {
        struct DuplicateKeyStruct;

        impl Serialize for DuplicateKeyStruct {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut value = serializer.serialize_struct("DuplicateKeyStruct", 2)?;
                value.serialize_field("same", &1_u8)?;
                value.serialize_field("same", &2_u8)?;
                value.end()
            }
        }

        struct ShortStruct;

        impl Serialize for ShortStruct {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut value = serializer.serialize_struct("ShortStruct", 2)?;
                value.serialize_field("only", &1_u8)?;
                value.end()
            }
        }

        assert!(matches!(
            to_bytes(&DuplicateKeyStruct, 1, LengthMode::U32),
            Err(Error::DuplicateKey(key)) if key == "same"
        ));
        assert!(matches!(
            to_bytes(&ShortStruct, 1, LengthMode::U32),
            Err(Error::Serde(detail)) if detail.contains("declared 2 fields but serialized 1")
        ));
        assert_optional_encoding_error_and_rollback(
            &DuplicateKeyStruct,
            Error::DuplicateKey("same".to_owned()),
        );
        assert_optional_encoding_error_and_rollback(
            &ShortStruct,
            Error::Serde("struct declared 2 fields but serialized 1".to_owned()),
        );
    }

    #[test]
    fn serde_map_decoder_rejects_duplicate_keys_before_visiting_target() {
        let mut map = BTreeMap::new();
        map.insert("keya".to_owned(), 1_u8);
        map.insert("keyb".to_owned(), 2_u8);
        let mut encoded = to_bytes(&map, 1, LengthMode::U32).expect("encode map");
        let key_offset = encoded
            .windows(4)
            .position(|window| window == b"keyb")
            .expect("second key");
        encoded[key_offset..key_offset + 4].copy_from_slice(b"keya");

        assert!(matches!(
            from_bytes::<BTreeMap<String, u8>>(&encoded),
            Err(Error::DuplicateKey(key)) if key == "keya"
        ));
    }

    #[test]
    fn large_byte_list_round_trips_without_per_element_blowup() {
        // ~4 MiB list<uint8>. If each element materialized an ~80-byte Value this
        // would allocate hundreds of MiB; streaming keeps it ~O(data).
        let data: Vec<u8> = (0..4_000_000_u32).map(|i| i as u8).collect();
        let encoded = to_bytes(&data, 1, LengthMode::U32).expect("encode");
        assert!(
            encoded.len() < data.len() + 64,
            "expected compact list<uint8>"
        );
        let decoded: Vec<u8> = from_bytes(&encoded).expect("decode");
        assert_eq!(decoded, data);
    }

    fn slice_is_borrowed_from(slice: &[u8], source: &[u8]) -> bool {
        let source_start = source.as_ptr() as usize;
        let source_end = source_start + source.len();
        let slice_start = slice.as_ptr() as usize;
        let slice_end = slice_start + slice.len();
        slice_start >= source_start && slice_end <= source_end
    }
}
