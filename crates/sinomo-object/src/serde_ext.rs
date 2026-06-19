use serde::Serialize;
use serde::de::Deserializer as _;
use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};
use serde::ser::{
    self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};

use crate::{
    AlignmentHint, Array, Document, ElementType, Error, Field, LengthMode, List, MAGIC, Object,
    OptionalValue, Reader, Result, TypeId, Value, encode_element_type, patch_length,
    read_element_type, read_schema, write_bytes, write_length, write_string, write_u16,
};

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
    out.extend_from_slice(MAGIC);
    write_u16(&mut out, version_id);
    out.push(length_mode.code());
    out.push(AlignmentHint::NotAligned.code());
    let length_offset = out.len();
    write_length(&mut out, length_mode, 0)?; // document length, patched below
    write_length(&mut out, length_mode, 0)?; // empty dynamic schema (0 types)
    serialize_element_into(&mut out, length_mode, value)?;
    let document_length = u64::try_from(out.len()).map_err(|_| Error::LengthTooLarge)?;
    length_mode.validate(document_length)?;
    patch_length(
        &mut out[length_offset..length_offset + length_mode.width()],
        length_mode,
        document_length,
    )?;
    Ok(out)
}

pub fn from_bytes<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    from_borrowed_bytes(bytes)
}

pub fn from_borrowed_bytes<'de, T>(bytes: &'de [u8]) -> Result<T>
where
    T: serde::Deserialize<'de>,
{
    // Stream the value straight out of the document buffer rather than parsing it
    // into an `ElementView`/`Value` tree first.
    let mut reader = Reader::new(bytes, LengthMode::U64);
    if reader.take(4)? != MAGIC {
        return Err(Error::InvalidMagic);
    }
    let _version_id = reader.read_u16()?;
    let length_mode = LengthMode::from_code(reader.read_u8()?)?;
    reader.mode = length_mode;
    let _alignment_hint = AlignmentHint::from_code(reader.read_u8()?)?;
    let document_length = reader.read_length()?;
    let actual = u64::try_from(bytes.len()).map_err(|_| Error::LengthTooLarge)?;
    if document_length != actual {
        return Err(Error::DocumentLengthMismatch {
            expected: document_length,
            actual,
        });
    }
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
fn serialize_element_into<T: Serialize + ?Sized>(
    out: &mut Vec<u8>,
    mode: LengthMode,
    value: &T,
) -> Result<()> {
    let type_id_offset = out.len();
    out.push(0); // patched once the element type is known
    let element_type = value.serialize(ByteSerializer { out, mode })?;
    out[type_id_offset] = element_type_id(&element_type)?;
    Ok(())
}

/// serde `Serializer` that writes a value's bytes straight into `out` and returns
/// the value's `ElementType` (so a containing list/optional/element can frame it).
struct ByteSerializer<'a> {
    out: &'a mut Vec<u8>,
    mode: LengthMode,
}

impl<'a> ByteSerializer<'a> {
    fn builtin(self, type_id: TypeId, write: impl FnOnce(&mut Vec<u8>)) -> Result<ElementType> {
        write(self.out);
        Ok(ElementType::Builtin(type_id))
    }
}

impl<'a> ser::Serializer for ByteSerializer<'a> {
    type Ok = ElementType;
    type Error = Error;
    type SerializeSeq = ByteSeqSerializer<'a>;
    type SerializeTuple = ByteSeqSerializer<'a>;
    type SerializeTupleStruct = ByteSeqSerializer<'a>;
    type SerializeTupleVariant = ByteVariantSeqSerializer<'a>;
    type SerializeMap = ByteMapSerializer<'a>;
    type SerializeStruct = ByteStructSerializer<'a>;
    type SerializeStructVariant = ByteVariantStructSerializer<'a>;

    fn serialize_bool(self, v: bool) -> Result<ElementType> {
        self.builtin(TypeId::Boolean, |out| out.push(u8::from(v)))
    }

    fn serialize_i8(self, v: i8) -> Result<ElementType> {
        self.builtin(TypeId::Int8, |out| out.push(v as u8))
    }

    fn serialize_i16(self, v: i16) -> Result<ElementType> {
        self.builtin(TypeId::Int16, |out| out.extend_from_slice(&v.to_le_bytes()))
    }

    fn serialize_i32(self, v: i32) -> Result<ElementType> {
        self.builtin(TypeId::Int32, |out| out.extend_from_slice(&v.to_le_bytes()))
    }

    fn serialize_i64(self, v: i64) -> Result<ElementType> {
        self.builtin(TypeId::Int64, |out| out.extend_from_slice(&v.to_le_bytes()))
    }

    fn serialize_u8(self, v: u8) -> Result<ElementType> {
        self.builtin(TypeId::Uint8, |out| out.push(v))
    }

    fn serialize_u16(self, v: u16) -> Result<ElementType> {
        self.builtin(TypeId::Uint16, |out| {
            out.extend_from_slice(&v.to_le_bytes())
        })
    }

    fn serialize_u32(self, v: u32) -> Result<ElementType> {
        self.builtin(TypeId::Uint32, |out| {
            out.extend_from_slice(&v.to_le_bytes())
        })
    }

    fn serialize_u64(self, v: u64) -> Result<ElementType> {
        self.builtin(TypeId::Uint64, |out| {
            out.extend_from_slice(&v.to_le_bytes())
        })
    }

    fn serialize_f32(self, v: f32) -> Result<ElementType> {
        self.builtin(TypeId::Float, |out| out.extend_from_slice(&v.to_le_bytes()))
    }

    fn serialize_f64(self, v: f64) -> Result<ElementType> {
        self.builtin(TypeId::Double, |out| {
            out.extend_from_slice(&v.to_le_bytes())
        })
    }

    fn serialize_char(self, v: char) -> Result<ElementType> {
        self.serialize_str(v.encode_utf8(&mut [0_u8; 4]))
    }

    fn serialize_str(self, v: &str) -> Result<ElementType> {
        write_string(self.out, self.mode, v)?;
        Ok(ElementType::Builtin(TypeId::String))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<ElementType> {
        write_bytes(self.out, self.mode, v)?;
        Ok(ElementType::Builtin(TypeId::Bytes))
    }

    fn serialize_none(self) -> Result<ElementType> {
        // Absent optional: element type defaults to `bytes`, presence byte 0.
        let inner = ElementType::Builtin(TypeId::Bytes);
        encode_element_type(self.out, self.mode, &inner)?;
        self.out.push(0);
        Ok(ElementType::Optional(Box::new(inner)))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<ElementType> {
        // The optional's element type prefixes its value, so serialize the inner
        // value to a small scratch buffer to learn its type, then frame it.
        let mut scratch = Vec::new();
        let inner = value.serialize(ByteSerializer {
            out: &mut scratch,
            mode: self.mode,
        })?;
        encode_element_type(self.out, self.mode, &inner)?;
        self.out.push(1);
        self.out.extend_from_slice(&scratch);
        Ok(ElementType::Optional(Box::new(inner)))
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
        write_length(self.out, self.mode, 1)?;
        write_string(self.out, self.mode, variant)?;
        serialize_element_into(self.out, self.mode, value)?;
        Ok(ElementType::Object)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<ByteSeqSerializer<'a>> {
        Ok(ByteSeqSerializer::new(self.out, self.mode, len))
    }

    fn serialize_tuple(self, len: usize) -> Result<ByteSeqSerializer<'a>> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<ByteSeqSerializer<'a>> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<ByteVariantSeqSerializer<'a>> {
        // `{ variant: [ .. ] }`: outer object header, then the list value.
        write_length(self.out, self.mode, 1)?;
        write_string(self.out, self.mode, variant)?;
        self.out.push(TypeId::List.byte());
        Ok(ByteVariantSeqSerializer {
            inner: ByteSeqSerializer::new(self.out, self.mode, Some(_len)),
        })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<ByteMapSerializer<'a>> {
        let length_offset = self.out.len();
        write_length(self.out, self.mode, 0)?; // patched in `end`
        Ok(ByteMapSerializer {
            out: self.out,
            mode: self.mode,
            length_offset,
            count: 0,
            pending_key: None,
        })
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<ByteStructSerializer<'a>> {
        write_length(self.out, self.mode, len as u64)?;
        Ok(ByteStructSerializer {
            out: self.out,
            mode: self.mode,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<ByteVariantStructSerializer<'a>> {
        // `{ variant: { .. } }`: outer object header, then the inner object value.
        write_length(self.out, self.mode, 1)?;
        write_string(self.out, self.mode, variant)?;
        self.out.push(TypeId::Object.byte());
        write_length(self.out, self.mode, len as u64)?;
        Ok(ByteVariantStructSerializer {
            inner: ByteStructSerializer {
                out: self.out,
                mode: self.mode,
            },
        })
    }
}

/// Streams a homogeneous sequence into `out` as `list<element_type>`. Only the
/// first element is staged in a scratch buffer (to discover the element type that
/// must be written before any values); every later element is written straight
/// into `out`. Memory stays O(serialized size) regardless of element count.
struct ByteSeqSerializer<'a> {
    out: &'a mut Vec<u8>,
    mode: LengthMode,
    element_type: Option<ElementType>,
    length_offset: usize,
    count: u64,
}

impl<'a> ByteSeqSerializer<'a> {
    fn new(out: &'a mut Vec<u8>, mode: LengthMode, _len: Option<usize>) -> Self {
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
                let mut scratch = Vec::new();
                let element_type = value.serialize(ByteSerializer {
                    out: &mut scratch,
                    mode: self.mode,
                })?;
                encode_element_type(self.out, self.mode, &element_type)?;
                self.length_offset = self.out.len();
                write_length(self.out, self.mode, 0)?; // patched in `finish`
                self.out.extend_from_slice(&scratch);
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
                let end = self.length_offset + self.mode.width();
                patch_length(
                    &mut self.out[self.length_offset..end],
                    self.mode,
                    self.count,
                )?;
                element_type
            }
            None => {
                // Empty sequence: element type defaults to `bytes`, length 0.
                let element_type = ElementType::Builtin(TypeId::Bytes);
                encode_element_type(self.out, self.mode, &element_type)?;
                write_length(self.out, self.mode, 0)?;
                element_type
            }
        };
        Ok(ElementType::List(Box::new(element_type)))
    }
}

impl<'a> SerializeSeq for ByteSeqSerializer<'a> {
    type Ok = ElementType;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.push(value)
    }

    fn end(self) -> Result<ElementType> {
        self.finish()
    }
}

impl<'a> SerializeTuple for ByteSeqSerializer<'a> {
    type Ok = ElementType;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.push(value)
    }

    fn end(self) -> Result<ElementType> {
        self.finish()
    }
}

impl<'a> SerializeTupleStruct for ByteSeqSerializer<'a> {
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
struct ByteVariantSeqSerializer<'a> {
    inner: ByteSeqSerializer<'a>,
}

impl<'a> SerializeTupleVariant for ByteVariantSeqSerializer<'a> {
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
struct ByteMapSerializer<'a> {
    out: &'a mut Vec<u8>,
    mode: LengthMode,
    length_offset: usize,
    count: u64,
    pending_key: Option<String>,
}

impl<'a> SerializeMap for ByteMapSerializer<'a> {
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
        write_string(self.out, self.mode, &key)?;
        serialize_element_into(self.out, self.mode, value)?;
        self.count += 1;
        Ok(())
    }

    fn end(self) -> Result<ElementType> {
        let end = self.length_offset + self.mode.width();
        patch_length(
            &mut self.out[self.length_offset..end],
            self.mode,
            self.count,
        )?;
        Ok(ElementType::Object)
    }
}

/// Streams a struct into `out` as an object. The field count is known up front, so
/// the object length is written immediately and each field is appended in place.
struct ByteStructSerializer<'a> {
    out: &'a mut Vec<u8>,
    mode: LengthMode,
}

impl<'a> ByteStructSerializer<'a> {
    fn field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> Result<()> {
        write_string(self.out, self.mode, key)?;
        serialize_element_into(self.out, self.mode, value)
    }
}

impl<'a> SerializeStruct for ByteStructSerializer<'a> {
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
        Ok(ElementType::Object)
    }
}

/// Struct-enum-variant: an inner object wrapped in a single-field object.
struct ByteVariantStructSerializer<'a> {
    inner: ByteStructSerializer<'a>,
}

impl<'a> SerializeStructVariant for ByteVariantStructSerializer<'a> {
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
        Ok(ElementType::Object)
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
        TypeId::Object => {
            let remaining = reader.read_len_usize()?;
            visitor.visit_map(ObjectMapAccess { reader, remaining })
        }
        TypeId::List => {
            let element_type = read_element_type(reader)?;
            let remaining = reader.read_len_usize()?;
            visitor.visit_seq(ListSeqAccess {
                reader,
                element_type,
                remaining,
            })
        }
        // serde never serializes a fixed-size `array` (only `list`s), so serde-encoded
        // data never contains one. Externally-built array payloads must be read
        // through the dynamic `Value`/`from_value` API instead.
        TypeId::Array => Err(Error::Serde(
            "array values are not supported by serde decoding; use the dynamic Value API"
                .to_owned(),
        )),
        TypeId::Optional => {
            let element_type = read_element_type(reader)?;
            match reader.read_u8()? {
                0 => visitor.visit_none(),
                1 => visitor.visit_some(SobjDeserializer {
                    reader,
                    shape: Shape::Typed(element_type),
                }),
                other => Err(Error::InvalidBoolean(other)),
            }
        }
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

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let SobjDeserializer { reader, shape } = self;
        let type_id = Self::type_id(reader, &shape)?;
        match type_id {
            TypeId::Optional => {
                read_element_type(reader)?;
                match reader.read_u8()? {
                    0 => visitor.visit_unit(),
                    other => Err(Error::InvalidBoolean(other)),
                }
            }
            TypeId::Object => {
                if reader.read_len_usize()? != 0 {
                    return Err(Error::Serde("expected unit".to_owned()));
                }
                visitor.visit_unit()
            }
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
                visitor.visit_enum(variant.into_deserializer())
            }
            TypeId::Object => {
                if reader.read_len_usize()? != 1 {
                    return Err(Error::Serde(
                        "expected single-field object for enum variant".to_owned(),
                    ));
                }
                let variant = reader.read_string()?;
                visitor.visit_enum(StreamEnumAccess { reader, variant })
            }
            _ => Err(Error::Serde("expected enum representation".to_owned())),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf
        option seq tuple tuple_struct map struct identifier ignored_any
    }
}

struct ObjectMapAccess<'r, 'a> {
    reader: &'r mut Reader<'a>,
    remaining: usize,
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
        seed.deserialize(de::value::BorrowedStrDeserializer::new(key))
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
        let variant = seed.deserialize(self.variant.into_deserializer())?;
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
        Ok(Value::String(v.to_string()))
    }

    fn serialize_str(self, v: &str) -> Result<Value> {
        Ok(Value::String(v.to_owned()))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Value> {
        Ok(Value::Bytes(v.to_vec()))
    }

    fn serialize_none(self) -> Result<Value> {
        Ok(Value::optional(OptionalValue::none(ElementType::Builtin(
            TypeId::Bytes,
        ))))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Value> {
        let value = to_value(value)?;
        let element_type = element_type_for_value(&value);
        Ok(Value::optional(OptionalValue::some(element_type, value)))
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
        Ok(Value::String(variant.to_owned()))
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
        Ok(SeqSerializer::new(len))
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
            inner: SeqSerializer::new(Some(len)),
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<MapSerializer> {
        Ok(MapSerializer {
            fields: Vec::with_capacity(len.unwrap_or(0)),
            pending_key: None,
        })
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<StructSerializer> {
        Ok(StructSerializer {
            fields: Vec::with_capacity(len),
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
            fields: Vec::with_capacity(len),
        })
    }
}

struct SeqSerializer {
    values: Vec<Value>,
}

impl SeqSerializer {
    fn new(len: Option<usize>) -> Self {
        Self {
            values: Vec::with_capacity(len.unwrap_or(0)),
        }
    }

    fn finish(self) -> Result<Value> {
        let element_type = common_element_type(&self.values)?;
        Ok(Value::list(List::new(element_type, self.values)))
    }
}

impl SerializeSeq for SeqSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.values.push(to_value(value)?);
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
        self.fields.push(Field::new(key, to_value(value)?));
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
        self.fields.push(Field::new(key, to_value(value)?));
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
        self.fields.push(Field::new(key, to_value(value)?));
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

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        match self.value {
            Value::String(variant) => visitor.visit_enum(variant.as_str().into_deserializer()),
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
                let bytes = list
                    .values
                    .iter()
                    .map(|value| match value {
                        Value::Uint8(byte) => Ok(*byte),
                        _ => Err(Error::TypeMismatch),
                    })
                    .collect::<Result<Vec<_>>>()?;
                visitor.visit_byte_buf(bytes)
            }
            _ => self.deserialize_any(visitor),
        }
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_bytes(visitor)
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
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
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
        seed.deserialize(field.key.as_str().into_deserializer())
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
        let variant = seed.deserialize(self.variant.into_deserializer())?;
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

fn element_type_for_value(value: &Value) -> ElementType {
    match value {
        Value::Object(_) => ElementType::Object,
        Value::List(list) => ElementType::List(Box::new(list.element_type.clone())),
        Value::Array(array) => ElementType::Array(Box::new(array.element_type.clone())),
        Value::Optional(optional) => ElementType::Optional(Box::new(optional.element_type.clone())),
        other => ElementType::Builtin(other.type_id()),
    }
}

fn common_element_type(values: &[Value]) -> Result<ElementType> {
    let Some(first) = values.first() else {
        return Ok(ElementType::Builtin(TypeId::Bytes));
    };
    let element_type = element_type_for_value(first);
    if values
        .iter()
        .all(|value| element_type_for_value(value) == element_type)
    {
        Ok(element_type)
    } else {
        Err(Error::TypeMismatch)
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

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
            assert_eq!(streamed, tree, "wire mismatch (mode {mode:?})");
            let decoded: T = from_bytes(&streamed).expect("decode");
            assert_eq!(&decoded, value, "round-trip mismatch (mode {mode:?})");
        }
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
