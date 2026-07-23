//! Owned serde decoding over a bounded iterator of non-contiguous chunks.
//!
//! Unlike the borrowed slice decoder, this path never needs the complete
//! encoded document in one allocation. It retains only the chunk currently
//! being consumed. Strings and byte values allocate their final owned serde
//! representation directly; scalar fields that straddle chunks use a small
//! stack buffer.

use crate::{
    AlignmentHint, DecodeOptions, ElementType, Error, LengthMode, MAGIC, Result, TypeId,
    duplicate_key_error, ensure_decode_limit, try_box, try_hashset_with_capacity,
    try_vec_with_capacity,
};
use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};
use std::collections::HashSet;

/// Decodes one SOBJ document from ownership-bearing, non-contiguous chunks.
///
/// `document_bytes` is the authenticated total body length. The iterator must
/// end exactly at that boundary. It may block waiting for the next chunk,
/// which lets an async caller run this decoder on a blocking worker and feed a
/// small bounded channel without retaining the complete encoded document.
pub fn from_chunk_iter_with_options<T, I, B>(
    chunks: I,
    document_bytes: usize,
    options: DecodeOptions,
) -> Result<T>
where
    T: DeserializeOwned,
    I: IntoIterator<Item = Result<B>>,
    B: AsRef<[u8]>,
{
    let actual = u64::try_from(document_bytes).map_err(|_| Error::LengthTooLarge)?;
    ensure_decode_limit("document bytes", actual, options.limits.max_document_bytes)?;
    let mut reader = ChunkReader::new(chunks.into_iter(), document_bytes, options);
    if reader.read_array::<4>()? != *MAGIC {
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
    reader.mode = LengthMode::from_code(reader.read_u8()?)?;
    AlignmentHint::from_code(reader.read_u8()?)?;
    let declared_bytes = reader.read_length()?;
    if declared_bytes != actual {
        return Err(Error::DocumentLengthMismatch {
            expected: declared_bytes,
            actual,
        });
    }
    let schema_types =
        reader.read_limited_len_usize("schema types", options.limits.max_schema_types)?;
    if schema_types != 0 {
        return Err(Error::Serde(
            "serde deserialization from raw schema payload is not implemented".to_owned(),
        ));
    }
    let value = T::deserialize(ChunkDeserializer {
        reader: &mut reader,
        shape: Shape::Element,
    })?;
    reader.skip_padding()?;
    reader.finish()?;
    Ok(value)
}

struct ChunkReader<I, B> {
    chunks: I,
    current: Option<B>,
    current_offset: usize,
    declared_bytes: usize,
    remaining_bytes: usize,
    mode: LengthMode,
    options: DecodeOptions,
    nesting_depth: usize,
}

impl<I, B> ChunkReader<I, B>
where
    I: Iterator<Item = Result<B>>,
    B: AsRef<[u8]>,
{
    fn new(chunks: I, document_bytes: usize, options: DecodeOptions) -> Self {
        Self {
            chunks,
            current: None,
            current_offset: 0,
            declared_bytes: document_bytes,
            remaining_bytes: document_bytes,
            mode: LengthMode::U64,
            options,
            nesting_depth: 0,
        }
    }

    fn ensure_current(&mut self) -> Result<bool> {
        loop {
            if self
                .current
                .as_ref()
                .is_some_and(|chunk| self.current_offset < chunk.as_ref().len())
            {
                return Ok(true);
            }
            self.current = None;
            self.current_offset = 0;
            let Some(chunk) = self.chunks.next().transpose()? else {
                return Ok(false);
            };
            if chunk.as_ref().is_empty() {
                continue;
            }
            self.current = Some(chunk);
        }
    }

    fn read_into(&mut self, output: &mut [u8]) -> Result<()> {
        if output.len() > self.remaining_bytes {
            return Err(Error::UnexpectedEof);
        }
        let mut filled = 0;
        while filled < output.len() {
            if !self.ensure_current()? {
                return Err(Error::UnexpectedEof);
            }
            let current = self.current.as_ref().expect("ensured chunk").as_ref();
            let available = &current[self.current_offset..];
            let copied = available.len().min(output.len() - filled);
            output[filled..filled + copied].copy_from_slice(&available[..copied]);
            self.current_offset += copied;
            self.remaining_bytes -= copied;
            filled += copied;
        }
        Ok(())
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut bytes = [0; N];
        self.read_into(&mut bytes)?;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_array::<1>()?[0])
    }

    fn peek_u8(&mut self) -> Result<Option<u8>> {
        if self.remaining_bytes == 0 {
            return Ok(None);
        }
        if !self.ensure_current()? {
            return Err(Error::UnexpectedEof);
        }
        Ok(Some(
            self.current.as_ref().expect("ensured chunk").as_ref()[self.current_offset],
        ))
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    fn read_i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }

    fn read_i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.read_array()?))
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
        let len = self
            .read_limited_len_usize("container items", self.options.limits.max_container_items)?;
        if len > self.remaining_bytes {
            return Err(Error::LengthTooLarge);
        }
        Ok(len)
    }

    fn read_vec(&mut self, len: usize, context: &'static str) -> Result<Vec<u8>> {
        let mut bytes = try_vec_with_capacity(len, context)?;
        bytes.resize(len, 0);
        self.read_into(&mut bytes)?;
        Ok(bytes)
    }

    fn read_string(&mut self) -> Result<String> {
        let len =
            self.read_limited_len_usize("string bytes", self.options.limits.max_string_bytes)?;
        String::from_utf8(self.read_vec(len, "streamed serde string")?)
            .map_err(|_| Error::InvalidUtf8)
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.read_limited_len_usize(
            "bytes value bytes",
            self.options.limits.max_bytes_value_bytes,
        )?;
        self.read_vec(len, "streamed serde bytes")
    }

    fn with_nesting<T>(&mut self, decode: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let next_depth = self
            .nesting_depth
            .checked_add(1)
            .ok_or(Error::SizeOverflow)?;
        ensure_decode_limit(
            "nesting depth",
            u64::try_from(next_depth).unwrap_or(u64::MAX),
            u64::try_from(self.options.limits.max_nesting_depth).unwrap_or(u64::MAX),
        )?;
        let previous = self.nesting_depth;
        self.nesting_depth = next_depth;
        let result = decode(self);
        self.nesting_depth = previous;
        result
    }

    fn skip_padding(&mut self) -> Result<()> {
        while self.peek_u8()? == Some(TypeId::Padding.byte()) {
            self.read_u8()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        if self.remaining_bytes != 0 {
            return Err(Error::UnexpectedEof);
        }
        if self.ensure_current()? {
            let current = self.current.as_ref().expect("ensured chunk").as_ref();
            let extra = current.len().saturating_sub(self.current_offset);
            return Err(Error::DocumentLengthMismatch {
                expected: self.declared_bytes as u64,
                actual: self.declared_bytes.saturating_add(extra) as u64,
            });
        }
        Ok(())
    }
}

enum Shape {
    Element,
    Typed(ElementType),
}

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

fn read_element_type<I, B>(reader: &mut ChunkReader<I, B>) -> Result<ElementType>
where
    I: Iterator<Item = Result<B>>,
    B: AsRef<[u8]>,
{
    read_element_type_at_depth(reader, 1)
}

fn read_element_type_at_depth<I, B>(
    reader: &mut ChunkReader<I, B>,
    depth: usize,
) -> Result<ElementType>
where
    I: Iterator<Item = Result<B>>,
    B: AsRef<[u8]>,
{
    let total_depth = reader
        .nesting_depth
        .checked_add(depth)
        .ok_or(Error::SizeOverflow)?;
    ensure_decode_limit(
        "nesting depth",
        total_depth as u64,
        reader.options.limits.max_nesting_depth as u64,
    )?;
    let byte = reader.read_u8()?;
    let next = || depth.checked_add(1).ok_or(Error::SizeOverflow);
    match byte {
        0x01..=0x0B | 0x24 | 0x25 => Ok(ElementType::Builtin(TypeId::from_byte(byte)?)),
        0x40 => Ok(ElementType::SchemaRef(reader.read_u16()?)),
        0x41 => Ok(ElementType::List(try_box(
            read_element_type_at_depth(reader, next()?)?,
            "streamed list element type",
        )?)),
        0x42 => Ok(ElementType::Array(try_box(
            read_element_type_at_depth(reader, next()?)?,
            "streamed array element type",
        )?)),
        0x43 => Ok(ElementType::Object),
        0x44 => Ok(ElementType::Optional(try_box(
            read_element_type_at_depth(reader, next()?)?,
            "streamed optional element type",
        )?)),
        0x80..=0xFF => ElementType::try_extension(byte, reader.read_bytes()?),
        other => Err(Error::InvalidTypeId(other)),
    }
}

struct ChunkDeserializer<'r, I, B> {
    reader: &'r mut ChunkReader<I, B>,
    shape: Shape,
}

impl<'r, I, B> ChunkDeserializer<'r, I, B>
where
    I: Iterator<Item = Result<B>>,
    B: AsRef<[u8]>,
{
    fn type_id(reader: &mut ChunkReader<I, B>, shape: &Shape) -> Result<TypeId> {
        match shape {
            Shape::Element => {
                reader.skip_padding()?;
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

struct TransientStrDeserializer<'a> {
    value: &'a str,
}

impl<'de> de::Deserializer<'de> for TransientStrDeserializer<'_> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_str(self.value)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_str(self.value)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_string(self.value.to_owned())
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_str(self.value)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map struct
        enum ignored_any
    }
}

fn deserialize_value<'de, I, B, V>(
    reader: &mut ChunkReader<I, B>,
    type_id: TypeId,
    visitor: V,
) -> Result<V::Value>
where
    I: Iterator<Item = Result<B>>,
    B: AsRef<[u8]>,
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
        TypeId::Int16 => visitor.visit_i16(i16::from_le_bytes(reader.read_array()?)),
        TypeId::Uint32 => visitor.visit_u32(reader.read_u32()?),
        TypeId::Int32 => visitor.visit_i32(reader.read_i32()?),
        TypeId::Uint64 => visitor.visit_u64(reader.read_u64()?),
        TypeId::Int64 => visitor.visit_i64(reader.read_i64()?),
        TypeId::Float => visitor.visit_f32(f32::from_le_bytes(reader.read_array()?)),
        TypeId::Double => visitor.visit_f64(f64::from_le_bytes(reader.read_array()?)),
        TypeId::String => visitor.visit_string(reader.read_string()?),
        TypeId::Bytes => visitor.visit_byte_buf(reader.read_bytes()?),
        TypeId::Object => reader.with_nesting(|reader| {
            let remaining = reader.read_container_len()?;
            visitor.visit_map(ChunkMapAccess {
                reader,
                remaining,
                keys: try_hashset_with_capacity(remaining, "streamed serde object keys")?,
            })
        }),
        TypeId::List => reader.with_nesting(|reader| {
            let element_type = read_element_type(reader)?;
            let remaining = reader.read_container_len()?;
            visitor.visit_seq(ChunkSeqAccess {
                reader,
                element_type,
                remaining,
            })
        }),
        TypeId::Array => Err(Error::Serde(
            "array values are not supported by serde decoding; use the dynamic Value API"
                .to_owned(),
        )),
        TypeId::Optional => reader.with_nesting(|reader| {
            let element_type = read_element_type(reader)?;
            match reader.read_u8()? {
                0 => visitor.visit_none(),
                1 => visitor.visit_some(ChunkDeserializer {
                    reader,
                    shape: Shape::Typed(element_type),
                }),
                other => Err(Error::InvalidBoolean(other)),
            }
        }),
        TypeId::Padding | TypeId::Document => Err(Error::UnsupportedTypeId(type_id.byte())),
    }
}

impl<'de, I, B> de::Deserializer<'de> for ChunkDeserializer<'_, I, B>
where
    I: Iterator<Item = Result<B>>,
    B: AsRef<[u8]>,
{
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let Self { reader, shape } = self;
        let type_id = Self::type_id(reader, &shape)?;
        deserialize_value(reader, type_id, visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let Self { reader, shape } = self;
        match Self::type_id(reader, &shape)? {
            TypeId::String => visitor.visit_string(reader.read_string()?),
            _ => Err(Error::Serde("expected string".to_owned())),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let Self { reader, shape } = self;
        match Self::type_id(reader, &shape)? {
            TypeId::Bytes => visitor.visit_byte_buf(reader.read_bytes()?),
            _ => Err(Error::Serde("expected bytes".to_owned())),
        }
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let Self { reader, shape } = self;
        match Self::type_id(reader, &shape)? {
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
        let Self { reader, shape } = self;
        match Self::type_id(reader, &shape)? {
            TypeId::String => visitor.visit_enum(UnitEnumAccess {
                variant: reader.read_string()?,
            }),
            TypeId::Object => reader.with_nesting(|reader| {
                if reader.read_container_len()? != 1 {
                    return Err(Error::Serde(
                        "expected single-field object for enum variant".to_owned(),
                    ));
                }
                visitor.visit_enum(ChunkEnumAccess {
                    variant: reader.read_string()?,
                    reader,
                })
            }),
            _ => Err(Error::Serde("expected enum representation".to_owned())),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char
        option seq tuple tuple_struct map struct identifier ignored_any
    }
}

struct ChunkMapAccess<'r, I, B> {
    reader: &'r mut ChunkReader<I, B>,
    remaining: usize,
    keys: HashSet<String>,
}

impl<'de, I, B> MapAccess<'de> for ChunkMapAccess<'_, I, B>
where
    I: Iterator<Item = Result<B>>,
    B: AsRef<[u8]>,
{
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let key = self.reader.read_string()?;
        if self.keys.contains(&key) {
            return duplicate_key_error(&key);
        }
        let value = seed.deserialize(TransientStrDeserializer { value: &key })?;
        self.keys.insert(key);
        Ok(Some(value))
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        seed.deserialize(ChunkDeserializer {
            reader: &mut *self.reader,
            shape: Shape::Element,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

struct ChunkSeqAccess<'r, I, B> {
    reader: &'r mut ChunkReader<I, B>,
    element_type: ElementType,
    remaining: usize,
}

impl<'de, I, B> SeqAccess<'de> for ChunkSeqAccess<'_, I, B>
where
    I: Iterator<Item = Result<B>>,
    B: AsRef<[u8]>,
{
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(ChunkDeserializer {
            reader: &mut *self.reader,
            shape: Shape::Typed(self.element_type.clone()),
        })
        .map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

struct UnitEnumAccess {
    variant: String,
}

struct UnitVariantAccess;

impl<'de> EnumAccess<'de> for UnitEnumAccess {
    type Error = Error;
    type Variant = UnitVariantAccess;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let variant = seed.deserialize(TransientStrDeserializer {
            value: &self.variant,
        })?;
        Ok((variant, UnitVariantAccess))
    }
}

impl<'de> VariantAccess<'de> for UnitVariantAccess {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, _seed: T) -> Result<T::Value> {
        Err(Error::Serde("expected unit enum variant".to_owned()))
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, _visitor: V) -> Result<V::Value> {
        Err(Error::Serde("expected unit enum variant".to_owned()))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value> {
        Err(Error::Serde("expected unit enum variant".to_owned()))
    }
}

struct ChunkEnumAccess<'r, I, B> {
    reader: &'r mut ChunkReader<I, B>,
    variant: String,
}

impl<'de, I, B> EnumAccess<'de> for ChunkEnumAccess<'_, I, B>
where
    I: Iterator<Item = Result<B>>,
    B: AsRef<[u8]>,
{
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let variant = seed.deserialize(TransientStrDeserializer {
            value: &self.variant,
        })?;
        Ok((variant, self))
    }
}

impl<'de, I, B> VariantAccess<'de> for ChunkEnumAccess<'_, I, B>
where
    I: Iterator<Item = Result<B>>,
    B: AsRef<[u8]>,
{
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        de::Deserializer::deserialize_any(
            ChunkDeserializer {
                reader: self.reader,
                shape: Shape::Element,
            },
            de::IgnoredAny,
        )
        .map(|_| ())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(ChunkDeserializer {
            reader: self.reader,
            shape: Shape::Element,
        })
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        de::Deserializer::deserialize_any(
            ChunkDeserializer {
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
            ChunkDeserializer {
                reader: self.reader,
                shape: Shape::Element,
            },
            visitor,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde_ext::to_bytes;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct ChunkedFixture {
        label: String,
        values: Vec<u8>,
        optional: Option<u64>,
    }

    #[test]
    fn decodes_owned_values_across_every_chunk_boundary() {
        let expected = ChunkedFixture {
            label: "segmented".repeat(17),
            values: (0..=255).cycle().take(256 * 1024 + 31).collect(),
            optional: Some(42),
        };
        let encoded = to_bytes(&expected, 7, LengthMode::U64).expect("encode fixture");
        for chunk_bytes in [1, 2, 3, 7, 64, 4093, 64 * 1024] {
            let chunks = encoded
                .chunks(chunk_bytes)
                .map(|chunk| Ok(chunk.to_vec()))
                .collect::<Vec<_>>();
            let decoded: ChunkedFixture = from_chunk_iter_with_options(
                chunks,
                encoded.len(),
                DecodeOptions::default().with_expected_version_id(7),
            )
            .expect("decode segmented fixture");
            assert_eq!(decoded, expected, "chunk size {chunk_bytes}");
        }
    }

    #[test]
    fn rejects_truncated_and_trailing_chunk_streams() {
        let encoded = to_bytes(&"bounded", 9, LengthMode::U64).expect("encode fixture");
        let truncated = vec![Ok(encoded[..encoded.len() - 1].to_vec())];
        assert_eq!(
            from_chunk_iter_with_options::<String, _, _>(
                truncated,
                encoded.len(),
                DecodeOptions::default().with_expected_version_id(9),
            ),
            Err(Error::UnexpectedEof)
        );
        let with_extra = vec![Ok(encoded.clone()), Ok(vec![0xaa])];
        assert!(matches!(
            from_chunk_iter_with_options::<String, _, _>(
                with_extra,
                encoded.len(),
                DecodeOptions::default().with_expected_version_id(9),
            ),
            Err(Error::DocumentLengthMismatch { .. })
        ));
    }
}
