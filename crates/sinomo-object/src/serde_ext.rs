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
    Array, Document, DocumentView, ElementType, ElementView, Error, Field, LengthMode, List,
    Object, OptionalValue, PayloadView, Result, TypeId, Value,
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
    to_document(value, version_id, length_mode)?.to_bytes()
}

pub fn from_bytes<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let view = DocumentView::parse(bytes)?;
    let value = value_from_payload_view(&view.payload)?;
    from_value(&value)
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
        Ok(Value::Optional(OptionalValue::none(ElementType::Builtin(
            TypeId::Bytes,
        ))))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Value> {
        let value = to_value(value)?;
        let element_type = element_type_for_value(&value);
        Ok(Value::Optional(OptionalValue::some(element_type, value)))
    }

    fn serialize_unit(self) -> Result<Value> {
        Ok(Value::Optional(OptionalValue::none(ElementType::Builtin(
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
        Ok(Value::List(List::new(element_type, self.values)))
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

fn value_from_payload_view(payload: &PayloadView<'_>) -> Result<Value> {
    match payload {
        PayloadView::Dynamic(value) => value_from_view(value),
        PayloadView::Schema { .. } => Err(Error::Serde(
            "serde deserialization from raw schema payload is not implemented".to_owned(),
        )),
    }
}

fn value_from_view(value: &ElementView<'_>) -> Result<Value> {
    Ok(match value {
        ElementView::Boolean(value) => Value::Boolean(*value),
        ElementView::Uint8(value) => Value::Uint8(*value),
        ElementView::Int8(value) => Value::Int8(*value),
        ElementView::Uint16(value) => Value::Uint16(*value),
        ElementView::Int16(value) => Value::Int16(*value),
        ElementView::Uint32(value) => Value::Uint32(*value),
        ElementView::Int32(value) => Value::Int32(*value),
        ElementView::Uint64(value) => Value::Uint64(*value),
        ElementView::Int64(value) => Value::Int64(*value),
        ElementView::Float(value) => Value::Float(*value),
        ElementView::Double(value) => Value::Double(*value),
        ElementView::String(value) => Value::String((*value).to_owned()),
        ElementView::Bytes(value) => Value::Bytes((*value).to_vec()),
        ElementView::Object(object) => Value::Object(Object::new(
            object
                .fields
                .iter()
                .map(|field| Ok(Field::new(field.key, value_from_view(&field.value)?)))
                .collect::<Result<Vec<_>>>()?,
        )),
        ElementView::List(list) => Value::List(List::new(
            list.element_type.clone(),
            list.values
                .iter()
                .map(value_from_view)
                .collect::<Result<Vec<_>>>()?,
        )),
        ElementView::Array(array) => Value::Array(Array::new(
            array.element_type.clone(),
            array.element_size,
            array.dimensions.clone(),
            array.data.to_vec(),
        )?),
        ElementView::Optional(optional) => Value::Optional(OptionalValue {
            element_type: optional.element_type.clone(),
            value: optional
                .value
                .as_deref()
                .map(value_from_view)
                .transpose()?
                .map(Box::new),
        }),
    })
}
