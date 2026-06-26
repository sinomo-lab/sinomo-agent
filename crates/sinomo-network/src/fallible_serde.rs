use crate::error::network_fallible_vec_resource_exhausted;
use serde::de::{self, Deserialize, SeqAccess, Visitor};
use serde::ser;
use std::marker::PhantomData;

fn requested_bytes<T>(items: usize) -> usize {
    items
        .checked_mul(std::mem::size_of::<T>().max(1))
        .unwrap_or(usize::MAX)
}

fn try_reserve_vec<T, E>(vec: &mut Vec<T>, additional: usize, operation: &str) -> Result<(), E>
where
    E: de::Error,
{
    let requested_items = vec.len().checked_add(additional).unwrap_or(usize::MAX);
    let requested_bytes = requested_bytes::<T>(requested_items);
    vec.try_reserve(additional).map_err(|_| {
        E::custom(network_fallible_vec_resource_exhausted(
            operation,
            requested_bytes,
        ))
    })
}

pub(crate) fn serialize_bytes<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: ser::Serializer,
{
    serializer.serialize_bytes(bytes)
}

pub(crate) fn deserialize_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserialize_bytes_with_operation(deserializer, "network bytes")
}

pub(crate) fn deserialize_bytes_with_operation<'de, D>(
    deserializer: D,
    operation: &'static str,
) -> Result<Vec<u8>, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserializer.deserialize_byte_buf(BytesVisitor { operation })
}

pub(crate) fn serialize_optional_bytes<S>(
    bytes: &Option<Vec<u8>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: ser::Serializer,
{
    match bytes {
        Some(bytes) => serializer.serialize_some(serde_bytes::Bytes::new(bytes)),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn deserialize_optional_bytes<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<u8>>, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserialize_optional_bytes_with_operation(deserializer, "network optional bytes")
}

pub(crate) fn deserialize_optional_bytes_with_operation<'de, D>(
    deserializer: D,
    operation: &'static str,
) -> Result<Option<Vec<u8>>, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserializer.deserialize_option(OptionalBytesVisitor { operation })
}

pub(crate) fn deserialize_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: de::Deserializer<'de>,
    T: Deserialize<'de>,
{
    deserialize_vec_with_operation(deserializer, "network vector")
}

pub(crate) fn deserialize_vec_with_operation<'de, D, T>(
    deserializer: D,
    operation: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: de::Deserializer<'de>,
    T: Deserialize<'de>,
{
    deserializer.deserialize_seq(VecVisitor {
        operation,
        marker: PhantomData,
    })
}

struct BytesVisitor {
    operation: &'static str,
}

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("bytes")
    }

    fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let mut out = Vec::new();
        try_reserve_vec(&mut out, bytes.len(), self.operation)?;
        out.extend_from_slice(bytes);
        Ok(out)
    }

    fn visit_borrowed_bytes<E>(self, bytes: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_bytes(bytes)
    }

    fn visit_byte_buf<E>(self, bytes: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(bytes)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut out = Vec::new();
        if let Some(size_hint) = seq.size_hint() {
            try_reserve_vec(&mut out, size_hint, self.operation)?;
        }
        while let Some(byte) = seq.next_element::<u8>()? {
            try_reserve_vec(&mut out, 1, self.operation)?;
            out.push(byte);
        }
        Ok(out)
    }
}

struct OptionalBytesVisitor {
    operation: &'static str,
}

impl<'de> Visitor<'de> for OptionalBytesVisitor {
    type Value = Option<Vec<u8>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("optional bytes")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserialize_bytes_with_operation(deserializer, self.operation).map(Some)
    }
}

struct VecVisitor<T> {
    operation: &'static str,
    marker: PhantomData<T>,
}

impl<'de, T> Visitor<'de> for VecVisitor<T>
where
    T: Deserialize<'de>,
{
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a sequence")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        if let Some(size_hint) = seq.size_hint() {
            try_reserve_vec(&mut values, size_hint, self.operation)?;
        }
        while let Some(value) = seq.next_element()? {
            try_reserve_vec(&mut values, 1, self.operation)?;
            values.push(value);
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NetworkError;

    struct HugeSizeHintDeserializer;

    impl<'de> de::Deserializer<'de> for HugeSizeHintDeserializer {
        type Error = sinomo_object::Error;

        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_seq(HugeSizeHintSeqAccess)
        }

        fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_seq(HugeSizeHintSeqAccess)
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
            byte_buf option unit unit_struct newtype_struct tuple tuple_struct map
            struct enum identifier ignored_any
        }
    }

    struct HugeSizeHintSeqAccess;

    impl<'de> SeqAccess<'de> for HugeSizeHintSeqAccess {
        type Error = sinomo_object::Error;

        fn next_element_seed<T>(&mut self, _seed: T) -> Result<Option<T::Value>, Self::Error>
        where
            T: de::DeserializeSeed<'de>,
        {
            Ok(None)
        }

        fn size_hint(&self) -> Option<usize> {
            Some(usize::MAX)
        }
    }

    #[test]
    fn network_vec_deserializer_rejects_huge_size_hint_before_collecting() {
        let error = deserialize_vec::<_, u8>(HugeSizeHintDeserializer)
            .expect_err("huge vector size hint should fail fallibly");

        assert_eq!(
            NetworkError::object(error),
            NetworkError::ResourceExhausted {
                operation: "network vector".to_string(),
                requested_bytes: usize::MAX,
            }
        );
    }
}
