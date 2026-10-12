//! Owned serde decoding over a bounded iterator of non-contiguous chunks.
//!
//! Unlike the borrowed slice decoder, this path never needs the complete
//! encoded document in one allocation. It retains only the chunk currently
//! being consumed. Strings and byte values allocate their final owned serde
//! representation directly; scalar fields that straddle chunks use a small
//! stack buffer.

use crate::{
    AlignmentHint, DecodeOptions, ElementType, Error, LengthMode, MAGIC, Result, TypeId,
    ensure_decode_limit, enter_decode_nesting, try_box, try_vec_with_capacity,
};
use serde::de::DeserializeOwned;

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
    let mut source = ChunkCursor {
        chunks: chunks.into_iter(),
        current: None,
        current_offset: 0,
    };
    decode_chunk_document(&mut source, document_bytes, actual, options)
}

// Keep input iterator and chunk types out of the serde visitor graph. Only the
// cursor below specializes for each source; decoding specializes for T alone.
fn decode_chunk_document<T: DeserializeOwned>(
    source: &mut dyn ChunkSource,
    document_bytes: usize,
    actual: u64,
    options: DecodeOptions,
) -> Result<T> {
    let mut reader = ChunkReader::new(source, document_bytes, options);
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
    let value = T::deserialize(crate::serde_ext::SobjDeserializer {
        reader: &mut reader,
        shape: crate::serde_ext::Shape::Element,
    })?;
    reader.skip_padding()?;
    reader.finish()?;
    Ok(value)
}

/// Borrow the current source chunk until the next consumption or lookup.
/// The source owns that chunk; the decoder never copies or boxes it.
trait ChunkSource {
    fn available(&mut self) -> Result<&[u8]>;
    fn consume(&mut self, bytes: usize);
}

struct ChunkCursor<I, B> {
    chunks: I,
    current: Option<B>,
    current_offset: usize,
}

impl<I, B> ChunkSource for ChunkCursor<I, B>
where
    I: Iterator<Item = Result<B>>,
    B: AsRef<[u8]>,
{
    fn available(&mut self) -> Result<&[u8]> {
        while !self
            .current
            .as_ref()
            .is_some_and(|chunk| self.current_offset < chunk.as_ref().len())
        {
            // Release the previous chunk before requesting its successor, so
            // even a blocking producer retains only one chunk in this cursor.
            self.current = None;
            self.current_offset = 0;
            let Some(chunk) = self.chunks.next().transpose()? else {
                return Ok(&[]);
            };
            if !chunk.as_ref().is_empty() {
                self.current = Some(chunk);
            }
        }
        Ok(&self.current.as_ref().expect("available chunk").as_ref()[self.current_offset..])
    }

    fn consume(&mut self, bytes: usize) {
        debug_assert!(
            self.current
                .as_ref()
                .is_some_and(|chunk| bytes <= chunk.as_ref().len() - self.current_offset)
        );
        self.current_offset += bytes;
    }
}

struct ChunkReader<'source> {
    source: &'source mut dyn ChunkSource,
    declared_bytes: usize,
    remaining_bytes: usize,
    mode: LengthMode,
    options: DecodeOptions,
    nesting_depth: usize,
}

impl<'source> ChunkReader<'source> {
    fn new(
        source: &'source mut dyn ChunkSource,
        document_bytes: usize,
        options: DecodeOptions,
    ) -> Self {
        Self {
            source,
            declared_bytes: document_bytes,
            remaining_bytes: document_bytes,
            mode: LengthMode::U64,
            options,
            nesting_depth: 0,
        }
    }

    fn read_into(&mut self, output: &mut [u8]) -> Result<()> {
        if output.len() > self.remaining_bytes {
            return Err(Error::UnexpectedEof);
        }
        let mut filled = 0;
        while filled < output.len() {
            let available = self.source.available()?;
            if available.is_empty() {
                return Err(Error::UnexpectedEof);
            }
            let copied = available.len().min(output.len() - filled);
            output[filled..filled + copied].copy_from_slice(&available[..copied]);
            self.source.consume(copied);
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
        self.source
            .available()?
            .first()
            .copied()
            .map(Some)
            .ok_or(Error::UnexpectedEof)
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

    fn skip_padding(&mut self) -> Result<()> {
        while self.peek_u8()? == Some(TypeId::Padding.byte()) {
            self.read_u8()?;
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        if self.remaining_bytes != 0 {
            return Err(Error::UnexpectedEof);
        }
        let extra = self.source.available()?.len();
        if extra != 0 {
            return Err(Error::DocumentLengthMismatch {
                expected: self.declared_bytes as u64,
                actual: self.declared_bytes.saturating_add(extra) as u64,
            });
        }
        Ok(())
    }
}

impl<'de> crate::serde_ext::SobjInput<'de> for ChunkReader<'_> {
    crate::serde_ext::forward_input_numbers!();
    fn read_text(&mut self) -> Result<crate::serde_ext::SobjText<'de>> {
        self.read_string().map(crate::serde_ext::SobjText::Owned)
    }
    fn read_blob(&mut self) -> Result<crate::serde_ext::SobjBytes<'de>> {
        self.read_bytes().map(crate::serde_ext::SobjBytes::Owned)
    }
    fn read_element_type(&mut self) -> Result<ElementType> {
        read_element_type_at_depth(self, 1)
    }
    fn skip_padding(&mut self) -> Result<()> {
        ChunkReader::skip_padding(self)
    }
    fn enter_nesting(&mut self) -> Result<usize> {
        enter_decode_nesting(
            &mut self.nesting_depth,
            self.options.limits.max_nesting_depth,
        )
    }
    fn restore_nesting(&mut self, previous: usize) {
        self.nesting_depth = previous;
    }
    fn borrowed_keys(&self) -> bool {
        false
    }
    #[cfg(test)]
    fn nesting_depth(&self) -> usize {
        self.nesting_depth
    }
}
// Cursor-specific bounded type-descriptor reads stay non-generic.
fn read_element_type_at_depth(reader: &mut ChunkReader<'_>, depth: usize) -> Result<ElementType> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DecodeLimits;
    use crate::serde_ext::to_bytes;
    use serde::{Deserialize, Serialize};
    use std::cell::Cell;

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
    fn borrowed_chunks_release_before_fetching_the_next_chunk() {
        struct BorrowedChunk<'a> {
            bytes: &'a [u8],
            live: &'a Cell<bool>,
        }
        impl AsRef<[u8]> for BorrowedChunk<'_> {
            fn as_ref(&self) -> &[u8] {
                self.bytes
            }
        }
        impl Drop for BorrowedChunk<'_> {
            fn drop(&mut self) {
                assert!(self.live.replace(false), "chunk released twice");
            }
        }

        let expected = ChunkedFixture {
            label: "segmented €".to_owned(),
            values: vec![0, 1, 255],
            optional: None,
        };
        for mode in [LengthMode::U16, LengthMode::U32, LengthMode::U64] {
            let encoded = to_bytes(&expected, 7, mode).expect("encode fixture");
            let live = Cell::new(false);
            let chunks = std::iter::once(&[][..])
                .chain(encoded.chunks(3).flat_map(|chunk| [chunk, &[][..]]))
                .map(|bytes| {
                    assert!(!live.replace(true), "previous chunk retained during fetch");
                    Ok(BorrowedChunk { bytes, live: &live })
                });
            let decoded: ChunkedFixture = from_chunk_iter_with_options(
                chunks,
                encoded.len(),
                DecodeOptions::default().with_expected_version_id(7),
            )
            .expect("decode borrowed and empty chunks");
            assert_eq!(decoded, expected, "length mode {mode:?}");
            assert!(!live.get(), "final chunk retained after decode");
        }
    }

    #[test]
    fn document_limit_rejects_before_converting_the_chunk_source() {
        struct UntouchedSource;
        impl IntoIterator for UntouchedSource {
            type Item = Result<&'static [u8]>;
            type IntoIter = std::iter::Empty<Self::Item>;

            fn into_iter(self) -> Self::IntoIter {
                panic!("over-limit document must not touch its chunk source");
            }
        }
        assert_eq!(
            from_chunk_iter_with_options::<(), _, _>(
                UntouchedSource,
                4,
                DecodeOptions::new(DecodeLimits {
                    max_document_bytes: 3,
                    ..Default::default()
                }),
            ),
            Err(Error::DecodeLimitExceeded {
                limit: "document bytes",
                max: 3,
                actual: 4,
            })
        );
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
        let mut current_with_extra = encoded.clone();
        current_with_extra.push(0xaa);
        for with_extra in [
            vec![Ok(current_with_extra)],
            vec![Ok(encoded.clone()), Ok(Vec::new()), Ok(vec![0xaa])],
        ] {
            assert_eq!(
                from_chunk_iter_with_options::<String, _, _>(
                    with_extra,
                    encoded.len(),
                    DecodeOptions::default().with_expected_version_id(9),
                ),
                Err(Error::DocumentLengthMismatch {
                    expected: encoded.len() as u64,
                    actual: encoded.len() as u64 + 1,
                })
            );
        }
        for before_error in [&encoded[..3], encoded.as_slice()] {
            let chunks = [
                Ok(before_error),
                Err(Error::Text("source failed".to_owned())),
            ];
            assert_eq!(
                from_chunk_iter_with_options::<String, _, _>(
                    chunks,
                    encoded.len(),
                    DecodeOptions::default(),
                ),
                Err(Error::Text("source failed".to_owned()))
            );
        }
    }

    #[test]
    fn nesting_scope_restores_depth_after_error_and_rejects_before_decoding() {
        let mut source = ChunkCursor {
            chunks: std::iter::empty::<Result<&[u8]>>(),
            current: None,
            current_offset: 0,
        };
        let mut reader = ChunkReader::new(
            &mut source,
            0,
            DecodeOptions::new(DecodeLimits {
                max_nesting_depth: 1,
                ..Default::default()
            }),
        );
        let error = crate::serde_ext::with_input_nesting(&mut reader, |reader| {
            assert_eq!(reader.nesting_depth(), 1);
            let inner = crate::serde_ext::with_input_nesting::<()>(reader, |_| {
                panic!("over-limit scope must not invoke its decoder")
            });
            assert_eq!(reader.nesting_depth(), 1);
            inner
        })
        .expect_err("nested scope exceeds the limit");
        assert_eq!(
            error,
            Error::DecodeLimitExceeded {
                limit: "nesting depth",
                max: 1,
                actual: 2,
            }
        );
        assert_eq!(reader.nesting_depth, 0);
        assert_eq!(
            crate::serde_ext::with_input_nesting(&mut reader, |reader| Ok(reader.nesting_depth())),
            Ok(1)
        );
        assert_eq!(reader.nesting_depth, 0);

        reader.nesting_depth = usize::MAX;
        assert_eq!(
            crate::serde_ext::with_input_nesting::<()>(&mut reader, |_| panic!(
                "overflow must not invoke its decoder"
            )),
            Err(Error::SizeOverflow)
        );
        assert_eq!(reader.nesting_depth, usize::MAX);
    }

    #[test]
    fn segmented_decoding_preserves_header_and_value_validation() {
        let encoded = to_bytes(&vec![vec![1_u8, 2, 3]], 7, LengthMode::U32).expect("encode");
        for (options, expected) in [
            (
                DecodeOptions::default().with_expected_version_id(8),
                Error::UnexpectedVersionId {
                    expected: 8,
                    actual: 7,
                },
            ),
            (
                DecodeOptions::new(DecodeLimits {
                    max_container_items: 2,
                    ..Default::default()
                }),
                Error::DecodeLimitExceeded {
                    limit: "container items",
                    max: 2,
                    actual: 3,
                },
            ),
            (
                DecodeOptions::new(DecodeLimits {
                    max_nesting_depth: 2,
                    ..Default::default()
                }),
                Error::DecodeLimitExceeded {
                    limit: "nesting depth",
                    max: 2,
                    actual: 3,
                },
            ),
        ] {
            assert_eq!(
                from_chunk_iter_with_options::<Vec<Vec<u8>>, _, _>(
                    encoded.chunks(1).map(Ok),
                    encoded.len(),
                    options,
                ),
                Err(expected)
            );
        }
        let mut bad_length = encoded.clone();
        bad_length[8..12].copy_from_slice(&(encoded.len() as u32 + 1).to_le_bytes());
        assert_eq!(
            from_chunk_iter_with_options::<Vec<Vec<u8>>, _, _>(
                bad_length.chunks(1).map(Ok),
                encoded.len(),
                DecodeOptions::default(),
            ),
            Err(Error::DocumentLengthMismatch {
                expected: encoded.len() as u64 + 1,
                actual: encoded.len() as u64,
            })
        );

        let mut map = std::collections::BTreeMap::new();
        map.insert("keya", 1_u8);
        map.insert("keyb", 2_u8);
        let mut duplicate = to_bytes(&map, 7, LengthMode::U16).expect("encode map");
        let second_key = duplicate
            .windows(4)
            .position(|bytes| bytes == b"keyb")
            .unwrap();
        duplicate[second_key..second_key + 4].copy_from_slice(b"keya");
        assert_eq!(
            from_chunk_iter_with_options::<std::collections::BTreeMap<String, u8>, _, _>(
                duplicate.chunks(1).map(Ok),
                duplicate.len(),
                DecodeOptions::default(),
            ),
            Err(Error::DuplicateKey("keya".to_owned()))
        );
    }
}
