//! Recover the original ordered JSON hash of a retained dynamic SOBJ value.
//! Retired v1 prefix proofs used JSON field order, so serde_json::Value's
//! sorted map representation cannot authenticate that source history.
use super::{MetadataBackendError, Result};
use sinomo_core::object::{LengthMode, TypeId};
use std::io::{Read, Write};
fn error(e: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(e.to_string())
}
fn byte(input: &mut impl Read) -> Result<u8> {
    let mut v = [0];
    input.read_exact(&mut v).map_err(error)?;
    Ok(v[0])
}
fn number(input: &mut impl Read, mode: LengthMode) -> Result<u64> {
    let mut v = [0; 8];
    input.read_exact(&mut v[..mode.width()]).map_err(error)?;
    Ok(u64::from_le_bytes(v))
}
fn bytes(input: &mut impl Read, mode: LengthMode) -> Result<Vec<u8>> {
    let n = number(input, mode)?;
    if n > 64 * 1024 * 1024 {
        return Err(error("retained JSON field exceeds conversion bound"));
    }
    let mut value = vec![0; usize::try_from(n).map_err(error)?];
    input.read_exact(&mut value).map_err(error)?;
    Ok(value)
}
#[derive(Clone)]
enum Shape {
    Scalar(u8),
    Object,
    List(Box<Shape>),
    Array(Box<Shape>),
    Optional(Box<Shape>),
}
fn shape(input: &mut impl Read, depth: usize) -> Result<Shape> {
    if depth > 64 {
        return Err(error("retained JSON descriptor nesting exceeds bound"));
    }
    Ok(match byte(input)? {
        tag @ (1..=11 | 0x24 | 0x25) => Shape::Scalar(tag),
        0x43 => Shape::Object,
        0x41 => Shape::List(Box::new(shape(input, depth + 1)?)),
        0x42 => Shape::Array(Box::new(shape(input, depth + 1)?)),
        0x44 => Shape::Optional(Box::new(shape(input, depth + 1)?)),
        _ => return Err(error("unsupported retained JSON descriptor")),
    })
}
fn json<T: serde::Serialize>(out: &mut impl Write, value: &T) -> Result<()> {
    serde_json::to_writer(out, value).map_err(error)
}
fn write(out: &mut impl Write, bytes: &[u8]) -> Result<()> {
    out.write_all(bytes).map_err(error)
}
fn object(
    input: &mut impl Read,
    out: &mut impl Write,
    mode: LengthMode,
    depth: usize,
    exclude_proofs: bool,
) -> Result<()> {
    let fields = number(input, mode)?;
    write(out, b"{")?;
    let mut first = true;
    for _ in 0..fields {
        let key = String::from_utf8(bytes(input, mode)?).map_err(error)?;
        let tag = byte(input)?;
        if exclude_proofs && matches!(key.as_str(), "semantic_state_fingerprint" | "prefix_proof") {
            element(input, &mut std::io::sink(), mode, tag, depth + 1, false)?;
            continue;
        }
        if !first {
            write(out, b",")?;
        }
        first = false;
        json(out, &key)?;
        write(out, b":")?;
        element(input, out, mode, tag, depth + 1, false)?;
    }
    write(out, b"}")
}
fn typed(
    input: &mut impl Read,
    out: &mut impl Write,
    mode: LengthMode,
    kind: &Shape,
    depth: usize,
) -> Result<()> {
    match kind {
        Shape::Scalar(tag) => element(input, out, mode, *tag, depth, false),
        Shape::Object => object(input, out, mode, depth, false),
        Shape::List(inner) => {
            let _ = inner;
            list(input, out, mode, depth)
        }
        Shape::Array(inner) => {
            let _ = inner;
            array(input, out, mode, depth)
        }
        Shape::Optional(inner) => match byte(input)? {
            0 => write(out, b"null"),
            1 => typed(input, out, mode, inner, depth + 1),
            _ => Err(error("invalid retained JSON optional presence")),
        },
    }
}
fn list(input: &mut impl Read, out: &mut impl Write, mode: LengthMode, depth: usize) -> Result<()> {
    let kind = shape(input, depth)?;
    let count = number(input, mode)?;
    write(out, b"[")?;
    for index in 0..count {
        if index > 0 {
            write(out, b",")?;
        }
        typed(input, out, mode, &kind, depth + 1)?;
    }
    write(out, b"]")
}
fn array(
    input: &mut impl Read,
    out: &mut impl Write,
    mode: LengthMode,
    depth: usize,
) -> Result<()> {
    let kind = shape(input, depth)?;
    let width = number(input, mode)?;
    let rank = byte(input)?;
    let mut count = 1u64;
    for _ in 0..rank {
        count = count
            .checked_mul(number(input, mode)?)
            .ok_or_else(|| error("retained array count overflow"))?;
    }
    let Shape::Scalar(tag) = kind else {
        return Err(error("retained JSON array requires fixed scalar elements"));
    };
    let expected = match tag {
        1..=3 => 1,
        4..=5 => 2,
        6..=7 | 10 => 4,
        8..=9 | 11 => 8,
        _ => return Err(error("retained array scalar is variable width")),
    };
    if width != expected {
        return Err(error("retained array element width mismatch"));
    }
    write(out, b"[")?;
    for index in 0..count {
        if index > 0 {
            write(out, b",")?;
        }
        element(input, out, mode, tag, depth + 1, false)?;
    }
    write(out, b"]")
}
fn element(
    input: &mut impl Read,
    out: &mut impl Write,
    mode: LengthMode,
    tag: u8,
    depth: usize,
    exclude_proofs: bool,
) -> Result<()> {
    if depth > 64 {
        return Err(error("retained JSON value nesting exceeds bound"));
    }
    macro_rules! scalar {
        ($ty:ty,$n:expr) => {{
            let mut v = [0; $n];
            input.read_exact(&mut v).map_err(error)?;
            json(out, &<$ty>::from_le_bytes(v))
        }};
    }
    match TypeId::from_byte(tag).map_err(error)? {
        TypeId::Boolean => match byte(input)? {
            0 => write(out, b"false"),
            1 => write(out, b"true"),
            _ => Err(error("invalid retained JSON boolean")),
        },
        TypeId::Uint8 => json(out, &byte(input)?),
        TypeId::Int8 => json(out, &(byte(input)? as i8)),
        TypeId::Uint16 => scalar!(u16, 2),
        TypeId::Int16 => scalar!(i16, 2),
        TypeId::Uint32 => scalar!(u32, 4),
        TypeId::Int32 => scalar!(i32, 4),
        TypeId::Uint64 => scalar!(u64, 8),
        TypeId::Int64 => scalar!(i64, 8),
        TypeId::Float => scalar!(f32, 4),
        TypeId::Double => scalar!(f64, 8),
        TypeId::Object => object(input, out, mode, depth, exclude_proofs),
        TypeId::List => list(input, out, mode, depth),
        TypeId::Array => array(input, out, mode, depth),
        TypeId::String => json(out, &String::from_utf8(bytes(input, mode)?).map_err(error)?),
        TypeId::Bytes => {
            let count = number(input, mode)?;
            write(out, b"[")?;
            for index in 0..count {
                if index > 0 {
                    write(out, b",")?;
                }
                json(out, &byte(input)?)?;
            }
            write(out, b"]")
        }
        TypeId::Optional => {
            let kind = shape(input, depth)?;
            match byte(input)? {
                0 => write(out, b"null"),
                1 => typed(input, out, mode, &kind, depth + 1),
                _ => Err(error("invalid retained JSON optional presence")),
            }
        }
        _ => Err(error(
            "retained JSON document contains an unsupported value",
        )),
    }
}
pub(super) fn write_document(
    input: &mut impl Read,
    out: &mut impl Write,
    exclude_proofs: bool,
) -> Result<()> {
    let mut header = [0; 8];
    input.read_exact(&mut header).map_err(error)?;
    if &header[..4] != b"SOBJ" {
        return Err(error("retained ordered JSON input is not SOBJ"));
    }
    let mode = LengthMode::from_code(header[6]).map_err(error)?;
    let _length = number(input, mode)?;
    if number(input, mode)? != 0 {
        return Err(error("retained JSON requires a dynamic source document"));
    }
    let tag = byte(input)?;
    element(input, out, mode, tag, 0, exclude_proofs)?;
    let mut trailing = [0];
    if input.read(&mut trailing).map_err(error)? != 0 {
        return Err(error("trailing retained JSON source bytes"));
    }
    Ok(())
}
struct HashWriter<'a>(&'a mut blake3::Hasher);
impl Write for HashWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(super) fn snapshot_fingerprint(path: &std::path::Path) -> Result<String> {
    let mut file = std::fs::File::open(path).map_err(error)?;
    let mut hash = blake3::Hasher::new();
    write_document(&mut file, &mut HashWriter(&mut hash), true)?;
    Ok(format!("blake3:{}", hash.finalize().to_hex()))
}
pub(super) fn journal_json(exact_frame: &[u8]) -> Result<Vec<u8>> {
    let mut input = std::io::Cursor::new(exact_frame);
    let mut out = Vec::new();
    write_document(&mut input, &mut out, false)?;
    Ok(out)
}
