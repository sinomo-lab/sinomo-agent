//! Seekable cursor for the dynamic SOBJ map used by retired packed snapshots.
//! Individual values are decoded by the shared SOBJ serde implementation.
use super::*;
use sinomo_core::object::{LengthMode, TypeId};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct MapCursor {
    pub offset: u64,
    pub remaining: u64,
    mode: u8,
}
fn number(file: &mut File, mode: LengthMode) -> Result<u64> {
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes[..mode.width()]).map_err(error)?;
    Ok(u64::from_le_bytes(bytes))
}
fn string(file: &mut File, mode: LengthMode) -> Result<String> {
    let len = usize::try_from(number(file, mode)?).map_err(error)?;
    let position = file.stream_position().map_err(error)?;
    if len>64*1024 {return Err(error("packed snapshot single string exceeds bounded record limit"));}
    if len as u64
        > file
            .metadata()
            .map_err(error)?
            .len()
            .saturating_sub(position)
    {
        return Err(error("packed snapshot string exceeds source boundary"));
    }
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes).map_err(error)?;
    String::from_utf8(bytes).map_err(error)
}
fn skip(file: &mut File, mode: LengthMode, kind: TypeId, depth: usize, start:u64) -> Result<()> {
    if file.stream_position().map_err(error)?.saturating_sub(start)>64*1024 {return Err(error("packed snapshot single value exceeds bounded record limit"));}
    if depth > 16 {
        return Err(error("packed snapshot row nesting is unsupported"));
    }
    match kind {
        TypeId::Boolean | TypeId::Uint8 | TypeId::Int8 => {
            file.seek(SeekFrom::Current(1)).map_err(error)?;
        }
        TypeId::Uint16 | TypeId::Int16 => {
            file.seek(SeekFrom::Current(2)).map_err(error)?;
        }
        TypeId::Uint32 | TypeId::Int32 | TypeId::Float => {
            file.seek(SeekFrom::Current(4)).map_err(error)?;
        }
        TypeId::Uint64 | TypeId::Int64 | TypeId::Double => {
            file.seek(SeekFrom::Current(8)).map_err(error)?;
        }
        TypeId::String | TypeId::Bytes => {
            let count = number(file, mode)?;
            file.seek(SeekFrom::Current(i64::try_from(count).map_err(error)?))
                .map_err(error)?;
        }
        TypeId::Object => {
            for _ in 0..number(file, mode)? {
                let _ = string(file, mode)?;
                let mut kind = [0];
                file.read_exact(&mut kind).map_err(error)?;
                skip(
                    file,
                    mode,
                    TypeId::from_byte(kind[0]).map_err(error)?,
                    depth + 1,
                    start,
                )?;
            }
        }
        _ => return Err(error("packed snapshot contains an unsupported value shape")),
    }
    Ok(())
}
pub(super) fn open(path: &Path) -> Result<MapCursor> {
    let mut file = File::open(path).map_err(|e| io(path, e))?;
    let actual = file.metadata().map_err(error)?.len();
    let mut header = [0; 8];
    file.read_exact(&mut header).map_err(error)?;
    if &header[..4] != b"SOBJ" || u16::from_le_bytes([header[4], header[5]]) != 1 {
        return Err(error("unsupported packed snapshot document"));
    }
    let mode = LengthMode::from_code(header[6]).map_err(error)?;
    if number(&mut file, mode)? != actual || number(&mut file, mode)? != 0 {
        return Err(error(
            "packed snapshot has an invalid length or non-dynamic schema",
        ));
    }
    let mut kind = [0];
    file.read_exact(&mut kind).map_err(error)?;
    if TypeId::from_byte(kind[0]).map_err(error)? != TypeId::Object {
        return Err(error("packed snapshot root is not an object"));
    }
    let fields = number(&mut file, mode)?;
    let mut version = false;
    for _ in 0..fields {
        let key = string(&mut file, mode)?;
        file.read_exact(&mut kind).map_err(error)?;
        let kind = TypeId::from_byte(kind[0]).map_err(error)?;
        if key == "version" {
            if kind != TypeId::Uint32 {
                return Err(error("packed snapshot version has wrong type"));
            }
            let mut value = [0; 4];
            file.read_exact(&mut value).map_err(error)?;
            if u32::from_le_bytes(value) != 1 {
                return Err(error("unsupported packed snapshot version"));
            }
            version = true;
        } else if key == "entries" {
            if !version || kind != TypeId::Object {
                return Err(error("packed snapshot entries are not a certified map"));
            }
            let remaining = number(&mut file, mode)?;
            return Ok(MapCursor {
                offset: file.stream_position().map_err(error)?,
                remaining,
                mode: mode.code(),
            });
        } else {
            let start=file.stream_position().map_err(error)?;
            skip(&mut file, mode, kind, 0,start)?;
        }
    }
    Err(error("packed snapshot has no entries map"))
}
pub(super) fn next<T: for<'de> Deserialize<'de>>(
    path: &Path,
    cursor: &mut MapCursor,
    record_limit:u64,
    remaining:u64,
) -> Result<Option<(String, T, u64)>> {
    if cursor.remaining == 0 {
        return Ok(None);
    }
    let mut file = File::open(path).map_err(|e| io(path, e))?;
    file.seek(SeekFrom::Start(cursor.offset)).map_err(error)?;
    let original_offset = cursor.offset;
    let mode = LengthMode::from_code(cursor.mode).map_err(error)?;
    let key = string(&mut file, mode)?;
    let start = file.stream_position().map_err(error)?;
    let mut kind = [0];
    file.read_exact(&mut kind).map_err(error)?;
    skip(
        &mut file,
        mode,
        TypeId::from_byte(kind[0]).map_err(error)?,
        0,
        start,
    )?;
    let end = file.stream_position().map_err(error)?;
    if end > file.metadata().map_err(error)?.len() {
        return Err(error("packed snapshot entry exceeds certified source"));
    }
    let consumed=end-original_offset;
    if end-start>64*1024 || consumed>record_limit {return Err(error("packed snapshot single row exceeds configured record/turn limit; aggregate source preserved"));}
    if consumed>remaining {return Err(MetadataBackendError::StoragePreparationPending{root:path.to_owned(),processed_records:0});}
    let mut element = vec![0; usize::try_from(end - start).map_err(error)?];
    file.seek(SeekFrom::Start(start)).map_err(error)?;
    file.read_exact(&mut element).map_err(error)?;
    let mut document = b"SOBJ".to_vec();
    document.extend_from_slice(&1u16.to_le_bytes());
    document.extend_from_slice(&[mode.code(), 0]);
    let length = (8 + 2 * mode.width() + element.len()) as u64;
    document.extend_from_slice(&length.to_le_bytes()[..mode.width()]);
    document.extend_from_slice(&[0; 8][..mode.width()]);
    document.extend(element);
    let value = serde_ext::from_bytes(&document).map_err(error)?;
    cursor.offset = end;
    cursor.remaining -= 1;
    Ok(Some((key, value, original_offset)))
}
