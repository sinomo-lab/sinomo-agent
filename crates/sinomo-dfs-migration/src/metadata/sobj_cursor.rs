//! Bounded row cursor for retained dynamic SOBJ checkpoints.
use super::*;
use sinomo_core::object::{LengthMode, TypeId, serde_ext};
use std::io::{Read, Seek, SeekFrom};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Cursor {
    offset: u64,
    mode: u8,
    root_fields: u64,
    table_fields: u64,
    table: String,
    rows: u64,
    row_kind: u8,
    #[serde(default)]
    sequence_field: Option<String>,
}
pub(crate) enum Item {
    Header(String, Value),
    Row(String, Value, super::source_document::SourceRawDocument),
    Op(Value, super::source_document::SourceRawDocument),
}
fn fail(e: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(e.to_string())
}
fn byte(file: &mut File) -> Result<u8> {
    let mut value = [0];
    file.read_exact(&mut value).map_err(fail)?;
    Ok(value[0])
}
fn number(file: &mut File, mode: LengthMode) -> Result<u64> {
    let mut value = [0; 8];
    file.read_exact(&mut value[..mode.width()]).map_err(fail)?;
    Ok(u64::from_le_bytes(value))
}
fn seek(file: &mut File, count: u64) -> Result<()> {
    let position = file.stream_position().map_err(fail)?;
    let end = position
        .checked_add(count)
        .ok_or_else(|| fail("retained SOBJ offset overflow"))?;
    if end > file.metadata().map_err(fail)?.len() {
        return Err(fail("retained SOBJ value exceeds file boundary"));
    }
    file.seek(SeekFrom::Start(end)).map_err(fail)?;
    Ok(())
}
fn string(file: &mut File, mode: LengthMode) -> Result<String> {
    let count = number(file, mode)?;
    if count > 1024 * 1024 {
        return Err(fail("retained SOBJ key exceeds conversion bound"));
    }
    let mut bytes = vec![0; usize::try_from(count).map_err(fail)?];
    file.read_exact(&mut bytes).map_err(fail)?;
    String::from_utf8(bytes).map_err(fail)
}
#[derive(Clone)]
enum Descriptor {
    Builtin(TypeId),
    Object,
    List(Box<Descriptor>),
    Array(Box<Descriptor>),
    Optional(Box<Descriptor>),
}
fn descriptor(file: &mut File, depth: usize) -> Result<Descriptor> {
    if depth > 64 {
        return Err(fail(
            "retained SOBJ descriptor nesting exceeds conversion bound",
        ));
    }
    Ok(match byte(file)? {
        k @ (0x01..=0x0b | 0x24 | 0x25) => Descriptor::Builtin(TypeId::from_byte(k).map_err(fail)?),
        0x43 => Descriptor::Object,
        0x41 => Descriptor::List(Box::new(descriptor(file, depth + 1)?)),
        0x42 => Descriptor::Array(Box::new(descriptor(file, depth + 1)?)),
        0x44 => Descriptor::Optional(Box::new(descriptor(file, depth + 1)?)),
        _ => {
            return Err(fail(
                "retained SOBJ schema/extension descriptor is unsupported",
            ));
        }
    })
}
fn skip_typed(file: &mut File, mode: LengthMode, shape: &Descriptor, depth: usize) -> Result<()> {
    match shape {
        Descriptor::Builtin(k) => skip(file, mode, *k, depth),
        Descriptor::Object => skip(file, mode, TypeId::Object, depth),
        Descriptor::List(inner) => {
            let actual = descriptor(file, depth + 1)?;
            let _ = inner;
            for _ in 0..number(file, mode)? {
                skip_typed(file, mode, &actual, depth + 1)?;
            }
            Ok(())
        }
        Descriptor::Array(inner) => {
            let _ = inner;
            skip(file, mode, TypeId::Array, depth)
        }
        Descriptor::Optional(inner) => match byte(file)? {
            0 => Ok(()),
            1 => skip_typed(file, mode, inner, depth + 1),
            _ => Err(fail("invalid retained optional presence")),
        },
    }
}
fn skip(file: &mut File, mode: LengthMode, kind: TypeId, depth: usize) -> Result<()> {
    if depth > 64 {
        return Err(fail("retained SOBJ value nesting exceeds conversion bound"));
    }
    match kind {
        TypeId::Boolean | TypeId::Uint8 | TypeId::Int8 => seek(file, 1),
        TypeId::Uint16 | TypeId::Int16 => seek(file, 2),
        TypeId::Uint32 | TypeId::Int32 | TypeId::Float => seek(file, 4),
        TypeId::Uint64 | TypeId::Int64 | TypeId::Double => seek(file, 8),
        TypeId::String | TypeId::Bytes => {
            let count = number(file, mode)?;
            seek(file, count)
        }
        TypeId::Object => {
            for _ in 0..number(file, mode)? {
                let _ = string(file, mode)?;
                let kind = TypeId::from_byte(byte(file)?).map_err(fail)?;
                skip(file, mode, kind, depth + 1)?;
            }
            Ok(())
        }
        TypeId::List => {
            let shape = descriptor(file, depth + 1)?;
            for _ in 0..number(file, mode)? {
                skip_typed(file, mode, &shape, depth + 1)?;
            }
            Ok(())
        }
        TypeId::Array => {
            let _ = descriptor(file, depth + 1)?;
            let width = number(file, mode)?;
            if width == 0 {
                return Err(fail("zero retained array element width"));
            }
            let rank = byte(file)?;
            let mut count = 1u64;
            for _ in 0..rank {
                count = count
                    .checked_mul(number(file, mode)?)
                    .ok_or_else(|| fail("retained array count overflow"))?;
            }
            seek(
                file,
                count
                    .checked_mul(width)
                    .ok_or_else(|| fail("retained array bytes overflow"))?,
            )
        }
        TypeId::Optional => {
            let shape = descriptor(file, depth + 1)?;
            match byte(file)? {
                0 => Ok(()),
                1 => skip_typed(file, mode, &shape, depth + 1),
                _ => Err(fail("invalid retained optional presence")),
            }
        }
        _ => Err(fail("retained SOBJ value shape is unsupported")),
    }
}
fn decode_original(
    file: &mut File,
    mode: LengthMode,
    kind: u8,
) -> Result<(Value, super::source_document::SourceRawDocument)> {
    let start = file.stream_position().map_err(fail)?;
    skip(file, mode, TypeId::from_byte(kind).map_err(fail)?, 0)?;
    let end = file.stream_position().map_err(fail)?;
    if end - start > 64 * 1024 * 1024 {
        return Err(fail("retained metadata row exceeds 64MiB conversion bound"));
    }
    let mut value = vec![0; usize::try_from(end - start).map_err(fail)?];
    file.seek(SeekFrom::Start(start)).map_err(fail)?;
    file.read_exact(&mut value).map_err(fail)?;
    let length = (8 + 2 * mode.width() + 1 + value.len()) as u64;
    let mut document = b"SOBJ".to_vec();
    document.extend_from_slice(&1u16.to_le_bytes());
    document.extend_from_slice(&[mode.code(), 0]);
    document.extend_from_slice(&length.to_le_bytes()[..mode.width()]);
    document.extend_from_slice(&[0; 8][..mode.width()]);
    document.push(kind);
    document.extend(value);
    let value = super::source_document::decode_original_document(&document)?;
    Ok((
        value,
        super::source_document::SourceRawDocument::row(document),
    ))
}
fn decode(file: &mut File, mode: LengthMode, kind: u8) -> Result<Value> {
    decode_original(file, mode, kind).map(|(value, _)| value)
}
impl Cursor {
    pub(crate) fn position(&self) -> u64 {
        self.offset
    }
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path).map_err(fail)?;
        let mut header = [0; 8];
        file.read_exact(&mut header).map_err(fail)?;
        if &header[..4] != b"SOBJ" {
            return Err(fail("retained metadata checkpoint is not SOBJ"));
        }
        let mode = LengthMode::from_code(header[6]).map_err(fail)?;
        if number(&mut file, mode)? != file.metadata().map_err(fail)?.len()
            || number(&mut file, mode)? != 0
            || byte(&mut file)? != TypeId::Object.byte()
        {
            return Err(fail(
                "retained metadata checkpoint has invalid dynamic framing",
            ));
        }
        let root_fields = number(&mut file, mode)?;
        Ok(Self {
            offset: file.stream_position().map_err(fail)?,
            mode: mode.code(),
            root_fields,
            table_fields: 0,
            table: String::new(),
            rows: 0,
            row_kind: 0,
            sequence_field: None,
        })
    }
    pub(crate) fn open_sequence(path: &Path, field: &str) -> Result<Self> {
        let mut cursor = Self::open(path)?;
        cursor.sequence_field = Some(field.into());
        Ok(cursor)
    }
    pub(crate) fn next(&mut self, path: &Path) -> Result<Option<Item>> {
        let mut file = File::open(path).map_err(fail)?;
        file.seek(SeekFrom::Start(self.offset)).map_err(fail)?;
        let mode = LengthMode::from_code(self.mode).map_err(fail)?;
        loop {
            if self.rows > 0 {
                let (value, raw_document) = decode_original(&mut file, mode, self.row_kind)?;
                self.rows -= 1;
                self.offset = file.stream_position().map_err(fail)?;
                return Ok(Some(if self.table == "$ops" {
                    Item::Op(value, raw_document)
                } else {
                    Item::Row(self.table.clone(), value, raw_document)
                }));
            }
            if self.table_fields > 0 {
                self.table = string(&mut file, mode)?;
                self.table_fields -= 1;
                let kind = byte(&mut file)?;
                if kind == TypeId::List.byte() {
                    let shape = descriptor(&mut file, 0)?;
                    self.rows = number(&mut file, mode)?;
                    self.row_kind = match shape {
                        Descriptor::Object => TypeId::Object.byte(),
                        Descriptor::Builtin(k) => k.byte(),
                        _ => {
                            return Err(fail(
                                "metadata table rows require object or scalar elements",
                            ));
                        }
                    };
                    continue;
                }
                let (value, raw_document) = decode_original(&mut file, mode, kind)?;
                self.offset = file.stream_position().map_err(fail)?;
                return Ok(Some(Item::Row(self.table.clone(), value, raw_document)));
            }
            if self.root_fields == 0 {
                if file.stream_position().map_err(fail)? != file.metadata().map_err(fail)?.len() {
                    return Err(fail("retained metadata checkpoint has trailing bytes"));
                }
                self.offset = file.stream_position().map_err(fail)?;
                return Ok(None);
            }
            let key = string(&mut file, mode)?;
            self.root_fields -= 1;
            let kind = byte(&mut file)?;
            if key == "tables" && kind == TypeId::Object.byte() {
                self.table_fields = number(&mut file, mode)?;
                continue;
            }
            if (key == "ops" || self.sequence_field.as_deref() == Some(key.as_str()))
                && kind == TypeId::List.byte()
            {
                let shape = descriptor(&mut file, 0)?;
                let rows = number(&mut file, mode)?;
                // The ordinary SOBJ serializer uses a Bytes descriptor for an
                // empty sequence. Only actual nonempty operation rows need an
                // Object descriptor; framing/count/EOF remain fully checked.
                if rows != 0 && !matches!(shape, Descriptor::Object) {
                    return Err(fail("retained snapshot operations are not objects"));
                }
                self.table = if key == "ops" { "$ops".into() } else { key };
                self.row_kind = TypeId::Object.byte();
                self.rows = rows;
                if self.sequence_field.as_deref() == Some(self.table.as_str()) {
                    self.offset = file.stream_position().map_err(fail)?;
                    return Ok(Some(Item::Header(self.table.clone(), Value::Null)));
                }
                continue;
            }
            let value = decode(&mut file, mode, kind)?;
            self.offset = file.stream_position().map_err(fail)?;
            return Ok(Some(Item::Header(key, value)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct Snapshot {
        before: u32,
        ops: Vec<serde_bytes::ByteBuf>,
        entries: Vec<serde_bytes::ByteBuf>,
        after: u32,
    }

    fn fixture(
        name: &str,
        ops: bool,
        entries: bool,
    ) -> (sinomo_testdir::TestDir, PathBuf, Vec<u8>) {
        let root = sinomo_testdir::TestDir::new(name);
        let path = root.path().join("snapshot.sobj");
        let bytes = serde_ext::to_bytes(
            &Snapshot {
                before: 1,
                ops: if ops {
                    vec![serde_bytes::ByteBuf::from(vec![7])]
                } else {
                    Vec::new()
                },
                entries: if entries {
                    vec![serde_bytes::ByteBuf::from(vec![9])]
                } else {
                    Vec::new()
                },
                after: 2,
            },
            1,
            LengthMode::U32,
        )
        .unwrap();
        std::fs::write(&path, &bytes).unwrap();
        (root, path, bytes)
    }

    #[test]
    fn ordinary_empty_ops_and_named_sequence_preserve_restart_and_eof() {
        let (_root, path, bytes) = fixture("mig-empty-sobj-sequence", false, false);
        for named in [false, true] {
            let mut cursor = if named {
                Cursor::open_sequence(&path, "entries")
            } else {
                Cursor::open(&path)
            }
            .unwrap();
            assert!(
                matches!(cursor.next(&path).unwrap(), Some(Item::Header(key, value))
                if key == "before" && value == serde_json::json!(1))
            );
            let prior_offset = cursor.offset;
            assert!(
                matches!(cursor.next(&path).unwrap(), Some(Item::Header(key, value))
                if key == "entries" && value == if named { Value::Null } else { serde_json::json!([]) })
            );
            assert!(cursor.offset > prior_offset);
            let saved = serde_json::to_vec(&cursor).unwrap();
            let mut resumed: Cursor = serde_json::from_slice(&saved).unwrap();
            assert_eq!(resumed.offset, cursor.offset);
            assert!(
                matches!(resumed.next(&path).unwrap(), Some(Item::Header(key, value))
                if key == "after" && value == serde_json::json!(2))
            );
            assert!(resumed.next(&path).unwrap().is_none());
            assert_eq!(resumed.offset, bytes.len() as u64);
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn ordinary_nonempty_bytes_ops_and_named_sequence_are_rejected() {
        for (ops, entries) in [(true, false), (false, true)] {
            let (_root, path, _) = fixture("mig-nonobject-sobj-sequence", ops, entries);
            let mut cursor = Cursor::open_sequence(&path, "entries").unwrap();
            assert!(cursor.next(&path).unwrap().is_some());
            assert!(
                matches!(cursor.next(&path), Err(MetadataBackendError::Serde(reason))
                if reason == "retained snapshot operations are not objects")
            );
        }
    }

    #[test]
    fn empty_sequence_preserves_document_framing_and_trailing_byte_rejection() {
        let (_root, path, mut bytes) = fixture("mig-empty-sobj-sequence-framing", false, false);
        let valid = bytes.clone();
        bytes.pop();
        std::fs::write(&path, &bytes).unwrap();
        assert!(Cursor::open(&path).is_err());
        bytes = valid;
        bytes.push(0);
        // Give the malformed document a truthful outer length so the cursor
        // reaches its ordinary root-field EOF check, rather than only framing.
        let length = u32::try_from(bytes.len()).unwrap().to_le_bytes();
        bytes[8..12].copy_from_slice(&length);
        std::fs::write(&path, &bytes).unwrap();
        let mut cursor = Cursor::open(&path).unwrap();
        for _ in 0..3 {
            assert!(cursor.next(&path).unwrap().is_some());
        }
        assert!(
            matches!(cursor.next(&path), Err(MetadataBackendError::Serde(reason))
            if reason == "retained metadata checkpoint has trailing bytes")
        );
    }

    #[test]
    fn empty_ops_still_requires_its_complete_list_count() {
        let (_root, path, mut bytes) = fixture("mig-empty-sobj-sequence-count", false, false);
        let mut cursor = Cursor::open(&path).unwrap();
        assert!(cursor.next(&path).unwrap().is_some());
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(cursor.offset)).unwrap();
        assert_eq!(string(&mut file, LengthMode::U32).unwrap(), "ops");
        assert_eq!(byte(&mut file).unwrap(), TypeId::List.byte());
        assert!(matches!(
            descriptor(&mut file, 0).unwrap(),
            Descriptor::Builtin(TypeId::Bytes)
        ));
        let count_offset = usize::try_from(file.stream_position().unwrap()).unwrap();
        drop(file);
        bytes.truncate(count_offset + LengthMode::U32.width() - 1);
        let length = u32::try_from(bytes.len()).unwrap().to_le_bytes();
        bytes[8..12].copy_from_slice(&length);
        std::fs::write(&path, &bytes).unwrap();
        let mut cursor = Cursor::open(&path).unwrap();
        assert!(cursor.next(&path).unwrap().is_some());
        assert!(
            matches!(cursor.next(&path), Err(MetadataBackendError::Serde(reason))
            if reason == "failed to fill whole buffer")
        );
    }
}
