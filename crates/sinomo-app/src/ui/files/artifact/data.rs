use super::*;
use npyz::{Endianness, NpyHeader, TypeChar};
use parquet::{
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
    schema::types::Type,
};
use rusqlite::{Connection, OpenFlags, types::ValueRef};
use sinomo_runtime::object::TypeId;
use std::io::{Read, Seek};

// csv's growable record buffer must not expand to the size of an unterminated
// record. Reset this I/O budget for each logical (possibly multiline) record.
pub(super) struct CsvInput {
    reader: std::io::BufReader<input::RangeFile>,
    remaining: usize,
}
impl Read for CsvInput {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            return Err(std::io::Error::other(
                "CSV record exceeds the 8 MiB preview limit",
            ));
        }
        let limit = out.len().min(self.remaining);
        let n = self.reader.read(&mut out[..limit])?;
        self.remaining -= n;
        Ok(n)
    }
}
impl Seek for CsvInput {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.reader.seek(pos)
    }
}
fn read_record(
    reader: &mut csv::Reader<CsvInput>,
    record: &mut csv::StringRecord,
) -> Result<bool, String> {
    reader.get_mut().remaining = 8 * 1024 * 1024;
    let found = reader.read_record(record).map_err(err)?;
    if record.len() > 4096 {
        return Err("CSV record exceeds the 4,096-column preview limit".into());
    }
    Ok(found)
}

pub(super) enum Data {
    Csv {
        reader: csv::Reader<CsvInput>,
        anchors: BTreeMap<u64, csv::Position>,
        stride: u64,
    },
    Sqlite {
        db: Connection,
        _snapshot: Option<tempfile::TempDir>,
    },
    Parquet(SerializedFileReader<Backing>),
    Array {
        input: Backing,
        header: NpyHeader,
        start: u64,
    },
}

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn columns(q: &Query, count: usize) -> Vec<usize> {
    if q.columns.is_empty() {
        (0..count.min(MAX_COLS)).collect()
    } else {
        q.columns
            .iter()
            .copied()
            .filter(|i| *i < count)
            .take(MAX_COLS)
            .collect()
    }
}

impl Data {
    pub(super) fn open(
        format: DocumentFormat,
        source: Backing,
        local_db: Option<PathBuf>,
    ) -> Result<Self, String> {
        match format {
            DocumentFormat::Csv | DocumentFormat::Tsv => {
                let reader = csv::ReaderBuilder::new()
                    .flexible(true)
                    .has_headers(false)
                    .delimiter(if format == DocumentFormat::Csv {
                        b','
                    } else {
                        b'\t'
                    })
                    .from_reader(CsvInput {
                        reader: source.reader(),
                        remaining: 8 * 1024 * 1024,
                    });
                Ok(Self::Csv {
                    reader,
                    stride: 1024,
                    anchors: BTreeMap::from([(0, csv::Position::new())]),
                })
            }
            DocumentFormat::Sqlite => {
                let mut tmp = None;
                let path = if let Some(path) = local_db {
                    path
                } else {
                    if source.size() > 512 * 1024 * 1024 {
                        return Err("SQLite snapshot exceeds the 512 MiB preview limit".into());
                    }
                    let directory = tempfile::tempdir().map_err(err)?;
                    sinomo_runtime::core::storage::durable_file::ensure_private_directory(
                        directory.path(),
                    )
                    .map_err(err)?;
                    let path = directory.path().join("snapshot.sqlite");
                    let mut file = fs::File::create(&path).map_err(err)?;
                    std::io::copy(&mut source.reader(), &mut file).map_err(err)?;
                    tmp = Some(directory);
                    path
                };
                let mut db = Connection::open_with_flags(
                    path,
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .map_err(err)?;
                let closed = source.closed.clone();
                db.progress_handler(10000, Some(move || closed.load(Ordering::Acquire)))
                    .map_err(err)?;
                db.busy_timeout(Duration::from_millis(500)).map_err(err)?;
                db.set_limit(
                    rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
                    16 * 1024 * 1024,
                )
                .map_err(err)?;
                db.execute_batch("PRAGMA query_only=ON; PRAGMA trusted_schema=OFF; BEGIN")
                    .map_err(err)?;
                Ok(Self::Sqlite { db, _snapshot: tmp })
            }
            DocumentFormat::Parquet => Ok(Self::Parquet(
                SerializedFileReader::new(source).map_err(err)?,
            )),
            DocumentFormat::Npy => {
                let mut r = source.reader();
                let mut pre = [0; 8];
                r.read_exact(&mut pre).map_err(err)?;
                if &pre[..6] != b"\x93NUMPY" {
                    return Err("Invalid NumPy signature".into());
                }
                let mut len = [0; 4];
                let n = match pre[6] {
                    1 => 2,
                    2 | 3 => 4,
                    _ => return Err("Unsupported NPY version".into()),
                };
                r.read_exact(&mut len[..n]).map_err(err)?;
                let header_len = u32::from_le_bytes(len) as usize;
                if header_len > 65536 {
                    return Err("NPY header exceeds 64 KiB".into());
                }
                let mut raw = vec![0; header_len];
                r.read_exact(&mut raw).map_err(err)?;
                // Bound nesting before handing Python literal syntax to the header parser.
                let mut depth = 0usize;
                let mut quote = None;
                let mut escaped = false;
                for &b in &raw {
                    if let Some(mark) = quote {
                        if escaped {
                            escaped = false;
                        } else if b == b'\\' {
                            escaped = true;
                        } else if b == mark {
                            quote = None;
                        }
                        continue;
                    }
                    if matches!(b, b'\'' | b'"') {
                        quote = Some(b);
                        continue;
                    }
                    match b {
                        b'(' | b'[' | b'{' => {
                            depth += 1;
                            if depth > 64 {
                                return Err("NPY header is too deeply nested".into());
                            }
                        }
                        b')' | b']' | b'}' => depth = depth.saturating_sub(1),
                        _ => {}
                    }
                }
                let start = r.stream_position().map_err(err)?;
                let header = std::panic::catch_unwind(|| {
                    NpyHeader::from_reader([&pre[..], &len[..n], &raw].concat().as_slice())
                })
                .map_err(|_| "Invalid NPY dimensions")?
                .map_err(err)?;
                let elements = header
                    .shape()
                    .iter()
                    .try_fold(1u64, |n, d| n.checked_mul(*d))
                    .ok_or("NPY shape overflow")?;
                if let Some(size) = header.dtype().num_bytes()
                    && !header.uses_pickled_array()
                    && elements
                        .checked_mul(size as u64)
                        .and_then(|n| n.checked_add(start))
                        != Some(source.size())
                {
                    return Err("NPY payload length does not match its shape".into());
                }
                Ok(Self::Array {
                    input: source,
                    header,
                    start,
                })
            }
            _ => Err("Unsupported data format".into()),
        }
    }

    pub(super) fn page(&mut self, q: &Query) -> Result<Screen, String> {
        match self {
            Self::Sqlite { db, .. } => sqlite_page(db, q),
            Self::Parquet(reader) => parquet_page(reader, q),
            Self::Array {
                input,
                header,
                start,
            } => array_page(input, header, *start, q),
            Self::Csv {
                reader,
                anchors,
                stride,
            } => {
                reader.seek(csv::Position::new()).map_err(err)?;
                let mut record = csv::StringRecord::new();
                read_record(reader, &mut record)?;
                let header: Vec<_> = record
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        if q.header && !name.is_empty() {
                            short(name)
                        } else {
                            format!("Column {}", i + 1)
                        }
                    })
                    .collect();
                let (mut n, anchor) = anchors
                    .range(..=q.offset.saturating_add(q.header as u64))
                    .next_back()
                    .map(|(n, pos)| (*n, pos.clone()))
                    .unwrap();
                reader.seek(anchor).map_err(err)?;
                let mut rows = vec![];
                let mut count = header.len();
                let desired = q.offset.saturating_add(q.header as u64);
                while read_record(reader, &mut record)? {
                    if n % *stride == 0 {
                        anchors.insert(n, record.position().unwrap().clone());
                        if anchors.len() > 4096 {
                            *stride *= 2;
                            anchors.retain(|index, _| *index % *stride == 0);
                        }
                    }
                    if n >= desired {
                        count = count.max(record.len());
                        let indices = columns(q, 4096);
                        rows.push(
                            indices
                                .into_iter()
                                .map(|i| {
                                    record
                                        .get(i)
                                        .map(|v| Json::String(short(v)))
                                        .unwrap_or(Json::Null)
                                })
                                .collect::<Vec<_>>(),
                        );
                        if rows.len() > PAGE {
                            break;
                        }
                    }
                    n += 1;
                }
                let mut header = header;
                header.extend((header.len()..count).map(|i| format!("Column {}", i + 1)));
                let schema = header
                    .into_iter()
                    .map(|name| (name, "Text".into()))
                    .collect::<Vec<_>>();
                let indices = columns(q, schema.len());
                for row in &mut rows {
                    row.truncate(indices.len());
                }
                Ok(grid(
                    q,
                    schema,
                    indices,
                    rows,
                    None,
                    "Delimited text".into(),
                ))
            }
        }
    }
}

fn sqlite_page(db: &Connection, q: &Query) -> Result<Screen, String> {
    let limit = PAGE as i64 + 1;
    let offset = i64::try_from(q.offset).map_err(|_| "Row number too large")?;
    if q.table.is_none() {
        let mut s=db.prepare("SELECT name FROM pragma_table_list WHERE schema='main' AND type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name LIMIT ?1 OFFSET ?2").map_err(err)?;
        let names = s
            .query_map((limit, offset), |r| r.get::<_, String>(0))
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?;
        return Ok(list(
            "Tables",
            names
                .into_iter()
                .map(|n| Entry {
                    name: n.clone(),
                    detail: "Table".into(),
                    action: Action::Table(n),
                })
                .collect(),
            None,
        ));
    }
    let name = q.table.as_deref().unwrap();
    let allowed:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM pragma_table_list WHERE schema='main' AND type='table' AND name=?1)",[name],|r|r.get(0)).map_err(err)?;
    if !allowed {
        return Err("Table is unavailable".into());
    }
    let mut s = db
        .prepare("SELECT name,type,pk FROM pragma_table_info(?1) ORDER BY cid")
        .map_err(err)?;
    let schema = s
        .query_map([name], |r| {
            let t: String = r.get(1)?;
            let pk: i64 = r.get(2)?;
            Ok((
                r.get::<_, String>(0)?,
                if pk > 0 {
                    format!("{t} · primary key {pk}")
                } else {
                    t
                },
            ))
        })
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    let cols = columns(q, schema.len());
    if q.schema {
        return Ok(schema_page(q, &schema));
    }
    let mut keys = db
        .prepare("SELECT name FROM pragma_table_info(?1) WHERE pk>0 ORDER BY pk")
        .map_err(err)?;
    let mut order = keys
        .query_map([name], |r| r.get::<_, String>(0))
        .map_err(err)?
        .map(|v| v.map(|s| ident(&s)))
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    if order.is_empty()
        && let Some(alias) = ["rowid", "_rowid_", "oid"]
            .iter()
            .find(|key| !schema.iter().any(|(n, _)| n.eq_ignore_ascii_case(key)))
    {
        order.push((*alias).into());
    }
    let ordered = if order.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {}", order.join(","))
    };
    let sql = format!(
        "SELECT {} FROM main.{}{ordered} LIMIT ?1 OFFSET ?2",
        cols.iter()
            .map(|i| ident(&schema[*i].0))
            .collect::<Vec<_>>()
            .join(","),
        ident(name)
    );
    let mut stmt = db.prepare(&sql).map_err(err)?;
    let rows = stmt
        .query_map((limit, offset), |r| {
            cols.iter()
                .enumerate()
                .map(|(i, _)| {
                    Ok(match r.get_ref(i)? {
                        ValueRef::Null => Json::Null,
                        ValueRef::Integer(v) => v.into(),
                        ValueRef::Real(v) if !v.is_finite() => v.to_string().into(),
                        ValueRef::Real(v) => Json::from(v),
                        ValueRef::Text(v) => Json::String(short(&String::from_utf8_lossy(v))),
                        ValueRef::Blob(b) => Json::String(format!(
                            "BLOB · {} bytes · {}",
                            b.len(),
                            formats::encode_hex(&b[..b.len().min(16)])
                        )),
                    })
                })
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(grid(q, schema, cols, rows, None, name.into()))
}

fn parquet_value(v: &parquet::record::Field, depth: usize, budget: &mut usize) -> Json {
    use parquet::record::Field;
    if depth >= 32 || *budget == 0 {
        return "… preview limit".into();
    }
    *budget -= 1;
    match v {
        Field::Bytes(b) => format!(
            "BLOB · {} bytes · {}",
            b.len(),
            formats::encode_hex(&b.data()[..b.len().min(16)])
        )
        .into(),
        Field::Str(s) => short(s).into(),
        Field::Float(n) if !n.is_finite() => n.to_string().into(),
        Field::Double(n) if !n.is_finite() => n.to_string().into(),
        Field::Float16(n) if !n.is_finite() => n.to_string().into(),
        Field::Group(row) => Json::Object(
            row.get_column_iter()
                .take(256)
                .map(|(k, v)| (k.clone(), parquet_value(v, depth + 1, budget)))
                .collect(),
        ),
        Field::ListInternal(list) => {
            let mut out = Vec::new();
            for e in list.elements().iter().take(256) {
                if *budget == 0 {
                    break;
                }
                out.push(parquet_value(e, depth + 1, budget));
            }
            if out.len() < list.len() {
                out.push(format!("… {} more items", list.len() - out.len()).into());
            }
            Json::Array(out)
        }
        Field::MapInternal(map) => {
            let mut out = Vec::new();
            for (k, v) in map.entries().iter().take(256) {
                if *budget == 0 {
                    break;
                }
                out.push(serde_json::json!({"key":parquet_value(k,depth+1,budget),"value":parquet_value(v,depth+1,budget)}));
            }
            if out.len() < map.len() {
                out.push(format!("… {} more entries", map.len() - out.len()).into());
            }
            Json::Array(out)
        }
        _ => v.to_json_value(),
    }
}
fn parquet_page(reader: &SerializedFileReader<Backing>, q: &Query) -> Result<Screen, String> {
    let meta = reader.metadata();
    let file = meta.file_metadata();
    let root = file.schema_descr().root_schema();
    let fields = root.get_fields();
    let schema = fields
        .iter()
        .map(|t| {
            let mut text = Vec::new();
            parquet::schema::printer::print_schema(&mut text, t);
            (
                t.name().into(),
                short(String::from_utf8_lossy(&text).trim()),
            )
        })
        .collect::<Vec<_>>();
    if q.schema {
        return Ok(schema_page(q, &schema));
    }
    let cols = columns(q, schema.len());
    let projection = Type::group_type_builder(root.name())
        .with_fields(cols.iter().map(|i| fields[*i].clone()).collect())
        .build()
        .map_err(err)?;
    let mut skip = q.offset;
    let mut rows = vec![];
    for i in 0..reader.num_row_groups() {
        let n = meta.row_group(i).num_rows() as u64;
        if skip >= n {
            skip -= n;
            continue;
        }
        let mut decoded = 0u64;
        for column in meta.row_group(i).columns() {
            if cols.iter().any(|j| {
                column
                    .column_path()
                    .parts()
                    .first()
                    .is_some_and(|name| name == fields[*j].name())
            }) {
                let size = u64::try_from(column.uncompressed_size()).map_err(err)?;
                decoded = decoded.saturating_add(size);
            }
        }
        if decoded > 128 * 1024 * 1024 {
            return Err("Selected columns exceed 128 MiB in this row group. Choose fewer columns in Schema.".into());
        }
        let group = reader.get_row_group(i).map_err(err)?;
        for row in group
            .get_row_iter(Some(projection.clone()))
            .map_err(err)?
            .skip(skip as usize)
            .take(PAGE + 1 - rows.len())
        {
            rows.push(
                row.map_err(err)?
                    .get_column_iter()
                    .map(|(_, v)| parquet_value(v, 0, &mut 4096))
                    .collect(),
            );
        }
        if rows.len() > PAGE {
            break;
        }
        skip = 0;
    }
    Ok(grid(
        q,
        schema,
        cols,
        rows,
        Some(file.num_rows() as u64),
        format!("{} row groups", reader.num_row_groups()),
    ))
}

fn array_page(source: &Backing, h: &NpyHeader, start: u64, q: &Query) -> Result<Screen, String> {
    let note = format!(
        "{} · shape {:?} · {:?} order",
        h.dtype().descr(),
        h.shape(),
        h.order()
    );
    if h.uses_pickled_array() {
        return Ok(Screen::Info(format!(
            "{note}\nObject array · metadata only"
        )));
    }
    let npyz::DType::Plain(t) = h.dtype() else {
        return Ok(Screen::Info(format!(
            "{note}\nStructured dtype · metadata only"
        )));
    };
    enum Scalar {
        Basic(TypeId),
        Half,
        Complex(TypeId),
    }
    let id = match (t.type_char(), t.size_field()) {
        (TypeChar::Bool, 1) => Scalar::Basic(TypeId::Boolean),
        (TypeChar::Int, 1) => Scalar::Basic(TypeId::Int8),
        (TypeChar::Int, 2) => Scalar::Basic(TypeId::Int16),
        (TypeChar::Int, 4) => Scalar::Basic(TypeId::Int32),
        (TypeChar::Int, 8) => Scalar::Basic(TypeId::Int64),
        (TypeChar::Uint, 1) => Scalar::Basic(TypeId::Uint8),
        (TypeChar::Uint, 2) => Scalar::Basic(TypeId::Uint16),
        (TypeChar::Uint, 4) => Scalar::Basic(TypeId::Uint32),
        (TypeChar::Uint, 8) => Scalar::Basic(TypeId::Uint64),
        (TypeChar::Float, 4) => Scalar::Basic(TypeId::Float),
        (TypeChar::Float, 8) => Scalar::Basic(TypeId::Double),
        (TypeChar::Float, 2) => Scalar::Half,
        (TypeChar::Complex, 8) => Scalar::Complex(TypeId::Float),
        (TypeChar::Complex, 16) => Scalar::Complex(TypeId::Double),
        _ => {
            return Ok(Screen::Info(format!(
                "{note}\nMetadata only for this dtype"
            )));
        }
    };
    let schema = vec![
        ("Index".into(), "Coordinates".into()),
        ("Value".into(), t.to_string()),
    ];
    if q.schema {
        return Ok(schema_page(q, &schema));
    }
    let n = h.len().saturating_sub(q.offset).min(PAGE as u64 + 1);
    let w = t.size_field() as usize;
    let mut r = source.reader();
    r.seek(std::io::SeekFrom::Start(
        start + q.offset.min(h.len()) * w as u64,
    ))
    .map_err(err)?;
    let cols = columns(q, 2);
    let mut rows = vec![];
    for i in 0..n {
        let mut b = [0; 16];
        r.read_exact(&mut b[..w]).map_err(err)?;
        let number = |id: TypeId, data: &mut [u8]| {
            if t.endianness() == Endianness::Big {
                data.reverse();
            }
            sobj::display(id, data).unwrap_or_else(|| "Invalid value".into())
        };
        let value = match id {
            Scalar::Basic(id) => number(id, &mut b[..w]),
            Scalar::Half => {
                let bits = if t.endianness() == Endianness::Big {
                    u16::from_be_bytes([b[0], b[1]])
                } else {
                    u16::from_le_bytes([b[0], b[1]])
                };
                half::f16::from_bits(bits).to_string()
            }
            Scalar::Complex(id) => {
                let (real, imag) = b[..w].split_at_mut(w / 2);
                format!("({}, {}j)", number(id, real), number(id, imag))
            }
        };
        let index = h
            .shape()
            .iter()
            .zip(h.strides())
            .map(|(d, stride)| ((q.offset + i) / stride % d).to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let values = [Json::String(format!("[{index}]")), Json::String(value)];
        rows.push(cols.iter().map(|i| values[*i].clone()).collect());
    }
    Ok(grid(q, schema, cols, rows, Some(h.len()), note))
}
