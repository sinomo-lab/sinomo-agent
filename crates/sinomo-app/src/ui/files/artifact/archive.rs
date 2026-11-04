use super::*;
use std::io::{BufReader, Read};

const MAX_MEMBER: u64 = 64 * 1024 * 1024;
#[derive(Clone)]
pub(crate) struct Archive {
    source: Backing,
    zip: Arc<Mutex<zip::ZipArchive<BufReader<input::RangeFile>>>>,
    entries: Arc<BTreeMap<String, Member>>,
    omitted: usize,
}
#[derive(Clone)]
struct Member {
    index: Option<usize>,
    dir: bool,
}
fn safe(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 4096
        && name.split('/').count() <= 64
        && !name.contains(['\\', ':'])
        && !name.chars().any(char::is_control)
        && name.split('/').all(|s| !matches!(s, "" | "." | ".."))
}
impl Archive {
    pub(in crate::ui) fn open(input: Backing) -> Result<Self, String> {
        let zip = zip::ZipArchive::new(input.reader()).map_err(err)?;
        if zip.len() > 100_000 {
            return Err("Archive exceeds the 100,000-entry browsing limit".into());
        }
        let mut entries = BTreeMap::new();
        let mut omitted = 0;
        for (i, raw_name) in zip.file_names().enumerate() {
            let name = raw_name.trim_end_matches('/');
            if !safe(name) {
                omitted += 1;
                continue;
            }
            let mut prefix = String::new();
            for part in name.split('/') {
                if !prefix.is_empty() {
                    prefix.push('/');
                }
                prefix.push_str(part);
                let last = prefix == name;
                let dir = !last || raw_name.ends_with('/');
                if entries.len() >= 100_000 && !entries.contains_key(&prefix) {
                    return Err("Archive directory index exceeds the browsing limit".into());
                }
                let e = entries
                    .entry(prefix.clone())
                    .or_insert(Member { index: None, dir });
                if e.dir != dir || last && e.index.is_some() {
                    return Err("Archive contains conflicting entry paths".into());
                }
                if last {
                    e.index = Some(i);
                }
            }
        }
        Ok(Self {
            source: input,
            zip: Arc::new(Mutex::new(zip)),
            entries: Arc::new(entries),
            omitted,
        })
    }
    // Stored members are addressable directly, including very large .npy arrays.
    // Compressed members are inflated only once they are opened.
    pub(super) fn input(&self, name: &str) -> Result<Backing, String> {
        let index = self
            .entries
            .get(name)
            .filter(|e| !e.dir)
            .and_then(|e| e.index)
            .ok_or("Archive member unavailable")?;
        let mut zip = self.zip.lock().map_err(|_| "Archive reader unavailable")?;
        let file = zip.by_index(index).map_err(err)?;
        if file.encrypted() || file.is_symlink() {
            return Err("Encrypted entries and symbolic links cannot be previewed".into());
        }
        let size = file.size();
        if file.compression() == zip::CompressionMethod::Stored && file.compressed_size() == size {
            return self
                .source
                .slice(file.data_start().ok_or("Missing member offset")?, size);
        }
        if size > MAX_MEMBER {
            return Err("Member exceeds the 64 MiB decompression limit".into());
        }
        let mut bytes = Vec::new();
        file.take(size + 1).read_to_end(&mut bytes).map_err(err)?;
        if bytes.len() as u64 != size {
            return Err("Invalid uncompressed member length".into());
        }
        let mut backing = Backing::memory(bytes);
        backing.closed = self.source.closed.clone();
        Ok(backing)
    }
    pub(crate) fn read(&self, name: &str) -> Result<Vec<u8>, String> {
        self.input(name)?.bytes(MAX_MEMBER)
    }
    pub(super) fn has(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }
    pub(super) fn listing(&self, q: &Query) -> Result<Screen, String> {
        let prefix = if q.folder.is_empty() {
            String::new()
        } else {
            format!("{}/", q.folder)
        };
        let mut children = self
            .entries
            .range(prefix.clone()..)
            .take_while(|(s, _)| s.starts_with(&prefix))
            .filter_map(|(name, e)| {
                let part = &name[prefix.len()..];
                (!part.is_empty() && !part.contains('/')).then_some((part, name, e))
            })
            .collect::<Vec<_>>();
        children.sort_by_key(|(_, _, e)| !e.dir);
        let count = children.len();
        let mut rows = vec![];
        let mut zip = self.zip.lock().map_err(|_| "Archive reader unavailable")?;
        for (part, name, e) in children.into_iter().skip(q.offset as usize).take(PAGE + 1) {
            let (detail, action) = if e.dir {
                ("Folder".into(), Action::Folder(name.clone()))
            } else {
                let f = zip
                    .by_index_raw(e.index.ok_or("Missing archive member")?)
                    .map_err(err)?;
                if f.is_symlink() || f.encrypted() {
                    ("Encrypted or symbolic link".into(), Action::None)
                } else {
                    (
                        format!(
                            "{} · {} compressed",
                            human_size(Some(f.size())),
                            human_size(Some(f.compressed_size()))
                        ),
                        Action::Member(name.clone()),
                    )
                }
            };
            rows.push(Entry {
                name: part.into(),
                detail,
                action,
            });
        }
        let mut title = format!("/{} · {count} entries", q.folder);
        if self.omitted > 0 {
            title.push_str(&format!(" · {} unsafe paths omitted", self.omitted));
        }
        let mut page = list(&title, rows, Some(count as u64));
        if let Screen::List { directory, .. } = &mut page {
            *directory = true;
        }
        Ok(page)
    }
}
#[derive(Clone)]
pub(crate) struct Assets {
    pub archive: Archive,
    pub directory: String,
}
impl Assets {
    pub(crate) fn read(&self, relative: &str) -> Result<Vec<u8>, String> {
        if !safe(relative) {
            return Err("Invalid archive asset path".into());
        }
        self.archive.read(&if self.directory.is_empty() {
            relative.into()
        } else {
            format!("{}/{relative}", self.directory)
        })
    }
}
