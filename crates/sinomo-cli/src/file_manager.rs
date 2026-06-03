use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) const FILE_MANAGER_PREVIEW_MAX_BYTES: u64 = 64 * 1024;
pub(crate) const FILE_MANAGER_PREVIEW_MAX_LINES: usize = 200;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FileManagerEntryKind {
    Parent,
    Directory,
    File,
    Symlink,
    Other,
}

impl FileManagerEntryKind {
    pub(crate) fn sort_rank(&self) -> u8 {
        match self {
            Self::Parent => 0,
            Self::Directory => 1,
            Self::File => 2,
            Self::Symlink => 3,
            Self::Other => 4,
        }
    }

    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Parent => "[up]",
            Self::Directory => "[dir]",
            Self::File => "[file]",
            Self::Symlink => "[link]",
            Self::Other => "[item]",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FileManagerEntry {
    pub(crate) name: String,
    pub(crate) path: String,
    pub(crate) kind: FileManagerEntryKind,
    pub(crate) size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FileManagerPreview {
    pub(crate) title: String,
    pub(crate) lines: Vec<String>,
}

impl FileManagerPreview {
    pub(crate) fn new(title: impl Into<String>, lines: Vec<String>) -> Self {
        Self {
            title: title.into(),
            lines,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FileManagerMetadata {
    pub(crate) kind: FileManagerEntryKind,
    pub(crate) size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FileManagerRead {
    pub(crate) bytes: Vec<u8>,
    pub(crate) truncated: bool,
}

pub(crate) trait FileManagerBackend: fmt::Debug + Send + Sync {
    fn source_label(&self) -> &str;
    fn normalize_path(&self, path: &str) -> Result<String, String>;
    fn parent_path(&self, path: &str) -> Option<String>;
    fn child_path(&self, current_dir: &str, name: &str) -> Result<String, String>;
    fn display_name(&self, path: &str) -> String;
    fn stat(&self, path: &str) -> Result<FileManagerMetadata, String>;
    fn list_dir(&self, path: &str) -> Result<Vec<FileManagerEntry>, String>;
    fn read_file_prefix(&self, path: &str, max_bytes: u64) -> Result<FileManagerRead, String>;
    fn directory_entry_count(&self, path: &str) -> Result<usize, String>;
    fn create_file(&self, path: &str) -> Result<(), String>;
    fn create_dir(&self, path: &str) -> Result<(), String>;
    fn rename(&self, from: &str, to: &str) -> Result<(), String>;
    fn delete_file(&self, path: &str) -> Result<(), String>;
    fn delete_dir(&self, path: &str) -> Result<(), String>;
}

pub(crate) trait FileManagerPreviewer: fmt::Debug + Send + Sync {
    fn preview_file(
        &self,
        entry: &FileManagerEntry,
        read: &FileManagerRead,
    ) -> Option<FileManagerPreview>;
}

#[derive(Clone, Debug)]
pub(crate) struct FileManagerState {
    backend: Arc<dyn FileManagerBackend>,
    file_previewers: Vec<Arc<dyn FileManagerPreviewer>>,
    current_path: String,
    pub(crate) entries: Vec<FileManagerEntry>,
    pub(crate) selected: usize,
    pub(crate) preview: FileManagerPreview,
}

impl FileManagerState {
    pub(crate) fn open(
        backend: Arc<dyn FileManagerBackend>,
        path: impl AsRef<str>,
    ) -> Result<Self, String> {
        let path = backend.normalize_path(path.as_ref())?;
        let metadata = backend.stat(&path)?;
        let (current_path, selected_path) = if metadata.kind == FileManagerEntryKind::Directory {
            (path, None)
        } else {
            let parent = backend
                .parent_path(&path)
                .ok_or_else(|| format!("cannot open {}: not inside a directory", path))?;
            (parent, Some(path))
        };
        let mut browser = Self {
            backend,
            file_previewers: Vec::new(),
            current_path,
            entries: Vec::new(),
            selected: 0,
            preview: FileManagerPreview::new("Preview", Vec::new()),
        };
        browser.refresh_select(selected_path.as_deref())?;
        Ok(browser)
    }

    pub(crate) fn open_local(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        let path = if path.as_os_str().is_empty() {
            String::new()
        } else {
            path.to_string_lossy().into_owned()
        };
        Self::open(Arc::new(LocalFileManagerBackend), path)
    }

    pub(crate) fn source_label(&self) -> &str {
        self.backend.source_label()
    }

    pub(crate) fn current_path(&self) -> &str {
        &self.current_path
    }

    pub(crate) fn set_file_previewers(
        &mut self,
        file_previewers: Vec<Arc<dyn FileManagerPreviewer>>,
    ) {
        self.file_previewers = file_previewers;
        self.update_preview();
    }

    pub(crate) fn refresh(&mut self) -> Result<(), String> {
        let selected_path = self.selected_entry().map(|entry| entry.path.clone());
        self.refresh_select(selected_path.as_deref())
    }

    pub(crate) fn refresh_select(&mut self, selected_path: Option<&str>) -> Result<(), String> {
        self.entries = self.load_entries()?;
        if let Some(selected_path) = selected_path
            && let Some(index) = self
                .entries
                .iter()
                .position(|entry| entry.path == selected_path)
        {
            self.selected = index;
        } else if self.entries.is_empty() {
            self.selected = 0;
        } else {
            self.selected = self.selected.min(self.entries.len() - 1);
        }
        self.update_preview();
        Ok(())
    }

    pub(crate) fn selected_entry(&self) -> Option<&FileManagerEntry> {
        self.entries.get(self.selected)
    }

    pub(crate) fn move_selection(&mut self, delta: isize) {
        if self.entries.is_empty() {
            return;
        }
        let last = self.entries.len() - 1;
        self.selected = if delta.is_negative() {
            self.selected.saturating_sub(delta.unsigned_abs())
        } else {
            self.selected.saturating_add(delta as usize).min(last)
        };
        self.update_preview();
    }

    pub(crate) fn select_first(&mut self) {
        self.selected = 0;
        self.update_preview();
    }

    pub(crate) fn select_last(&mut self) {
        if !self.entries.is_empty() {
            self.selected = self.entries.len() - 1;
            self.update_preview();
        }
    }

    pub(crate) fn open_selected(&mut self) -> Result<String, String> {
        let Some(entry) = self.selected_entry().cloned() else {
            return Ok("no file selected".to_string());
        };
        if entry.kind == FileManagerEntryKind::Parent {
            self.go_parent()?;
            return Ok(format!("opened {}", self.current_path));
        }
        if self.entry_is_navigable_directory(&entry) {
            self.current_path = entry.path;
            self.refresh_select(None)?;
            return Ok(format!("opened {}", self.current_path));
        }
        self.update_preview();
        Ok(format!("previewing {}", entry.name))
    }

    pub(crate) fn go_parent(&mut self) -> Result<String, String> {
        let Some(parent) = self.backend.parent_path(&self.current_path) else {
            return Ok("already at filesystem root".to_string());
        };
        let previous = self.current_path.clone();
        self.current_path = parent;
        self.refresh_select(Some(&previous))?;
        Ok(format!("opened {}", self.current_path))
    }

    pub(crate) fn create_file(&mut self, name: &str) -> Result<String, String> {
        let path = self.backend.child_path(&self.current_path, name)?;
        self.backend.create_file(&path)?;
        self.refresh_select(Some(&path))?;
        Ok(format!("created file {}", self.backend.display_name(&path)))
    }

    pub(crate) fn create_dir(&mut self, name: &str) -> Result<String, String> {
        let path = self.backend.child_path(&self.current_path, name)?;
        self.backend.create_dir(&path)?;
        self.refresh_select(Some(&path))?;
        Ok(format!(
            "created directory {}",
            self.backend.display_name(&path)
        ))
    }

    pub(crate) fn rename_selected(&mut self, target_name: &str) -> Result<String, String> {
        let Some(entry) = self.selected_entry().cloned() else {
            return Err("no file selected".to_string());
        };
        if entry.kind == FileManagerEntryKind::Parent {
            return Err("cannot rename parent directory entry".to_string());
        }
        let target = self.backend.child_path(&self.current_path, target_name)?;
        self.backend.rename(&entry.path, &target)?;
        self.refresh_select(Some(&target))?;
        Ok(format!(
            "renamed {} to {}",
            entry.name,
            self.backend.display_name(&target)
        ))
    }

    pub(crate) fn delete_selected(&mut self, confirmation: &str) -> Result<String, String> {
        let Some(entry) = self.selected_entry().cloned() else {
            return Err("no file selected".to_string());
        };
        if entry.kind == FileManagerEntryKind::Parent {
            return Err("cannot delete parent directory entry".to_string());
        }
        if confirmation.trim() != "DELETE" {
            return Err(format!("type DELETE to remove {}", entry.name));
        }
        if entry.kind == FileManagerEntryKind::Directory {
            self.backend.delete_dir(&entry.path)?;
        } else {
            self.backend.delete_file(&entry.path)?;
        }
        self.refresh()?;
        Ok(format!("deleted {}", entry.name))
    }

    pub(crate) fn update_preview(&mut self) {
        self.preview = match self.selected_entry() {
            Some(entry) => self.preview_entry(entry),
            None => FileManagerPreview::new("Empty", vec!["directory has no entries".to_string()]),
        };
    }

    fn load_entries(&self) -> Result<Vec<FileManagerEntry>, String> {
        let mut entries = Vec::new();
        if let Some(parent) = self.backend.parent_path(&self.current_path) {
            entries.push(FileManagerEntry {
                name: "..".to_string(),
                path: parent,
                kind: FileManagerEntryKind::Parent,
                size: None,
            });
        }
        entries.extend(self.backend.list_dir(&self.current_path)?);
        entries.sort_by(|left, right| {
            left.kind
                .sort_rank()
                .cmp(&right.kind.sort_rank())
                .then_with(|| {
                    left.name
                        .to_ascii_lowercase()
                        .cmp(&right.name.to_ascii_lowercase())
                })
                .then_with(|| left.name.cmp(&right.name))
        });
        Ok(entries)
    }

    fn entry_is_navigable_directory(&self, entry: &FileManagerEntry) -> bool {
        entry.kind == FileManagerEntryKind::Directory
            || self
                .backend
                .stat(&entry.path)
                .map(|metadata| metadata.kind == FileManagerEntryKind::Directory)
                .unwrap_or(false)
    }

    fn preview_entry(&self, entry: &FileManagerEntry) -> FileManagerPreview {
        match entry.kind {
            FileManagerEntryKind::Parent => {
                FileManagerPreview::new("Parent", vec![format!("open {}", entry.path)])
            }
            FileManagerEntryKind::Directory => self.directory_preview(entry),
            FileManagerEntryKind::Symlink if self.entry_is_navigable_directory(entry) => {
                self.directory_preview(entry)
            }
            _ => self.file_preview(entry),
        }
    }

    fn directory_preview(&self, entry: &FileManagerEntry) -> FileManagerPreview {
        match self.backend.directory_entry_count(&entry.path) {
            Ok(count) => FileManagerPreview::new(
                format!("Directory {}", entry.name),
                vec![format!("path: {}", entry.path), format!("entries: {count}")],
            ),
            Err(error) => FileManagerPreview::new(
                format!("Directory {}", entry.name),
                vec![format!("cannot read directory: {error}")],
            ),
        }
    }

    fn file_preview(&self, entry: &FileManagerEntry) -> FileManagerPreview {
        let read = match self
            .backend
            .read_file_prefix(&entry.path, FILE_MANAGER_PREVIEW_MAX_BYTES)
        {
            Ok(read) => read,
            Err(error) => {
                return FileManagerPreview::new(
                    format!("Preview {}", entry.name),
                    vec![format!("cannot open file: {error}")],
                );
            }
        };
        for previewer in &self.file_previewers {
            if let Some(preview) = previewer.preview_file(entry, &read) {
                return preview;
            }
        }
        if read.bytes.iter().any(|byte| *byte == 0) {
            return FileManagerPreview::new(
                format!("Binary {}", entry.name),
                vec![format!(
                    "binary file; preview disabled after {}",
                    format_file_size(read.bytes.len() as u64)
                )],
            );
        }
        let text = match String::from_utf8(read.bytes) {
            Ok(text) => text,
            Err(error) => {
                return FileManagerPreview::new(
                    format!("Binary {}", entry.name),
                    vec![format!(
                        "non UTF-8 file; preview disabled after {}",
                        format_file_size(error.as_bytes().len() as u64)
                    )],
                );
            }
        };

        let line_count = text.lines().count();
        let mut lines = if text.is_empty() {
            vec!["empty file".to_string()]
        } else {
            text.lines()
                .take(FILE_MANAGER_PREVIEW_MAX_LINES)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        };
        if line_count > FILE_MANAGER_PREVIEW_MAX_LINES {
            lines.push(format!(
                "... truncated after {FILE_MANAGER_PREVIEW_MAX_LINES} lines"
            ));
        }
        if read.truncated {
            lines.push(format!(
                "... truncated after {}",
                format_file_size(FILE_MANAGER_PREVIEW_MAX_BYTES)
            ));
        }
        FileManagerPreview::new(format!("Preview {}", entry.name), lines)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct LocalFileManagerBackend;

impl FileManagerBackend for LocalFileManagerBackend {
    fn source_label(&self) -> &str {
        "local"
    }

    fn normalize_path(&self, path: &str) -> Result<String, String> {
        let path = if path.is_empty() {
            std::env::current_dir()
                .map_err(|error| format!("cannot read current directory: {error}"))?
        } else {
            PathBuf::from(path)
        };
        let metadata = fs::metadata(&path)
            .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
        let normalized = if metadata.is_dir() {
            path.canonicalize().unwrap_or(path)
        } else {
            path.canonicalize().unwrap_or(path)
        };
        Ok(normalized.to_string_lossy().into_owned())
    }

    fn parent_path(&self, path: &str) -> Option<String> {
        Path::new(path)
            .parent()
            .map(|parent| parent.to_string_lossy().into_owned())
    }

    fn child_path(&self, current_dir: &str, name: &str) -> Result<String, String> {
        let child = validated_relative_child_path(name)?;
        Ok(Path::new(current_dir)
            .join(child)
            .to_string_lossy()
            .into_owned())
    }

    fn display_name(&self, path: &str) -> String {
        Path::new(path)
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.to_string())
    }

    fn stat(&self, path: &str) -> Result<FileManagerMetadata, String> {
        let metadata =
            fs::metadata(path).map_err(|error| format!("cannot inspect {path}: {error}"))?;
        let kind = if metadata.is_dir() {
            FileManagerEntryKind::Directory
        } else if metadata.is_file() {
            FileManagerEntryKind::File
        } else {
            FileManagerEntryKind::Other
        };
        let size = (kind == FileManagerEntryKind::File).then_some(metadata.len());
        Ok(FileManagerMetadata { kind, size })
    }

    fn list_dir(&self, path: &str) -> Result<Vec<FileManagerEntry>, String> {
        let read_dir =
            fs::read_dir(path).map_err(|error| format!("cannot list {path}: {error}"))?;
        let mut entries = Vec::new();
        for entry in read_dir {
            let entry = entry.map_err(|error| format!("cannot read entry in {path}: {error}"))?;
            let entry_path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|error| format!("cannot inspect {}: {error}", entry_path.display()))?;
            let metadata = entry.metadata().ok();
            let kind = if file_type.is_dir() {
                FileManagerEntryKind::Directory
            } else if file_type.is_file() {
                FileManagerEntryKind::File
            } else if file_type.is_symlink() {
                FileManagerEntryKind::Symlink
            } else {
                FileManagerEntryKind::Other
            };
            let size = metadata
                .as_ref()
                .filter(|_| kind == FileManagerEntryKind::File)
                .map(|metadata| metadata.len());
            entries.push(FileManagerEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                path: entry_path.to_string_lossy().into_owned(),
                kind,
                size,
            });
        }
        Ok(entries)
    }

    fn read_file_prefix(&self, path: &str, max_bytes: u64) -> Result<FileManagerRead, String> {
        let file = fs::File::open(path).map_err(|error| format!("cannot open file: {error}"))?;
        let mut bytes = Vec::new();
        let mut reader = file.take(max_bytes + 1);
        reader
            .read_to_end(&mut bytes)
            .map_err(|error| format!("cannot read file: {error}"))?;
        let truncated = bytes.len() > max_bytes as usize;
        if truncated {
            bytes.truncate(max_bytes as usize);
        }
        Ok(FileManagerRead { bytes, truncated })
    }

    fn directory_entry_count(&self, path: &str) -> Result<usize, String> {
        let read_dir = fs::read_dir(path).map_err(|error| error.to_string())?;
        Ok(read_dir.filter(Result::is_ok).count())
    }

    fn create_file(&self, path: &str) -> Result<(), String> {
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map(|_| ())
            .map_err(|error| format!("cannot create file {path}: {error}"))
    }

    fn create_dir(&self, path: &str) -> Result<(), String> {
        fs::create_dir(path).map_err(|error| format!("cannot create directory {path}: {error}"))
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), String> {
        fs::rename(from, to).map_err(|error| format!("cannot rename {from} to {to}: {error}"))
    }

    fn delete_file(&self, path: &str) -> Result<(), String> {
        fs::remove_file(path).map_err(|error| format!("cannot delete file {path}: {error}"))
    }

    fn delete_dir(&self, path: &str) -> Result<(), String> {
        fs::remove_dir(path).map_err(|error| {
            format!(
                "cannot delete directory {path}: {error}; only empty directories can be deleted"
            )
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FileManagerPrompt {
    NewFile,
    NewDirectory,
    Rename,
    Delete,
}

impl FileManagerPrompt {
    pub(crate) fn title(&self) -> &'static str {
        match self {
            Self::NewFile => "New File",
            Self::NewDirectory => "New Directory",
            Self::Rename => "Rename To",
            Self::Delete => "Confirm Delete",
        }
    }
}

pub(crate) fn format_file_size(size: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    if size >= GIB {
        format!("{:.1} GiB", size as f64 / GIB as f64)
    } else if size >= MIB {
        format!("{:.1} MiB", size as f64 / MIB as f64)
    } else if size >= KIB {
        format!("{:.1} KiB", size as f64 / KIB as f64)
    } else {
        format!("{size} B")
    }
}

pub(crate) fn validated_relative_child_path(input: &str) -> Result<PathBuf, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("name cannot be empty".to_string());
    }
    let path = Path::new(trimmed);
    if path.is_absolute() {
        return Err("use a relative name inside the current directory".to_string());
    }

    let mut has_name = false;
    for component in path.components() {
        match component {
            std::path::Component::Normal(_) => has_name = true,
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => {
                return Err("name cannot leave the current directory".to_string());
            }
        }
    }
    if !has_name {
        return Err("name cannot be empty".to_string());
    }
    Ok(path.to_path_buf())
}
