//! A write-through FUSE directory. Only declared mount targets and their
//! missing ancestors have synthetic entries. All other directory operations
//! resolve the name against its backing directory at the time of the request.
mod operations;
use fuser::*;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{CString, OsStr, OsString};
use std::fs::{self, File};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::{
    ffi::{OsStrExt, OsStringExt},
    fs::{DirEntryExt, FileExt, FileTypeExt, MetadataExt, PermissionsExt, symlink},
};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU32, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TTL: Duration = Duration::ZERO;

#[derive(Clone)]
pub(super) struct Branch {
    pub backing: PathBuf,
    directory: bool,
    children: BTreeMap<OsString, Branch>,
}

impl Branch {
    pub fn directory(backing: PathBuf) -> Self {
        Self {
            backing,
            directory: true,
            children: BTreeMap::new(),
        }
    }
    pub fn unused_control_name(&self) -> io::Result<OsString> {
        loop {
            let name = OsString::from(format!(
                ".sinomo-native-{}",
                sinomo_sandbox::SandboxCommandId::generate()
            ));
            if !self.children.contains_key(&name) {
                match fs::symlink_metadata(self.backing.join(&name)) {
                    Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(name),
                    Err(e) => return Err(e),
                    Ok(_) => (),
                }
            }
        }
    }
    pub fn reserve(&mut self, path: &Path, directory: bool) {
        let mut branch = self;
        for name in path.iter() {
            let backing = branch.backing.join(name);
            branch = branch
                .children
                .entry(name.to_owned())
                .or_insert_with(|| Self::directory(backing));
        }
        branch.directory = directory;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NameOperation {
    Lookup,
    Mutate,
    Create,
}

/// Keep the parent directory open across the backing syscall, including when
/// a host rename happens between FUSE lookup and the mutation.
struct ChildPath {
    path: PathBuf,
    _parent: File,
}
impl std::ops::Deref for ChildPath {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.path
    }
}
impl AsRef<Path> for ChildPath {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

struct Source {
    path: PathBuf,
    root: File,
    read_only: bool,
}
struct Directory {
    region: usize,
    native: PathBuf,
    directory: bool,
    children: BTreeMap<OsString, INodeNo>,
}

enum Node {
    Catalog,
    Branch(Directory),
    Host(File, usize),
    // MKNOD must acknowledge the requested native type. A subsequent LOOKUP
    // follows the native alias so FIFO readers and writers share its pipe.
    CreatedFifo(File),
    Aliases,
    AliasTarget,
    // A pathname Unix socket is attached to a kernel inode. The published
    // pathname must lead back to the very same inode for host and guest.
    Endpoints,
    Endpoint(File, AtomicU32, usize),
}

type Key = (usize, u64, u64);
struct Entry {
    node: Arc<Node>,
    key: Option<Key>,
    lookups: u64,
}
struct State {
    next: u64,
    nodes: HashMap<INodeNo, Entry>,
    host: HashMap<Key, INodeNo>,
    files: HashMap<FileHandle, Arc<File>>,
    listings: HashMap<FileHandle, Vec<(OsString, FileType, u64)>>,
}
impl State {
    fn id(&mut self) -> u64 {
        self.next += 1;
        self.next
    }
    fn fixed(&mut self, node: Node) -> INodeNo {
        let id = INodeNo(self.id());
        self.nodes.insert(
            id,
            Entry {
                node: Arc::new(node),
                key: None,
                lookups: 0,
            },
        );
        id
    }
    fn branch(&mut self, branch: Branch, region: usize) -> INodeNo {
        let children = branch
            .children
            .into_iter()
            .map(|(name, b)| (name, self.branch(b, region)))
            .collect();
        self.fixed(Node::Branch(Directory {
            region,
            native: branch.backing,
            directory: branch.directory,
            children,
        }))
    }
    fn file_handle(&mut self, file: File) -> FileHandle {
        let handle = FileHandle(self.id());
        self.files.insert(handle, Arc::new(file));
        handle
    }
}

pub(super) struct Router {
    state: Mutex<State>,
    roots: BTreeMap<OsString, INodeNo>,
    endpoints: INodeNo,
    aliases: PathBuf,
    aliases_id: INodeNo,
    alias_targets: BTreeMap<OsString, INodeNo>,
    commands: PathBuf,
    sources: Vec<Source>,
    mount: PathBuf,
    stage_identity: (u64, u64),
}
pub(super) fn os_error(code: i32) -> io::Error {
    io::Error::from_raw_os_error(code)
}
pub(super) fn cpath(path: &OsStr) -> io::Result<CString> {
    CString::new(path.as_bytes()).map_err(|_| os_error(libc::EINVAL))
}
fn check(result: libc::c_int) -> io::Result<()> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
fn open(path: &Path, flags: i32, mode: u32) -> io::Result<File> {
    let p = cpath(path.as_os_str())?;
    let fd = unsafe { libc::open(p.as_ptr(), flags | libc::O_CLOEXEC, mode as libc::mode_t) };
    check(fd)?;
    Ok(unsafe { File::from_raw_fd(fd) })
}
fn procfd(file: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}
fn kind(ty: fs::FileType) -> FileType {
    if ty.is_dir() {
        FileType::Directory
    } else if ty.is_symlink()
        || ty.is_socket()
        || ty.is_fifo()
        || ty.is_char_device()
        || ty.is_block_device()
    {
        FileType::Symlink
    } else {
        FileType::RegularFile
    }
}
fn timestamp(s: i64, n: i64) -> SystemTime {
    let t = if s < 0 {
        UNIX_EPOCH - Duration::from_secs(s.unsigned_abs())
    } else {
        UNIX_EPOCH + Duration::from_secs(s as u64)
    };
    t + Duration::from_nanos(n as u64)
}
fn attributes(id: INodeNo, m: &fs::Metadata) -> FileAttr {
    FileAttr {
        ino: id,
        size: m.size(),
        blocks: m.blocks(),
        atime: timestamp(m.atime(), m.atime_nsec()),
        mtime: timestamp(m.mtime(), m.mtime_nsec()),
        ctime: timestamp(m.ctime(), m.ctime_nsec()),
        crtime: UNIX_EPOCH,
        kind: kind(m.file_type()),
        perm: (m.mode() & 0o7777) as u16,
        nlink: m.nlink() as u32,
        uid: m.uid(),
        gid: m.gid(),
        rdev: m.rdev() as u32,
        blksize: m.blksize() as u32,
        flags: 0,
    }
}
fn dummy(id: INodeNo, directory: bool) -> FileAttr {
    FileAttr {
        ino: id,
        size: 0,
        blocks: 0,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: if directory {
            FileType::Directory
        } else {
            FileType::RegularFile
        },
        perm: if directory { 0o755 } else { 0o644 },
        nlink: 1,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}
fn missing_directory(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::ENOENT | libc::ENOTDIR))
}
fn checked_name(name: &OsStr) -> io::Result<&OsStr> {
    if name.is_empty() || name == "." || name == ".." || name.as_bytes().contains(&b'/') {
        Err(os_error(libc::EINVAL))
    } else {
        Ok(name)
    }
}
fn read_pinned_link(file: &File) -> io::Result<PathBuf> {
    let mut bytes = vec![0; libc::PATH_MAX as usize];
    let n = unsafe {
        libc::readlinkat(
            file.as_raw_fd(),
            c"".as_ptr(),
            bytes.as_mut_ptr().cast(),
            bytes.len(),
        )
    };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    bytes.truncate(n as usize);
    Ok(OsString::from_vec(bytes).into())
}

impl Router {
    pub fn new(
        branches: Vec<(Branch, bool)>,
        aliases: PathBuf,
        commands: PathBuf,
        mount: PathBuf,
    ) -> io::Result<Self> {
        let mut state = State {
            next: 0,
            nodes: Default::default(),
            host: Default::default(),
            files: Default::default(),
            listings: Default::default(),
        };
        state.fixed(Node::Catalog);
        let stage_meta = fs::metadata(mount.parent().expect("router staging"))?;
        let stage_identity = (stage_meta.dev(), stage_meta.ino());
        let endpoints = state.fixed(Node::Endpoints);
        let aliases_id = state.fixed(Node::Aliases);
        let alias_targets = (0..branches.len())
            .map(|i| OsString::from(i.to_string()))
            .chain([OsString::from("executions")])
            .map(|n| (n, state.fixed(Node::AliasTarget)))
            .collect();
        let sources = branches
            .iter()
            .map(|(b, read_only)| {
                Ok(Source {
                    path: b.backing.clone(),
                    root: open(
                        &b.backing,
                        libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW,
                        0,
                    )?,
                    read_only: *read_only,
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        let roots = branches
            .into_iter()
            .enumerate()
            .map(|(i, (b, _))| (OsString::from(i.to_string()), state.branch(b, i)))
            .collect();
        Ok(Self {
            state: Mutex::new(state),
            roots,
            endpoints,
            aliases,
            aliases_id,
            alias_targets,
            commands,
            sources,
            mount,
            stage_identity,
        })
    }
    fn node(&self, id: INodeNo) -> io::Result<Arc<Node>> {
        self.state
            .lock()
            .unwrap()
            .nodes
            .get(&id)
            .map(|n| n.node.clone())
            .ok_or_else(|| os_error(libc::ESTALE))
    }
    fn attr(&self, id: INodeNo) -> io::Result<FileAttr> {
        Ok(match self.node(id)?.as_ref() {
            Node::Catalog | Node::Endpoints | Node::Aliases | Node::AliasTarget => dummy(id, true),
            Node::Endpoint(file, mode, _) => {
                let mut a = attributes(id, &file.metadata()?);
                a.kind = FileType::Socket;
                a.perm = (mode.load(Ordering::Relaxed) & 0o7777) as u16;
                a
            }
            Node::Branch(b) => {
                if b.children.is_empty() {
                    dummy(id, b.directory)
                } else {
                    match self.branch_directory(b, false).and_then(|f| f.metadata()) {
                        Ok(meta) if meta.is_dir() => attributes(id, &meta),
                        Ok(_) => return Err(os_error(libc::ENOTDIR)),
                        Err(e) if missing_directory(&e) => dummy(id, true),
                        Err(e) => return Err(e),
                    }
                }
            }
            Node::CreatedFifo(file) => {
                let mut a = attributes(id, &file.metadata()?);
                a.kind = FileType::NamedPipe;
                a
            }
            Node::Host(file, _) => attributes(id, &file.metadata()?),
        })
    }
    fn branch_directory(&self, branch: &Directory, create: bool) -> io::Result<File> {
        let source = &self.sources[branch.region];
        let mut directory = source.root.try_clone()?;
        for name in branch
            .native
            .strip_prefix(&source.path)
            .expect("branch in source")
            .iter()
        {
            let path = procfd(&directory).join(name);
            let flags = libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW;
            directory = match open(&path, flags, 0) {
                Err(e) if create && e.kind() == io::ErrorKind::NotFound => {
                    match fs::create_dir(&path) {
                        Ok(()) => (),
                        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
                        Err(e) => return Err(e),
                    }
                    open(&path, flags, 0)?
                }
                result => result?,
            }
        }
        Ok(directory)
    }
    fn writable(&self, id: INodeNo) -> io::Result<()> {
        if self.sources[self.region(id)?].read_only {
            Err(os_error(libc::EROFS))
        } else {
            Ok(())
        }
    }
    fn is_staging(&self, file: &File) -> bool {
        file.metadata()
            .is_ok_and(|m| (m.dev(), m.ino()) == self.stage_identity)
    }
    fn child_path(
        &self,
        parent: INodeNo,
        name: &OsStr,
        op: NameOperation,
    ) -> io::Result<ChildPath> {
        checked_name(name)?;
        if op != NameOperation::Lookup {
            self.writable(parent)?;
        }
        if self.roots.get(OsStr::new("0")) == Some(&parent)
            && self.aliases.file_name() == Some(name)
        {
            return Err(os_error(libc::EBUSY));
        }
        let directory = match self.node(parent)?.as_ref() {
            Node::Host(file, _) => file.try_clone()?,
            Node::Branch(b) if b.directory && !b.children.is_empty() => {
                if b.children.contains_key(name) {
                    return Err(os_error(libc::EBUSY));
                }
                self.branch_directory(b, op == NameOperation::Create)?
            }
            _ => return Err(os_error(libc::EROFS)),
        };
        if name == "router" && self.is_staging(&directory) {
            // Looking up our own host staging mount through FUSE would send a
            // recursive request to this server. Its socket catalog has a native bind.
            return Err(os_error(if op == NameOperation::Lookup {
                libc::ENOENT
            } else {
                libc::EBUSY
            }));
        }
        Ok(ChildPath {
            path: procfd(&directory).join(name),
            _parent: directory,
        })
    }
    fn lookup_node(&self, parent: INodeNo, name: &OsStr) -> io::Result<INodeNo> {
        checked_name(name)?;
        match self.node(parent)?.as_ref() {
            Node::Catalog => {
                return if name == "sockets" {
                    Ok(self.endpoints)
                } else {
                    self.roots
                        .get(name)
                        .copied()
                        .ok_or_else(|| os_error(libc::ENOENT))
                };
            }
            Node::Aliases => {
                return self
                    .alias_targets
                    .get(name)
                    .copied()
                    .ok_or_else(|| os_error(libc::ENOENT));
            }
            Node::Branch(_)
                if self.roots.get(OsStr::new("0")) == Some(&parent)
                    && self.aliases.file_name() == Some(name) =>
            {
                return Ok(self.aliases_id);
            }
            Node::Endpoints => {
                let id = name
                    .to_str()
                    .and_then(|s| s.parse().ok())
                    .map(INodeNo)
                    .ok_or_else(|| os_error(libc::ENOENT))?;
                let mut state = self.state.lock().unwrap();
                if let Some(entry) = state.nodes.get_mut(&id)
                    && matches!(&*entry.node, Node::Endpoint(..))
                {
                    entry.lookups += 1;
                    return Ok(id);
                }
                return Err(os_error(libc::ENOENT));
            }
            Node::Branch(b) if b.children.contains_key(name) => return Ok(b.children[name]),
            _ => (),
        }
        let file = open(
            &self.child_path(parent, name, NameOperation::Lookup)?,
            libc::O_PATH | libc::O_NOFOLLOW,
            0,
        )?;
        self.remember(file, self.region(parent)?)
    }
    fn region(&self, id: INodeNo) -> io::Result<usize> {
        match &*self.node(id)? {
            Node::Branch(b) => Ok(b.region),
            Node::Host(_, i) | Node::Endpoint(_, _, i) => Ok(*i),
            _ => Err(os_error(libc::EROFS)),
        }
    }
    fn remember(&self, file: File, region: usize) -> io::Result<INodeNo> {
        let meta = file.metadata()?;
        let key = (region, meta.dev(), meta.ino());
        let mut state = self.state.lock().unwrap();
        if let Some(id) = state.host.get(&key).copied() {
            state.nodes.get_mut(&id).unwrap().lookups += 1;
            return Ok(id);
        }
        let id = INodeNo(state.id());
        state.nodes.insert(
            id,
            Entry {
                node: Arc::new(Node::Host(file, region)),
                key: Some(key),
                lookups: 1,
            },
        );
        state.host.insert(key, id);
        Ok(id)
    }
    fn host_file(&self, id: INodeNo) -> io::Result<Arc<Node>> {
        let n = self.node(id)?;
        if matches!(&*n, Node::Host(..)) {
            Ok(n)
        } else {
            Err(os_error(libc::EBUSY))
        }
    }
    fn host_path(&self, id: INodeNo) -> io::Result<PathBuf> {
        match &*self.host_file(id)? {
            Node::Host(f, _) => Ok(procfd(f)),
            _ => unreachable!(),
        }
    }
    fn metadata_file(&self, id: INodeNo) -> io::Result<File> {
        match &*self.node(id)? {
            Node::Host(file, _) if !file.metadata()?.is_symlink() => file.try_clone(),
            Node::Branch(dir) if !dir.children.is_empty() => self.branch_directory(dir, false),
            _ => Err(os_error(libc::EOPNOTSUPP)),
        }
    }
    fn published_socket_suffix<'a>(&self, target: &'a Path) -> Option<&'a Path> {
        let suffix = target.strip_prefix(&self.commands).ok()?;
        let mut names = suffix.iter();
        sinomo_sandbox::SandboxCommandId::new(names.next()?.to_str()?.to_owned()).ok()?;
        for name in ["mount-view", "router", "sockets"] {
            if names.next()? != name {
                return None;
            }
        }
        names.next()?.to_str()?.parse::<u64>().ok()?;
        names.next().is_none().then_some(suffix)
    }
    fn file(&self, fh: FileHandle) -> io::Result<Arc<File>> {
        self.state
            .lock()
            .unwrap()
            .files
            .get(&fh)
            .cloned()
            .ok_or_else(|| os_error(libc::EBADF))
    }
    fn entries(&self, id: INodeNo) -> io::Result<Vec<(OsString, FileType, u64)>> {
        let mut entries = BTreeMap::new();
        let n = self.node(id)?;
        let hide_router = matches!(&*n,Node::Host(file,_) if self.is_staging(file));
        let mut directory = None;
        let backing = match &*n {
            Node::Catalog => {
                for (k, v) in &self.roots {
                    entries.insert(k.clone(), (FileType::Directory, v.0));
                }
                entries.insert("sockets".into(), (FileType::Directory, self.endpoints.0));
                None
            }
            Node::Branch(b) => {
                for (k, v) in &b.children {
                    entries.insert(k.clone(), (self.attr(*v)?.kind, v.0));
                }
                if b.children.is_empty() {
                    None
                } else {
                    match self.branch_directory(b, false) {
                        Ok(f) => {
                            directory = Some(f);
                            directory.as_ref().map(procfd)
                        }
                        Err(e) if missing_directory(&e) => None,
                        Err(e) => return Err(e),
                    }
                }
            }
            Node::Host(f, _) => Some(procfd(f)),
            Node::Endpoints | Node::Aliases | Node::AliasTarget => None,
            Node::Endpoint(..) | Node::CreatedFifo(_) => return Err(os_error(libc::ENOTDIR)),
        };
        if let Some(p) = backing {
            match fs::read_dir(p) {
                Ok(dir) => {
                    for entry in dir {
                        let e = entry?;
                        if hide_router && e.file_name() == "router" {
                            continue;
                        }
                        if let std::collections::btree_map::Entry::Vacant(slot) =
                            entries.entry(e.file_name())
                        {
                            match e.file_type() {
                                Ok(ty) => {
                                    slot.insert((kind(ty), e.ino()));
                                }
                                Err(err) if err.kind() == io::ErrorKind::NotFound => (),
                                Err(err) => return Err(err),
                            }
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound && matches!(&*n, Node::Branch(_)) => {
                }
                Err(e) => return Err(e),
            }
        }
        drop(directory);
        let mut result = vec![
            (".".into(), FileType::Directory, id.0),
            ("..".into(), FileType::Directory, 0),
        ];
        result.extend(entries.into_iter().map(|(k, (t, i))| (k, t, i)));
        Ok(result)
    }
    fn entry(&self, result: io::Result<INodeNo>, reply: ReplyEntry) {
        match result.and_then(|i| self.attr(i)) {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(e) => reply.error(e.into()),
        }
    }
    fn created(
        &self,
        parent: INodeNo,
        name: &OsStr,
        op: impl FnOnce(&Path) -> io::Result<()>,
        reply: ReplyEntry,
    ) {
        self.entry(
            self.child_path(parent, name, NameOperation::Create)
                .and_then(|p| op(&p))
                .and_then(|_| self.lookup_node(parent, name)),
            reply,
        )
    }
}
