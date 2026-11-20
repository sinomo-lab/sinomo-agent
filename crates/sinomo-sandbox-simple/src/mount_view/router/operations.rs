use super::*;

fn empty(reply: ReplyEmpty, result: io::Result<()>) {
    match result {
        Ok(()) => reply.ok(),
        Err(e) => reply.error(e.into()),
    }
}

impl Filesystem for Router {
    fn init(&mut self, _: &Request, config: &mut KernelConfig) -> io::Result<()> {
        let _ = config.add_capabilities(InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP);
        Ok(())
    }
    fn statfs(&self, _: &Request, id: INodeNo, reply: ReplyStatfs) {
        // The catalog and yet-unmaterialized branch directories inherit the
        // capacity of the corresponding backing filesystem.
        let result = self
            .metadata_file(id)
            .or_else(|_| self.sources[self.region(id).unwrap_or(0)].root.try_clone());
        match result {
            Ok(file) => crate::mount_view::metadata::statfs(&procfd(&file), reply),
            Err(e) => reply.error(e.into()),
        }
    }
    fn fsyncdir(&self, _: &Request, id: INodeNo, _: FileHandle, datasync: bool, reply: ReplyEmpty) {
        empty(
            reply,
            self.metadata_file(id)
                .and_then(|directory| {
                    open(&procfd(&directory), libc::O_DIRECTORY | libc::O_RDONLY, 0)
                })
                .and_then(|f| {
                    if datasync {
                        f.sync_data()
                    } else {
                        f.sync_all()
                    }
                }),
        )
    }
    fn getxattr(&self, _: &Request, id: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        match self.metadata_file(id) {
            Ok(file) => {
                crate::mount_view::metadata::read_xattr(&procfd(&file), Some(name), size, reply)
            }
            Err(e) => reply.error(e.into()),
        }
    }
    fn listxattr(&self, _: &Request, id: INodeNo, size: u32, reply: ReplyXattr) {
        match self.metadata_file(id) {
            Ok(file) => crate::mount_view::metadata::read_xattr(&procfd(&file), None, size, reply),
            Err(e) => reply.error(e.into()),
        }
    }
    fn setxattr(
        &self,
        _: &Request,
        id: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        _: u32,
        reply: ReplyEmpty,
    ) {
        empty(
            reply,
            self.writable(id)
                .and_then(|_| self.metadata_file(id))
                .and_then(|file| {
                    crate::mount_view::metadata::write_xattr(
                        &procfd(&file),
                        name,
                        Some((value, flags)),
                    )
                }),
        )
    }
    fn removexattr(&self, _: &Request, id: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        empty(
            reply,
            self.writable(id)
                .and_then(|_| self.metadata_file(id))
                .and_then(|file| {
                    crate::mount_view::metadata::write_xattr(&procfd(&file), name, None)
                }),
        )
    }
    fn getattr(&self, _: &Request, id: INodeNo, _: Option<FileHandle>, reply: ReplyAttr) {
        match self.attr(id) {
            Ok(a) => reply.attr(&TTL, &a),
            Err(e) => reply.error(e.into()),
        }
    }
    fn lookup(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        self.entry(self.lookup_node(parent, name), reply)
    }
    fn forget(&self, _: &Request, id: INodeNo, n: u64) {
        let mut s = self.state.lock().unwrap();
        if s.nodes
            .get(&id)
            .is_some_and(|e| matches!(&*e.node, Node::CreatedFifo(_)))
        {
            s.nodes.remove(&id);
            return;
        }
        if let Some(e) = s.nodes.get_mut(&id)
            && let Some(key) = e.key
        {
            e.lookups = e.lookups.saturating_sub(n);
            if e.lookups == 0
                && !matches!(&*e.node,Node::Endpoint(file,_,_) if file.metadata().is_ok_and(|m|m.nlink()>0))
            {
                s.nodes.remove(&id);
                s.host.remove(&key);
            }
        }
    }
    fn readlink(&self, _: &Request, id: INodeNo, reply: ReplyData) {
        let result: io::Result<PathBuf> = (|| {
            let node = self.host_file(id)?;
            let Node::Host(file, region) = &*node else {
                unreachable!()
            };
            let meta = file.metadata()?;
            if meta.is_symlink() {
                let target = read_pinned_link(file)?;
                if let Some(suffix) = self.published_socket_suffix(&target) {
                    return Ok(self.aliases.join("executions").join(suffix));
                }
                Ok(target)
            } else {
                let native = fs::read_link(procfd(file))?;
                Ok(self.aliases.join(region.to_string()).join(
                    native
                        .strip_prefix(fs::read_link(procfd(&self.sources[*region].root))?)
                        .map_err(|_| os_error(libc::ESTALE))?,
                ))
            }
        })();
        match result {
            Ok(p) => reply.data(p.as_os_str().as_bytes()),
            Err(e) => reply.error(e.into()),
        }
    }
    fn opendir(&self, _: &Request, _: INodeNo, _: OpenFlags, reply: ReplyOpen) {
        let h = FileHandle(self.state.lock().unwrap().id());
        reply.opened(h, FopenFlags::empty())
    }
    fn readdir(
        &self,
        _: &Request,
        id: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        if offset == 0 {
            match self.entries(id) {
                Ok(entries) => {
                    self.state.lock().unwrap().listings.insert(fh, entries);
                }
                Err(e) => return reply.error(e.into()),
            }
        }
        let state = self.state.lock().unwrap();
        let Some(entries) = state.listings.get(&fh) else {
            return reply.error(Errno::EBADF);
        };
        for (i, (n, t, ino)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(*ino), (i + 1) as u64, *t, n) {
                break;
            }
        }
        reply.ok()
    }
    fn releasedir(&self, _: &Request, _: INodeNo, fh: FileHandle, _: OpenFlags, reply: ReplyEmpty) {
        self.state.lock().unwrap().listings.remove(&fh);
        reply.ok()
    }
    fn open(&self, _: &Request, id: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let result = (|| {
            if flags.0 & libc::O_ACCMODE != libc::O_RDONLY || flags.0 & libc::O_TRUNC != 0 {
                self.writable(id)?;
            }
            let p = self.host_path(id)?;
            open(
                &p,
                flags.0 & !(libc::O_NOFOLLOW | libc::O_CREAT | libc::O_EXCL),
                0,
            )
        })();
        match result {
            Ok(f) => reply.opened(
                self.state.lock().unwrap().file_handle(f),
                FopenFlags::FOPEN_DIRECT_IO,
            ),
            Err(e) => reply.error(e.into()),
        }
    }
    fn create(
        &self,
        _: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        mask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let result = (|| {
            let path = self.child_path(parent, name, NameOperation::Create)?;
            let file = open(
                &path,
                flags | libc::O_CREAT | libc::O_NOFOLLOW,
                mode & !mask,
            )?;
            let id = self.remember(open(&procfd(&file), libc::O_PATH, 0)?, self.region(parent)?)?;
            Ok::<_, io::Error>((self.attr(id)?, self.state.lock().unwrap().file_handle(file)))
        })();
        match result {
            Ok((a, fh)) => reply.created(&TTL, &a, Generation(0), fh, FopenFlags::FOPEN_DIRECT_IO),
            Err(e) => reply.error(e.into()),
        }
    }
    fn read(
        &self,
        _: &Request,
        _: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _: OpenFlags,
        _: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let mut buf = vec![0; size as usize];
        match self.file(fh).and_then(|f| f.read_at(&mut buf, offset)) {
            Ok(n) => reply.data(&buf[..n]),
            Err(e) => reply.error(e.into()),
        }
    }
    fn write(
        &self,
        _: &Request,
        _: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _: WriteFlags,
        _: OpenFlags,
        _: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        match self.file(fh).and_then(|f| f.write_at(data, offset)) {
            Ok(n) => reply.written(n as u32),
            Err(e) => reply.error(e.into()),
        }
    }
    fn release(
        &self,
        _: &Request,
        _: INodeNo,
        fh: FileHandle,
        _: OpenFlags,
        _: Option<LockOwner>,
        _: bool,
        reply: ReplyEmpty,
    ) {
        self.state.lock().unwrap().files.remove(&fh);
        reply.ok()
    }
    fn flush(&self, _: &Request, _: INodeNo, fh: FileHandle, _: LockOwner, reply: ReplyEmpty) {
        empty(reply, self.file(fh).map(|_| ()))
    }
    fn fsync(&self, _: &Request, _: INodeNo, fh: FileHandle, data: bool, reply: ReplyEmpty) {
        empty(
            reply,
            self.file(fh)
                .and_then(|f| if data { f.sync_data() } else { f.sync_all() }),
        )
    }
    fn mkdir(
        &self,
        _: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        mask: u32,
        reply: ReplyEntry,
    ) {
        use std::os::unix::fs::DirBuilderExt;
        self.created(
            parent,
            name,
            |p| fs::DirBuilder::new().mode(mode & !mask).create(p),
            reply,
        )
    }
    fn symlink(
        &self,
        _: &Request,
        parent: INodeNo,
        name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        self.created(parent, name, |p| symlink(target, p), reply)
    }
    fn unlink(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        empty(
            reply,
            self.child_path(parent, name, NameOperation::Mutate)
                .and_then(fs::remove_file),
        )
    }
    fn rmdir(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        empty(
            reply,
            self.child_path(parent, name, NameOperation::Mutate)
                .and_then(fs::remove_dir),
        )
    }
    fn rename(
        &self,
        _: &Request,
        parent: INodeNo,
        name: &OsStr,
        to: INodeNo,
        new: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        empty(
            reply,
            (|| {
                let from_path = self.child_path(parent, name, NameOperation::Mutate)?;
                let a = cpath(from_path.as_os_str())?;
                let to_path = self.child_path(to, new, NameOperation::Create)?;
                let b = cpath(to_path.as_os_str())?;
                check(unsafe {
                    libc::syscall(
                        libc::SYS_renameat2,
                        libc::AT_FDCWD,
                        a.as_ptr(),
                        libc::AT_FDCWD,
                        b.as_ptr(),
                        flags.bits(),
                    )
                } as i32)
            })(),
        )
    }
    fn link(&self, _: &Request, id: INodeNo, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        self.created(
            parent,
            name,
            |dest| {
                let src = cpath(self.host_path(id)?.as_os_str())?;
                let dest = cpath(dest.as_os_str())?;
                check(unsafe {
                    libc::linkat(
                        libc::AT_FDCWD,
                        src.as_ptr(),
                        libc::AT_FDCWD,
                        dest.as_ptr(),
                        libc::AT_SYMLINK_FOLLOW,
                    )
                })
            },
            reply,
        )
    }
    fn mknod(
        &self,
        _: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        mask: u32,
        dev: u32,
        reply: ReplyEntry,
    ) {
        let result = (|| {
            let path = self.child_path(parent, name, NameOperation::Create)?;
            if mode & libc::S_IFMT == libc::S_IFSOCK {
                let region = self.region(parent)?;
                let mut s = self.state.lock().unwrap();
                // A host unlink does not issue a FUSE forget. Release pins for
                // published endpoints whose final native link has since gone.
                let retired: Vec<_> = s
                    .nodes
                    .iter()
                    .filter_map(|(id, entry)| {
                        if let Node::Endpoint(file, _, _) = &*entry.node
                            && entry.lookups == 0
                            && file.metadata().is_ok_and(|m| m.nlink() == 0)
                        {
                            Some((*id, entry.key.unwrap()))
                        } else {
                            None
                        }
                    })
                    .collect();
                for (id, key) in retired {
                    s.nodes.remove(&id);
                    s.host.remove(&key);
                }
                let id = INodeNo(s.id());
                let target = self.mount.join("sockets").join(id.0.to_string());
                symlink(target, &path)?;
                let file = open(&path, libc::O_PATH | libc::O_NOFOLLOW, 0)?;
                let meta = file.metadata()?;
                let key = (region, meta.dev(), meta.ino());
                s.host.insert(key, id);
                s.nodes.insert(
                    id,
                    Entry {
                        node: Arc::new(Node::Endpoint(file, AtomicU32::new(mode & !mask), region)),
                        key: Some(key),
                        lookups: 1,
                    },
                );
                Ok(id)
            } else {
                // A host filesystem server must not exercise CAP_MKNOD on
                // behalf of an unprivileged guest. Existing devices retain their native routes.
                let ty = mode & libc::S_IFMT;
                if ty != libc::S_IFIFO && ty != libc::S_IFREG {
                    return Err(os_error(libc::EPERM));
                }
                let p = cpath(path.as_os_str())?;
                check(unsafe { libc::mknod(p.as_ptr(), mode & !mask, dev.into()) })?;
                if ty == libc::S_IFIFO {
                    let file = open(&path, libc::O_PATH | libc::O_NOFOLLOW, 0)?;
                    Ok(self.state.lock().unwrap().fixed(Node::CreatedFifo(file)))
                } else {
                    self.lookup_node(parent, name)
                }
            }
        })();
        self.entry(result, reply)
    }
    fn setattr(
        &self,
        _: &Request,
        id: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _: Option<SystemTime>,
        _: Option<FileHandle>,
        _: Option<SystemTime>,
        _: Option<SystemTime>,
        _: Option<SystemTime>,
        _: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let result = (|| {
            self.writable(id)?;
            let n = self.node(id)?;
            let dir_handle;
            let file = match &*n {
                Node::Branch(b) if !b.children.is_empty() => {
                    dir_handle = self.branch_directory(b, true)?;
                    &dir_handle
                }
                Node::Host(f, _) | Node::Endpoint(f, _, _) => f,
                _ => return Err(os_error(libc::EBUSY)),
            };
            let path = procfd(file);
            if let (Node::Endpoint(_, perm, _), Some(m)) = (&*n, mode) {
                perm.store(m, Ordering::Relaxed);
            } else if let Some(m) = mode {
                if file.metadata()?.is_symlink() {
                    return Err(os_error(libc::EOPNOTSUPP));
                }
                fs::set_permissions(&path, fs::Permissions::from_mode(m))?;
            }
            if uid.is_some() || gid.is_some() {
                check(unsafe {
                    libc::fchownat(
                        file.as_raw_fd(),
                        c"".as_ptr(),
                        uid.unwrap_or(u32::MAX),
                        gid.unwrap_or(u32::MAX),
                        libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW,
                    )
                })?;
            }
            if let Some(size) = size {
                if !file.metadata()?.is_file() {
                    return Err(os_error(libc::EINVAL));
                }
                open(&path, libc::O_WRONLY, 0)?.set_len(size)?;
            }
            if atime.is_some() || mtime.is_some() {
                let ts = [time_spec(atime), time_spec(mtime)];
                check(unsafe {
                    libc::utimensat(
                        file.as_raw_fd(),
                        c"".as_ptr(),
                        ts.as_ptr(),
                        libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW,
                    )
                })?;
            }
            self.attr(id)
        })();
        match result {
            Ok(a) => reply.attr(&TTL, &a),
            Err(e) => reply.error(e.into()),
        }
    }
}
fn time_spec(t: Option<TimeOrNow>) -> libc::timespec {
    match t {
        Some(TimeOrNow::SpecificTime(t)) => {
            let (sec, ns) = match t.duration_since(UNIX_EPOCH) {
                Ok(d) => (d.as_secs() as i64, d.subsec_nanos() as i64),
                Err(e) => {
                    let d = e.duration();
                    let ns = d.subsec_nanos() as i64;
                    (
                        -(d.as_secs() as i64) - i64::from(ns != 0),
                        (1_000_000_000 - ns) % 1_000_000_000,
                    )
                }
            };
            libc::timespec {
                tv_sec: sec as libc::time_t,
                tv_nsec: ns as libc::c_long,
            }
        }
        _ => libc::timespec {
            tv_sec: 0,
            tv_nsec: if t.is_some() {
                libc::UTIME_NOW
            } else {
                libc::UTIME_OMIT
            },
        },
    }
}
