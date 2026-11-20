//! Metadata operations use the same pinned backing object as file I/O.
use super::router::{cpath, os_error};
use fuser::{ReplyStatfs, ReplyXattr};
use std::{ffi::OsStr, io, mem::MaybeUninit, os::unix::ffi::OsStrExt, path::Path};

pub fn statfs(path: &Path, reply: ReplyStatfs) {
    let result = (|| {
        let path = cpath(path.as_os_str())?;
        let mut native = MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::statvfs(path.as_ptr(), native.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { native.assume_init() })
    })();
    match result {
        Ok(s) => reply.statfs(
            s.f_blocks,
            s.f_bfree,
            s.f_bavail,
            s.f_files,
            s.f_ffree,
            s.f_bsize as u32,
            s.f_namemax as u32,
            s.f_frsize as u32,
        ),
        Err(e) => reply.error(e.into()),
    }
}

pub fn read_xattr(path: &Path, name: Option<&OsStr>, size: u32, reply: ReplyXattr) {
    let result = (|| {
        let path = cpath(path.as_os_str())?;
        let name = name.map(cpath).transpose()?;
        let mut buf = vec![0u8; size.min(64 * 1024) as usize];
        let ptr = if size == 0 {
            std::ptr::null_mut()
        } else {
            buf.as_mut_ptr()
        };
        let n = unsafe {
            match name {
                Some(name) => libc::getxattr(path.as_ptr(), name.as_ptr(), ptr.cast(), buf.len()),
                None => libc::listxattr(path.as_ptr(), ptr.cast(), buf.len()),
            }
        };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        buf.truncate(n as usize);
        Ok((n as u32, buf))
    })();
    match result {
        Ok((n, _)) if size == 0 => reply.size(n),
        Ok((_, data)) => reply.data(&data),
        Err(e) => reply.error(e.into()),
    }
}

pub fn write_xattr(path: &Path, name: &OsStr, value: Option<(&[u8], i32)>) -> io::Result<()> {
    // The request originates in Bubblewrap's user namespace. Never use the
    // serving process's capabilities to install host security or trusted xattrs.
    if !name.as_bytes().starts_with(b"user.") {
        return Err(os_error(libc::EOPNOTSUPP));
    }
    let path = cpath(path.as_os_str())?;
    let name = cpath(name)?;
    let rc = unsafe {
        match value {
            Some((v, flags)) => libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                v.as_ptr().cast(),
                v.len(),
                flags,
            ),
            None => libc::removexattr(path.as_ptr(), name.as_ptr()),
        }
    };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
