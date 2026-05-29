#[cfg(target_os = "linux")]
mod platform {
    use std::{ffi::CString, io, os::fd::RawFd};

    #[derive(Debug)]
    pub(crate) struct WasiSharedMemoryBacking {
        fd: RawFd,
    }

    impl WasiSharedMemoryBacking {
        pub(crate) fn new(bytes: &[u8]) -> Result<Self, io::Error> {
            let name = CString::new("sinomo-wasix-shm").expect("static memfd name has no nul");
            let fd =
                unsafe { libc::syscall(libc::SYS_memfd_create, name.as_ptr(), libc::MFD_CLOEXEC) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let backing = Self { fd: fd as RawFd };
            if let Err(err) = backing.initialize(bytes) {
                drop(backing);
                return Err(err);
            }
            Ok(backing)
        }

        pub(crate) fn map_at(&self, base: *mut u8, len: usize) -> Result<(), io::Error> {
            let mapped = unsafe {
                libc::mmap(
                    base.cast(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED | libc::MAP_FIXED,
                    self.fd,
                    0,
                )
            };
            if mapped == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            if mapped != base.cast() {
                return Err(io::Error::from_raw_os_error(libc::EIO));
            }
            Ok(())
        }

        pub(crate) fn read(&self, len: usize) -> Result<Vec<u8>, io::Error> {
            self.read_at(0, len)
        }

        pub(crate) fn read_at(&self, offset: usize, len: usize) -> Result<Vec<u8>, io::Error> {
            let mut bytes = vec![0; len];
            let mut read = 0usize;
            while read < len {
                let ret = unsafe {
                    libc::pread(
                        self.fd,
                        bytes[read..].as_mut_ptr().cast(),
                        len - read,
                        (offset + read) as libc::off_t,
                    )
                };
                if ret < 0 {
                    return Err(io::Error::last_os_error());
                }
                if ret == 0 {
                    return Err(io::Error::from_raw_os_error(libc::EIO));
                }
                read += ret as usize;
            }
            Ok(bytes)
        }

        pub(crate) fn write(&self, bytes: &[u8]) -> Result<(), io::Error> {
            self.write_at(0, bytes)
        }

        pub(crate) fn write_at(&self, offset: usize, bytes: &[u8]) -> Result<(), io::Error> {
            let mut written = 0usize;
            while written < bytes.len() {
                let ret = unsafe {
                    libc::pwrite(
                        self.fd,
                        bytes[written..].as_ptr().cast(),
                        bytes.len() - written,
                        (offset + written) as libc::off_t,
                    )
                };
                if ret < 0 {
                    return Err(io::Error::last_os_error());
                }
                if ret == 0 {
                    return Err(io::Error::from_raw_os_error(libc::EIO));
                }
                written += ret as usize;
            }
            Ok(())
        }

        fn initialize(&self, bytes: &[u8]) -> Result<(), io::Error> {
            let len = i64::try_from(bytes.len())
                .map_err(|_| io::Error::from_raw_os_error(libc::EOVERFLOW))?;
            if unsafe { libc::ftruncate(self.fd, len) } != 0 {
                return Err(io::Error::last_os_error());
            }
            self.write(bytes)
        }
    }

    impl Drop for WasiSharedMemoryBacking {
        fn drop(&mut self) {
            unsafe {
                libc::close(self.fd);
            }
        }
    }

    pub(crate) fn host_page_size() -> Result<usize, io::Error> {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err(io::Error::last_os_error());
        }
        usize::try_from(page_size).map_err(|_| io::Error::from_raw_os_error(libc::EOVERFLOW))
    }

    #[allow(dead_code)]
    pub(crate) fn unsupported_error() -> io::Error {
        io::Error::from_raw_os_error(libc::ENOTSUP)
    }

    pub(crate) fn is_unsupported_error(err: &io::Error) -> bool {
        err.raw_os_error() == Some(libc::ENOTSUP)
    }
}

#[cfg(target_os = "windows")]
mod platform {
    use std::{io, ptr};

    use windows_sys::Win32::{
        Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE},
        System::{
            Memory::{
                CreateFileMappingW, FILE_MAP_READ, FILE_MAP_WRITE, MapViewOfFile,
                MapViewOfFileEx, PAGE_READWRITE, UnmapViewOfFile,
            },
            SystemInformation::{GetSystemInfo, SYSTEM_INFO},
        },
    };

    #[derive(Debug)]
    pub(crate) struct WasiSharedMemoryBacking {
        handle: HANDLE,
    }

    unsafe impl Send for WasiSharedMemoryBacking {}
    unsafe impl Sync for WasiSharedMemoryBacking {}

    impl WasiSharedMemoryBacking {
        pub(crate) fn new(bytes: &[u8]) -> Result<Self, io::Error> {
            let len = u64::try_from(bytes.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mapping is too large"))?;
            let handle = unsafe {
                CreateFileMappingW(
                    INVALID_HANDLE_VALUE,
                    ptr::null(),
                    PAGE_READWRITE,
                    (len >> 32) as u32,
                    len as u32,
                    ptr::null(),
                )
            };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            let backing = Self { handle };
            if let Err(err) = backing.initialize(bytes) {
                drop(backing);
                return Err(err);
            }
            Ok(backing)
        }

        pub(crate) fn map_at(&self, base: *mut u8, len: usize) -> Result<(), io::Error> {
            let mapped = unsafe {
                MapViewOfFileEx(
                    self.handle,
                    FILE_MAP_READ | FILE_MAP_WRITE,
                    0,
                    0,
                    len,
                    base.cast_const().cast(),
                )
            };
            if mapped.Value.is_null() {
                return Err(io::Error::last_os_error());
            }
            if mapped.Value != base.cast() {
                unsafe {
                    UnmapViewOfFile(mapped);
                }
                return Err(io::Error::other("mapped view was not placed at the requested address"));
            }
            Ok(())
        }

        pub(crate) fn read(&self, len: usize) -> Result<Vec<u8>, io::Error> {
            self.read_at(0, len)
        }

        pub(crate) fn read_at(&self, offset: usize, len: usize) -> Result<Vec<u8>, io::Error> {
            if len == 0 {
                return Ok(Vec::new());
            }
            let mapped = unsafe {
                MapViewOfFile(
                    self.handle,
                    FILE_MAP_READ | FILE_MAP_WRITE,
                    0,
                    0,
                    offset.saturating_add(len),
                )
            };
            if mapped.Value.is_null() {
                return Err(io::Error::last_os_error());
            }
            let mut bytes = vec![0; len];
            unsafe {
                ptr::copy_nonoverlapping(mapped.Value.cast::<u8>().add(offset), bytes.as_mut_ptr(), len);
                if UnmapViewOfFile(mapped) == 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(bytes)
        }

        pub(crate) fn write(&self, bytes: &[u8]) -> Result<(), io::Error> {
            self.write_at(0, bytes)
        }

        pub(crate) fn write_at(&self, offset: usize, bytes: &[u8]) -> Result<(), io::Error> {
            if bytes.is_empty() {
                return Ok(());
            }
            let mapped = unsafe {
                MapViewOfFile(
                    self.handle,
                    FILE_MAP_READ | FILE_MAP_WRITE,
                    0,
                    0,
                    offset.saturating_add(bytes.len()),
                )
            };
            if mapped.Value.is_null() {
                return Err(io::Error::last_os_error());
            }
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_ptr(), mapped.Value.cast::<u8>().add(offset), bytes.len());
                if UnmapViewOfFile(mapped) == 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        }

        fn initialize(&self, bytes: &[u8]) -> Result<(), io::Error> {
            self.write(bytes)
        }
    }

    impl Drop for WasiSharedMemoryBacking {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.handle);
            }
        }
    }

    pub(crate) fn host_page_size() -> Result<usize, io::Error> {
        let mut info = std::mem::MaybeUninit::<SYSTEM_INFO>::uninit();
        unsafe {
            GetSystemInfo(info.as_mut_ptr());
            let info = info.assume_init();
            Ok(info.dwPageSize as usize)
        }
    }

    #[allow(dead_code)]
    pub(crate) fn unsupported_error() -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, "unsupported host shared-memory operation")
    }

    pub(crate) fn is_unsupported_error(err: &io::Error) -> bool {
        err.kind() == io::ErrorKind::Unsupported
    }
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
mod platform {
    use std::io;

    #[derive(Debug)]
    pub(crate) struct WasiSharedMemoryBacking;

    impl WasiSharedMemoryBacking {
        pub(crate) fn new(_bytes: &[u8]) -> Result<Self, io::Error> {
            Err(unsupported_error())
        }

        pub(crate) fn map_at(&self, _base: *mut u8, _len: usize) -> Result<(), io::Error> {
            Err(unsupported_error())
        }

        pub(crate) fn read(&self, _len: usize) -> Result<Vec<u8>, io::Error> {
            Err(unsupported_error())
        }

        pub(crate) fn read_at(&self, _offset: usize, _len: usize) -> Result<Vec<u8>, io::Error> {
            Err(unsupported_error())
        }

        pub(crate) fn write(&self, _bytes: &[u8]) -> Result<(), io::Error> {
            Err(unsupported_error())
        }

        pub(crate) fn write_at(&self, _offset: usize, _bytes: &[u8]) -> Result<(), io::Error> {
            Err(unsupported_error())
        }
    }

    pub(crate) fn host_page_size() -> Result<usize, io::Error> {
        Ok(4096)
    }

    pub(crate) fn unsupported_error() -> io::Error {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "cross-process shared memory mappings are not implemented on this host platform",
        )
    }

    pub(crate) fn is_unsupported_error(err: &io::Error) -> bool {
        err.kind() == io::ErrorKind::Unsupported
    }
}

pub(crate) use platform::{
    WasiSharedMemoryBacking, host_page_size, is_unsupported_error,
};
