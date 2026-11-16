//! Lease-owned MIT-SHM 1.2 FD readback. No SysV IDs or global scratch segments.
use super::*;
use std::{fs::File, ptr::NonNull};
use x11rb::protocol::shm;

pub(super) enum Pixels<'a> {
    Shared(&'a mut [u8]),
    Owned(Vec<u8>),
}
impl AsRef<[u8]> for Pixels<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Shared(v) => v,
            Self::Owned(v) => v,
        }
    }
}
impl AsMut<[u8]> for Pixels<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Shared(v) => v,
            Self::Owned(v) => v,
        }
    }
}
impl Pixels<'_> {
    pub(super) fn into_arc(self) -> Arc<[u8]> {
        match self {
            Self::Shared(v) => Arc::from(&*v),
            Self::Owned(v) => v.into(),
        }
    }
}
struct Mapping {
    id: u32,
    address: NonNull<libc::c_void>,
    len: usize,
}
impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: this mapping is owned by one capture worker and no borrowed
        // frame survives a readback call. Returned frames own separate storage.
        unsafe {
            libc::munmap(self.address.as_ptr(), self.len);
        }
    }
}
pub(super) struct Readback {
    enabled: bool,
    mapping: Option<Mapping>,
}
impl Readback {
    pub(super) fn new(conn: &RustConnection) -> Self {
        let enabled = shm::query_version(conn)
            .ok()
            .and_then(|c| c.reply().ok())
            .is_some_and(|r| r.major_version > 1 || (r.major_version == 1 && r.minor_version >= 2));
        Self {
            enabled,
            mapping: None,
        }
    }
    pub(super) fn detach(&mut self, conn: &RustConnection) {
        if let Some(mapping) = self.mapping.take() {
            let _ = shm::detach(conn, mapping.id);
            drop(mapping);
        }
    }
    fn ensure(&mut self, conn: &RustConnection, len: usize) -> DesktopResult<()> {
        if self.mapping.as_ref().is_some_and(|m| m.len >= len) {
            return Ok(());
        }
        self.detach(conn);
        let id = conn.generate_id().map_err(platform)?;
        let reply = match shm::create_segment(conn, id, len as u32, false)
            .map_err(platform)
            .and_then(|cookie| cookie.reply().map_err(platform))
        {
            Ok(reply) => reply,
            Err(error) => {
                let _ = shm::detach(conn, id);
                return Err(error);
            }
        };
        let mapped = (|| {
            let fd = File::from(reply.shm_fd);
            if fd.metadata().map_err(platform)?.len() < len as u64 {
                return Err(platform("short X11 shared-memory file"));
            }
            // SAFETY: FD size is checked and kept open through mmap. The mapping
            // remains valid after closing the FD; server writes are fenced by
            // each GetImage reply before this worker reads or modifies pixels.
            let address = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    fd.as_raw_fd(),
                    0,
                )
            };
            if address == libc::MAP_FAILED {
                return Err(platform(std::io::Error::last_os_error()));
            }
            let Some(address) = NonNull::new(address) else {
                unsafe {
                    libc::munmap(address, len);
                }
                return Err(platform("null X11 shared-memory mapping"));
            };
            Ok(Mapping { id, address, len })
        })();
        match mapped {
            Ok(mapping) => {
                self.mapping = Some(mapping);
                Ok(())
            }
            Err(error) => {
                let _ = shm::detach(conn, id);
                Err(error)
            }
        }
    }
    pub(super) fn image<'a>(
        &'a mut self,
        conn: &RustConnection,
        drawable: u32,
        border: i16,
        geo: &GetGeometryReply,
    ) -> DesktopResult<(u8, Pixels<'a>)> {
        let format = conn
            .setup()
            .pixmap_formats
            .iter()
            .find(|f| f.depth == geo.depth)
            .ok_or_else(|| platform("unknown X image depth"))?;
        if ![16, 24, 32].contains(&format.bits_per_pixel)
            || ![8, 16, 32].contains(&format.scanline_pad)
        {
            return Err(DesktopError::Unsupported(
                "unsupported X11 image layout".into(),
            ));
        }
        let stride = (usize::from(geo.width) * usize::from(format.bits_per_pixel))
            .div_ceil(usize::from(format.scanline_pad))
            * usize::from(format.scanline_pad)
            / 8;
        let len = stride * usize::from(geo.height);
        if len == 0 || len > MAX_FRAME_BYTES {
            return Err(platform("X11 image exceeds frame limit"));
        }
        if self.enabled {
            let result = self.ensure(conn, len).and_then(|()| {
                shm::get_image(
                    conn,
                    drawable,
                    border,
                    border,
                    geo.width,
                    geo.height,
                    u32::MAX,
                    ImageFormat::Z_PIXMAP.into(),
                    self.mapping.as_ref().unwrap().id,
                    0,
                )
                .map_err(platform)?
                .reply()
                .map_err(platform)
            });
            if let Ok(reply) = result.as_ref() {
                if reply.size as usize == len && reply.depth == geo.depth {
                    let mapping = self.mapping.as_mut().unwrap();
                    // SAFETY: the checked reply completed the only server write.
                    // &mut self prevents another capture until this slice is gone.
                    let data = unsafe {
                        std::slice::from_raw_parts_mut(mapping.address.as_ptr().cast::<u8>(), len)
                    };
                    return Ok((reply.depth, Pixels::Shared(data)));
                }
            }
            // Optional acceleration must not remove capture support on servers
            // lacking FD passing or rejecting a shared readback. Retry through
            // ordinary GetImage and disable SHM for the rest of this lease.
            self.enabled = false;
            self.detach(conn);
            if super::super::frame_trace_enabled() {
                super::super::trace_frame(
                    serde_json::json!({"kind":"x11-shm-fallback","reason":result.err().map(|e|e.to_string()).unwrap_or_else(||"invalid shared image layout".into())}),
                );
            }
        }
        let image = conn
            .get_image(
                ImageFormat::Z_PIXMAP,
                drawable,
                border,
                border,
                geo.width,
                geo.height,
                u32::MAX,
            )
            .map_err(platform)?
            .reply()
            .map_err(unavailable)?;
        Ok((image.depth, Pixels::Owned(image.data)))
    }
}
