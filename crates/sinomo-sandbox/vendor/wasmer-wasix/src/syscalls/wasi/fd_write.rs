use std::task::Waker;

use super::*;
use virtual_fs::DeviceFile;
#[cfg(feature = "journal")]
use crate::{
    journal::{JournalEffector, JournalEntry},
    utils::map_snapshot_err,
};
use crate::{net::socket::TimeType, syscalls::*};

/// ### `fd_write()`
/// Write data to the file descriptor
/// Inputs:
/// - `Fd`
///     File descriptor (opened with writing) to write to
/// - `const __wasi_ciovec_t *iovs`
///     List of vectors to read data from
/// - `u32 iovs_len`
///     Length of data in `iovs`
/// Output:
/// - `u32 *nwritten`
///     Number of bytes written
/// Errors:
///
#[instrument(level = "trace", skip_all, fields(%fd, nwritten = field::Empty), ret)]
pub fn fd_write<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    iovs: WasmPtr<__wasi_ciovec_t<M>, M>,
    iovs_len: M::Offset,
    nwritten: WasmPtr<M::Offset, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    if let Some(err) = maybe_stop_background_terminal_write(&mut ctx, fd)? {
        return Ok(err);
    }

    let env = ctx.data();
    let enable_journal = env.enable_journal;
    let offset: usize = {
        let state = env.state.clone();
        let inodes = state.inodes.clone();

        let fd_entry = wasi_try_ok!(state.fs.get_fd(fd));
        wasi_try_ok!(
            fd_entry
                .inner
                .offset
                .load(Ordering::Acquire)
                .try_into()
                .map_err(|_| Errno::Overflow)
        )
    };

    let bytes_written = wasi_try_ok!(fd_write_internal::<M>(
        &mut ctx,
        fd,
        FdWriteSource::Iovs { iovs, iovs_len },
        offset as u64,
        true,
        enable_journal,
    )?);

    Span::current().record("nwritten", bytes_written);

    let mut env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let nwritten_ref = nwritten.deref(&memory);
    let bytes_written: M::Offset =
        wasi_try_ok!(bytes_written.try_into().map_err(|_| Errno::Overflow));
    wasi_try_mem_ok!(nwritten_ref.write(bytes_written));
    sync_process_metadata(env);

    Ok(Errno::Success)
}

/// ### `fd_pwrite()`
/// Write to a file without adjusting its offset
/// Inputs:
/// - `Fd`
///     File descriptor (opened with writing) to write to
/// - `const __wasi_ciovec_t *iovs`
///     List of vectors to read data from
/// - `u32 iovs_len`
///     Length of data in `iovs`
/// - `Filesize offset`
///     The offset to write at
/// Output:
/// - `u32 *nwritten`
///     Number of bytes written
#[instrument(level = "trace", skip_all, fields(%fd, %offset, nwritten = field::Empty), ret)]
pub fn fd_pwrite<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    iovs: WasmPtr<__wasi_ciovec_t<M>, M>,
    iovs_len: M::Offset,
    offset: Filesize,
    nwritten: WasmPtr<M::Offset, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let enable_snapshot_capture = ctx.data().enable_journal;

    let bytes_written = wasi_try_ok!(fd_write_internal::<M>(
        &mut ctx,
        fd,
        FdWriteSource::Iovs { iovs, iovs_len },
        offset,
        false,
        enable_snapshot_capture,
    )?);

    Span::current().record("nwritten", bytes_written);

    let mut env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let nwritten_ref = nwritten.deref(&memory);
    let bytes_written: M::Offset =
        wasi_try_ok!(bytes_written.try_into().map_err(|_| Errno::Overflow));
    wasi_try_mem_ok!(nwritten_ref.write(bytes_written));

    Ok(Errno::Success)
}

fn maybe_stop_background_terminal_write(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
) -> Result<Option<Errno>, WasiError> {
    if fd != DeviceFile::STDOUT && fd != DeviceFile::STDERR {
        return Ok(None);
    }

    if !is_background_terminal_writer(ctx, fd) {
        return Ok(None);
    }

    ctx.data().process.signal_process_direct(Signal::Sigttou);
    match WasiEnv::process_signals_and_exit(ctx)? {
        Ok(_) => {}
        Err(err) => return Ok(Some(err)),
    }

    if is_background_terminal_writer(ctx, fd) {
        Ok(Some(Errno::Io))
    } else {
        Ok(None)
    }
}

fn is_background_terminal_writer(ctx: &FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> bool {
    let env = ctx.data();
    let Some(bridge) = env.runtime.tty() else {
        return false;
    };
    let state = bridge.tty_get();
    if !state.tostop {
        return false;
    }
    if fd == DeviceFile::STDOUT && !state.stdout_tty {
        return false;
    }
    if fd == DeviceFile::STDERR && !state.stderr_tty {
        return false;
    }
    bridge
        .foreground_process_group()
        .is_some_and(|fg_pgid| fg_pgid != env.process.pgid().raw())
}

pub(crate) enum FdWriteSource<'a, M: MemorySize> {
    Iovs {
        iovs: WasmPtr<__wasi_ciovec_t<M>, M>,
        iovs_len: M::Offset,
    },
    Buffer(Cow<'a, [u8]>),
}

#[allow(clippy::await_holding_lock)]
pub(crate) fn fd_write_internal<M: MemorySize>(
    mut ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    data: FdWriteSource<'_, M>,
    offset: u64,
    should_update_cursor: bool,
    should_snapshot: bool,
) -> Result<Result<usize, Errno>, WasiError> {
    let mut offset = offset;
    let mut env = ctx.data();
    let state = env.state.clone();

    let fd_entry = wasi_try_ok_ok!(state.fs.get_fd(fd));
    let is_stdio = fd_entry.is_stdio;

    let bytes_written = {
        if !is_stdio && !fd_entry.inner.rights.contains(Rights::FD_WRITE) {
            return Ok(Err(Errno::Access));
        }

        if is_dev_full(&fd_entry) {
            return Ok(Err(Errno::Nospc));
        }

        let fd_flags = fd_entry.inner.flags;
        let mut memory = unsafe { env.memory_view(&ctx) };

        let (bytes_written, is_file, can_snapshot) = {
            let (mut memory, _) = unsafe { env.get_memory_and_wasi_state(&ctx, 0) };
            let mut guard = fd_entry.inode.write();
            match guard.deref_mut() {
                Kind::File { handle, .. } => {
                    if let Some(handle) = handle {
                        let handle = handle.clone();
                        drop(guard);

                        let res = __asyncify_light(
                            env,
                            if fd_entry.inner.flags.contains(Fdflags::NONBLOCK) {
                                Some(Duration::ZERO)
                            } else {
                                None
                            },
                            async {
                                let mut handle = handle.write().unwrap();
                                if !is_stdio {
                                    if fd_entry.inner.flags.contains(Fdflags::APPEND) {
                                        // `fdflags::append` means we need to seek to the end before writing.
                                        offset = fd_entry.inode.stat.read().unwrap().st_size;
                                        fd_entry.inner.offset.store(offset, Ordering::Release);
                                    }

                                    handle
                                        .seek(std::io::SeekFrom::Start(offset))
                                        .await
                                        .map_err(map_io_err)?;
                                }

                                let mut written = 0usize;
                                let mut file_size_remaining = if is_stdio {
                                    None
                                } else {
                                    Some(file_size_write_remaining(
                                        env.process.file_size_limit(),
                                        offset,
                                    ))
                                };

                                match &data {
                                    FdWriteSource::Iovs { iovs, iovs_len } => {
                                        let iovs_arr = iovs
                                            .slice(&memory, *iovs_len)
                                            .map_err(mem_error_to_wasi)?;
                                        let iovs_arr =
                                            iovs_arr.access().map_err(mem_error_to_wasi)?;
                                        for iovs in iovs_arr.iter() {
                                            let buf = WasmPtr::<u8, M>::new(iovs.buf)
                                                .slice(&memory, iovs.buf_len)
                                                .map_err(mem_error_to_wasi)?
                                                .access()
                                                .map_err(mem_error_to_wasi)?;
                                            let buf = limit_regular_file_write_buffer(
                                                buf.as_ref(),
                                                &mut file_size_remaining,
                                                written,
                                            )?;
                                            let local_written =
                                                match handle.write(buf).await {
                                                    Ok(s) => s,
                                                    Err(_) if written > 0 => break,
                                                    Err(err) => return Err(map_io_err(err)),
                                                };
                                            written += local_written;
                                            if local_written != buf.len() {
                                                break;
                                            }
                                        }
                                    }
                                    FdWriteSource::Buffer(data) => {
                                        let data = limit_regular_file_write_buffer(
                                            data,
                                            &mut file_size_remaining,
                                            written,
                                        )?;
                                        handle.write_all(data).await?;
                                        written += data.len();
                                    }
                                }

                                if is_stdio {
                                    handle.flush().await.map_err(map_io_err)?;
                                }
                                Ok(written)
                            },
                        );
                        let written = match res?.map_err(|err| match err {
                            Errno::Timedout => Errno::Again,
                            a => a,
                        }) {
                            Ok(written) => written,
                            Err(Errno::Fbig) => {
                                env.process.signal_process(Signal::Sigxfsz);
                                wasi_try_ok_ok!(WasiEnv::process_signals_and_exit(ctx)?);
                                return Ok(Err(Errno::Fbig));
                            }
                            Err(err) => return Ok(Err(err)),
                        };

                        (written, true, true)
                    } else {
                        return Ok(Err(Errno::Inval));
                    }
                }
                Kind::Socket { socket } => {
                    let socket = socket.clone();
                    drop(guard);

                    let nonblocking = fd_flags.contains(Fdflags::NONBLOCK);
                    let timeout = socket
                        .opt_time(TimeType::WriteTimeout)
                        .ok()
                        .flatten()
                        .unwrap_or(Duration::from_secs(30));

                    let tasks = env.tasks().clone();

                    let res = __asyncify_light(env, None, async {
                        let mut sent = 0usize;

                        match &data {
                            FdWriteSource::Iovs { iovs, iovs_len } => {
                                let iovs_arr =
                                    iovs.slice(&memory, *iovs_len).map_err(mem_error_to_wasi)?;
                                let iovs_arr = iovs_arr.access().map_err(mem_error_to_wasi)?;
                                for iovs in iovs_arr.iter() {
                                    let buf = WasmPtr::<u8, M>::new(iovs.buf)
                                        .slice(&memory, iovs.buf_len)
                                        .map_err(mem_error_to_wasi)?
                                        .access()
                                        .map_err(mem_error_to_wasi)?;
                                    let local_sent = socket
                                        .send(
                                            tasks.deref(),
                                            buf.as_ref(),
                                            Some(timeout),
                                            nonblocking,
                                        )
                                        .await?;
                                    sent += local_sent;
                                    if local_sent != buf.len() {
                                        break;
                                    }
                                }
                            }
                            FdWriteSource::Buffer(data) => {
                                sent += socket
                                    .send(tasks.deref(), data.as_ref(), Some(timeout), nonblocking)
                                    .await?;
                            }
                        }
                        Ok(sent)
                    });
                    let written = wasi_try_ok_ok!(res?);
                    (written, false, false)
                }
                Kind::PipeRx { .. } => {
                    return Ok(Err(Errno::Badf));
                }
                Kind::PipeTx { tx } => {
                    let mut written = 0usize;

                    match &data {
                        FdWriteSource::Iovs { iovs, iovs_len } => {
                            let mut raise_sigpipe = false;
                            let iovs_arr = wasi_try_ok_ok!(
                                iovs.slice(&memory, *iovs_len).map_err(mem_error_to_wasi)
                            );
                            let iovs_arr =
                                wasi_try_ok_ok!(iovs_arr.access().map_err(mem_error_to_wasi));
                            for iovs in iovs_arr.iter() {
                                let buf = wasi_try_ok_ok!(
                                    WasmPtr::<u8, M>::new(iovs.buf)
                                        .slice(&memory, iovs.buf_len)
                                        .map_err(mem_error_to_wasi)
                                );
                                let buf = wasi_try_ok_ok!(buf.access().map_err(mem_error_to_wasi));
                                let write_result = std::io::Write::write(tx, buf.as_ref());
                                let local_written = match write_result {
                                    Ok(w) => w,
                                    Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                                        // Need to do this to avoid double borrow on ctx with iovs_arr
                                        raise_sigpipe = true;
                                        break;
                                    }
                                    Err(e) => return Ok(Err(map_io_err(e))),
                                };

                                written += local_written;
                                if local_written != buf.len() {
                                    break;
                                }
                            }

                            drop(iovs_arr);

                            if raise_sigpipe {
                                env.process.signal_process(Signal::Sigpipe);
                                wasi_try_ok_ok!(WasiEnv::process_signals_and_exit(ctx)?);
                                return Ok(Err(Errno::Pipe));
                            }
                        }
                        FdWriteSource::Buffer(data) => {
                            match std::io::Write::write_all(tx, data) {
                                Ok(()) => (),
                                Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                                    env.process.signal_process(Signal::Sigpipe);
                                    wasi_try_ok_ok!(WasiEnv::process_signals_and_exit(ctx)?);
                                    return Ok(Err(Errno::Pipe));
                                }
                                Err(e) => return Ok(Err(map_io_err(e))),
                            };
                            written += data.len();
                        }
                    }

                    (written, false, true)
                }
                Kind::DuplexPipe { pipe } => {
                    let mut written = 0usize;

                    match &data {
                        FdWriteSource::Iovs { iovs, iovs_len } => {
                            let mut raise_sigpipe = false;
                            let iovs_arr = wasi_try_ok_ok!(
                                iovs.slice(&memory, *iovs_len).map_err(mem_error_to_wasi)
                            );
                            let iovs_arr =
                                wasi_try_ok_ok!(iovs_arr.access().map_err(mem_error_to_wasi));
                            for iovs in iovs_arr.iter() {
                                let buf = wasi_try_ok_ok!(
                                    WasmPtr::<u8, M>::new(iovs.buf)
                                        .slice(&memory, iovs.buf_len)
                                        .map_err(mem_error_to_wasi)
                                );
                                let buf = wasi_try_ok_ok!(buf.access().map_err(mem_error_to_wasi));
                                let write_result = std::io::Write::write(pipe, buf.as_ref());
                                let local_written = match write_result {
                                    Ok(w) => w,
                                    Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                                        // Need to do this to avoid double borrow on ctx with iovs_arr
                                        raise_sigpipe = true;
                                        break;
                                    }
                                    Err(e) => return Ok(Err(map_io_err(e))),
                                };

                                written += local_written;
                                if local_written != buf.len() {
                                    break;
                                }
                            }

                            drop(iovs_arr);

                            if raise_sigpipe {
                                env.process.signal_process(Signal::Sigpipe);
                                wasi_try_ok_ok!(WasiEnv::process_signals_and_exit(ctx)?);
                                return Ok(Err(Errno::Pipe));
                            }
                        }
                        FdWriteSource::Buffer(data) => {
                            match std::io::Write::write_all(pipe, data) {
                                Ok(()) => (),
                                Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                                    env.process.signal_process(Signal::Sigpipe);
                                    wasi_try_ok_ok!(WasiEnv::process_signals_and_exit(ctx)?);
                                    return Ok(Err(Errno::Pipe));
                                }
                                Err(e) => return Ok(Err(map_io_err(e))),
                            };
                            written += data.len();
                        }
                    }

                    (written, false, true)
                }
                Kind::Dir { .. } | Kind::Root { .. } => {
                    // TODO: verify
                    return Ok(Err(Errno::Isdir));
                }
                Kind::EventNotifications { inner } => {
                    let mut written = 0usize;

                    match &data {
                        FdWriteSource::Iovs { iovs, iovs_len } => {
                            let mut raise_sigxfsz = false;
                            let iovs_arr = wasi_try_ok_ok!(
                                iovs.slice(&memory, *iovs_len).map_err(mem_error_to_wasi)
                            );
                            let iovs_arr =
                                wasi_try_ok_ok!(iovs_arr.access().map_err(mem_error_to_wasi));
                            for iovs in iovs_arr.iter() {
                                let buf_len: usize = wasi_try_ok_ok!(
                                    iovs.buf_len.try_into().map_err(|_| Errno::Inval)
                                );
                                let will_be_written = buf_len;

                                let val_cnt = buf_len / std::mem::size_of::<u64>();
                                let val_cnt: M::Offset =
                                    wasi_try_ok_ok!(val_cnt.try_into().map_err(|_| Errno::Inval));

                                let vals = wasi_try_ok_ok!(
                                    WasmPtr::<u64, M>::new(iovs.buf)
                                        .slice(&memory, val_cnt as M::Offset)
                                        .map_err(mem_error_to_wasi)
                                );
                                let vals =
                                    wasi_try_ok_ok!(vals.access().map_err(mem_error_to_wasi));
                                for val in vals.iter() {
                                    inner.write(*val);
                                }

                                written += will_be_written;
                            }
                        }
                        FdWriteSource::Buffer(data) => {
                            let cnt = data.len() / std::mem::size_of::<u64>();
                            for n in 0..cnt {
                                let start = n * std::mem::size_of::<u64>();
                                let data = [
                                    data[start],
                                    data[start + 1],
                                    data[start + 2],
                                    data[start + 3],
                                    data[start + 4],
                                    data[start + 5],
                                    data[start + 6],
                                    data[start + 7],
                                ];
                                inner.write(u64::from_ne_bytes(data));
                            }
                        }
                    }

                    (written, false, true)
                }
                Kind::Symlink { .. } | Kind::Epoll { .. } => return Ok(Err(Errno::Inval)),
                Kind::Buffer { buffer } => {
                    let mut written = 0usize;
                    if fd_flags.contains(Fdflags::APPEND) {
                        offset = buffer.len() as u64;
                    }
                    let mut file_size_remaining =
                        Some(file_size_write_remaining(env.process.file_size_limit(), offset));
                    let write_offset: usize =
                        wasi_try_ok_ok!(offset.try_into().map_err(|_| Errno::Overflow));
                    if write_offset > buffer.len() {
                        buffer.resize(write_offset, 0);
                    }

                    match &data {
                        FdWriteSource::Iovs { iovs, iovs_len } => {
                            let mut raise_sigxfsz = false;
                            let iovs_arr = wasi_try_ok_ok!(
                                iovs.slice(&memory, *iovs_len).map_err(mem_error_to_wasi)
                            );
                            let iovs_arr =
                                wasi_try_ok_ok!(iovs_arr.access().map_err(mem_error_to_wasi));
                            for iovs in iovs_arr.iter() {
                                let buf = wasi_try_ok_ok!(
                                    WasmPtr::<u8, M>::new(iovs.buf)
                                        .slice(&memory, iovs.buf_len)
                                        .map_err(mem_error_to_wasi)
                                );
                                let buf = wasi_try_ok_ok!(buf.access().map_err(mem_error_to_wasi));
                                let buf = match limit_regular_file_write_buffer(
                                    buf.as_ref(),
                                    &mut file_size_remaining,
                                    written,
                                ) {
                                    Ok(buf) => buf,
                                    Err(Errno::Fbig) => {
                                        raise_sigxfsz = true;
                                        break;
                                    }
                                    Err(err) => return Ok(Err(err)),
                                };
                                let end = wasi_try_ok_ok!(
                                    write_offset
                                        .checked_add(written)
                                        .and_then(|start| start.checked_add(buf.len()))
                                        .ok_or(Errno::Overflow)
                                );
                                if end > buffer.len() {
                                    buffer.resize(end, 0);
                                }
                                let start = write_offset + written;
                                buffer[start..end].copy_from_slice(buf);
                                let local_written = buf.len();
                                written += buf.len();
                                if local_written != buf.len() {
                                    break;
                                }
                            }
                            drop(iovs_arr);
                            if raise_sigxfsz {
                                env.process.signal_process(Signal::Sigxfsz);
                                wasi_try_ok_ok!(WasiEnv::process_signals_and_exit(ctx)?);
                                return Ok(Err(Errno::Fbig));
                            }
                        }
                        FdWriteSource::Buffer(data) => {
                            let data = match limit_regular_file_write_buffer(
                                data,
                                &mut file_size_remaining,
                                written,
                            ) {
                                Ok(data) => data,
                                Err(Errno::Fbig) => {
                                    env.process.signal_process(Signal::Sigxfsz);
                                    wasi_try_ok_ok!(WasiEnv::process_signals_and_exit(ctx)?);
                                    return Ok(Err(Errno::Fbig));
                                }
                                Err(err) => return Ok(Err(err)),
                            };
                            let end = wasi_try_ok_ok!(
                                write_offset
                                    .checked_add(written)
                                    .and_then(|start| start.checked_add(data.len()))
                                    .ok_or(Errno::Overflow)
                            );
                            if end > buffer.len() {
                                buffer.resize(end, 0);
                            }
                            let start = write_offset + written;
                            buffer[start..end].copy_from_slice(data);
                            written += data.len();
                        }
                    }

                    (written, true, true)
                }
            }
        };

        #[cfg(feature = "journal")]
        if should_snapshot
            && can_snapshot
            && bytes_written > 0
            && let FdWriteSource::Iovs { iovs, iovs_len } = data
        {
            JournalEffector::save_fd_write(ctx, fd, offset, bytes_written, iovs, iovs_len)
                .map_err(|err| {
                    tracing::error!("failed to save terminal data - {}", err);
                    WasiError::Exit(ExitCode::from(Errno::Fault))
                })?;
        }

        env = ctx.data();
        memory = unsafe { env.memory_view(&ctx) };

        // reborrow and update the size
        if !is_stdio {
            let curr_offset = if is_file && should_update_cursor {
                let bytes_written = bytes_written as u64;
                let mut fd_map = state.fs.fd_map.write().unwrap();
                let fd_entry = wasi_try_ok_ok!(fd_map.get_mut(fd).ok_or(Errno::Badf));
                fd_entry
                    .offset
                    .fetch_add(bytes_written, Ordering::AcqRel)
                    // fetch_add returns the previous value, we have to add bytes_written again here
                    + bytes_written
            } else {
                fd_entry.inner.offset.load(Ordering::Acquire)
            };

            // we set the size but we don't return any errors if it fails as
            // pipes and sockets will not do anything with this
            let (mut memory, _, inodes) =
                unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };
            if is_file {
                let mut stat = fd_entry.inode.stat.write().unwrap();
                if should_update_cursor {
                    // If we wrote before the end, the current size is still correct.
                    // Otherwise, we only got as far as the current cursor. So, the
                    // max of the two is the correct new size.
                    stat.st_size = stat.st_size.max(curr_offset);
                } else {
                    // pwrite does not update the cursor of the file so to calculate the final
                    // size of the file we compute where the cursor would have been if it was updated,
                    // and get the max value between it and the current size.
                    stat.st_size = stat.st_size.max(offset + bytes_written as u64);
                }
            } else {
                // Cast is valid because we don't support 128 bit systems...
                fd_entry.inode.stat.write().unwrap().st_size += bytes_written as u64;
            }
        }
        bytes_written
    };

    Ok(Ok(bytes_written))
}

fn is_dev_full(fd_entry: &Fd) -> bool {
    let name = fd_entry.inode.name.read().unwrap();
    name.as_ref() == "/dev/full" || name.as_ref() == "full" || name.as_ref().ends_with("/dev/full")
}

fn file_size_write_remaining(limit: u64, offset: u64) -> u64 {
    limit.saturating_sub(offset)
}

fn limit_regular_file_write_buffer<'a>(
    buf: &'a [u8],
    remaining: &mut Option<u64>,
    already_written: usize,
) -> Result<&'a [u8], Errno> {
    let Some(bytes_remaining) = remaining else {
        return Ok(buf);
    };
    if *bytes_remaining == 0 {
        return if already_written == 0 && !buf.is_empty() {
            Err(Errno::Fbig)
        } else {
            Ok(&buf[..0])
        };
    }

    let allowed = (*bytes_remaining).min(buf.len() as u64) as usize;
    *bytes_remaining -= allowed as u64;
    Ok(&buf[..allowed])
}
