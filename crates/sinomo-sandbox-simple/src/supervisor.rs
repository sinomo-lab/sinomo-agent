//! Linux adopts orphaned descendants at the execution boundary. The monitor
//! remains in the host PID namespace, including when the guest uses Bubblewrap.
use std::io;
use std::os::fd::RawFd;

/// Called between fork and exec. Only the guest returns to Command's exec path.
/// The monitor must use fork-safe syscalls and stack storage until _exit.
pub(super) unsafe fn install(control: RawFd) -> io::Result<()> {
    unsafe {
        if libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1) < 0 {
            return Err(io::Error::last_os_error());
        }
        // Recovery requires Linux procfs child enumeration even for programs
        // which leave the foreground process group.
        let children = libc::open(c"/proc/thread-self/children".as_ptr(), libc::O_RDONLY);
        if children < 0 {
            return Err(io::Error::last_os_error());
        }
        libc::close(children);
        // The embedding process may ignore SIGCHLD. This supervisor must be
        // able to wait for each child and observe the foreground exit status.
        let mut disposition: libc::sigaction = std::mem::zeroed();
        disposition.sa_sigaction = libc::SIG_DFL;
        libc::sigemptyset(&mut disposition.sa_mask);
        if libc::sigaction(libc::SIGCHLD, &disposition, std::ptr::null_mut()) < 0 {
            return Err(io::Error::last_os_error());
        }
        let guest = libc::fork();
        if guest == -1 {
            return Err(io::Error::last_os_error());
        }
        if guest == 0 {
            return Ok(());
        }
        // Keep only the control socket. In particular, close Rust's exec
        // error pipe and every copy of the host end of the socket.
        if libc::dup2(control, 0) < 0 {
            libc::kill(guest, libc::SIGKILL);
            libc::_exit(125);
        }
        if libc::syscall(libc::SYS_close_range, 1u32, u32::MAX, 0u32) < 0 {
            let max = libc::sysconf(libc::_SC_OPEN_MAX);
            for fd in 1..max as i32 {
                libc::close(fd);
            }
        }
        monitor(guest)
    }
}

unsafe fn monitor(guest: libc::pid_t) -> ! {
    unsafe {
        let mut mask = std::mem::zeroed();
        libc::sigemptyset(&mut mask);
        libc::sigaddset(&mut mask, libc::SIGCHLD);
        libc::sigprocmask(libc::SIG_BLOCK, &mask, std::ptr::null_mut());
        let child_events = libc::signalfd(-1, &mask, libc::SFD_NONBLOCK);
        let mut cancelling = false;
        let mut foreground = None;
        loop {
            let mut status = 0;
            let pid = libc::waitpid(-1, &mut status, libc::WNOHANG);
            if pid > 0 {
                if pid == guest {
                    foreground = Some(status);
                }
                continue;
            }
            let empty = pid < 0 && *libc::__errno_location() == libc::ECHILD;
            if let Some(status) = foreground.take() {
                // The foreground status message tells the host whether it can
                // await terminal audit without blocking on a background child.
                let message = [status, i32::from(!empty)];
                if libc::send(0, message.as_ptr().cast(), 8, libc::MSG_NOSIGNAL) != 8 {
                    cancelling = true;
                }
            }
            if empty {
                libc::_exit(0);
            }
            if cancelling {
                kill_direct_children();
                libc::poll(std::ptr::null_mut(), 0, 5);
            } else {
                let mut events = [
                    libc::pollfd {
                        fd: 0,
                        events: libc::POLLIN,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: child_events,
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                if libc::poll(
                    events.as_mut_ptr(),
                    2,
                    if child_events < 0 { 50 } else { -1 },
                ) > 0
                {
                    cancelling = events[0].revents != 0;
                    if events[1].revents != 0 {
                        let mut signal = std::mem::MaybeUninit::<libc::signalfd_siginfo>::uninit();
                        libc::read(
                            child_events,
                            signal.as_mut_ptr().cast(),
                            std::mem::size_of::<libc::signalfd_siginfo>(),
                        );
                    }
                }
            }
        }
    }
}

/// After each killed parent exits, the kernel reparents its children here,
/// even if they called setsid. A listed child cannot reuse its PID until this
/// single-threaded monitor next calls waitpid.
unsafe fn kill_direct_children() {
    unsafe {
        let fd = libc::open(c"/proc/thread-self/children".as_ptr(), libc::O_RDONLY);
        if fd < 0 {
            return;
        }
        let mut bytes = [0u8; 4096];
        let n = libc::read(fd, bytes.as_mut_ptr().cast(), bytes.len());
        libc::close(fd);
        let mut pid = 0i32;
        for &b in &bytes[..n.max(0) as usize] {
            if b.is_ascii_digit() {
                pid = pid * 10 + i32::from(b - b'0');
            } else {
                if pid > 0 {
                    libc::kill(pid, libc::SIGKILL);
                }
                pid = 0;
            }
        }
    }
}

pub(super) fn wait(
    child: &mut std::process::Child,
    control: &mut std::os::unix::net::UnixStream,
    timeout_ms: u64,
    cancellation: &super::SandboxCancellationToken,
    budget: &super::BoundedOutputBudget,
    cleanup_on_exit: bool,
    report: &dyn Fn(i32),
    pipes: &mut [super::NativePipe; 2],
) -> Result<(std::process::ExitStatus, Option<super::SandboxWasiError>), super::SandboxWasiError> {
    use std::io::Read;
    use std::os::unix::process::ExitStatusExt;
    let fail = |e: io::Error| super::SandboxWasiError::Runtime {
        detail: format!("native execution supervisor: {e}"),
    };
    control.set_nonblocking(true).map_err(fail)?;
    let started = std::time::Instant::now();
    let mut foreground = None;
    let mut message = [0; 8];
    let mut received = 0;
    let mut terminal = None;
    let mut stopping = None;
    let mut output_closed = [false; 2];
    loop {
        super::drain_pipes(pipes, budget)?;
        if foreground.is_none() {
            match control.read(&mut message[received..]) {
                Ok(n) => received += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                Err(e) => return Err(fail(e)),
            }
            if received == 8 {
                let status = std::process::ExitStatus::from_raw(i32::from_ne_bytes(
                    message[..4].try_into().unwrap(),
                ));
                foreground = Some(status);
                if message[4..] != [0; 4] && terminal.is_none() {
                    if cleanup_on_exit {
                        control.shutdown(std::net::Shutdown::Write).map_err(fail)?;
                        stopping = Some(std::time::Instant::now());
                    } else {
                        super::drain_pipes(pipes, budget)?;
                        if !cancellation.is_cancelled() && !budget.exceeded() {
                            report(super::exit_status_code(status));
                        }
                    }
                }
            }
        }
        if terminal.is_none() {
            terminal = if cancellation.is_cancelled() {
                Some(super::SandboxWasiError::Cancelled)
            } else if budget.exceeded() {
                Some(super::SandboxWasiError::OutputLimitExceeded {
                    limit: budget.limit as usize,
                })
            } else {
                super::native_wait_poll_duration(started, timeout_ms).err()
            };
            if terminal.is_some() {
                control.shutdown(std::net::Shutdown::Write).map_err(fail)?;
                stopping = Some(std::time::Instant::now());
            }
        }
        if let Some(status) = child.try_wait().map_err(fail)? {
            // A status packet can arrive after this iteration's nonblocking read.
            if foreground.is_none() && status.success() && received < 8 {
                control.read_exact(&mut message[received..]).map_err(fail)?;
                foreground = Some(std::process::ExitStatus::from_raw(i32::from_ne_bytes(
                    message[..4].try_into().unwrap(),
                )));
            }
            return Ok((foreground.unwrap_or(status), terminal));
        }
        if stopping.is_some_and(|t| {
            t.elapsed().as_millis() >= super::SIMPLE_NATIVE_TERMINATION_WAIT_MS.into()
        }) {
            return Err(super::SandboxWasiError::CancelledCleanupFailed {
                detail: "native supervisor still owns descendants".into(),
            });
        }
        use std::os::fd::AsRawFd;
        let pollfd = |fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let mut events = [
            pollfd(control.as_raw_fd()),
            pollfd(if output_closed[0] {
                -1
            } else {
                pipes[0].file.as_raw_fd()
            }),
            pollfd(if output_closed[1] {
                -1
            } else {
                pipes[1].file.as_raw_fd()
            }),
        ];
        // Wake on output, foreground exit or tree drain. The bounded wait also
        // observes independently requested cancellation and deadlines.
        unsafe {
            libc::poll(
                events.as_mut_ptr(),
                3,
                super::SIMPLE_NATIVE_WAIT_POLL_MS as i32,
            );
        }
        for (closed, event) in output_closed.iter_mut().zip(&events[1..]) {
            *closed |= event.revents & libc::POLLHUP != 0;
        }
    }
}
