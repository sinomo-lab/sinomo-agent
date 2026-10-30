//! Real terminal tests run in child processes: stdio and panic hooks are process-wide.
use super::*;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};

#[test]
#[ignore = "PTY child, invoked by terminal ownership and cleanup regressions"]
fn terminal_fixture() {
    let mode = std::env::var("SINOMO_TUI_TERMINAL_FIXTURE").unwrap_or_default();
    println!("BEFORE_TERMINAL_ENTER");
    if mode == "raw" {
        enable_raw_mode().unwrap();
    }
    if mode == "external-subscriber" {
        tracing::subscriber::set_global_default(tracing::subscriber::NoSubscriber::default())
            .unwrap();
    }
    let result = std::panic::catch_unwind(|| -> Result<(), CliError> {
        if mode == "init-error" {
            let error = TuiTerminalSession::enter_with_setup(|_| {
                Err(io::Error::other("CONTROLLED_INIT_FAILURE"))
            })
            .err()
            .expect("injected initialization failure");
            assert!(error.to_string().contains("CONTROLLED_INIT_FAILURE"));
            assert!(!crossterm::terminal::is_raw_mode_enabled().unwrap());
            return Ok(());
        }
        let mut session = TuiTerminalSession::enter()?;
        session
            .terminal
            .draw(|frame| {
                frame.render_widget(Paragraph::new("OWNED_UI_FRAME"), frame.area());
            })
            .unwrap();
        // A second owner cannot redirect an already-owned terminal.
        assert!(TuiTerminalSession::enter().is_err());
        if mode == "panic" {
            panic!("CONTROLLED_UI_PANIC");
        }
        if mode == "error" {
            return Err(CliError::stdio(io::Error::other("CONTROLLED_UI_ERROR")));
        }
        let worker = std::thread::spawn(|| {
            eprintln!("STRAY_WORKER_WARNING");
            let text = b"STRAY_NATIVE_ERROR\n";
            unsafe {
                libc::write(2, text.as_ptr().cast(), text.len());
            }
            Command::new("sh")
                .args(["-c", "echo STRAY_CHILD_STDOUT; echo STRAY_CHILD_STDERR >&2"])
                .status()
                .unwrap();
            tracing::warn!(target: "sinomo.test.tui", operation_id = "original-id", "STRUCTURED_WARNING");
        });
        worker.join().unwrap();
        let mut version = 0;
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut snapshot = terminal_io::OutputSnapshot::default();
        loop {
            if let Some(current) = session.output.snapshot(&mut version) {
                snapshot = current;
            }
            if [
                "STRAY_WORKER_WARNING",
                "STRAY_NATIVE_ERROR",
                "STRAY_CHILD_STDOUT",
                "STRAY_CHILD_STDERR",
            ]
            .iter()
            .all(|text| snapshot.lines.iter().any(|line| line.text.contains(text)))
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "missing diagnostics: {snapshot:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        if mode != "external-subscriber" {
            let logs = sinomo_runtime::observe::read_observe_logs(
                Path::new(""),
                &sinomo_runtime::observe::ObserveLogsRequest {
                    source: Some(sinomo_runtime::observe::ObserveLogSource::Runtime),
                    ..Default::default()
                },
            );
            assert!(
                logs.records
                    .iter()
                    .any(|r| r.message == "STRUCTURED_WARNING"
                        && r.operation_id.as_deref() == Some("original-id"))
            );
            assert!(
                !snapshot
                    .lines
                    .iter()
                    .any(|r| r.text.contains("STRUCTURED_WARNING")),
                "console sink must stay suppressed"
            );
        }
        if mode == "background-panic" {
            assert!(
                std::thread::spawn(|| panic!("CONTROLLED_BACKGROUND_PANIC"))
                    .join()
                    .is_err()
            );
            assert!(crossterm::terminal::is_raw_mode_enabled().unwrap());
        }
        if mode == "flood" {
            let line = "界".repeat(2000);
            for i in 0..1000 {
                eprintln!("FLOOD_{i} {line}");
            }
            let until = Instant::now() + Duration::from_secs(3);
            loop {
                if let Some(current) = session.output.snapshot(&mut version) {
                    snapshot = current;
                }
                if snapshot.evicted > 0 {
                    break;
                }
                assert!(Instant::now() < until);
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(snapshot.lines.len() <= 256);
        }
        if mode == "inherited" {
            let mut holder = Command::new("sh")
                .args(["-c", "exec sleep 10"])
                .spawn()
                .unwrap();
            let start = Instant::now();
            drop(session);
            let elapsed = start.elapsed();
            holder.kill().unwrap();
            holder.wait().unwrap();
            assert!(
                elapsed < Duration::from_secs(1),
                "capture must not wait for inherited pipe EOF"
            );
        } else {
            drop(session);
        }
        Ok(())
    });
    if mode == "panic" {
        assert!(result.is_err());
    } else if mode == "error" {
        assert!(
            result
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("CONTROLLED_UI_ERROR")
        );
    } else {
        result.unwrap().unwrap();
    }
    if mode == "raw" {
        assert!(crossterm::terminal::is_raw_mode_enabled().unwrap());
        disable_raw_mode().unwrap();
    }
    assert!(!crossterm::terminal::is_raw_mode_enabled().unwrap());
    println!("AFTER_TERMINAL_EXIT");
    println!("{{\"post_tui\":true}}");
    eprintln!("AFTER_STDERR");
}

fn run_pty(mode: &str) -> String {
    let mut master = -1;
    let mut slave = -1;
    let size = libc::winsize {
        ws_row: 24,
        ws_col: 100,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null(),
                &size,
            )
        },
        0
    );
    let mut master = unsafe { std::fs::File::from_raw_fd(master) };
    let slave = unsafe { std::fs::File::from_raw_fd(slave) };
    let mut before: libc::termios = unsafe { std::mem::zeroed() };
    assert_eq!(
        unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut before) },
        0
    );
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "tui::terminal_output_tests::terminal_fixture",
            "--ignored",
            "--nocapture",
        ])
        .env("SINOMO_TUI_TERMINAL_FIXTURE", mode)
        .env("SINOMO_OBSERVE_FILTER", "warn")
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave.try_clone().unwrap()));
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 || libc::ioctl(0, libc::TIOCSCTTY, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 8192];
    let mut status = None;
    loop {
        loop {
            match master.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    bytes.extend_from_slice(&chunk[..n]);
                    assert!(bytes.len() < 2 * 1024 * 1024);
                }
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.raw_os_error() == Some(libc::EIO) =>
                {
                    break;
                }
                Err(e) => panic!("PTY read: {e}"),
            }
        }
        if status.is_some() {
            break;
        }
        status = child.try_wait().unwrap();
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("PTY fixture hung: {mode}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = String::from_utf8_lossy(&bytes).into_owned();
    assert!(status.unwrap().success(), "{mode}: {output}");
    let mut after: libc::termios = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut after) }, 0);
    assert_eq!(
        (
            before.c_iflag,
            before.c_oflag,
            before.c_cflag,
            before.c_lflag,
            before.c_cc
        ),
        (
            after.c_iflag,
            after.c_oflag,
            after.c_cflag,
            after.c_lflag,
            after.c_cc
        ),
        "termios restoration: {mode}"
    );
    assert!(
        output.contains("AFTER_TERMINAL_EXIT")
            && output.contains("{\"post_tui\":true}")
            && output.contains("AFTER_STDERR"),
        "{mode}: {output}"
    );
    for stray in [
        "STRAY_WORKER_WARNING",
        "STRAY_NATIVE_ERROR",
        "STRAY_CHILD_STDOUT",
        "STRAY_CHILD_STDERR",
        "STRUCTURED_WARNING",
        "FLOOD_",
        "CONTROLLED_BACKGROUND_PANIC",
    ] {
        assert!(
            !output.contains(stray),
            "diagnostic corrupted terminal: {mode}: {output}"
        );
    }
    output
}

#[test]
fn terminal_owns_stdout_and_stderr_in_a_real_pty() {
    assert!(run_pty("").contains("OWNED_UI_FRAME"));
}

#[test]
fn terminal_cleanup_preserves_output_and_modes_on_failure_and_panic() {
    for mode in [
        "error",
        "init-error",
        "panic",
        "background-panic",
        "raw",
        "inherited",
        "flood",
        "external-subscriber",
    ] {
        let output = run_pty(mode);
        if mode == "panic" {
            let restored = output.find("\x1b[?1049l").unwrap();
            assert!(
                restored < output.find("CONTROLLED_UI_PANIC").unwrap(),
                "restore before panic hook prints"
            );
        }
    }
}

#[test]
fn release_abort_policy_restores_for_fatal_workers_without_closing_unwind_workers() {
    let dir = sinomo_testdir::TestDir::new("tui-panic-policy");
    let source = dir.join("probe.rs");
    let implementation = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tui/panic_restore.rs");
    // Compile the actual panic dispatcher under BOTH strategies. Test-profile
    // catch_unwind alone cannot qualify the release profile's panic=abort path.
    fs::write(
        &source,
        format!(
            r#"
#[path = {implementation:?}] mod panic_restore;
use std::io::Write;
use std::sync::Arc;
struct Restore(std::fs::File);
impl panic_restore::PanicRestore for Restore {{
    fn restore_terminal(&self) {{ (&self.0).write_all(b"restored").unwrap(); }}
}}
fn main() {{
    let args: Vec<_> = std::env::args().collect();
    let restore = Arc::new(Restore(std::fs::File::create(&args[2]).unwrap()));
    let _guard = panic_restore::install(restore.clone());
    if args[1] == "worker" {{
        assert!(std::thread::spawn(|| panic!("original worker panic")).join().is_err());
    }} else {{
        assert!(std::panic::catch_unwind(|| panic!("original foreground panic")).is_err());
    }}
}}
"#
        ),
    )
    .unwrap();
    for strategy in ["abort", "unwind"] {
        let binary = dir.join(strategy);
        let output = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
            .args(["--edition=2024", "-C", &format!("panic={strategy}")])
            .arg(&source)
            .arg("-o")
            .arg(&binary)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        for origin in ["worker", "foreground"] {
            let marker = dir.join(format!("{strategy}-{origin}"));
            let mut command = Command::new(&binary);
            command.args([origin]).arg(&marker);
            unsafe {
                command.pre_exec(|| {
                    let limit = libc::rlimit {
                        rlim_cur: 0,
                        rlim_max: 0,
                    };
                    if libc::setrlimit(libc::RLIMIT_CORE, &limit) == -1 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let output = command.output().unwrap();
            assert_eq!(output.status.success(), strategy == "unwind", "{output:?}");
            if strategy == "abort" {
                use std::os::unix::process::ExitStatusExt;
                assert_eq!(output.status.signal(), Some(libc::SIGABRT));
            }
            assert!(String::from_utf8_lossy(&output.stderr).contains("original"));
            let restored = fs::read(&marker).unwrap();
            assert_eq!(
                restored == b"restored",
                strategy == "abort" || origin == "foreground"
            );
        }
    }
}
