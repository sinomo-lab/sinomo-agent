use super::*;

pub(super) fn available() -> bool {
    let Some(bwrap) = find_host_program("bwrap") else {
        return false;
    };
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")
        .is_ok()
        && Command::new(bwrap)
            .args(["--dev-bind", "/", "/", "--", "/bin/true"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
}

pub(super) fn start_shell(
    store: &LocalSandboxInstanceStore,
    id: &SandboxId,
    sifs_root: &Path,
    script: &str,
    args: &[String],
) -> SandboxNativeExecution {
    let runner = SandboxSimpleNativeCommandRunner::new(SandboxSimpleNativeConfig {
        host_sifs_root: Some(sifs_root.to_owned()),
        ..Default::default()
    });
    let gate = acquire_sandbox_native_launch_lease(store, id).expect("native lease");
    let (manifest, _) = store.load(id).unwrap();
    let policy = SandboxPolicy {
        allowed_mount_prefixes: manifest
            .mounts
            .into_iter()
            .map(|m| m.source_sifs_path)
            .collect(),
        allow_read_write_sifs_mounts: true,
        ..Default::default()
    };
    let mut argv = vec!["sh".into(), "-c".into(), script.into(), "mount-test".into()];
    argv.extend_from_slice(args);
    runner
        .start_toolchain_command(
            store,
            id,
            "sh",
            argv,
            None,
            10_000,
            &NodeId::from_legacy_named("native-mount-test").unwrap(),
            &policy,
            None,
            Default::default(),
            gate,
            SandboxCommandId::generate(),
            Default::default(),
        )
        .expect("launch native shell with declared mounts")
}
#[test]
fn crashed_native_host_releases_routing_mount_without_touching_host_siblings() {
    use sinomo_sandbox::{read_linux_sandbox_mounts, recover_sandbox_local_execution_journal};
    const CHILD: &str = "SINOMO_TEST_CRASH_MOUNT_ROUTER";
    let id = SandboxId::from_legacy_named("mount-view-crash").unwrap();
    if let Some(root) = env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let store = LocalSandboxInstanceStore::new(root.join("runtime"));
        let job = start_shell(
            &store,
            &id,
            &root,
            r#"set -e; printf preserved > ../sibling; sleep 30 &"#,
            &[],
        );
        let receipt = job.wait().unwrap();
        assert_eq!(receipt.response.exit_status, 0);
        fs::write(root.join("receipt"), receipt.response.command_id.as_str()).unwrap();
        std::thread::park();
        return;
    }
    if !available() {
        return;
    }
    let dir = sinomo_testdir::TestDir::new("native-route-recovery");
    let root = dir.path();
    let host_parent = root.join("host");
    let guest = host_parent.join("missing");
    fs::create_dir(&host_parent).unwrap();
    fs::create_dir(root.join("sifs")).unwrap();
    let store = LocalSandboxInstanceStore::new(root.join("runtime"));
    let (mut m, layout) = store
        .create(
            id.clone(),
            SandboxToolchainId::from_legacy_named("simple-native").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
    m.cwd = SandboxPath::new(guest.to_string_lossy()).unwrap();
    m.mounts = vec![SandboxMount {
        mount_path: m.cwd.clone(),
        source_sifs_path: "/sifs".into(),
        mode: SandboxMountMode::ReadWrite,
    }];
    store.save_manifest(&m).unwrap();
    let mut host=Command::new(env::current_exe().unwrap()).args(["--exact","tests::mount_view::crashed_native_host_releases_routing_mount_without_touching_host_siblings"]).env(CHILD,root).spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(8);
    while !root.join("receipt").exists()
        && host.try_wait().unwrap().is_none()
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    host.kill().unwrap();
    host.wait().unwrap();
    let command = fs::read_to_string(root.join("receipt")).unwrap();
    let mount = layout
        .metadata
        .join("native-commands")
        .join(command)
        .join("mount-view/router");
    assert!(
        read_linux_sandbox_mounts()
            .unwrap()
            .iter()
            .any(|m| m.target == mount),
        "probe must exercise a disconnected router"
    );
    assert!(
        recover_sandbox_local_execution_journal(&store, &id, OffsetDateTime::now_utc()).unwrap()
    );
    assert!(
        !read_linux_sandbox_mounts()
            .unwrap()
            .iter()
            .any(|m| m.target == mount)
    );
    assert_eq!(fs::read(host_parent.join("sibling")).unwrap(), b"preserved");
    assert!(!guest.exists());
}

/// Also exercises the real logical home on hosts where its absence is guaranteed
/// by the host directory permissions. Writable-host cases use a disposable parent.
#[test]
fn absent_home_agent_mounts_without_host_directory() {
    if Path::new("/home/agent").symlink_metadata().is_ok()
        || unsafe { libc::access(c"/home".as_ptr(), libc::W_OK) } == 0
        || !available()
    {
        return;
    }
    let (dir, store, id) = create_store();
    fs::create_dir(dir.path().join("sifs-home")).unwrap();
    let (mut m, _) = store.load(&id).unwrap();
    m.cwd = SandboxPath::new("/home/agent").unwrap();
    m.mounts = vec![SandboxMount {
        mount_path: m.cwd.clone(),
        source_sifs_path: "/sifs-home".into(),
        mode: SandboxMountMode::ReadWrite,
    }];
    m.env.insert("HOME".into(), "/home/agent".into());
    let root_guest = format!("/sinomo-guest-{}", id.as_str());
    if unsafe { libc::access(c"/".as_ptr(), libc::W_OK) } != 0 {
        m.mounts.push(SandboxMount {
            mount_path: SandboxPath::new(&root_guest).unwrap(),
            source_sifs_path: "/sifs-home".into(),
            mode: SandboxMountMode::ReadOnly,
        });
        m.env.insert("CHECK_ROOT_MOUNT".into(), root_guest.clone());
    }
    store.save_manifest(&m).unwrap();
    let job = start_shell(
        &store,
        &id,
        dir.path(),
        r#"set -e; printf probe >/dev/null; test -e /proc/self/fd/1; test "$(pwd -P)" = /home/agent; ls -1 /home | grep -Fx agent; printf home > "$HOME/proof"; if test -n "${CHECK_ROOT_MOUNT:-}"; then test "$(cat "$CHECK_ROOT_MOUNT/proof")" = home; fi"#,
        &[],
    );
    let result = job.wait().unwrap();
    assert_eq!(
        result.response.exit_status,
        0,
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        fs::read(dir.path().join("sifs-home/proof")).unwrap(),
        b"home"
    );
    assert!(!Path::new(&root_guest).exists());
    assert_eq!(
        fs::symlink_metadata("/home/agent").unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
}

#[test]
fn nested_guest_only_mounts_preserve_read_only_parent_and_live_writable_child() {
    if !available() {
        return;
    }
    let (dir, store, id) = create_store();
    let source = dir.path().join("sifs");
    let base = source.join("base");
    let work = source.join("work");
    let host = dir.path().join("host");
    for p in [&base, &work, &host] {
        fs::create_dir_all(p).unwrap();
    }
    fs::write(base.join("origin"), b"base").unwrap();
    let guest = host.join("missing/home");
    let (mut m, _) = store.load(&id).unwrap();
    m.cwd = SandboxPath::new(guest.to_string_lossy()).unwrap();
    m.mounts = vec![
        SandboxMount {
            source_sifs_path: "/work".into(),
            mount_path: SandboxPath::new(guest.join("nested/work").to_string_lossy()).unwrap(),
            mode: SandboxMountMode::ReadWrite,
        },
        SandboxMount {
            source_sifs_path: "/base".into(),
            mount_path: m.cwd.clone(),
            mode: SandboxMountMode::ReadOnly,
        },
    ];
    store.save_manifest(&m).unwrap();
    let exec = start_shell(
        &store,
        &id,
        &source,
        r#"set -eu
test "$(cat origin)" = base
if touch denied; then exit 2; fi
printf workspace > nested/work/data
printf write-through > ../../sibling
printf ancestor > ../created"#,
        &[],
    );
    let result = exec.wait().unwrap();
    assert_eq!(
        result.response.exit_status,
        0,
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(fs::read(work.join("data")).unwrap(), b"workspace");
    assert_eq!(fs::read(host.join("sibling")).unwrap(), b"write-through");
    assert_eq!(fs::read(host.join("missing/created")).unwrap(), b"ancestor");
    assert!(!guest.exists());
    assert!(!base.join("nested").exists());
    assert!(!base.join("denied").exists());
}

#[test]
fn missing_mount_routes_live_parent_names_and_unix_sockets() {
    use std::os::unix::net::{UnixListener, UnixStream};

    const PEER: &str = "SINOMO_TEST_LIVE_PARENT_PEER";
    let deadline = Instant::now() + Duration::from_secs(8);
    let wait = |path: &Path| {
        while !path.exists() {
            assert!(Instant::now() < deadline, "waiting for {}", path.display());
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    if let Some(parent) = env::var_os(PEER) {
        let parent = PathBuf::from(parent);
        if env::var_os("SINOMO_TEST_LISTEN_IN_LATER_EXECUTION").is_some() {
            let listener = UnixListener::bind(parent.join("later.sock")).unwrap();
            listener.set_nonblocking(true).unwrap();
            fs::write("later-listening", b"").unwrap();
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buf = [0; 4];
                        stream.read_exact(&mut buf).unwrap();
                        assert_eq!(&buf, b"peer");
                        break;
                    }
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(5))
                    }
                    Err(e) => panic!("accept earlier sandbox execution: {e}"),
                }
            }
            fs::remove_file(parent.join("later.sock")).unwrap();
            return;
        }
        let names = || {
            fs::read_dir(&parent)
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect::<std::collections::BTreeSet<_>>()
        };
        assert!(names().contains(std::ffi::OsStr::new("agent")));
        assert!(!parent.join("late").exists());
        let mut old = fs::File::open(parent.join("existing")).unwrap();
        fs::write("ready", b"").unwrap();
        wait(Path::new("host-writes-done"));
        UnixStream::connect(parent.join("later.sock"))
            .unwrap()
            .write_all(b"peer")
            .unwrap();
        assert!(names().contains(std::ffi::OsStr::new("late")));
        assert!(!names().contains(std::ffi::OsStr::new("gone")));
        assert_eq!(fs::read(parent.join("existing")).unwrap(), b"replacement");
        let mut bytes = Vec::new();
        old.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"old");
        fs::write(parent.join("late"), b"guest edit").unwrap();
        fs::rename(parent.join("late"), parent.join("renamed")).unwrap();
        fs::create_dir(parent.join("new-dir")).unwrap();
        fs::write(parent.join("new-dir/file"), b"created").unwrap();
        fs::remove_file(parent.join("remove-me")).unwrap();
        let fifo = parent.join("guest.fifo");
        assert!(
            Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        UnixStream::connect(parent.join("host.sock"))
            .unwrap()
            .write_all(b"guest")
            .unwrap();
        let listener = UnixListener::bind(parent.join("guest.sock")).unwrap();
        listener.set_nonblocking(true).unwrap();
        fs::write("guest-writes-done", b"").unwrap();
        let mut client = loop {
            match listener.accept() {
                Ok((client, _)) => break client,
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(e) => panic!("accept guest socket: {e}"),
            }
        };
        let mut buf = [0; 4];
        client.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"host");
        fs::remove_file(parent.join("guest.sock")).unwrap();
        let mut pipe = fs::File::open(parent.join("guest.fifo")).unwrap();
        pipe.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"pipe");
        return;
    }
    if !available() {
        return;
    }
    let (dir, store, id) = create_store();
    let real = dir.path().join("host");
    // Some systems restrict AF_UNIX pathnames to 108 bytes.
    let short_dir = tempfile::tempdir().unwrap();
    let parent = short_dir.path();
    fs::create_dir_all(&real).unwrap();
    fs::write(parent.join("existing"), b"old").unwrap();
    fs::write(parent.join("gone"), b"").unwrap();
    fs::write(parent.join("remove-me"), b"").unwrap();
    let (mut manifest, _) = store.load(&id).unwrap();
    manifest
        .env
        .insert(PEER.to_string(), parent.display().to_string());
    let guest = parent.join("agent");
    manifest.cwd = SandboxPath::new(guest.display().to_string()).unwrap();
    manifest.mounts = vec![SandboxMount {
        mount_path: manifest.cwd.clone(),
        source_sifs_path: "/host".into(),
        mode: SandboxMountMode::ReadWrite,
    }];
    store.save_manifest(&manifest).unwrap();
    let exe = env::current_exe().unwrap();
    let peer_args = [
        exe.display().to_string(),
        "--exact".into(),
        "tests::mount_view::missing_mount_routes_live_parent_names_and_unix_sockets".into(),
        "--nocapture".into(),
    ];
    let execution = start_shell(&store, &id, dir.path(), r#"exec "$@""#, &peer_args);
    wait(&real.join("ready"));
    let later = start_shell(
        &store,
        &id,
        dir.path(),
        r#"export SINOMO_TEST_LISTEN_IN_LATER_EXECUTION=1; exec "$@""#,
        &peer_args,
    );
    wait(&real.join("later-listening"));
    fs::remove_file(parent.join("gone")).unwrap();
    fs::write(parent.join("late"), b"host").unwrap();
    fs::write(parent.join("new"), b"replacement").unwrap();
    fs::rename(parent.join("new"), parent.join("existing")).unwrap();
    let server = UnixListener::bind(parent.join("host.sock")).unwrap();
    server.set_nonblocking(true).unwrap();
    fs::write(real.join("host-writes-done"), b"").unwrap();
    wait(&real.join("guest-writes-done"));
    let (mut conn, _) = server.accept().unwrap();
    let mut buf = [0; 5];
    conn.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"guest");
    assert_eq!(fs::read(parent.join("renamed")).unwrap(), b"guest edit");
    assert_eq!(fs::read(parent.join("new-dir/file")).unwrap(), b"created");
    use std::os::unix::fs::FileTypeExt;
    assert!(
        fs::symlink_metadata(parent.join("guest.fifo"))
            .unwrap()
            .file_type()
            .is_fifo()
    );
    assert!(!parent.join("remove-me").exists());
    assert!(!parent.join("late").exists());
    UnixStream::connect(parent.join("guest.sock"))
        .unwrap()
        .write_all(b"host")
        .unwrap();
    use std::os::unix::fs::OpenOptionsExt;
    let mut pipe = loop {
        match fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(parent.join("guest.fifo"))
        {
            Ok(f) => break f,
            Err(e) if e.raw_os_error() == Some(libc::ENXIO) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5))
            }
            Err(e) => panic!("connect guest FIFO: {e}"),
        }
    };
    pipe.write_all(b"pipe").unwrap();
    let outcome = execution.wait().unwrap();
    assert_eq!(
        outcome.response.exit_status,
        0,
        "{}",
        String::from_utf8_lossy(&outcome.stderr)
    );
    let result = later.wait().unwrap();
    assert_eq!(
        result.response.exit_status,
        0,
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(!parent.join("guest.sock").exists());
    assert!(!parent.join("later.sock").exists());
    assert!(!guest.exists());
}
