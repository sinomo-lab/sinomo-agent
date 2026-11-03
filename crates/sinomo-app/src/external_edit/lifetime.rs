//! One durable ticket per launch. A missing monitor can only leave a ticket
//! outstanding. Positive completion retires that ticket, never another open.
use super::*;
use std::{
    collections::HashMap,
    ffi::OsString,
    io::{BufRead, Write},
    process::{Child, Stdio},
    sync::OnceLock,
    time::Instant,
};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

const HANDOFF: Duration = Duration::from_secs(2);
const UNKNOWN: &str = "unknown";
fn tickets(dir: &Path) -> PathBuf {
    dir.join("opens")
}
fn ticket(dir: &Path) -> Result<PathBuf> {
    private_dir(&tickets(dir))?;
    let path = tickets(dir).join(id());
    write(&path, boot().unwrap_or(UNKNOWN).as_bytes())?;
    Ok(path)
}
fn boot() -> Option<&'static str> {
    static BOOT: OnceLock<Option<String>> = OnceLock::new();
    BOOT.get_or_init(platform_boot).as_deref()
}
#[cfg(windows)]
fn platform_boot() -> Option<String> {
    // SYSTEM_BOOT_ENVIRONMENT_INFORMATION begins with the per-boot GUID.
    #[link(name = "ntdll")]
    unsafe extern "system" {
        fn NtQuerySystemInformation(
            class: u32,
            buffer: *mut std::ffi::c_void,
            length: u32,
            used: *mut u32,
        ) -> i32;
    }
    #[repr(C)]
    struct Boot {
        guid: [u8; 16],
        firmware: u32,
        flags: u64,
    }
    let mut b = Boot {
        guid: [0; 16],
        firmware: 0,
        flags: 0,
    };
    let mut used = 0;
    let status = unsafe {
        NtQuerySystemInformation(
            90,
            (&mut b as *mut Boot).cast(),
            std::mem::size_of::<Boot>() as u32,
            &mut used,
        )
    };
    (status >= 0 && used >= 16 && b.guid != [0; 16])
        .then(|| b.guid.iter().map(|n| format!("{n:02x}")).collect())
}
#[cfg(any(target_os = "linux", target_os = "android"))]
fn platform_boot() -> Option<String> {
    normalized_boot(&fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?)
}
#[cfg(target_os = "macos")]
fn platform_boot() -> Option<String> {
    let out = Command::new("/usr/sbin/sysctl")
        .args(["-n", "kern.bootsessionuuid"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    normalized_boot(std::str::from_utf8(&out.stdout).ok()?)
}
#[cfg(not(any(
    windows,
    target_os = "linux",
    target_os = "macos",
    target_os = "android"
)))]
fn platform_boot() -> Option<String> {
    None
}
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
fn normalized_boot(s: &str) -> Option<String> {
    let v = s.trim().replace('-', "").to_ascii_lowercase();
    valid_boot(&v).then_some(v)
}
fn valid_boot(s: &str) -> bool {
    s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit())
}
pub(super) fn ended(dir: &Path, current: Option<&str>) -> Result<bool> {
    if !tickets(dir).exists() {
        return Ok(false);
    }
    checked_dir(&tickets(dir))?;
    let mut seen = false;
    for item in fs::read_dir(tickets(dir)).map_err(err)? {
        let path = item.map_err(err)?.path();
        seen = true;
        if path.extension().is_some_and(|ext| ext == "closed") {
            continue;
        }
        let bytes = read(&path, 64)?;
        let saved = std::str::from_utf8(&bytes).map_err(err)?;
        if !valid_boot(saved) || !current.is_some_and(|now| valid_boot(now) && now != saved) {
            return Ok(false);
        }
    }
    Ok(seen)
}
pub(super) fn lock(dir: &Path) -> Result<fs::File> {
    let f = durable::open_file_for_append(&dir.join("prepare.lock"))
        .map_err(err)?
        .file;
    f.lock_exclusive().map_err(err)?;
    Ok(f)
}
pub(super) fn initialize(dir: &Path) -> Result<()> {
    let _lock = lock(dir)?;
    initialize_locked(dir)
}

pub(super) fn initialize_locked(dir: &Path) -> Result<()> {
    // Existing sessions initially have no evidence of an editor exit.
    if !tickets(dir).exists() || fs::read_dir(tickets(dir)).map_err(err)?.next().is_none() {
        ticket(dir)?;
    }
    if let Some(now) = boot() {
        for entry in fs::read_dir(tickets(dir)).map_err(err)? {
            let path = entry.map_err(err)?.path();
            if path.extension().is_none() && read(&path, 64)? == UNKNOWN.as_bytes() {
                write(&path, now.as_bytes())?;
            }
        }
    }
    Ok(())
}
pub(super) fn clear(dir: &Path) -> Result<()> {
    if !tickets(dir).exists() {
        return Ok(());
    }
    checked_dir(&tickets(dir))?;
    for entry in fs::read_dir(tickets(dir)).map_err(err)? {
        durable::remove_file_if_exists_and_sync_parent(&entry.map_err(err)?.path()).map_err(err)?;
    }
    Ok(())
}
pub(super) fn open_locked(data: &SinomoDataDir, dir: &Path, key: &str) -> Result<()> {
    let path = ticket(dir)?;
    spawn_helper(data, key)?;
    let mut cmd = Command::new(std::env::current_exe().map_err(err)?);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000);
    }
    let mut child = cmd
        .arg("--external-edit-launch")
        .arg(key)
        .arg(path.file_name().unwrap())
        .env("SINOMO_DATA_DIR", data.root())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(err)?;
    let mut reply = String::new();
    std::io::BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut reply)
        .map_err(err)?;
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    if reply.trim() == "OK" {
        Ok(())
    } else {
        Err(format!("External editor launch failed: {}", reply.trim()))
    }
}

enum Process {
    Child(Child),
    #[cfg(windows)]
    Shell(std::os::windows::io::OwnedHandle),
}
impl Process {
    fn pid(&self) -> u32 {
        match self {
            Self::Child(c) => c.id(),
            #[cfg(windows)]
            Self::Shell(h) => {
                use std::os::windows::io::AsRawHandle;
                unsafe { windows_sys::Win32::System::Threading::GetProcessId(h.as_raw_handle()) }
            }
        }
    }
    fn exited(&mut self) -> Result<Option<bool>> {
        match self {
            Self::Child(c) => c.try_wait().map(|o| o.map(|s| s.success())).map_err(err),
            #[cfg(windows)]
            Self::Shell(h) => {
                use std::os::windows::io::AsRawHandle;
                use windows_sys::Win32::System::Threading::{
                    GetExitCodeProcess, WaitForSingleObject,
                };
                let mut code = 0;
                match unsafe { WaitForSingleObject(h.as_raw_handle(), 0) } {
                    258 => Ok(None),
                    0 if unsafe { GetExitCodeProcess(h.as_raw_handle(), &mut code) } != 0 => {
                        Ok(Some(code == 0))
                    }
                    _ => Err(err(std::io::Error::last_os_error())),
                }
            }
        }
    }
}
enum Launch {
    Wait(Process),
    Ordinary(Process),
    Ambiguous,
}
fn start(cmd: &mut Command, path: &Path, wait: bool) -> Result<Launch> {
    let child = cmd
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(err)?;
    Ok(if wait {
        Launch::Wait(Process::Child(child))
    } else {
        Launch::Ordinary(Process::Child(child))
    })
}
#[cfg(windows)]
fn vscode_cli(program: &Path, stem: &str) -> Result<Command> {
    let (exe, wrapper) = match stem {
        "code-insiders" | "code - insiders" => ("Code - Insiders.exe", "code-insiders.cmd"),
        "codium" | "vscodium" => ("VSCodium.exe", "codium.cmd"),
        _ => ("Code.exe", "code.cmd"),
    };
    let selected = if program.is_absolute() || program.components().count() > 1 {
        program.canonicalize().map_err(err)?
    } else {
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .filter(|dir| dir.is_absolute())
            .flat_map(|dir| [dir.join(wrapper), dir.join(exe)])
            .find(|file| file.is_file())
            .ok_or("Editor CLI is unavailable on PATH")?
    };
    let parent = selected.parent().ok_or("Invalid editor path")?;
    let root = if parent
        .file_name()
        .is_some_and(|n| n.eq_ignore_ascii_case("bin"))
    {
        parent.parent().ok_or("Invalid editor installation")?
    } else {
        parent
    };
    let cli = if let Ok(script) = fs::read_to_string(root.join("bin").join(wrapper)) {
        // Read the vendor launcher's active version directory as data. Never
        // pass a document name to the batch interpreter.
        script
            .split('"')
            .filter_map(|part| part.strip_prefix(r"%~dp0..\"))
            .map(Path::new)
            .find(|rel| {
                rel.ends_with(r"resources\app\out\cli.js")
                    && rel
                        .components()
                        .all(|p| matches!(p, std::path::Component::Normal(_)))
            })
            .map(|rel| root.join(rel))
            .unwrap_or_else(|| root.join("resources/app/out/cli.js"))
    } else {
        root.join("resources/app/out/cli.js")
    };
    if !cli.is_file() {
        return Err("Cannot locate the installed editor CLI".into());
    }
    let mut cmd = Command::new(root.join(exe));
    cmd.env_remove("VSCODE_DEV")
        .env("ELECTRON_RUN_AS_NODE", "1")
        .arg(cli);
    Ok(cmd)
}
fn editor(program: &Path, path: &Path) -> Result<Launch> {
    let stem = program
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_ascii_lowercase();
    let mut cmd = Command::new(program);
    let wait = match stem.as_str() {
        "code" | "code-insiders" | "code - insiders" | "codium" | "vscodium" => {
            #[cfg(windows)]
            {
                cmd = vscode_cli(program, &stem)?;
            }
            cmd.arg("--wait");
            true
        }
        "subl" => {
            cmd.arg("--wait");
            true
        }
        "sublime_text" => {
            let cli = program.with_file_name(format!("subl{}", std::env::consts::EXE_SUFFIX));
            if cli.is_file() {
                cmd = Command::new(cli);
                cmd.arg("--wait");
                true
            } else {
                false
            }
        }
        _ => false,
    };
    if cfg!(windows)
        && !wait
        && program
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("cmd") || e.eq_ignore_ascii_case("bat"))
    {
        return Err("Choose an editor executable".into());
    }
    start(&mut cmd, path, wait)
}
#[cfg(windows)]
fn default_launch(path: &Path) -> Result<Launch> {
    use std::os::windows::{
        ffi::{OsStrExt, OsStringExt},
        io::{FromRawHandle, OwnedHandle},
    };
    use windows_sys::Win32::System::Com::{
        COINIT_APARTMENTTHREADED, CoInitializeEx, CoUninitialize,
    };
    use windows_sys::Win32::UI::Shell::*;
    struct Apartment;
    impl Drop for Apartment {
        fn drop(&mut self) {
            unsafe { CoUninitialize() }
        }
    }
    let hr = unsafe { CoInitializeEx(std::ptr::null(), COINIT_APARTMENTTHREADED as u32) };
    if hr < 0 {
        return Err(format!("Cannot initialize editor activation ({hr:#x})"));
    }
    let _apartment = Apartment;
    let wide = |s: &std::ffi::OsStr| s.encode_wide().chain(Some(0)).collect::<Vec<_>>();
    let verb = wide("edit".as_ref());
    let file = wide(path.as_os_str());
    if let Some(ext) = path.extension() {
        let mut extension = OsString::from(".");
        extension.push(ext);
        for association in ["edit", "open"] {
            let association = wide(association.as_ref());
            let mut buf = vec![0u16; 32768];
            let mut len = buf.len() as u32;
            if unsafe {
                AssocQueryStringW(
                    0,
                    ASSOCSTR_EXECUTABLE,
                    wide(&extension).as_ptr(),
                    association.as_ptr(),
                    buf.as_mut_ptr(),
                    &mut len,
                )
            } == 0
                && len > 1
            {
                let p = PathBuf::from(OsString::from_wide(&buf[..len as usize - 1]));
                if p.file_stem().is_some_and(|s| {
                    [
                        "code",
                        "code - insiders",
                        "codium",
                        "vscodium",
                        "sublime_text",
                    ]
                    .iter()
                    .any(|n| s.eq_ignore_ascii_case(n))
                }) {
                    if let Ok(launched) = editor(&p, path) {
                        return Ok(launched);
                    }
                }
            }
        }
    }
    let mut info = SHELLEXECUTEINFOW::default();
    info.cbSize = std::mem::size_of_val(&info) as u32;
    info.fMask = SEE_MASK_NOCLOSEPROCESS | SEE_MASK_FLAG_NO_UI | SEE_MASK_NOASYNC;
    info.lpVerb = verb.as_ptr();
    info.lpFile = file.as_ptr();
    info.nShow = 1;
    if unsafe { ShellExecuteExW(&mut info) } != 0 {
        return if info.hProcess.is_null() {
            Ok(Launch::Ambiguous)
        } else {
            Ok(Launch::Ordinary(Process::Shell(unsafe {
                OwnedHandle::from_raw_handle(info.hProcess)
            })))
        };
    }
    super::launch(path, false)?;
    Ok(Launch::Ambiguous)
}
#[cfg(not(windows))]
fn default_launch(path: &Path) -> Result<Launch> {
    #[cfg(target_os = "linux")]
    if let Ok(mime) = Command::new("xdg-mime")
        .args(["query", "filetype"])
        .arg(path)
        .output()
        && mime.status.success()
        && let Ok(app) = Command::new("xdg-mime")
            .args([
                "query",
                "default",
                String::from_utf8_lossy(&mime.stdout).trim(),
            ])
            .output()
        && app.status.success()
    {
        let name = String::from_utf8_lossy(&app.stdout);
        let cli = match name.trim() {
            "code.desktop" => "code",
            "code-insiders.desktop" => "code-insiders",
            "codium.desktop" | "vscodium.desktop" => "codium",
            "sublime_text.desktop" => "subl",
            _ => "",
        };
        if !cli.is_empty()
            && let Ok(launched) = editor(Path::new(cli), path)
        {
            return Ok(launched);
        }
    }
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut c = Command::new("/usr/bin/open");
        c.arg("-W");
        c
    };
    #[cfg(not(target_os = "macos"))]
    let mut command = Command::new("xdg-open");
    start(&mut command, path, false)
}

fn refresh(system: &mut System) -> bool {
    system.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing().without_tasks(),
    ) > 0
}
fn ordinary(mut process: Process, ticket: &Path, mut system: System) -> Result<bool> {
    if process.pid() == 0 {
        return Ok(false);
    }
    let root = Pid::from_u32(process.pid());
    let start = Instant::now();
    let mut known: HashMap<Pid, u64> = HashMap::new();
    // A launcher can already be gone at the first snapshot while its child
    // still reports the launch PID as its parent.
    let earliest = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_sub(2);
    known.insert(root, earliest);
    let mut root_ended = false;
    let mut child_outlived_launcher = false;
    let mut quick = false;
    let mut same_image = false;
    let mut broker =
        cfg!(target_os = "linux") && std::env::var_os("SINOMO_EXTERNAL_EDITOR").is_none();
    let previous: Vec<_> = system
        .processes()
        .iter()
        .map(|(pid, p)| (*pid, p.start_time(), p.name().to_owned()))
        .collect();
    while ticket.exists() {
        if !refresh(&mut system) {
            return Ok(false);
        }
        if !root_ended && let Some(p) = system.process(root) {
            known.insert(root, p.start_time());
            broker |= [
                "rundll32.exe",
                "openwith.exe",
                "explorer.exe",
                "xdg-open",
                "gio",
            ]
            .iter()
            .any(|name| p.name().eq_ignore_ascii_case(name));
            same_image |= previous.iter().any(|(pid, t, n)| {
                *pid != root
                    && n == p.name()
                    && system
                        .process(*pid)
                        .is_some_and(|prior| prior.start_time() == *t)
            });
        }
        loop {
            let mut found = false;
            for (pid, p) in system.processes() {
                if known.contains_key(pid) {
                    continue;
                }
                if let Some(parent) = p.parent()
                    && known.get(&parent).is_some_and(|birth| {
                        p.start_time() >= *birth
                            && system
                                .process(parent)
                                .is_none_or(|pp| pp.start_time() == *birth)
                    })
                {
                    known.insert(*pid, p.start_time());
                    found = true;
                }
            }
            if !found {
                break;
            }
        }
        if !root_ended && let Some(success) = process.exited()? {
            if !success {
                return Ok(false);
            }
            root_ended = true;
            quick = start.elapsed() < HANDOFF;
        }
        let children_alive = known.iter().any(|(pid, birth)| {
            *pid != root
                && system.process(*pid).is_some_and(|p| {
                    p.start_time() == *birth && p.status() != sysinfo::ProcessStatus::Zombie
                })
        });
        child_outlived_launcher |= root_ended && children_alive;
        if root_ended && !children_alive {
            return Ok((!quick && (!broker || child_outlived_launcher))
                || (child_outlived_launcher && !same_image && start.elapsed() >= HANDOFF));
        }
        std::thread::sleep(if start.elapsed() < HANDOFF {
            Duration::from_millis(200)
        } else {
            Duration::from_secs(2)
        });
    }
    Ok(false)
}
pub fn monitor() -> Result<()> {
    let key = std::env::args().nth(2).ok_or("Missing edit session")?;
    let token = std::env::args().nth(3).ok_or("Missing launch ticket")?;
    if !valid_id(&key) || token.len() != 26 || !token.bytes().all(|c| c.is_ascii_alphanumeric()) {
        return Err("Invalid external launch ticket".into());
    }
    let data = SinomoDataDir::from_env_or_home().map_err(err)?;
    let dir = root(&data).join(key);
    let t = tickets(&dir).join(token);
    if !t.is_file() {
        return Err("Launch ticket expired".into());
    }
    let s = Session::load(&dir)?;
    if s.finished {
        return Err("External session is finished".into());
    }
    let working = s.working(&dir);
    checked_dir(&dir.join("working"))?;
    read(&working, LIMIT)?;
    let mut sys = System::new();
    refresh(&mut sys);
    let launch = if let Some(program) =
        std::env::var_os("SINOMO_EXTERNAL_EDITOR").filter(|p| !p.is_empty())
    {
        editor(Path::new(&program), &working)?
    } else {
        default_launch(&working)?
    };
    {
        let mut out = std::io::stdout().lock();
        writeln!(out, "OK").map_err(err)?;
        out.flush().map_err(err)?;
    }
    let done = match launch {
        Launch::Ambiguous => false,
        Launch::Wait(mut process) => loop {
            if !t.exists() {
                break false;
            }
            if let Some(ok) = process.exited()? {
                break ok;
            }
            std::thread::sleep(Duration::from_millis(400));
        },
        Launch::Ordinary(p) => ordinary(p, &t, sys)?,
    };
    if t.exists() {
        let _lock = lock(&dir)?;
        // Keep a positive close record. An empty or partially initialized
        // directory can never be mistaken for proof that all documents closed.
        if let Err(error) = fs::rename(
            &t,
            t.with_extension(if done { "closed" } else { "ambiguous" }),
        ) && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(err(error));
        }
    }
    Ok(())
}
pub(super) fn ambiguous(dir: &Path) -> bool {
    fs::read_dir(tickets(dir)).ok().is_some_and(|entries| {
        entries.flatten().any(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|ext| ext == "ambiguous")
        })
    })
}

pub(super) fn automatic(dir: &Path) -> Result<bool> {
    ended(dir, boot())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn editor_lifetime_all_opens_must_close_or_belong_to_an_older_boot() {
        let dir = std::env::temp_dir().join(format!("life-{}", id()));
        fs::create_dir_all(tickets(&dir)).unwrap();
        let before = "11111111111111111111111111111111";
        let now = "22222222222222222222222222222222";
        write(&tickets(&dir).join("first"), before.as_bytes()).unwrap();
        write(&tickets(&dir).join("second"), now.as_bytes()).unwrap();
        assert!(!ended(&dir, Some(now)).unwrap());
        fs::remove_file(tickets(&dir).join("second")).unwrap();
        assert!(!ended(&dir, None).unwrap());
        assert!(ended(&dir, Some(now)).unwrap());
        write(&tickets(&dir).join("first"), UNKNOWN.as_bytes()).unwrap();
        assert!(!ended(&dir, Some(now)).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }
}
