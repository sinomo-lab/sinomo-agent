use sinomo_core::local::SinomoDataDir;
use std::net::{IpAddr, SocketAddr};
#[cfg(any(target_os = "linux", windows))]
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeNfsMountError {
    pub code: &'static str,
    pub detail: String,
    pub resolution: String,
}

impl std::fmt::Display for NativeNfsMountError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for NativeNfsMountError {}

fn mount_error(
    code: &'static str,
    detail: impl Into<String>,
    resolution: impl Into<String>,
) -> NativeNfsMountError {
    NativeNfsMountError {
        code,
        detail: detail.into(),
        resolution: resolution.into(),
    }
}

pub fn prepare_native_mount_target(
    data_dir: &SinomoDataDir,
    value: &str,
    create_missing: bool,
) -> Result<String, NativeNfsMountError> {
    #[cfg(target_os = "linux")]
    {
        let target = linux_mount_target(data_dir, value, create_missing, true)?;
        Ok(target.display().to_string())
    }
    #[cfg(windows)]
    {
        let _ = (data_dir, create_missing);
        normalize_windows_drive(value)
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = (data_dir, value, create_missing);
        Err(mount_error(
            "unsupported-platform",
            "native NFS mounting is supported only on Linux and Windows",
            "Use `dfs nfs serve` with a platform-specific client, or use FUSE on Linux.",
        ))
    }
}

pub async fn mount_native_nfs_target(
    target: &str,
    address: SocketAddr,
    export_name: &str,
    _read_only: bool,
) -> Result<(), NativeNfsMountError> {
    #[cfg(target_os = "linux")]
    {
        let target = target.to_string();
        let export_name = export_name.to_string();
        tokio::task::spawn_blocking(move || {
            let mount_point = PathBuf::from(&target);
            validate_linux_mount_directory(&mount_point, true)?;
            let source = linux_nfs_source(address.ip(), &export_name);
            let options = linux_nfs_mount_options(address.port(), _read_only);
            let direct = std::process::Command::new("mount.nfs")
                .args(["-o", &options, &source])
                .arg(&mount_point)
                .output();
            let output = match direct {
                Ok(output) => output,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::process::Command::new("mount")
                        .args(["-t", "nfs", "-o", &options, &source])
                        .arg(&mount_point)
                        .output()
                        .map_err(|fallback| {
                            mount_error(
                                "nfs-client-missing",
                                format!(
                                    "`mount.nfs` was not found and `mount -t nfs` could not start: {fallback}"
                                ),
                                linux_client_resolution(),
                            )
                        })?
                }
                Err(error) => {
                    return Err(mount_error(
                        "mount-helper-failed",
                        format!("start Linux NFS mount helper: {error}"),
                        linux_client_resolution(),
                    ));
                }
            };
            if output.status.success() {
                return Ok(());
            }
            let detail = command_output_detail(&output);
            let permission = detail.contains("Operation not permitted")
                || detail.contains("must be superuser")
                || detail.contains("permission denied");
            Err(mount_error(
                if permission {
                    "mount-permission-required"
                } else {
                    "native-mount-failed"
                },
                format!(
                    "Linux NFS client could not mount {source} at {}: {detail}",
                    mount_point.display()
                ),
                if permission {
                    "Grant the daemon service CAP_SYS_ADMIN/root mount authority, then disable and re-enable this mount."
                } else {
                    "Check the NFS client installation and mount target, then disable and re-enable this mount."
                },
            ))
        })
        .await
        .map_err(|error| {
            mount_error(
                "mount-worker-failed",
                format!("Linux NFS mount worker failed: {error}"),
                "Disable and re-enable the mount. If it repeats, inspect the daemon log.",
            )
        })?
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;

        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let target = normalize_windows_drive(target)?;
        if address.ip() != IpAddr::V4(std::net::Ipv4Addr::LOCALHOST) || address.port() != 111 {
            return Err(mount_error(
                "windows-endpoint-incompatible",
                format!("Windows Client for NFS requires 127.0.0.1:111, got {address}"),
                "Set the loopback endpoint to 127.0.0.1:111 and ensure the daemon can bind that privileged port.",
            ));
        }
        let mount_program = windows_nfs_client_program("mount.exe")?;
        let export = format!(r"\\{}\{}", address.ip(), export_name);
        tokio::task::spawn_blocking(move || {
            let output = std::process::Command::new(&mount_program)
                .creation_flags(CREATE_NO_WINDOW)
                .args([
                    "-o",
                    "anon,nolock,mtype=soft,fileaccess=764,casesensitive,rsize=32,wsize=32,timeout=1,retry=2",
                    &export,
                    &target,
                ])
                .output()
                .map_err(|error| {
                    mount_error(
                        "nfs-client-missing",
                        format!("start Windows Client for NFS mount utility: {error}"),
                        "Install the Windows Client for NFS optional feature and restart the daemon.",
                    )
                })?;
            if output.status.success() {
                Ok(())
            } else {
                Err(mount_error(
                    "native-mount-failed",
                    format!(
                        "Windows Client for NFS could not map {export} to {target}: {}",
                        command_output_detail(&output)
                    ),
                    "Check Client for NFS, drive availability, and port 111 authority; then disable and re-enable this mount.",
                ))
            }
        })
        .await
        .map_err(|error| {
            mount_error(
                "mount-worker-failed",
                format!("Windows NFS mount worker failed: {error}"),
                "Disable and re-enable the mount. If it repeats, inspect the daemon log.",
            )
        })?
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = (target, address, export_name, _read_only);
        Err(mount_error(
            "unsupported-platform",
            "native NFS mounting is supported only on Linux and Windows",
            "Use `dfs nfs serve` with a platform-specific client.",
        ))
    }
}

pub async fn unmount_native_nfs_target(target: &str) -> Result<(), NativeNfsMountError> {
    #[cfg(target_os = "linux")]
    {
        let target = target.to_string();
        tokio::task::spawn_blocking(move || {
            let mount_point = PathBuf::from(&target);
            validate_linux_mount_directory(&mount_point, false)?;
            let mounted = linux_mount_info_for_path(&mount_point)?.ok_or_else(|| {
                mount_error(
                    "mount-disappeared",
                    format!("NFS mount disappeared before unmount: {}", mount_point.display()),
                    "Refresh the module status. No host mount remains at this target.",
                )
            })?;
            if mounted.file_system_type != "nfs" || !linux_source_is_loopback(&mounted.source) {
                return Err(mount_error(
                    "unsafe-unmount-target",
                    format!(
                        "refusing non-loopback NFS target {} (type={}, source={})",
                        mount_point.display(), mounted.file_system_type, mounted.source
                    ),
                    "Choose the exact loopback NFS mount created by this module.",
                ));
            }
            let output = std::process::Command::new("umount")
                .arg(&mount_point)
                .output()
                .map_err(|error| {
                    mount_error(
                        "unmount-helper-failed",
                        format!("start Linux unmount utility: {error}"),
                        "Install util-linux and ensure the daemon retains mount authority.",
                    )
                })?;
            if output.status.success() {
                Ok(())
            } else {
                Err(mount_error(
                    "native-unmount-failed",
                    format!(
                        "Linux could not unmount {}: {}",
                        mount_point.display(),
                        command_output_detail(&output)
                    ),
                    "Close processes using the mount, restore mount authority, and retry disabling it.",
                ))
            }
        })
        .await
        .map_err(|error| {
            mount_error(
                "unmount-worker-failed",
                format!("Linux NFS unmount worker failed: {error}"),
                "Retry disabling the mount and inspect the daemon log if it repeats.",
            )
        })?
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;

        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let target = normalize_windows_drive(target)?;
        let unmount_program = windows_nfs_client_program("umount.exe")?;
        tokio::task::spawn_blocking(move || {
            let output = std::process::Command::new(&unmount_program)
                .creation_flags(CREATE_NO_WINDOW)
                .arg(&target)
                .output()
                .map_err(|error| {
                    mount_error(
                        "unmount-helper-failed",
                        format!("start Windows Client for NFS unmount utility: {error}"),
                        "Install the Windows Client for NFS optional feature.",
                    )
                })?;
            if output.status.success() {
                Ok(())
            } else {
                Err(mount_error(
                    "native-unmount-failed",
                    format!(
                        "Windows Client for NFS could not unmap {target}: {}",
                        command_output_detail(&output)
                    ),
                    "Close applications using the drive and retry disabling the mount.",
                ))
            }
        })
        .await
        .map_err(|error| {
            mount_error(
                "unmount-worker-failed",
                format!("Windows NFS unmount worker failed: {error}"),
                "Retry disabling the mount and inspect the daemon log if it repeats.",
            )
        })?
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = target;
        Err(mount_error(
            "unsupported-platform",
            "native NFS unmounting is supported only on Linux and Windows",
            "Unmount with the platform-specific client.",
        ))
    }
}

#[cfg(target_os = "linux")]
fn linux_mount_target(
    data_dir: &SinomoDataDir,
    value: &str,
    create_missing: bool,
    require_unmounted: bool,
) -> Result<PathBuf, NativeNfsMountError> {
    if value.is_empty() || value.trim() != value || value.contains('\0') {
        return Err(mount_error(
            "invalid-target",
            "Linux NFS mount target must be non-empty without surrounding whitespace or NUL bytes",
            "Choose an absolute directory or a current-user ~/ path.",
        ));
    }
    let requested = expand_current_user_path(value)?;
    if !requested.is_absolute() {
        return Err(mount_error(
            "invalid-target",
            format!(
                "Linux NFS mount target must be absolute: {}",
                requested.display()
            ),
            "Choose an absolute directory or a current-user ~/ path.",
        ));
    }
    if create_missing && !requested.exists() {
        std::fs::create_dir_all(&requested).map_err(|error| {
            mount_error(
                "mount-target-create-failed",
                format!("create NFS mount target {}: {error}", requested.display()),
                "Choose a writable parent directory or create the target manually.",
            )
        })?;
    }
    let canonical = std::fs::canonicalize(&requested).map_err(|error| {
        mount_error(
            "invalid-target",
            format!("resolve NFS mount target {}: {error}", requested.display()),
            "Create the directory and ensure every path component is accessible.",
        )
    })?;
    if canonical.parent().is_none() {
        return Err(mount_error(
            "unsafe-target",
            "the host filesystem root cannot be used as an NFS mount target",
            "Choose a dedicated empty directory.",
        ));
    }
    let data_root = std::fs::canonicalize(data_dir.root()).unwrap_or_else(|_| data_dir.root());
    if canonical.starts_with(&data_root) || data_root.starts_with(&canonical) {
        return Err(mount_error(
            "unsafe-target",
            format!(
                "NFS mount target {} overlaps Sinomo data directory {}",
                canonical.display(),
                data_root.display()
            ),
            "Choose a mount directory outside the Sinomo data directory.",
        ));
    }
    validate_linux_mount_directory(&canonical, require_unmounted)?;
    Ok(canonical)
}

#[cfg(target_os = "linux")]
fn validate_linux_mount_directory(
    target: &Path,
    require_unmounted: bool,
) -> Result<(), NativeNfsMountError> {
    let metadata = std::fs::symlink_metadata(target).map_err(|error| {
        mount_error(
            "invalid-target",
            format!("inspect NFS mount target {}: {error}", target.display()),
            "Create a dedicated empty directory.",
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(mount_error(
            "unsafe-target",
            format!(
                "NFS mount target must be a real directory: {}",
                target.display()
            ),
            "Choose a dedicated non-symlink directory.",
        ));
    }
    let mounted = linux_mount_info_for_path(target)?;
    if require_unmounted {
        if mounted.is_some() {
            return Err(mount_error(
                "target-already-mounted",
                format!("NFS mount target is already mounted: {}", target.display()),
                "Disable the existing mount or choose a different target.",
            ));
        }
        if std::fs::read_dir(target)
            .map_err(|error| {
                mount_error(
                    "invalid-target",
                    format!("inspect NFS mount target {}: {error}", target.display()),
                    "Ensure the daemon can read the target directory.",
                )
            })?
            .next()
            .transpose()
            .map_err(|error| {
                mount_error(
                    "invalid-target",
                    format!("inspect NFS mount target entry: {error}"),
                    "Ensure the daemon can read the target directory.",
                )
            })?
            .is_some()
        {
            return Err(mount_error(
                "target-not-empty",
                format!("refusing to hide files beneath {}", target.display()),
                "Choose an empty directory or move its existing files elsewhere.",
            ));
        }
    } else if mounted.is_none() {
        return Err(mount_error(
            "mount-disappeared",
            format!("path is not mounted: {}", target.display()),
            "Refresh status; no native unmount is required.",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn expand_current_user_path(value: &str) -> Result<PathBuf, NativeNfsMountError> {
    if value == "~" || value.starts_with("~/") {
        let home = std::env::var_os("HOME").ok_or_else(|| {
            mount_error(
                "home-unavailable",
                "current user home directory is unavailable",
                "Use an absolute mount target.",
            )
        })?;
        return Ok(PathBuf::from(home).join(value.strip_prefix("~/").unwrap_or("")));
    }
    if value.starts_with('~') {
        return Err(mount_error(
            "invalid-target",
            "only the current-user ~/ mount path form is supported",
            "Use an absolute path or ~/path.",
        ));
    }
    Ok(PathBuf::from(value))
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct LinuxMountInfo {
    file_system_type: String,
    source: String,
}

#[cfg(target_os = "linux")]
fn linux_mount_info_for_path(path: &Path) -> Result<Option<LinuxMountInfo>, NativeNfsMountError> {
    let contents = std::fs::read_to_string("/proc/self/mountinfo").map_err(|error| {
        mount_error(
            "mount-topology-unavailable",
            format!("read /proc/self/mountinfo: {error}"),
            "Restore procfs access for the daemon before enabling host mounts.",
        )
    })?;
    Ok(parse_linux_mountinfo_for_path(&contents, path))
}

#[cfg(target_os = "linux")]
fn parse_linux_mountinfo_for_path(contents: &str, path: &Path) -> Option<LinuxMountInfo> {
    contents.lines().find_map(|line| {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        let separator = fields.iter().position(|field| *field == "-")?;
        if separator + 2 >= fields.len()
            || decode_linux_mountinfo_path(fields.get(4)?)?.as_path() != path
        {
            return None;
        }
        Some(LinuxMountInfo {
            file_system_type: fields[separator + 1].to_string(),
            source: fields[separator + 2].to_string(),
        })
    })
}

#[cfg(target_os = "linux")]
fn decode_linux_mountinfo_path(value: &str) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt;

    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            let escape = bytes.get(index + 1..index + 4)?;
            decoded.push(match escape {
                b"040" => b' ',
                b"011" => b'\t',
                b"012" => b'\n',
                b"134" => b'\\',
                _ => return None,
            });
            index += 4;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Some(PathBuf::from(std::ffi::OsString::from_vec(decoded)))
}

#[cfg(target_os = "linux")]
fn linux_nfs_source(address: IpAddr, export_name: &str) -> String {
    match address {
        IpAddr::V4(address) => format!("{address}:/{export_name}"),
        IpAddr::V6(address) => format!("[{address}]:/{export_name}"),
    }
}

#[cfg(target_os = "linux")]
fn linux_nfs_mount_options(port: u16, read_only: bool) -> String {
    format!(
        "{},vers=3,proto=tcp,mountproto=tcp,nolock,soft,timeo=10,retrans=2,port={port},mountport={port},rsize=1048576,wsize=1048576,actimeo=1",
        if read_only { "ro" } else { "rw" }
    )
}

#[cfg(target_os = "linux")]
fn linux_source_is_loopback(source: &str) -> bool {
    source.starts_with("127.0.0.1:/")
        || source.starts_with("[::1]:/")
        || source.starts_with("::1:/")
}

#[cfg(target_os = "linux")]
fn linux_client_resolution() -> &'static str {
    "Install nfs-utils/nfs-common and grant the daemon service CAP_SYS_ADMIN/root mount authority, then disable and re-enable this mount."
}

#[cfg(windows)]
fn normalize_windows_drive(value: &str) -> Result<String, NativeNfsMountError> {
    let bytes = value.as_bytes();
    if bytes.len() != 2 || !bytes[0].is_ascii_alphabetic() || bytes[1] != b':' {
        return Err(mount_error(
            "invalid-target",
            format!("invalid Windows drive `{value}`; expected a drive letter such as S:"),
            "Choose an unused drive letter followed by a colon.",
        ));
    }
    Ok(format!("{}:", (bytes[0] as char).to_ascii_uppercase()))
}

#[cfg(windows)]
fn windows_nfs_client_program(program: &str) -> Result<PathBuf, NativeNfsMountError> {
    let system_root = std::env::var_os("SystemRoot").ok_or_else(|| {
        mount_error(
            "nfs-client-missing",
            "Windows SystemRoot is unavailable; refusing to resolve the NFS client through PATH",
            "Install the Windows Client for NFS optional feature and restart the daemon.",
        )
    })?;
    windows_nfs_client_program_in(&PathBuf::from(system_root), program)
}

#[cfg(windows)]
fn windows_nfs_client_program_in(
    system_root: &Path,
    program: &str,
) -> Result<PathBuf, NativeNfsMountError> {
    let path = system_root.join("System32").join(program);
    if !path.is_file() {
        return Err(mount_error(
            "nfs-client-missing",
            format!(
                "Windows Client for NFS utility {} is unavailable; refusing to use a PATH substitute",
                path.display()
            ),
            "Install the Windows Client for NFS optional feature and restart the daemon.",
        ));
    }
    Ok(path)
}

fn command_output_detail(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        format!("process exited with {}", output.status)
    }
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn windows_nfs_helpers_are_resolved_only_from_system32() {
        let root = sinomo_testdir::TestDir::new("sinomo-nfs-system32-helper");
        let system32 = root.path().join("System32");
        std::fs::create_dir_all(&system32).expect("create fake System32");
        let native_mount = system32.join("mount.exe");
        std::fs::write(&native_mount, b"test helper").expect("seed native helper");

        assert_eq!(
            windows_nfs_client_program_in(root.path(), "mount.exe")
                .expect("resolve System32 helper"),
            native_mount
        );
        let missing = windows_nfs_client_program_in(root.path(), "umount.exe")
            .expect_err("PATH substitutes must not satisfy a missing System32 helper");
        assert_eq!(missing.code, "nfs-client-missing");
        assert!(missing.detail.contains("System32"));
        assert!(missing.detail.contains("refusing to use a PATH substitute"));
    }
}
