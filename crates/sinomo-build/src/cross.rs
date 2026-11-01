//! Build-owned, content-addressed cross toolchains. Discovery is read-only;
//! installation and link qualification happen only in an admitted build job.

use crate::{
    BuildArtifactKind, BuildError, BuildPaths, BuildTarget, BuildTargetCapability, Result,
    RustToolchainInfo,
};
use fs2::FileExt as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

const LAYOUT: &str = "sinomo-managed-cross-v1";
const ZIG_VERSION: &str = "0.14.1";
const ZIGBUILD_VERSION: &str = "0.21.8";
const GLIBC_BASELINE: &str = "2.17";
const TARGETS: &[&str] = &[
    "aarch64-unknown-linux-gnu",
    "x86_64-unknown-linux-gnu",
    "armv7-unknown-linux-gnueabihf",
    "aarch64-unknown-linux-musl",
    "x86_64-unknown-linux-musl",
    "armv7-unknown-linux-musleabihf",
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct Download {
    url: String,
    sha256: &'static str,
    directory: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManagedCrossToolchain {
    pub capability: BuildTargetCapability,
    root: PathBuf,
    zig: Download,
    zigbuild: Download,
    cargo: PathBuf,
    rustc: PathBuf,
    rust_sysroot: PathBuf,
    rustup: Option<PathBuf>,
    rustup_toolchain: String,
    base_fingerprint: String,
}

#[derive(Serialize, Deserialize)]
struct Qualification {
    fingerprint: String,
    rust_target: String,
}

pub(crate) fn configured(
    paths: &BuildPaths,
    cargo: &Path,
    rustc: &Path,
    info: &RustToolchainInfo,
) -> Result<Vec<ManagedCrossToolchain>> {
    // This is SDK host compatibility, not a classification of node capacity.
    // AArch64 cloud servers and x86-64 servers have the same target choices.
    let (host_arch, zig_hash, zigbuild_hash) = match info.host_target.as_str() {
        "x86_64-unknown-linux-gnu" => (
            "x86_64",
            "24aeeec8af16c381934a6cd7d95c807a8cb2cf7df9fa40d359aa884195c4716c",
            "8c66578e495cb7f124b2a904357c43716fb6d95a862e68654a45409f19555143",
        ),
        "aarch64-unknown-linux-gnu" => (
            "aarch64",
            "f7a654acc967864f7a050ddacfaa778c7504a0eca8d2b678839c21eea47c992b",
            "d34d0b7d69819876429a280d2cef5dc3f0d652904ed08fc8ccdaba9d1c7f9be8",
        ),
        _ => return Ok(Vec::new()),
    };
    let targets = match std::env::var("SINOMO_BUILD_CROSS_TARGETS") {
        Ok(value) if matches!(value.trim(), "" | "none" | "off") => return Ok(Vec::new()),
        Ok(value) => crate::parse_rust_target_list(&value)?,
        Err(_) => TARGETS.iter().map(|value| value.to_string()).collect(),
    };
    let rust_sysroot = PathBuf::from(
        crate::executor::command_stdout(rustc, &["--print".into(), "sysroot".into()])?.trim(),
    );
    let physical_rustc = rust_sysroot.join("bin/rustc");
    let physical_cargo = rust_sysroot.join("bin/cargo");
    if !physical_rustc.is_file() || !physical_cargo.is_file() {
        return Err(invalid(
            "managed cross compilation requires a complete installed Rust toolchain",
        ));
    }
    if crate::inspect_cargo_toolchain(&physical_rustc, &physical_cargo)?.fingerprint
        != info.fingerprint
    {
        return Err(invalid(
            "configured Cargo/rustc do not match their physical toolchain; refusing an unkeyed compiler substitution",
        ));
    }
    let rustup = find_program("rustup").or_else(|| {
        std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".cargo")))
            .map(|p| p.join("bin/rustup"))
            .filter(|p| p.is_file())
    });
    let toolchain_name = rust_sysroot
        .file_name()
        .ok_or_else(|| invalid("Rust sysroot has no toolchain name"))?
        .to_string_lossy()
        .into_owned();
    let zig_directory = format!("zig-{host_arch}-linux-{ZIG_VERSION}");
    let zigbuild_directory = format!("cargo-zigbuild-{host_arch}-unknown-linux-gnu");
    let bundle = blake3::hash(format!("{LAYOUT}\n{zig_hash}\n{zigbuild_hash}").as_bytes())
        .to_hex()
        .to_string();
    let root = paths.toolchains.join("managed-cross").join(bundle);
    let mut result = Vec::new();
    for triple in targets {
        if !TARGETS.contains(&triple.as_str()) {
            return Err(invalid(format!(
                "unsupported managed cross target {triple}; use an explicitly installed toolchain via SINOMO_BUILD_TARGETS"
            )));
        }
        if triple == info.host_target {
            continue;
        }
        let target = target(&triple)?;
        let fingerprint = fingerprint(&info.fingerprint, zig_hash, zigbuild_hash, &triple);
        result.push(ManagedCrossToolchain {
            capability: BuildTargetCapability { target, artifact_kinds: vec![BuildArtifactKind::Binary, BuildArtifactKind::NativeDylibModule], toolchain_fingerprint: fingerprint, supports_source_identity_hash: true },
            root: root.clone(),
            zig: Download { url: format!("https://ziglang.org/download/{ZIG_VERSION}/{zig_directory}.tar.xz"), sha256: zig_hash, directory: zig_directory.clone() },
            zigbuild: Download { url: format!("https://github.com/rust-cross/cargo-zigbuild/releases/download/v{ZIGBUILD_VERSION}/{zigbuild_directory}.tar.xz"), sha256: zigbuild_hash, directory: zigbuild_directory.clone() },
            cargo: physical_cargo.clone(), rustc: physical_rustc.clone(), rust_sysroot: rust_sysroot.clone(), rustup: rustup.clone(), rustup_toolchain: toolchain_name.clone(), base_fingerprint: info.fingerprint.clone(),
        });
    }
    let _ = cargo; // The matching physical Cargo is frozen above for cross jobs.
    Ok(result)
}

fn fingerprint(base: &str, zig: &str, zigbuild: &str, target: &str) -> String {
    format!(
        "managed-cross-blake3:{}",
        blake3::hash(
            format!(
                "{LAYOUT}\n{base}\n{zig}\n{zigbuild}\n{target}\n{GLIBC_BASELINE}\ndynamic-libc"
            )
            .as_bytes()
        )
        .to_hex()
    )
}

fn target(triple: &str) -> Result<BuildTarget> {
    let arch = if triple.starts_with("aarch64-") {
        "aarch64"
    } else if triple.starts_with("x86_64-") {
        "x86_64"
    } else {
        "arm"
    };
    BuildTarget::new("linux", arch)?
        .with_family("unix")?
        .with_rust_target(triple)
}

impl ManagedCrossToolchain {
    fn triple(&self) -> &str {
        self.capability
            .target
            .rust_target
            .as_deref()
            .expect("managed target is concrete")
    }
    fn zig_path(&self, root: &Path) -> PathBuf {
        root.join("zig").join(&self.zig.directory).join("zig")
    }
    pub(crate) fn program(&self) -> PathBuf {
        let nested = self
            .root
            .join("zigbuild")
            .join(&self.zigbuild.directory)
            .join("cargo-zigbuild");
        if nested.is_file() {
            nested
        } else {
            self.root.join("zigbuild/cargo-zigbuild")
        }
    }
    fn marker(&self) -> PathBuf {
        self.root.join(format!(
            "qualified-{}.json",
            self.capability
                .toolchain_fingerprint
                .trim_start_matches("managed-cross-blake3:")
        ))
    }
    fn has_std(&self) -> bool {
        fs::read_dir(
            self.rust_sysroot
                .join("lib/rustlib")
                .join(self.triple())
                .join("lib"),
        )
        .is_ok_and(|entries| {
            entries.filter_map(std::result::Result::ok).any(|e| {
                e.file_name().to_string_lossy().starts_with("libstd-")
                    && e.path().extension().is_some_and(|e| e == "rlib")
            })
        })
    }
    pub(crate) fn is_ready(&self) -> bool {
        self.has_std()
            && self.zig_path(&self.root).is_file()
            && self.program().is_file()
            && fs::read(self.marker())
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Qualification>(&bytes).ok())
                .is_some_and(|q| {
                    q.fingerprint == self.capability.toolchain_fingerprint
                        && q.rust_target == self.triple()
                })
    }
    pub(crate) fn blocker(&self) -> Option<String> {
        if self.is_ready() {
            return None;
        }
        if !self.has_std() && self.rustup.is_none() {
            return Some(format!(
                "{} needs Rust target std, but rustup is unavailable",
                self.triple()
            ));
        }
        if !self.root.is_dir() && find_program("curl").is_none() {
            return Some("managed SDK download requires curl".into());
        }
        None
    }
    pub(crate) fn target_argument(&self) -> String {
        if self.triple().ends_with("gnu") || self.triple().ends_with("gnueabihf") {
            format!("{}.{GLIBC_BASELINE}", self.triple())
        } else {
            self.triple().to_string()
        }
    }
    pub(crate) fn environment(&self) -> Result<Vec<(OsString, OsString)>> {
        let mut path = vec![self.rust_sysroot.join("bin")];
        path.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        let mut environment = vec![
            ("CARGO".into(), self.cargo.as_os_str().to_owned()),
            ("RUSTC".into(), self.rustc.as_os_str().to_owned()),
            (
                "PATH".into(),
                std::env::join_paths(path).map_err(|e| invalid(e.to_string()))?,
            ),
            (
                "CARGO_ZIGBUILD_ZIG_PATH".into(),
                self.zig_path(&self.root).into_os_string(),
            ),
            (
                "CARGO_ZIGBUILD_CACHE_DIR".into(),
                self.root.join("wrappers").into_os_string(),
            ),
            (
                "ZIG_GLOBAL_CACHE_DIR".into(),
                self.root.join("zig-cache").into_os_string(),
            ),
        ];
        if self.triple().contains("musl") {
            // Sinomo loads native modules; static musl excludes cdylib output.
            // This fixed ABI choice participates in the toolchain fingerprint.
            environment.push((
                "CARGO_ENCODED_RUSTFLAGS".into(),
                "-C\x1ftarget-feature=-crt-static".into(),
            ));
        }
        Ok(environment)
    }
    pub(crate) fn prepare(&self, log: Option<&Path>) -> Result<()> {
        if let Some(reason) = self.blocker() {
            return Err(invalid(reason));
        }
        let parent = self.root.parent().expect("managed root has parent");
        fs::create_dir_all(parent).map_err(|e| io(parent, e))?;
        let lock_path = parent.join(format!(
            "{}.lock",
            self.root.file_name().expect("bundle key").to_string_lossy()
        ));
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|e| io(&lock_path, e))?;
        lock.lock_exclusive().map_err(|e| io(&lock_path, e))?;
        if !self.root.is_dir() {
            note(
                log,
                &format!("preparing verified cross tools for {}", self.triple()),
            )?;
            let staging = TempDir::new(parent)?;
            for (name, download) in [("zig", &self.zig), ("zigbuild", &self.zigbuild)] {
                let archive = parent.join(format!("{}.tar.xz", download.sha256));
                if !archive.is_file() {
                    let temporary = staging.0.join(format!("{name}.tar.xz"));
                    run(
                        Path::new("curl"),
                        &[
                            "--fail".into(),
                            "--location".into(),
                            "--proto".into(),
                            "=https".into(),
                            "--proto-redir".into(),
                            "=https".into(),
                            "--connect-timeout".into(),
                            "30".into(),
                            "--max-time".into(),
                            "600".into(),
                            "--silent".into(),
                            "--show-error".into(),
                            "--output".into(),
                            temporary.as_os_str().to_owned(),
                            download.url.clone().into(),
                        ],
                        &[],
                        log,
                    )?;
                    verify_sha256(&temporary, download.sha256)?;
                    fs::rename(&temporary, &archive).map_err(|e| io(&archive, e))?;
                }
                verify_sha256(&archive, download.sha256)?;
                extract(&archive, &staging.0.join(name))?;
            }
            verify_version(&self.zig_path(&staging.0), &["version".into()], ZIG_VERSION)?;
            let plugin_nested = staging
                .0
                .join("zigbuild")
                .join(&self.zigbuild.directory)
                .join("cargo-zigbuild");
            let plugin = if plugin_nested.is_file() {
                plugin_nested
            } else {
                staging.0.join("zigbuild/cargo-zigbuild")
            };
            verify_version(&plugin, &["--version".into()], ZIGBUILD_VERSION)?;
            fs::rename(&staging.0, &self.root).map_err(|e| io(&self.root, e))?;
        }
        verify_version(&self.zig_path(&self.root), &["version".into()], ZIG_VERSION)?;
        verify_version(&self.program(), &["--version".into()], ZIGBUILD_VERSION)?;
        if !self.has_std() {
            let rustup = self.rustup.as_ref().ok_or_else(|| {
                invalid(format!(
                    "{} requires Rust target std; install rustup for the selected compiler",
                    self.triple()
                ))
            })?;
            note(
                log,
                &format!(
                    "installing {} Rust target for {}",
                    self.triple(),
                    self.rustup_toolchain
                ),
            )?;
            run(
                rustup,
                &[
                    "target".into(),
                    "add".into(),
                    "--toolchain".into(),
                    self.rustup_toolchain.clone().into(),
                    self.triple().into(),
                ],
                &[],
                log,
            )?;
        }
        if crate::inspect_cargo_toolchain(&self.rustc, &self.cargo)?.fingerprint
            != self.base_fingerprint
        {
            return Err(invalid(
                "Rust toolchain changed during cross preparation; request a new build key",
            ));
        }
        if !self.has_std() {
            return Err(invalid(format!(
                "Rust target {} is still unavailable after preparation",
                self.triple()
            )));
        }
        if !self.is_ready() {
            self.qualify(log)?;
            let bytes = serde_json::to_vec(&Qualification {
                fingerprint: self.capability.toolchain_fingerprint.clone(),
                rust_target: self.triple().to_string(),
            })
            .map_err(|e| invalid(e.to_string()))?;
            let marker = self.marker();
            let temporary = marker.with_extension("tmp");
            fs::write(&temporary, bytes).map_err(|e| io(&temporary, e))?;
            File::open(&temporary)
                .and_then(|f| f.sync_all())
                .map_err(|e| io(&temporary, e))?;
            fs::rename(&temporary, &marker).map_err(|e| io(&marker, e))?;
        }
        Ok(())
    }
    fn qualify(&self, log: Option<&Path>) -> Result<()> {
        note(
            log,
            &format!(
                "qualifying {} executable and native-library cross links",
                self.triple()
            ),
        )?;
        let temp = TempDir::new(&self.root)?;
        let source = temp.0.join("probe.rs");
        fs::write(&source,"fn main() { println!(\"sinomo-cross-probe\"); }\n#[unsafe(no_mangle)] pub extern \"C\" fn sinomo_cross_probe() -> u32 { 42 }\n").map_err(|e|io(&source,e))?;
        fs::write(temp.0.join("Cargo.toml"),"[package]\nname='sinomo-cross-probe'\nversion='0.0.0'\nedition='2024'\n[workspace]\n[[bin]]\nname='probe'\npath='probe.rs'\n[lib]\nname='probe'\npath='probe.rs'\ncrate-type=['cdylib']\n").map_err(|e|io(&temp.0,e))?;
        let mut env = self.environment()?;
        // Cargo's source-controlled configuration is retained, but host-only
        // ambient flags must not select an unkeyed linker or native CPU.
        env.push(("RUSTFLAGS".into(), "".into()));
        if !self.triple().contains("musl") {
            env.push(("CARGO_ENCODED_RUSTFLAGS".into(), "".into()));
        }
        run(
            &self.program(),
            &[
                "zigbuild".into(),
                "--manifest-path".into(),
                temp.0.join("Cargo.toml").into_os_string(),
                "--target".into(),
                self.target_argument().into(),
                "--target-dir".into(),
                temp.0.join("target").into_os_string(),
            ],
            &env,
            log,
        )?;
        let dir = temp.0.join("target").join(self.triple()).join("debug");
        verify_elf(&dir.join("probe"), &self.capability.target)?;
        verify_elf(&dir.join("libprobe.so"), &self.capability.target)?;
        let evidence = self.root.join("probes").join(
            self.capability
                .toolchain_fingerprint
                .trim_start_matches("managed-cross-blake3:"),
        );
        fs::create_dir_all(&evidence).map_err(|e| io(&evidence, e))?;
        for file in ["probe", "libprobe.so"] {
            fs::copy(dir.join(file), evidence.join(file)).map_err(|e| io(&evidence, e))?;
        }
        Ok(())
    }
}

fn find_program(name: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|p| p.join(name))
        .find(|p| p.is_file())
}
fn verify_version(program: &Path, args: &[OsString], expected: &str) -> Result<()> {
    let text = crate::executor::command_stdout(program, args)?;
    if !text.split_whitespace().any(|v| v == expected) {
        return Err(invalid(format!(
            "{} has unexpected version {text:?}; expected {expected}",
            program.display()
        )));
    }
    Ok(())
}
fn verify_sha256(path: &Path, expected: &str) -> Result<()> {
    let mut file = File::open(path).map_err(|e| io(path, e))?;
    let mut hash = Sha256::new();
    let mut chunk = [0; 128 * 1024];
    loop {
        let n = file.read(&mut chunk).map_err(|e| io(path, e))?;
        if n == 0 {
            break;
        }
        hash.update(&chunk[..n]);
    }
    let actual = hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if actual != expected {
        return Err(BuildError::HashMismatch {
            artifact: path.display().to_string(),
            expected: expected.into(),
            actual,
        });
    }
    Ok(())
}
fn extract(archive: &Path, root: &Path) -> Result<()> {
    fs::create_dir_all(root).map_err(|e| io(root, e))?;
    let file = File::open(archive).map_err(|e| io(archive, e))?;
    let mut tar = tar::Archive::new(xz2::read::XzDecoder::new(file));
    for entry in tar.entries().map_err(|e| io(archive, e))? {
        let mut entry = entry.map_err(|e| io(archive, e))?;
        let path = entry.path().map_err(|e| io(archive, e))?.into_owned();
        validate_relative(&path, Path::new(""))?;
        let kind = entry.header().entry_type();
        if !(kind.is_file() || kind.is_dir() || kind.is_symlink() || kind.is_hard_link()) {
            return Err(invalid("SDK archive contains a non-file entry"));
        }
        if let Some(link) = entry.link_name().map_err(|e| io(archive, e))? {
            validate_relative(
                &link,
                if kind.is_symlink() {
                    path.parent().unwrap_or(Path::new(""))
                } else {
                    Path::new("")
                },
            )?;
        }
        if !entry.unpack_in(root).map_err(|e| io(root, e))? {
            return Err(invalid("SDK archive path escapes its installation"));
        }
    }
    Ok(())
}
fn validate_relative(path: &Path, base: &Path) -> Result<()> {
    let mut depth = base
        .components()
        .filter(|c| matches!(c, Component::Normal(_)))
        .count();
    for c in path.components() {
        match c {
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            Component::ParentDir if depth > 0 => depth -= 1,
            _ => return Err(invalid("SDK archive path escapes its installation")),
        }
    }
    Ok(())
}
pub(crate) fn verify_elf(path: &Path, target: &BuildTarget) -> Result<()> {
    let mut header = [0u8; 20];
    File::open(path)
        .and_then(|mut f| f.read_exact(&mut header))
        .map_err(|e| io(path, e))?;
    let (class, machine) = match target.arch.as_str() {
        "aarch64" => (2, 183),
        "x86_64" => (2, 62),
        "arm" => (1, 40),
        _ => return Err(invalid("unsupported cross artifact architecture")),
    };
    if &header[..4] != b"\x7fELF"
        || header[4] != class
        || header[5] != 1
        || u16::from_le_bytes([header[18], header[19]]) != machine
    {
        return Err(invalid(format!(
            "{} is not an ELF artifact for {}",
            path.display(),
            target.path_key()
        )));
    }
    Ok(())
}
fn run(
    program: &Path,
    args: &[OsString],
    env: &[(OsString, OsString)],
    log: Option<&Path>,
) -> Result<()> {
    let mut command = crate::process::noninteractive_command(program);
    command.args(args).stdin(std::process::Stdio::null());
    for (k, v) in env {
        command.env(k, v);
    }
    if let Some(log) = log {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
            .map_err(|e| io(log, e))?;
        command
            .stdout(file.try_clone().map_err(|e| io(log, e))?)
            .stderr(file);
    }
    let status = command.status().map_err(|e| io(program, e))?;
    if !status.success() {
        return Err(BuildError::Process {
            command: program.display().to_string(),
            status: status.code().unwrap_or(-1),
            stderr: "cross-toolchain preparation failed; see build log".into(),
        });
    }
    Ok(())
}
fn note(log: Option<&Path>, message: &str) -> Result<()> {
    if let Some(path) = log {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| io(path, e))?;
        writeln!(f, "{message}").map_err(|e| io(path, e))?;
    }
    Ok(())
}
fn invalid(detail: impl Into<String>) -> BuildError {
    BuildError::InvalidValue {
        detail: detail.into(),
    }
}
fn io(path: &Path, e: std::io::Error) -> BuildError {
    BuildError::Io {
        path: path.display().to_string(),
        detail: e.to_string(),
    }
}
struct TempDir(PathBuf);
impl TempDir {
    fn new(parent: &Path) -> Result<Self> {
        let p = parent.join(format!(".prepare-{}", crate::BuildJobId::generate()));
        fs::create_dir(&p).map_err(|e| io(&p, e))?;
        Ok(Self(p))
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_and_command_planning_do_not_prepare_tools() {
        use crate::{
            BuildKeyInput, BuildOutputRequest, BuildPlan, BuildSource, CargoBuildExecutor,
            build_content_hash,
        };
        let root = sinomo_testdir::TestDir::new("cross-discovery-read-only");
        let paths = BuildPaths::new(&root);
        let info = crate::inspect_cargo_toolchain("rustc", "cargo").unwrap();
        let profiles = configured(&paths, Path::new("cargo"), Path::new("rustc"), &info).unwrap();
        let Some(cross) = profiles.first() else {
            return;
        };
        assert!(!paths.toolchains.exists());
        assert!(!cross.is_ready());
        let cap = cross.capability.clone();
        let native = BuildTargetCapability {
            target: BuildTarget::host_with_rust_target(&info.host_target).unwrap(),
            artifact_kinds: vec![BuildArtifactKind::Binary],
            toolchain_fingerprint: info.fingerprint.clone(),
            supports_source_identity_hash: true,
        };
        let executor = CargoBuildExecutor::new("cargo", vec![native])
            .unwrap()
            .with_managed_cross(profiles);
        assert_eq!(executor.native_and_installed_targets().len(), 1);
        assert!(executor.cross_capabilities().iter().all(|cap| !cap.ready));
        let source = root.join("source");
        fs::create_dir(&source).unwrap();
        fs::write(
            source.join("Cargo.toml"),
            "[package]\nname='probe'\nversion='0.0.0'\n",
        )
        .unwrap();
        let hash = build_content_hash(b"source");
        let plan = BuildPlan::new(
            BuildSource {
                label: "fixture".into(),
                revision: None,
                identity_hash: None,
                bundle_hash: hash.clone(),
                bundle_bytes: 6,
            },
            BuildKeyInput {
                source_hash: hash,
                source_revision: None,
                lockfile_hash: build_content_hash(b"lock"),
                target: cap.target,
                profile: "dev".into(),
                features: Vec::new(),
                toolchain_fingerprint: cap.toolchain_fingerprint,
                outputs: vec![BuildOutputRequest::binary("probe", "probe")],
            },
        )
        .unwrap();
        let command = executor
            .plan_command(&source, &plan, root.join("target"))
            .unwrap();
        assert!(command.args.iter().any(|arg| arg == "zigbuild"));
        assert!(!paths.toolchains.exists());
        assert!(
            executor
                .execute(root.join("missing-source"), &plan, root.join("artifacts"))
                .is_err()
        );
        assert!(
            !paths.toolchains.exists(),
            "invalid source must fail before any SDK preparation"
        );
    }

    #[test]
    fn target_and_sdk_versions_are_part_of_cross_cache_identity() {
        let a = fingerprint("rust-a", "zig-a", "plugin-a", "aarch64-unknown-linux-gnu");
        for b in [
            fingerprint("rust-b", "zig-a", "plugin-a", "aarch64-unknown-linux-gnu"),
            fingerprint("rust-a", "zig-b", "plugin-a", "aarch64-unknown-linux-gnu"),
            fingerprint("rust-a", "zig-a", "plugin-b", "aarch64-unknown-linux-gnu"),
            fingerprint("rust-a", "zig-a", "plugin-a", "aarch64-unknown-linux-musl"),
        ] {
            assert_ne!(a, b);
        }
    }

    #[test]
    fn sdk_paths_cannot_escape_the_installation() {
        for path in ["/etc/passwd", "../../outside", "a/../../outside"] {
            assert!(validate_relative(Path::new(path), Path::new("")).is_err());
        }
        assert!(validate_relative(Path::new("../include"), Path::new("sdk/lib")).is_ok());
        assert!(validate_relative(Path::new("../../../outside"), Path::new("sdk/lib")).is_err());
    }

    #[test]
    fn checksum_mismatch_is_rejected_before_sdk_execution() {
        let root = sinomo_testdir::TestDir::new("cross-sdk-integrity");
        let path = root.join("archive");
        fs::write(&path, b"abc").unwrap();
        verify_sha256(
            &path,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .unwrap();
        assert!(matches!(
            verify_sha256(&path, &"0".repeat(64)),
            Err(BuildError::HashMismatch { .. })
        ));
    }

    #[test]
    fn an_x86_executable_is_not_accepted_as_an_arm_artifact() {
        let root = sinomo_testdir::TestDir::new("cross-sdk-elf");
        let path = root.join("artifact");
        let mut header = [0u8; 20];
        header[..4].copy_from_slice(b"\x7fELF");
        header[4] = 2;
        header[5] = 1;
        header[18] = 62;
        fs::write(&path, header).unwrap();
        verify_elf(&path, &target("x86_64-unknown-linux-gnu").unwrap()).unwrap();
        assert!(verify_elf(&path, &target("aarch64-unknown-linux-gnu").unwrap()).is_err());
        header[18] = 183;
        fs::write(&path, header).unwrap();
        verify_elf(&path, &target("aarch64-unknown-linux-gnu").unwrap()).unwrap();
    }

    #[test]
    fn extraction_rejects_absolute_archive_members() {
        use xz2::write::XzEncoder;
        let root = sinomo_testdir::TestDir::new("cross-sdk-extraction");
        let path = root.join("archive.tar.xz");
        let mut header = tar::Header::new_gnu();
        header.set_size(1);
        header.set_mode(0o644);
        let name = b"/outside";
        header.as_mut_bytes()[..name.len()].copy_from_slice(name);
        header.set_cksum();
        let mut tar = tar::Builder::new(XzEncoder::new(File::create(&path).unwrap(), 1));
        tar.append(&header, &b"x"[..]).unwrap();
        tar.into_inner().unwrap().finish().unwrap();
        assert!(extract(&path, &root.join("sdk")).is_err());
    }

    #[test]
    #[ignore = "downloads pinned cross tools and installs target std via rustup"]
    fn real_managed_cross_executable_and_library() {
        let temporary = sinomo_testdir::TestDir::new("managed-cross-live");
        let root = std::env::var_os("SINOMO_CROSS_TEST_DATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| temporary.path().to_path_buf());
        let cargo = Path::new("cargo");
        let rustc = Path::new("rustc");
        let info = crate::inspect_cargo_toolchain(rustc, cargo).unwrap();
        let profiles = configured(&BuildPaths::new(&root), cargo, rustc, &info).unwrap();
        let requested = std::env::var("SINOMO_CROSS_TEST_TARGET").unwrap_or_else(|_| {
            if info.host_target.starts_with("aarch64-") {
                "x86_64-unknown-linux-gnu".into()
            } else {
                "aarch64-unknown-linux-gnu".into()
            }
        });
        let cross = profiles
            .iter()
            .find(|p| p.triple() == requested)
            .expect("cross target is configured");
        assert!(
            !cross.is_ready(),
            "qualification fixture must start without a ready marker"
        );
        let log = root.join("cross-qualification.log");
        fs::create_dir_all(&root).unwrap();
        cross.prepare(Some(&log)).unwrap();
        assert!(cross.is_ready());
        // Exercise the same executor, output paths, provenance and artifact
        // verification used by admitted service jobs, not just the SDK probe.
        use crate::{
            BuildKeyInput, BuildOutputRequest, BuildPlan, BuildSource, CargoBuildExecutor,
            NativeDylibArtifactMetadata, build_content_hash,
        };
        let source = root.join("executor-source");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("Cargo.toml"), "[package]\nname='cross-executor-probe'\nversion='0.0.0'\nedition='2024'\n[workspace]\n[[bin]]\nname='probe'\npath='probe.rs'\n[lib]\nname='probe'\npath='probe.rs'\ncrate-type=['cdylib']\n").unwrap();
        fs::write(source.join("probe.rs"), "fn main(){println!(\"sinomo-cross-executor\");}\n#[unsafe(no_mangle)] pub extern \"C\" fn sinomo_cross_probe()->u32{42}\n").unwrap();
        run(
            &cross.cargo,
            &[
                "generate-lockfile".into(),
                "--offline".into(),
                "--manifest-path".into(),
                source.join("Cargo.toml").into_os_string(),
            ],
            &cross.environment().unwrap(),
            Some(&log),
        )
        .unwrap();
        let hash = build_content_hash(b"fixture-source");
        let plan = BuildPlan::new(
            BuildSource {
                label: "fixture".into(),
                revision: None,
                identity_hash: None,
                bundle_hash: hash.clone(),
                bundle_bytes: 14,
            },
            BuildKeyInput {
                source_hash: hash,
                source_revision: None,
                lockfile_hash: build_content_hash(&fs::read(source.join("Cargo.lock")).unwrap()),
                target: cross.capability.target.clone(),
                profile: "dev".into(),
                features: Vec::new(),
                toolchain_fingerprint: cross.capability.toolchain_fingerprint.clone(),
                outputs: vec![
                    BuildOutputRequest::binary("probe", "cross-executor-probe"),
                    BuildOutputRequest::native_dylib_module(
                        "probe-library",
                        "cross-executor-probe",
                        "probe",
                        NativeDylibArtifactMetadata::new(
                            sinomo_module::ModuleId::from_legacy_named("module.cross-probe")
                                .unwrap(),
                            "v1",
                            "cross-probe",
                            "libprobe.so",
                            "cross-probe",
                        ),
                    ),
                ],
            },
        )
        .unwrap();
        let executor = CargoBuildExecutor::new(
            &cross.cargo,
            vec![BuildTargetCapability {
                target: BuildTarget::host_with_rust_target(&info.host_target).unwrap(),
                artifact_kinds: vec![BuildArtifactKind::Binary],
                toolchain_fingerprint: info.fingerprint.clone(),
                supports_source_identity_hash: true,
            }],
        )
        .unwrap()
        .with_managed_cross(profiles.clone());
        let result = executor
            .execute_with_target_dir_and_log(
                &source,
                &plan,
                root.join("artifacts"),
                root.join("executor-target"),
                None,
                Some(&log),
            )
            .unwrap();
        assert_eq!(result.artifact_set.artifacts.len(), 2);
        for artifact in &result.artifact_set.artifacts {
            let file = result.artifact_root.join(&artifact.relative_path);
            crate::verify_artifact_file(artifact, &file).unwrap();
            verify_elf(&file, &plan.input.target).unwrap();
        }
        fs::write(
            root.join("executor-artifact-set.json"),
            serde_json::to_vec_pretty(&result.artifact_set).unwrap(),
        )
        .unwrap();
        let evidence = cross.root.join("probes").join(
            cross
                .capability
                .toolchain_fingerprint
                .trim_start_matches("managed-cross-blake3:"),
        );
        println!(
            "cross qualification target={} tools={} evidence={} log={}",
            requested,
            cross.root.display(),
            evidence.display(),
            log.display()
        );
    }
}
