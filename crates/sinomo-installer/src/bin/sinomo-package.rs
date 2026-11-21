//! Host-native CI helper. Module selection is owned by sinomo-build metadata.
use sinomo_installer::{bundle::*, initial_setup::Result};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

fn run(command: &mut Command) -> Result<()> {
    if !command.status()?.success() {
        return Err(format!("package command failed: {command:?}").into());
    }
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.len() != 5 {
        return Err("usage: sinomo-package REPO BINARIES OUTPUT TARGET PROFILE".into());
    }
    let repo = sinomo_core::local::command_path(&fs::canonicalize(&args[0])?);
    let status = Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .output()?;
    if !status.status.success() || !status.stdout.is_empty() {
        return Err("package source must be a clean committed snapshot".into());
    }
    let binaries = PathBuf::from(&args[1]);
    let output = PathBuf::from(&args[2]);
    let target = sinomo_build::BuildTarget::host_with_rust_target(&args[3])?;
    fs::create_dir_all(&output)?;
    let work = output.join("initial-payload-work");
    fs::create_dir(&work)?;
    let _cleanup = sinomo_core::storage::scratch::ScratchPath::new(work.clone());
    let outputs = sinomo_build::discover_native_dylib_outputs(
        "cargo",
        &repo,
        &target,
        sinomo_build::NativeModuleSelection::Default,
    )?;
    let mut modules = Vec::new();
    let mut library_inputs = Vec::new();
    for (index, module) in outputs.into_iter().enumerate() {
        let mut command = Command::new("cargo");
        command.current_dir(&repo).args([
            "build",
            "--locked",
            "--profile",
            &args[4],
            "--target",
            &args[3],
            "-p",
            &module.package,
        ]);
        if !module.features.is_empty() {
            command.args(["--features", &module.features.join(",")]);
        }
        run(&mut command)?;
        let metadata = module.native_dylib.ok_or("native metadata missing")?;
        let file = format!(
            "module-{index}.{}",
            if cfg!(windows) {
                "dll"
            } else if cfg!(target_os = "macos") {
                "dylib"
            } else {
                "so"
            }
        );
        fs::copy(binaries.join(&metadata.library_name), work.join(&file))?;
        library_inputs.push(binaries.join(&metadata.library_name));
        #[cfg(target_os = "linux")]
        {
            let ancestors = 4 + metadata.cache_key.split('/').count();
            let rpath = format!("$ORIGIN/{}bin/lib", "../".repeat(ancestors));
            run(Command::new("patchelf")
                .args(["--set-rpath", &rpath])
                .arg(work.join(&file)))?;
        }
        modules.push(PayloadModule {
            hash: hash(&work.join(&file))?,
            file,
            metadata,
        });
    }
    let source = work.join("source");
    run(Command::new("git")
        .args([
            "clone",
            "--config",
            "core.autocrlf=false",
            "--config",
            "core.longpaths=true",
            "--no-local",
            "--no-hardlinks",
        ])
        .arg(&repo)
        .arg(&source))?;
    run(Command::new("git")
        .arg("-C")
        .arg(&source)
        .args(["remote", "remove", "origin"]))?;
    let revision = String::from_utf8(
        Command::new("git")
            .arg("-C")
            .arg(&source)
            .args(["rev-parse", "HEAD"])
            .output()?
            .stdout,
    )?
    .trim()
    .to_owned();
    let mut archive = tar::Builder::new(fs::File::create(work.join("source.tar"))?);
    for entry in fs::read_dir(&source)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            archive.append_dir_all(entry.file_name(), entry.path())?;
        } else {
            archive.append_path_with_name(entry.path(), Path::new(&entry.file_name()))?;
        }
    }
    archive.finish()?;
    #[cfg(target_os = "linux")]
    let libraries = {
        library_inputs.extend(
            [
                "sinomo",
                "sinomo-app",
                "sinomo-ui-extension-host",
                "sinomo-setup",
            ]
            .map(|name| binaries.join(name))
            .into_iter()
            .filter(|p| p.is_file()),
        );
        bundle_libraries(&library_inputs, &work)?
    };
    #[cfg(not(target_os = "linux"))]
    let libraries = Vec::new();
    let manifest = PayloadManifest {
        schema: 1,
        os: target.os,
        arch: target.arch,
        revision,
        source_hash: hash(&work.join("source.tar"))?,
        cli_hash: hash(&binaries.join(format!("sinomo{}", env::consts::EXE_SUFFIX)))?,
        modules,
        libraries,
    };
    fs::write(
        work.join("initial-payload.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    #[cfg(target_os = "linux")]
    {
        fs::create_dir_all(output.join("lib"))?;
        for library in &manifest.libraries {
            fs::copy(
                work.join("lib").join(&library.file),
                output.join("lib").join(&library.file),
            )?;
        }
    }
    let mut archive = tar::Builder::new(fs::File::create(output.join(PAYLOAD))?);
    for file in ["initial-payload.json", "source.tar"]
        .into_iter()
        .chain(manifest.modules.iter().map(|m| m.file.as_str()))
    {
        archive.append_path_with_name(work.join(file), file)?;
    }
    for library in manifest.libraries {
        let file = format!("lib/{}", library.file);
        archive.append_path_with_name(work.join(&file), &file)?;
    }
    archive.finish()?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn bundle_libraries(inputs: &[PathBuf], work: &Path) -> Result<Vec<PayloadLibrary>> {
    let lib = work.join("lib");
    fs::create_dir(&lib)?;
    // The X11 keyboard backend opens this library with dlopen, so ldd on the GUI
    // cannot discover it. Include its dependencies just like the GUI's linked ones.
    let mut dynamically_loaded = Vec::new();
    if inputs
        .iter()
        .any(|p| p.file_name().is_some_and(|name| name == "sinomo-app"))
    {
        let output = Command::new("pkg-config")
            .args(["--variable=libdir", "xkbcommon-x11"])
            .output()?;
        if !output.status.success() {
            return Err("desktop packaging requires the xkbcommon-x11 development package".into());
        }
        let dir = String::from_utf8(output.stdout)?;
        dynamically_loaded.push(Path::new(dir.trim()).join("libxkbcommon-x11.so.0"));
    }
    let mut dependencies = Vec::new();
    for input in inputs.iter().chain(&dynamically_loaded) {
        let output = Command::new("ldd").arg(input).output()?;
        let text = String::from_utf8(output.stdout)?;
        if text.contains("not found") {
            return Err(format!("unresolved package dependencies: {text}").into());
        }
        for line in text.lines() {
            let Some(path) = line.split_whitespace().find(|word| word.starts_with('/')) else {
                continue;
            };
            dependencies.push(PathBuf::from(path));
        }
    }
    let mut names = std::collections::BTreeSet::new();
    for source in dynamically_loaded.iter().chain(&dependencies) {
        let name = source
            .file_name()
            .ok_or("no dependency filename")?
            .to_str()
            .ok_or("invalid dependency filename")?;
        // glibc and its loader belong to the host. Packaging those independently
        // would couple the distribution to a different dynamic loader ABI.
        if name.starts_with("ld-linux")
            || [
                "libc.so.6",
                "libm.so.6",
                "libpthread.so.0",
                "librt.so.1",
                "libdl.so.2",
            ]
            .contains(&name)
        {
            continue;
        }
        let destination = lib.join(name);
        if names.insert(name.to_owned()) {
            fs::copy(source, &destination)?;
        } else if hash(source)? != hash(&destination)? {
            return Err(format!("conflicting runtime library: {name}").into());
        }
    }
    let mut result = Vec::new();
    for file in names {
        run(Command::new("patchelf")
            .args(["--set-rpath", "$ORIGIN"])
            .arg(lib.join(&file)))?;
        result.push(PayloadLibrary {
            hash: hash(&lib.join(&file))?,
            file,
        });
    }
    Ok(result)
}
