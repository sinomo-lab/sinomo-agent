//! Offline bootstrap payload. Initial setup seeds missing state only; subsequent
//! source replacement and module activation remain owned by rollout/evolution.
use crate::initial_setup::Result;
use serde::{Deserialize, Serialize};
use sinomo_build::NativeDylibArtifactMetadata;
use sinomo_core::{local, storage::scratch::ScratchDirectory};
use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path},
};

pub const PAYLOAD: &str = "initial-payload.tar";
const RETAINED_PAYLOAD: &str = "config/initial-payload.json";
#[derive(Debug, Serialize, Deserialize)]
pub struct PayloadModule {
    pub file: String,
    pub hash: String,
    pub metadata: NativeDylibArtifactMetadata,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct PayloadManifest {
    pub schema: u32,
    pub os: String,
    pub arch: String,
    pub revision: String,
    pub source_hash: String,
    pub cli_hash: String,
    pub modules: Vec<PayloadModule>,
    #[serde(default)]
    pub libraries: Vec<PayloadLibrary>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PayloadLibrary {
    pub file: String,
    pub hash: String,
}

pub fn hash(path: &Path) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update_reader(fs::File::open(path)?)?;
    Ok(hasher.finalize().to_hex().to_string())
}

fn unpack(archive: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    let mut seen = BTreeSet::new();
    for entry in tar::Archive::new(fs::File::open(archive)?).entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if path.is_absolute()
            || path
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
            || !seen.insert(path.clone())
            || !(entry.header().entry_type().is_file() || entry.header().entry_type().is_dir())
        {
            return Err(format!(
                "unsafe/duplicate bootstrap archive entry: {}",
                path.display()
            )
            .into());
        }
        if !entry.unpack_in(destination)? {
            return Err("bootstrap archive escaped destination".into());
        }
    }
    Ok(())
}

/// Validate every artifact before touching installed state. No compiler, Git,
/// display server or network access is needed on the installing machine.
pub fn prepare(package: &Path, data: &Path, executable: &Path) -> Result<Option<PreparedPayload>> {
    if data.join(RETAINED_PAYLOAD).try_exists()? {
        validate_retained(data)?;
        return Ok(None);
    }
    let archive = package.join(PAYLOAD);
    if !archive.is_file() {
        return Err("initial-payload.tar is missing; use the complete Sinomo distribution".into());
    }
    let scratch = ScratchDirectory::create(
        &std::env::temp_dir().join("sinomo-setup"),
        "initial-payload",
    )?;
    let stage = scratch.path();
    unpack(&archive, &stage)?;
    let manifest_bytes = fs::read(stage.join("initial-payload.json"))?;
    let manifest: PayloadManifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.schema != 1
        || manifest.os != std::env::consts::OS
        || manifest.arch != std::env::consts::ARCH
        || manifest.revision.is_empty()
        || manifest.modules.is_empty()
    {
        return Err("incompatible/incomplete initial payload manifest".into());
    }
    if hash(&stage.join("source.tar"))? != manifest.source_hash {
        return Err("bootstrap source digest mismatch".into());
    }
    let source = sinomo_build::BuildPaths::new(data).managed_source;
    let retained_cli = local::SinomoDataDir::new(data).binary("sinomo");
    if !source.exists()
        && hash(if retained_cli.is_file() {
            &retained_cli
        } else {
            executable
        })? != manifest.cli_hash
    {
        return Err("bootstrap CLI does not match its source/module payload".into());
    }
    let mut ids = BTreeSet::new();
    for module in &manifest.modules {
        module.metadata.validate()?;
        if !single_name(&module.file)
            || !ids.insert(module.metadata.module_id.clone())
            || hash(&stage.join(&module.file))? != module.hash
        {
            return Err("invalid bootstrap module artifact".into());
        }
    }
    for library in &manifest.libraries {
        if !single_name(&library.file)
            || hash(&stage.join("lib").join(&library.file))? != library.hash
        {
            return Err("invalid runtime dependency".into());
        }
    }
    if !source.exists() {
        let prepared = stage.join("source");
        unpack(&stage.join("source.tar"), &prepared)?;
        if !prepared.join(".git/HEAD").is_file() || !prepared.join("Cargo.lock").is_file() {
            return Err("bootstrap source is not a self-contained managed checkout".into());
        }
    }
    Ok(Some(PreparedPayload {
        scratch,
        manifest,
        manifest_bytes,
    }))
}

pub struct PreparedPayload {
    scratch: ScratchDirectory,
    manifest: PayloadManifest,
    manifest_bytes: Vec<u8>,
}

impl PreparedPayload {
    pub fn install(self, data: &Path, seed_defaults: bool) -> Result<()> {
        let stage = self.scratch.path();
        let manifest = self.manifest;
        let manifest_bytes = self.manifest_bytes;
        let source = sinomo_build::BuildPaths::new(data).managed_source;
        fs::create_dir_all(data.join("bin/lib"))?;
        for library in &manifest.libraries {
            let destination = data.join("bin/lib").join(&library.file);
            if !destination.exists() {
                fs::copy(stage.join("lib").join(&library.file), destination)?;
            }
        }
        if !source.exists() {
            let prepared = stage.join("source");
            let parent = source.parent().ok_or("no managed source parent")?;
            fs::create_dir_all(parent)?;
            // Preflight may use a different volume. Publish through an owned
            // stage beside the destination, retaining atomic visibility.
            let local = ScratchDirectory::create(parent, "bootstrap-source")?;
            let candidate = local.path().join("source");
            copy_source_tree(&prepared, &candidate)?;
            fs::rename(candidate, &source)?;
        }
        for module in manifest.modules {
            if !seed_defaults {
                continue;
            }
            let meta = module.metadata;
            // Preserve evolved binaries and even disabled module selections.
            if sinomo_module::read_native_dylib_module_loader_state(data, &meta.module_id)?
                .is_none()
            {
                sinomo_module::install_native_dylib_module_from_file(
                    data,
                    &stage.join(module.file),
                    sinomo_module::NativeDylibModuleInstallRequest {
                        expected_module_id: Some(meta.module_id),
                        artifact_id: None,
                        expected_interface_version: Some(meta.interface_version),
                        cache_key: Some(meta.cache_key),
                        library_name: Some(meta.library_name),
                        source: meta.source,
                    },
                )?;
            }
        }
        // The resolver is shared by the CLI, GUI and all evolved replacements.
        fs::write(
            data.join("bin").join(local::INSTALLED_DATA_DIR_FILE),
            data.to_str().ok_or("invalid data path")?,
        )?;
        fs::write(data.join(RETAINED_PAYLOAD), manifest_bytes)?;
        Ok(())
    }
}

fn copy_source_tree(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_source_tree(&entry.path(), &target)?;
        } else if entry.file_type()?.is_file() {
            fs::copy(entry.path(), target)?;
        } else {
            return Err("validated source changed before publication".into());
        }
    }
    Ok(())
}

pub fn install_missing(package: &Path, data: &Path, seed_defaults: bool) -> Result<()> {
    let executable = local::SinomoDataDir::new(data).binary("sinomo");
    if let Some(payload) = prepare(package, data, &executable)? {
        payload.install(data, seed_defaults)?;
    }
    Ok(())
}

/// Retained deployments can be re-enabled offline without reintroducing an
/// upstream payload or changing their evolved module selections.
fn validate_retained(data: &Path) -> Result<()> {
    let manifest: PayloadManifest =
        serde_json::from_slice(&fs::read(data.join(RETAINED_PAYLOAD))?)?;
    if manifest.schema != 1
        || manifest.modules.is_empty()
        || manifest.os != std::env::consts::OS
        || manifest.arch != std::env::consts::ARCH
        || !local::SinomoDataDir::new(data).binary("sinomo").is_file()
        || !sinomo_build::BuildPaths::new(data)
            .managed_source
            .join(".git/HEAD")
            .is_file()
    {
        return Err("retained deployment is incomplete or for another platform".into());
    }
    // Current loader state owns evolved component selection. The bootstrap
    // inventory must not resurrect modules that were intentionally removed.
    for state in sinomo_module::read_native_dylib_module_loader_states(data)? {
        let path = sinomo_module::native_module_cache_path(
            data,
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        if format!("blake3:{}", hash(&path)?) != state.content_hash {
            return Err("retained module digest mismatch".into());
        }
    }
    for library in &manifest.libraries {
        if !single_name(&library.file) || !data.join("bin/lib").join(&library.file).is_file() {
            return Err("retained runtime dependency is missing".into());
        }
    }
    Ok(())
}

fn single_name(name: &str) -> bool {
    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_nested_source_is_rejected_before_data_mutation() {
        let root = sinomo_testdir::TestDir::new("bootstrap-preflight-source");
        let package = root.join("package");
        let data = root.join("data");
        fs::create_dir_all(&package).unwrap();
        let cli = package.join("sinomo");
        fs::write(&cli, b"candidate").unwrap();
        fs::write(package.join("source.tar"), b"invalid nested tar archive").unwrap();
        fs::write(package.join("module.so"), b"module fixture").unwrap();
        let manifest = PayloadManifest {
            schema: 1,
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            revision: "fixture".into(),
            source_hash: hash(&package.join("source.tar")).unwrap(),
            cli_hash: hash(&cli).unwrap(),
            libraries: Vec::new(),
            modules: vec![PayloadModule {
                file: "module.so".into(),
                hash: hash(&package.join("module.so")).unwrap(),
                metadata: NativeDylibArtifactMetadata::new(
                    sinomo_module::ModuleId::from_legacy_named("module.preflight").unwrap(),
                    "v1",
                    "fixture",
                    "module.so",
                    "fixture",
                ),
            }],
        };
        fs::write(
            package.join("initial-payload.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let mut archive = tar::Builder::new(fs::File::create(package.join(PAYLOAD)).unwrap());
        for file in ["initial-payload.json", "source.tar", "module.so"] {
            archive
                .append_path_with_name(package.join(file), file)
                .unwrap();
        }
        archive.finish().unwrap();
        assert!(prepare(&package, &data, &cli).is_err());
        assert!(
            !data.exists(),
            "preflight must not create DATA, bindings, ownership or deployment fences"
        );
    }

    #[test]
    fn adopting_evolved_data_does_not_reintroduce_upstream_modules() {
        let root = sinomo_testdir::TestDir::new("retain-module-selection");
        let data = root.join("data");
        let package = root.join("package");
        for directory in [
            data.join("config"),
            data.join("bin"),
            data.join("source/sinomo/.git"),
            package.clone(),
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        fs::write(
            local::SinomoDataDir::new(&data).binary("sinomo"),
            b"evolved CLI",
        )
        .unwrap();
        fs::write(data.join("source/sinomo/.git/HEAD"), b"evolved source").unwrap();
        let unused_source = b"upstream source is intentionally not imported";
        let unused_module = b"upstream native module is intentionally not loaded";
        let manifest = PayloadManifest {
            schema: 1,
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            revision: "upstream".into(),
            source_hash: blake3::hash(unused_source).to_hex().to_string(),
            cli_hash: "upstream CLI".into(),
            libraries: vec![],
            modules: vec![PayloadModule {
                file: "module.dll".into(),
                hash: blake3::hash(unused_module).to_hex().to_string(),
                metadata: NativeDylibArtifactMetadata::new(
                    sinomo_module::ModuleId::from_legacy_named("module.evolution").unwrap(),
                    "fixture.v1",
                    "evolution",
                    "module.dll",
                    "upstream",
                ),
            }],
        };
        let json = serde_json::to_vec(&manifest).unwrap();
        let mut archive = tar::Builder::new(fs::File::create(package.join(PAYLOAD)).unwrap());
        for (name, bytes) in [
            ("source.tar", unused_source.as_slice()),
            ("module.dll", unused_module.as_slice()),
            ("initial-payload.json", json.as_slice()),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            archive.append_data(&mut header, name, bytes).unwrap();
        }
        archive.finish().unwrap();
        fs::create_dir_all(data.join("rollout")).unwrap();
        install_missing(&package, &data, false).unwrap();
        assert!(
            sinomo_module::read_native_dylib_module_loader_states(&data)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            fs::read(local::SinomoDataDir::new(&data).binary("sinomo")).unwrap(),
            b"evolved CLI"
        );
        assert_eq!(
            fs::read(data.join("source/sinomo/.git/HEAD")).unwrap(),
            b"evolved source"
        );
    }

    #[test]
    fn offline_reinstall_validates_evolved_artifacts_without_the_upstream_payload() {
        let root = sinomo_testdir::TestDir::new("offline-retained-deployment");
        let data = root.path();
        for directory in ["config", "bin", "source/sinomo/.git"] {
            fs::create_dir_all(data.join(directory)).unwrap();
        }
        fs::write(
            local::SinomoDataDir::new(data).binary("sinomo"),
            b"evolved CLI",
        )
        .unwrap();
        fs::write(data.join("source/sinomo/.git/HEAD"), b"evolved source").unwrap();
        let module_id = sinomo_module::ModuleId::from_legacy_named("module.evolution").unwrap();
        let content = b"evolved module";
        let content_hash = format!("blake3:{}", blake3::hash(content).to_hex());
        let state: sinomo_module::NativeDylibModuleLoaderState = serde_json::from_value(serde_json::json!({
            "module_id":module_id, "content_hash":content_hash, "cache_key":"evolution",
            "library_name":"module.dll", "platform_triple":"fixture", "native_abi_version":1,
            "interface_version":"fixture.v1", "activated_at":"fixture", "source":"approved evolution"
        })).unwrap();
        let library = sinomo_module::native_module_cache_path(
            data,
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        fs::create_dir_all(library.parent().unwrap()).unwrap();
        fs::write(&library, content).unwrap();
        sinomo_module::write_native_dylib_module_loader_state(data, &state).unwrap();
        let manifest = PayloadManifest {
            schema: 1,
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            revision: "initial upstream".into(),
            source_hash: "old source".into(),
            cli_hash: "old CLI".into(),
            libraries: vec![],
            modules: vec![PayloadModule {
                file: "old module".into(),
                hash: "old digest".into(),
                metadata: NativeDylibArtifactMetadata::new(
                    module_id,
                    "fixture.v1",
                    "evolution",
                    "module.dll",
                    "initial upstream",
                ),
            }],
        };
        fs::write(
            data.join(RETAINED_PAYLOAD),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        install_missing(&root.join("no-archive"), data, false).unwrap();
        assert_eq!(fs::read(&library).unwrap(), content);
        fs::write(&library, b"corrupted artifact").unwrap();
        assert!(install_missing(&root.join("no-archive"), data, false).is_err());
        fs::write(&library, content).unwrap();
        sinomo_module::remove_native_dylib_module_loader_state(data, &state.module_id).unwrap();
        install_missing(&root.join("no-archive"), data, false).unwrap();
        assert!(
            sinomo_module::read_native_dylib_module_loader_state(data, &state.module_id)
                .unwrap()
                .is_none()
        );
    }
}
