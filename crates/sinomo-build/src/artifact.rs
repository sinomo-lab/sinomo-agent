use crate::{
    BuildArtifactKind, BuildError, BuildKey, BuildKeyInput, BuildPlan, BuildSource, Result,
};
use serde::{Deserialize, Serialize};
use sinomo_module::ModuleId;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Cursor, Read};
use std::path::{Component, Path, PathBuf};
use time::OffsetDateTime;

pub const BUILD_ARTIFACT_SET_SCHEMA_VERSION: u16 = 1;

/// Immutable inputs that let a downloaded artifact be traced back to the exact
/// source, target, dependency graph, and toolchain that produced it.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildProvenance {
    pub build_key: BuildKey,
    pub source: BuildSource,
    pub input: BuildKeyInput,
}

impl BuildProvenance {
    pub fn from_plan(plan: &BuildPlan) -> Result<Self> {
        plan.verify_key()?;
        Ok(Self {
            build_key: plan.key.clone(),
            source: plan.source.clone(),
            input: plan.input.clone(),
        })
    }

    pub fn validate(&self) -> Result<()> {
        BuildPlan {
            key: self.build_key.clone(),
            source: self.source.clone(),
            input: self.input.clone(),
        }
        .validate()
    }
}

/// Loader metadata needed to turn a native module artifact into an activation
/// request without consulting build-server-local state.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct NativeDylibArtifactMetadata {
    pub module_id: ModuleId,
    pub interface_version: String,
    pub cache_key: String,
    pub library_name: String,
    pub source: String,
}

impl NativeDylibArtifactMetadata {
    pub fn new(
        module_id: ModuleId,
        interface_version: impl Into<String>,
        cache_key: impl Into<String>,
        library_name: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            module_id,
            interface_version: interface_version.into(),
            cache_key: cache_key.into(),
            library_name: library_name.into(),
            source: source.into(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_text(&self.interface_version, "native module interface version")?;
        validate_text(&self.cache_key, "native module cache key")?;
        validate_file_name(&self.library_name, "native module library name")?;
        validate_text(&self.source, "native module source")
    }
}

/// A single independently downloadable artifact. Its provenance and optional
/// native-module loader metadata make `/build/artifact` responses self-contained.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildArtifactManifest {
    pub artifact_id: String,
    pub name: String,
    pub kind: BuildArtifactKind,
    pub relative_path: String,
    pub size_bytes: u64,
    pub content_hash: String,
    pub mode: u32,
    pub provenance: BuildProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dylib: Option<NativeDylibArtifactMetadata>,
}

impl BuildArtifactManifest {
    pub fn binary_from_file(
        artifact_id: impl Into<String>,
        name: impl Into<String>,
        relative_path: impl Into<String>,
        provenance: BuildProvenance,
        source_root: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::from_file(
            artifact_id,
            name,
            BuildArtifactKind::Binary,
            relative_path,
            provenance,
            None,
            source_root.as_ref(),
        )
    }

    pub fn native_dylib_from_file(
        artifact_id: impl Into<String>,
        name: impl Into<String>,
        relative_path: impl Into<String>,
        provenance: BuildProvenance,
        native_dylib: NativeDylibArtifactMetadata,
        source_root: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::from_file(
            artifact_id,
            name,
            BuildArtifactKind::NativeDylibModule,
            relative_path,
            provenance,
            Some(native_dylib),
            source_root.as_ref(),
        )
    }

    fn from_file(
        artifact_id: impl Into<String>,
        name: impl Into<String>,
        kind: BuildArtifactKind,
        relative_path: impl Into<String>,
        provenance: BuildProvenance,
        native_dylib: Option<NativeDylibArtifactMetadata>,
        source_root: &Path,
    ) -> Result<Self> {
        let artifact_id = artifact_id.into();
        let name = name.into();
        let relative_path = relative_path.into();
        validate_token(&artifact_id, "artifact id")?;
        validate_text(&name, "artifact name")?;
        let source_path = safe_source_file(source_root, &relative_path)?;
        let bytes = fs::read(&source_path).map_err(|source| BuildError::Io {
            path: source_path.display().to_string(),
            detail: source.to_string(),
        })?;
        let metadata = fs::metadata(&source_path).map_err(|source| BuildError::Io {
            path: source_path.display().to_string(),
            detail: source.to_string(),
        })?;
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode() & 0o777
        };
        #[cfg(not(unix))]
        let mode = 0o755;
        let manifest = Self {
            artifact_id,
            name,
            kind,
            relative_path,
            size_bytes: bytes.len() as u64,
            content_hash: build_content_hash(&bytes),
            mode,
            provenance,
            native_dylib,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        self.provenance.validate()?;
        validate_token(&self.artifact_id, "artifact id")?;
        validate_text(&self.name, "artifact name")?;
        validate_relative_artifact_path(&self.relative_path)?;
        validate_blake3_hash(&self.content_hash, "artifact content hash")?;
        if self.mode & !0o777 != 0 {
            return Err(BuildError::InvalidValue {
                detail: format!("artifact mode {:o} contains non-permission bits", self.mode),
            });
        }
        match (self.kind, &self.native_dylib) {
            (BuildArtifactKind::Binary, None) => {}
            (BuildArtifactKind::NativeDylibModule, Some(metadata)) => metadata.validate()?,
            (BuildArtifactKind::Binary, Some(_)) => {
                return Err(BuildError::InvalidValue {
                    detail: "binary artifact must not carry native-dylib metadata".to_string(),
                });
            }
            (BuildArtifactKind::NativeDylibModule, None) => {
                return Err(BuildError::InvalidValue {
                    detail: "native-dylib artifact requires loader metadata".to_string(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildArtifactSetManifest {
    pub schema_version: u16,
    pub provenance: BuildProvenance,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<BuildArtifactManifest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_bytes: Option<u64>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl BuildArtifactSetManifest {
    pub fn new(provenance: BuildProvenance, artifacts: Vec<BuildArtifactManifest>) -> Result<Self> {
        let manifest = Self {
            schema_version: BUILD_ARTIFACT_SET_SCHEMA_VERSION,
            provenance,
            artifacts,
            bundle_hash: None,
            bundle_bytes: None,
            created_at: OffsetDateTime::now_utc(),
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != BUILD_ARTIFACT_SET_SCHEMA_VERSION {
            return Err(BuildError::InvalidValue {
                detail: format!(
                    "unsupported build artifact-set schema version {}",
                    self.schema_version
                ),
            });
        }
        if self.artifacts.is_empty() {
            return Err(BuildError::InvalidValue {
                detail: "build artifact set must contain at least one artifact".to_string(),
            });
        }
        self.provenance.validate()?;
        let mut ids = BTreeSet::new();
        let mut paths = BTreeSet::new();
        for artifact in &self.artifacts {
            artifact.validate()?;
            if artifact.provenance != self.provenance {
                return Err(BuildError::InvalidValue {
                    detail: format!(
                        "artifact {} provenance differs from its artifact set",
                        artifact.artifact_id
                    ),
                });
            }
            if !ids.insert(&artifact.artifact_id) {
                return Err(BuildError::InvalidValue {
                    detail: format!("duplicate artifact id {}", artifact.artifact_id),
                });
            }
            if !paths.insert(&artifact.relative_path) {
                return Err(BuildError::InvalidValue {
                    detail: format!("duplicate artifact path {}", artifact.relative_path),
                });
            }
        }
        match (&self.bundle_hash, self.bundle_bytes) {
            (None, None) => {}
            (Some(hash), Some(_)) => validate_blake3_hash(hash, "artifact bundle hash")?,
            _ => {
                return Err(BuildError::InvalidValue {
                    detail: "artifact bundle hash and size must either both be present or both be absent"
                        .to_string(),
                });
            }
        }
        Ok(())
    }

    pub fn artifact(&self, artifact_id: &str) -> Option<&BuildArtifactManifest> {
        self.artifacts
            .iter()
            .find(|artifact| artifact.artifact_id == artifact_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildArtifactBundle {
    pub manifest: BuildArtifactSetManifest,
    pub bytes: Vec<u8>,
}

/// Produces a deterministic tar stream. Only manifest-listed regular files are
/// included, and every file is re-hashed before the bundle is returned.
pub fn create_artifact_bundle(
    mut manifest: BuildArtifactSetManifest,
    source_root: impl AsRef<Path>,
) -> Result<BuildArtifactBundle> {
    manifest.bundle_hash = None;
    manifest.bundle_bytes = None;
    manifest.validate()?;
    let source_root = source_root.as_ref();
    let mut artifacts = manifest.artifacts.iter().collect::<Vec<_>>();
    artifacts.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));

    let mut builder = tar::Builder::new(Vec::new());
    for artifact in artifacts {
        let source_path = safe_source_file(source_root, &artifact.relative_path)?;
        let bytes = fs::read(&source_path).map_err(|source| BuildError::Io {
            path: source_path.display().to_string(),
            detail: source.to_string(),
        })?;
        verify_artifact_bytes(artifact, &bytes)?;
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(bytes.len() as u64);
        header.set_mode(artifact.mode);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_cksum();
        builder
            .append_data(&mut header, &artifact.relative_path, Cursor::new(bytes))
            .map_err(|source| BuildError::InvalidBundle {
                detail: format!(
                    "could not append artifact {}: {source}",
                    artifact.relative_path
                ),
            })?;
    }
    builder
        .finish()
        .map_err(|source| BuildError::InvalidBundle {
            detail: format!("could not finish artifact bundle: {source}"),
        })?;
    let bytes = builder
        .into_inner()
        .map_err(|source| BuildError::InvalidBundle {
            detail: format!("could not finalize artifact bundle: {source}"),
        })?;
    manifest.bundle_hash = Some(build_content_hash(&bytes));
    manifest.bundle_bytes = Some(bytes.len() as u64);
    Ok(BuildArtifactBundle { manifest, bytes })
}

/// Verifies bundle identity, membership, per-file size, and per-file content.
/// No filesystem changes are made.
pub fn verify_artifact_bundle(
    manifest: &BuildArtifactSetManifest,
    bundle_bytes: &[u8],
) -> Result<()> {
    manifest.validate()?;
    let expected_hash =
        manifest
            .bundle_hash
            .as_deref()
            .ok_or_else(|| BuildError::InvalidBundle {
                detail: "artifact-set manifest has no bundle hash".to_string(),
            })?;
    let expected_size = manifest
        .bundle_bytes
        .ok_or_else(|| BuildError::InvalidBundle {
            detail: "artifact-set manifest has no bundle size".to_string(),
        })?;
    if bundle_bytes.len() as u64 != expected_size {
        return Err(BuildError::InvalidBundle {
            detail: format!(
                "bundle size mismatch: expected {expected_size}, got {}",
                bundle_bytes.len()
            ),
        });
    }
    let actual_hash = build_content_hash(bundle_bytes);
    if actual_hash != expected_hash {
        return Err(BuildError::HashMismatch {
            artifact: "artifact bundle".to_string(),
            expected: expected_hash.to_string(),
            actual: actual_hash,
        });
    }
    verified_bundle_files(manifest, bundle_bytes).map(|_| ())
}

/// Extracts a bundle only after every entry has been validated in memory.
/// Absolute paths, parent traversal, links, devices, unlisted files, duplicate
/// entries, and destination symlink traversal are rejected.
pub fn extract_artifact_bundle(
    manifest: &BuildArtifactSetManifest,
    bundle_bytes: &[u8],
    destination: impl AsRef<Path>,
) -> Result<Vec<PathBuf>> {
    verify_artifact_bundle(manifest, bundle_bytes)?;
    let files = verified_bundle_files(manifest, bundle_bytes)?;
    let destination = destination.as_ref();
    fs::create_dir_all(destination).map_err(|source| BuildError::Io {
        path: destination.display().to_string(),
        detail: source.to_string(),
    })?;
    let destination = fs::canonicalize(destination).map_err(|source| BuildError::Io {
        path: destination.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut extracted = Vec::with_capacity(files.len());
    for (relative_path, bytes) in files {
        let output = safe_destination_file(&destination, &relative_path)?;
        fs::write(&output, bytes).map_err(|source| BuildError::Io {
            path: output.display().to_string(),
            detail: source.to_string(),
        })?;
        if let Some(artifact) = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.relative_path == relative_path)
        {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&output, fs::Permissions::from_mode(artifact.mode)).map_err(
                    |source| BuildError::Io {
                        path: output.display().to_string(),
                        detail: source.to_string(),
                    },
                )?;
            }
        }
        extracted.push(output);
    }
    Ok(extracted)
}

pub fn build_content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

pub fn verify_artifact_bytes(manifest: &BuildArtifactManifest, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 != manifest.size_bytes {
        return Err(BuildError::InvalidBundle {
            detail: format!(
                "artifact {} size mismatch: expected {}, got {}",
                manifest.artifact_id,
                manifest.size_bytes,
                bytes.len()
            ),
        });
    }
    let actual = build_content_hash(bytes);
    if actual != manifest.content_hash {
        return Err(BuildError::HashMismatch {
            artifact: manifest.artifact_id.clone(),
            expected: manifest.content_hash.clone(),
            actual,
        });
    }
    Ok(())
}

/// Streams an artifact from disk while verifying its exact size and content
/// hash, so downloaded binaries do not need to be buffered in memory.
pub fn verify_artifact_file(
    manifest: &BuildArtifactManifest,
    path: impl AsRef<Path>,
) -> Result<()> {
    let path = path.as_ref();
    let mut file = File::open(path).map_err(|source| BuildError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let size_bytes = file
        .metadata()
        .map_err(|source| BuildError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?
        .len();
    if size_bytes != manifest.size_bytes {
        return Err(BuildError::InvalidBundle {
            detail: format!(
                "artifact {} size mismatch: expected {}, got {size_bytes}",
                manifest.artifact_id, manifest.size_bytes
            ),
        });
    }
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|source| BuildError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual = format!("blake3:{}", hasher.finalize().to_hex());
    if actual != manifest.content_hash {
        return Err(BuildError::HashMismatch {
            artifact: manifest.artifact_id.clone(),
            expected: manifest.content_hash.clone(),
            actual,
        });
    }
    Ok(())
}

fn verified_bundle_files(
    manifest: &BuildArtifactSetManifest,
    bundle_bytes: &[u8],
) -> Result<Vec<(String, Vec<u8>)>> {
    let expected = manifest
        .artifacts
        .iter()
        .map(|artifact| (artifact.relative_path.as_str(), artifact))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    let mut files = Vec::with_capacity(expected.len());
    let mut archive = tar::Archive::new(Cursor::new(bundle_bytes));
    let entries = archive
        .entries()
        .map_err(|source| BuildError::InvalidBundle {
            detail: format!("could not read artifact bundle: {source}"),
        })?;
    for entry in entries {
        let entry = entry.map_err(|source| BuildError::InvalidBundle {
            detail: format!("could not read artifact bundle entry: {source}"),
        })?;
        if !entry.header().entry_type().is_file() {
            return Err(BuildError::InvalidBundle {
                detail: "artifact bundle contains a non-file entry".to_string(),
            });
        }
        let path = entry.path().map_err(|source| BuildError::InvalidBundle {
            detail: format!("artifact bundle path is invalid: {source}"),
        })?;
        let relative_path = path
            .to_str()
            .ok_or_else(|| BuildError::InvalidBundle {
                detail: "artifact bundle path is not UTF-8".to_string(),
            })?
            .to_string();
        validate_relative_artifact_path(&relative_path)?;
        let artifact =
            expected
                .get(relative_path.as_str())
                .ok_or_else(|| BuildError::InvalidBundle {
                    detail: format!("artifact bundle contains unlisted path {relative_path}"),
                })?;
        if !seen.insert(relative_path.clone()) {
            return Err(BuildError::InvalidBundle {
                detail: format!("artifact bundle repeats path {relative_path}"),
            });
        }
        let mut bytes = Vec::new();
        entry
            .take(artifact.size_bytes.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|source| BuildError::InvalidBundle {
                detail: format!("could not read artifact {relative_path}: {source}"),
            })?;
        verify_artifact_bytes(artifact, &bytes)?;
        files.push((relative_path, bytes));
    }
    if seen.len() != expected.len() {
        let missing = expected
            .keys()
            .filter(|path| !seen.contains(**path))
            .copied()
            .collect::<Vec<_>>();
        return Err(BuildError::InvalidBundle {
            detail: format!("artifact bundle is missing: {}", missing.join(", ")),
        });
    }
    Ok(files)
}

fn safe_source_file(source_root: &Path, relative_path: &str) -> Result<PathBuf> {
    validate_relative_artifact_path(relative_path)?;
    let root = fs::canonicalize(source_root).map_err(|source| BuildError::Io {
        path: source_root.display().to_string(),
        detail: source.to_string(),
    })?;
    let candidate = root.join(relative_path);
    let metadata = fs::symlink_metadata(&candidate).map_err(|source| BuildError::Io {
        path: candidate.display().to_string(),
        detail: source.to_string(),
    })?;
    if !metadata.file_type().is_file() {
        return Err(BuildError::InvalidValue {
            detail: format!(
                "artifact source {} is not a regular file",
                candidate.display()
            ),
        });
    }
    let canonical = fs::canonicalize(&candidate).map_err(|source| BuildError::Io {
        path: candidate.display().to_string(),
        detail: source.to_string(),
    })?;
    if !canonical.starts_with(&root) {
        return Err(BuildError::InvalidValue {
            detail: format!("artifact source {} escapes its root", candidate.display()),
        });
    }
    Ok(canonical)
}

fn safe_destination_file(root: &Path, relative_path: &str) -> Result<PathBuf> {
    validate_relative_artifact_path(relative_path)?;
    let relative = Path::new(relative_path);
    let mut current = root.to_path_buf();
    if let Some(parent) = relative.parent() {
        for component in parent.components() {
            let Component::Normal(segment) = component else {
                return Err(BuildError::InvalidBundle {
                    detail: format!("unsafe artifact destination path {relative_path}"),
                });
            };
            current.push(segment);
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_dir() => {}
                Ok(_) => {
                    return Err(BuildError::InvalidBundle {
                        detail: format!(
                            "artifact destination ancestor {} is not a directory",
                            current.display()
                        ),
                    });
                }
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&current).map_err(|source| BuildError::Io {
                        path: current.display().to_string(),
                        detail: source.to_string(),
                    })?;
                }
                Err(source) => {
                    return Err(BuildError::Io {
                        path: current.display().to_string(),
                        detail: source.to_string(),
                    });
                }
            }
            let canonical = fs::canonicalize(&current).map_err(|source| BuildError::Io {
                path: current.display().to_string(),
                detail: source.to_string(),
            })?;
            if !canonical.starts_with(root) {
                return Err(BuildError::InvalidBundle {
                    detail: format!(
                        "artifact destination ancestor {} escapes extraction root",
                        current.display()
                    ),
                });
            }
        }
    }
    let output = root.join(relative);
    if let Ok(metadata) = fs::symlink_metadata(&output)
        && !metadata.file_type().is_file()
    {
        return Err(BuildError::InvalidBundle {
            detail: format!(
                "artifact destination {} is not a regular file",
                output.display()
            ),
        });
    }
    Ok(output)
}

fn validate_relative_artifact_path(value: &str) -> Result<()> {
    if value.is_empty() || value.contains('\\') || value.ends_with('/') || value.contains("//") {
        return Err(BuildError::InvalidValue {
            detail: format!("artifact path `{value}` must be a normalized relative path"),
        });
    }
    let path = Path::new(value);
    if value
        .split('/')
        .any(|segment| segment == "." || segment == "..")
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(BuildError::InvalidValue {
            detail: format!("artifact path `{value}` must not escape its root"),
        });
    }
    Ok(())
}

fn validate_token(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(BuildError::InvalidValue {
            detail: format!("{label} `{value}` must be a non-empty path-safe token"),
        });
    }
    Ok(())
}

fn validate_file_name(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value == "."
        || value == ".."
    {
        return Err(BuildError::InvalidValue {
            detail: format!("{label} `{value}` must be a file name"),
        });
    }
    Ok(())
}

fn validate_text(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(BuildError::InvalidValue {
            detail: format!("{label} must not be empty"),
        });
    }
    Ok(())
}

fn validate_blake3_hash(value: &str, label: &str) -> Result<()> {
    let digest = value
        .strip_prefix("blake3:")
        .ok_or_else(|| BuildError::InvalidValue {
            detail: format!("{label} must use the blake3:<hex> format"),
        })?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(BuildError::InvalidValue {
            detail: format!("{label} must contain a 64-character BLAKE3 digest"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BuildKeyInput, BuildOutputRequest, BuildTarget};
    use sinomo_testdir::TestDir;

    fn provenance() -> BuildProvenance {
        let source_hash = build_content_hash(b"source bundle");
        let input = BuildKeyInput {
            source_hash: source_hash.clone(),
            source_revision: Some("deadbeef".to_string()),
            lockfile_hash: build_content_hash(b"lockfile"),
            target: BuildTarget::new("linux", "x86_64")
                .expect("target")
                .with_family("unix")
                .expect("family")
                .with_rust_target("x86_64-unknown-linux-gnu")
                .expect("triple"),
            profile: "release".to_string(),
            features: vec!["cluster".to_string()],
            toolchain_fingerprint: "rustc-test".to_string(),
            outputs: vec![BuildOutputRequest::binary("sinomo", "sinomo")],
        };
        let plan = BuildPlan::new(
            BuildSource {
                label: "test source".to_string(),
                revision: Some("deadbeef".to_string()),
                bundle_hash: source_hash,
                bundle_bytes: 13,
            },
            input,
        )
        .expect("plan");
        BuildProvenance::from_plan(&plan).expect("provenance")
    }

    #[test]
    fn artifact_bundle_round_trip_is_verified() {
        let root = TestDir::new("sinomo-build-bundle");
        let source = root.join("source");
        let output = root.join("output");
        fs::create_dir_all(source.join("bin")).expect("bin directory");
        fs::write(source.join("bin/sinomo"), b"verified binary").expect("binary");
        let artifact = BuildArtifactManifest::binary_from_file(
            "sinomo-linux-x86_64",
            "sinomo",
            "bin/sinomo",
            provenance(),
            &source,
        )
        .expect("artifact");
        let set = BuildArtifactSetManifest::new(provenance(), vec![artifact]).expect("set");

        let bundle = create_artifact_bundle(set, &source).expect("bundle");
        verify_artifact_bundle(&bundle.manifest, &bundle.bytes).expect("verify");
        let paths =
            extract_artifact_bundle(&bundle.manifest, &bundle.bytes, &output).expect("extract");

        assert_eq!(paths, vec![output.join("bin/sinomo")]);
        assert_eq!(fs::read(&paths[0]).expect("read"), b"verified binary");
        verify_artifact_file(&bundle.manifest.artifacts[0], &paths[0])
            .expect("streaming file verification");
    }

    #[test]
    fn artifact_bundle_hash_mismatch_is_rejected() {
        let root = TestDir::new("sinomo-build-bundle-hash");
        fs::write(root.join("sinomo"), b"binary").expect("binary");
        let artifact = BuildArtifactManifest::binary_from_file(
            "sinomo",
            "sinomo",
            "sinomo",
            provenance(),
            &root,
        )
        .expect("artifact");
        let set = BuildArtifactSetManifest::new(provenance(), vec![artifact]).expect("set");
        let mut bundle = create_artifact_bundle(set, &root).expect("bundle");
        bundle.bytes[0] ^= 0xff;

        assert!(matches!(
            verify_artifact_bundle(&bundle.manifest, &bundle.bytes),
            Err(BuildError::HashMismatch { .. })
        ));
    }

    #[test]
    fn traversal_paths_are_rejected_before_file_access() {
        let result = BuildArtifactManifest::binary_from_file(
            "escape",
            "escape",
            "../escape",
            provenance(),
            TestDir::new("sinomo-build-traversal"),
        );
        assert!(matches!(result, Err(BuildError::InvalidValue { .. })));
    }

    #[test]
    fn empty_artifact_sets_are_rejected() {
        assert!(matches!(
            BuildArtifactSetManifest::new(provenance(), Vec::new()),
            Err(BuildError::InvalidValue { .. })
        ));
    }

    #[test]
    fn native_module_manifest_is_self_contained() {
        let root = TestDir::new("sinomo-build-native-module");
        fs::write(root.join("libexample.so"), b"module").expect("module");
        let module_id = ModuleId::from_legacy_named("module.example").expect("module id");
        let artifact = BuildArtifactManifest::native_dylib_from_file(
            "example-module",
            "example",
            "libexample.so",
            provenance(),
            NativeDylibArtifactMetadata::new(
                module_id.clone(),
                "sinomo.native-module.v1",
                "cache-example",
                "libexample.so",
                "build:test",
            ),
            &root,
        )
        .expect("artifact");

        assert_eq!(
            artifact
                .native_dylib
                .as_ref()
                .map(|metadata| &metadata.module_id),
            Some(&module_id)
        );
        assert_eq!(
            artifact.provenance.source.revision.as_deref(),
            Some("deadbeef")
        );
    }
}
