use crate::internal::*;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct SandboxWasmModuleArtifactRegistry {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<SandboxWasmModuleArtifact>,
}

impl SandboxWasmModuleArtifactRegistry {
    pub fn artifact(&self, artifact_id: &ArtifactId) -> Option<&SandboxWasmModuleArtifact> {
        self.artifacts
            .iter()
            .find(|artifact| artifact.artifact_id == *artifact_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasmModuleArtifact {
    pub artifact_id: ArtifactId,
    pub wasm_path: PathBuf,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface_version: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub component_model: bool,
    #[serde(default = "default_sandbox_wasi_abi")]
    pub abi: SandboxWasiAbi,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_id: Option<SandboxId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<SandboxPath>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub limits: SandboxResourceLimits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

pub fn sandbox_wasm_module_artifact_registry_path(data_dir_root: &Path) -> PathBuf {
    data_dir_root
        .join("sandbox")
        .join("module-wasm-artifacts.sobj")
}

pub fn read_sandbox_wasm_module_artifact_registry(
    data_dir_root: &Path,
) -> Result<Option<SandboxWasmModuleArtifactRegistry>, SandboxStoreError> {
    let path = sandbox_wasm_module_artifact_registry_path(data_dir_root);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path).map_err(|source| SandboxStoreError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    sinomo_object_serde::from_bytes(&bytes)
        .map(Some)
        .map_err(|source| SandboxStoreError::Object {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

pub fn write_sandbox_wasm_module_artifact_registry(
    data_dir_root: &Path,
    registry: &SandboxWasmModuleArtifactRegistry,
) -> Result<PathBuf, SandboxStoreError> {
    let path = sandbox_wasm_module_artifact_registry_path(data_dir_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| SandboxStoreError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let bytes = sinomo_object_serde::to_bytes(
        registry,
        SANDBOX_MODULE_ARTIFACT_REGISTRY_OBJECT_VERSION,
        LengthMode::U32,
    )
    .map_err(|source| SandboxStoreError::Object {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("module-wasm-artifacts.sobj");
    let temp_path = parent.join(format!(".{file_name}.tmp-{}", std::process::id()));
    fs::write(&temp_path, bytes).map_err(|source| SandboxStoreError::Io {
        path: temp_path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::rename(&temp_path, &path).map_err(|source| SandboxStoreError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_wasm_module_registry_roundtrips_from_data_dir() {
        let root = unique_temp("sandbox-wasm-module-registry");
        let artifact_id = ArtifactId::from_legacy_named("artifact_wasm").expect("artifact");
        let registry = SandboxWasmModuleArtifactRegistry {
            artifacts: vec![SandboxWasmModuleArtifact {
                artifact_id: artifact_id.clone(),
                wasm_path: root.join("artifact.wasm"),
                content_hash: sandbox_module_hash(b"wasm bytes"),
                interface_version: None,
                component_model: false,
                abi: SandboxWasiAbi::Wasix,
                sandbox_id: None,
                argv: vec!["module.wasm".to_string()],
                cwd: None,
                env: BTreeMap::new(),
                limits: SandboxResourceLimits::default(),
                timeout_ms: Some(1_000),
            }],
        };

        let path =
            write_sandbox_wasm_module_artifact_registry(&root, &registry).expect("write registry");
        let read_back = read_sandbox_wasm_module_artifact_registry(&root)
            .expect("read registry")
            .expect("registry");
        let expected_hash = sandbox_module_hash(b"wasm bytes");

        assert_eq!(path, sandbox_wasm_module_artifact_registry_path(&root));
        assert_eq!(
            read_back
                .artifact(&artifact_id)
                .map(|artifact| artifact.content_hash.as_str()),
            Some(expected_hash.as_str())
        );
        assert_eq!(read_back, registry);
    }

    fn unique_temp(prefix: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(prefix)
    }
}
