//! Shared artifact emission around ordinary mounted runtime ownership.
use super::{NativeShardTraceRequest, NativeTraceArtifactBindingV1, request_native_shard_trace};
use crate::DfsMetadataShardId;
use sinomo_core::node::NodeId;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const OUTPUT_ENV: &str = "SINOMO_DFS_NATIVE_TRACE_OUTPUT_DIR";
const BINDING_ENV: &str = "SINOMO_DFS_NATIVE_TRACE_BINDING_JSON";
const MAX_FINITE_NODES: usize = 4;

/// Scoped opt-in artifact requests for existing normal runtime nodes. The
/// requests mint neither storage custody nor publication authority.
pub struct NativeShardTraceArtifactSession {
    case_id: String,
    output: PathBuf,
    actual_executable: PathBuf,
    binding: NativeTraceArtifactBindingV1,
    requests: Vec<(NodeId, PathBuf, NativeShardTraceRequest)>,
}

impl NativeShardTraceArtifactSession {
    /// Call before normal startup with the actual node data roots. The runner
    /// supplies immutable build hashes; the independent checker verifies them
    /// against its frozen source/build/executable artifacts.
    pub fn from_environment(
        case_id: &str,
        roots: impl IntoIterator<Item = (NodeId, PathBuf)>,
        shard: DfsMetadataShardId,
    ) -> Result<Option<Self>, String> {
        let (output, binding_path) =
            match (std::env::var_os(OUTPUT_ENV), std::env::var_os(BINDING_ENV)) {
                (None, None) => return Ok(None),
                (Some(output), Some(binding)) => (PathBuf::from(output), PathBuf::from(binding)),
                _ => {
                    return Err(format!(
                        "{OUTPUT_ENV} and {BINDING_ENV} must both be supplied"
                    ));
                }
            };
        if case_id.is_empty()
            || case_id.len() > 128
            || !case_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err("native trace artifact case ID is not a safe filename component".into());
        }
        let roots = roots.into_iter().collect::<Vec<_>>();
        if roots.is_empty() || roots.len() > MAX_FINITE_NODES {
            return Err(
                "native trace artifact requires one to four actual finite node roots".into(),
            );
        }
        let mut nodes = std::collections::BTreeSet::new();
        let mut paths = std::collections::BTreeSet::new();
        for (node, root) in &roots {
            if !root.is_absolute() || !nodes.insert(node.clone()) || !paths.insert(root.clone()) {
                return Err(
                    "native trace artifact has a relative or duplicate node/root binding".into(),
                );
            }
        }
        // This is explicit artifact setup, never an observation-triggered
        // metadata/KV read. The binding is read exactly once.
        let binding: NativeTraceArtifactBindingV1 = serde_json::from_slice(
            &std::fs::read(&binding_path)
                .map_err(|error| format!("read native trace binding: {error}"))?,
        )
        .map_err(|error| format!("decode native trace binding: {error}"))?;
        if binding.implementation_revision.is_empty() || binding.implementation_build_id.is_empty()
        {
            return Err("native trace binding lacks source/build identity".into());
        }
        for hash in [
            &binding.executable_sha256,
            &binding.source_manifest_sha256,
            &binding.schema_sha256,
            &binding.model_sha256,
        ] {
            if hash.len() != 64
                || !hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            {
                return Err("native trace binding contains an invalid SHA256".into());
            }
        }
        let actual_executable = std::env::current_exe().map_err(|error| error.to_string())?;
        let requests = roots
            .into_iter()
            .map(|(node, root)| {
                request_native_shard_trace(&root, shard.clone())
                    .map(|request| (node, root, request))
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(Self {
            case_id: case_id.to_owned(),
            output: output.join(case_id),
            actual_executable,
            binding,
            requests,
        }))
    }

    /// Stop new capture admission before ordinary runtime shutdown. Existing
    /// native writer/guard tickets remain retained until their real release.
    pub fn close(&self) -> Result<(), String> {
        for (_, _, request) in &self.requests {
            request
                .capture()
                .map_err(|error| error.to_string())?
                .close();
        }
        Ok(())
    }

    /// Call after ordinary runtime shutdown. Exported raw counters prove or
    /// reject actual drain; this helper does not assert that shutdown happened.
    pub fn export(self) -> Result<PathBuf, String> {
        std::fs::create_dir_all(&self.output).map_err(|error| error.to_string())?;
        let mut captures = Vec::new();
        for (node, root, request) in &self.requests {
            let capture = request.capture().map_err(|error| error.to_string())?;
            let raw_name = format!("{}.raw.json", node.as_str());
            let header_name = format!("{}.header.json", node.as_str());
            let mut raw = create(&self.output.join(&raw_name))?;
            capture
                .export_raw(&mut raw)
                .map_err(|error| error.to_string())?;
            raw.flush().map_err(|error| error.to_string())?;
            let mut header = create(&self.output.join(&header_name))?;
            capture
                .export_native_header(&self.binding, &mut header)
                .map_err(|error| error.to_string())?;
            header.flush().map_err(|error| error.to_string())?;
            captures.push(
                serde_json::json!({"node_id": node, "actual_data_root": root,
                "raw_file": raw_name, "header_file": header_name}),
            );
        }
        let mut manifest = create(&self.output.join("capture-manifest.json"))?;
        serde_json::to_writer_pretty(&mut manifest,
            &serde_json::json!({"artifact_version": 1, "case_id": self.case_id,
                "actual_executable": self.actual_executable, "artifact_binding": self.binding,
                "captures": captures, "classification": "actual raw capture; conformance not yet checked"}))
            .map_err(|error| error.to_string())?;
        manifest.flush().map_err(|error| error.to_string())?;
        Ok(self.output)
    }
}

fn create(path: &Path) -> Result<BufWriter<std::fs::File>, String> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map(BufWriter::new)
        .map_err(|error| format!("create native trace artifact {}: {error}", path.display()))
}
