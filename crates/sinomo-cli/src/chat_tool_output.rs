//! Project tool evidence into inspectable files without adding assistant messages.

use super::*;
use sinomo_runtime::api::app::chat::{ChatToolOutputFile, ChatToolOutputSnapshot};
use std::io::Read;

const MAX_FILE_BYTES: u64 = 128 * 1024 * 1024;

pub(super) fn snapshot(
    store: &dyn AgentChannelStore,
    data_dir: &SinomoDataDir,
    channel_id: &SessionId,
    event: AgentToolProgressEvent,
) -> Option<ChatToolOutputSnapshot> {
    let AgentToolProgressEvent::Artifacts {
        tool_call_id,
        step_index,
        call_index,
        call,
        status,
        artifacts,
    } = event
    else {
        return None;
    };
    let files = artifacts
        .into_iter()
        .map(|artifact| {
            let file_name = attachment_file_name(Path::new(&artifact.uri));
            let mut file = ChatToolOutputFile {
                path: String::new(),
                label: artifact.label.clone().unwrap_or_else(|| file_name.clone()),
                file_name,
                media_type: artifact.media_type.clone(),
                size_bytes: 0,
                error: None,
            };
            match read_artifact(data_dir, &artifact.uri).and_then(|bytes| {
                let path = store
                    .save_attachment(channel_id, &TaskId::generate(), &file.file_name, &bytes)
                    .map_err(|error| error.to_string())?;
                Ok((path, bytes.len() as u64))
            }) {
                Ok((path, size)) => {
                    file.path = path;
                    file.size_bytes = size;
                }
                Err(error) => file.error = Some(error),
            }
            file
        })
        .collect();
    Some(ChatToolOutputSnapshot {
        tool_call_id: tool_call_id.map(|id| id.to_string()),
        call: sinomo_runtime::api::app::chat::ChatToolCallSnapshot {
            step_index,
            call_index: Some(call_index),
            call_id: call.call_id.clone(),
            tool: call.tool,
            input: json_display(&call.input),
            input_preview: json_preview(&call.input, 220),
        },
        status: match status {
            AgentToolStatus::Succeeded => "completed",
            AgentToolStatus::Failed => "failed",
            AgentToolStatus::Blocked => "blocked",
            AgentToolStatus::Cancelled => "cancelled",
        }
        .to_string(),
        files,
    })
}

fn read_artifact(data_dir: &SinomoDataDir, uri: &str) -> Result<Vec<u8>, String> {
    if let Some(path) = uri.strip_prefix("sifs:") {
        let browser = SifsBrowser::open(data_dir)?;
        let details = browser.stat(&format!("/{}", path.trim_start_matches('/')))?;
        let size = details.size_bytes.ok_or("File size is unavailable")?;
        if details.kind != "file" || size > MAX_FILE_BYTES {
            return Err("File is not a regular file or exceeds 128 MiB".into());
        }
        let hash = details
            .content_hash
            .as_deref()
            .ok_or("File content hash is unavailable")?;
        let bytes =
            browser.read_object_version(&details.object_id, &details.version, hash, size)?;
        if bytes.len() as u64 != size {
            return Err("File snapshot size changed".into());
        }
        return Ok(bytes);
    }
    let path = Path::new(uri);
    if !path.is_absolute() {
        return Err("File preview requires an absolute artifact path or a SIFS URI".into());
    }
    let resolved = path.canonicalize().map_err(|error| error.to_string())?;
    let root = data_dir
        .root()
        .join("events/agent/evidence")
        .canonicalize()
        .map_err(|error| error.to_string())?;
    if !resolved.starts_with(root) {
        return Err("Artifact is outside the tool evidence directory".into());
    }
    let file = fs::File::open(resolved).map_err(|error| error.to_string())?;
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() || metadata.len() > MAX_FILE_BYTES {
        return Err("File is not a regular file or exceeds 128 MiB".into());
    }
    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err("File exceeds 128 MiB".into());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_output_preview_preserves_call_identity_and_isolates_file_errors() {
        let root = sinomo_testdir::TestDir::new("tool-output-preview");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let evidence = data_dir.root().join("events/agent/evidence");
        fs::create_dir_all(&evidence).unwrap();
        let store = SifsAgentChannelStore::open(&data_dir).unwrap();
        let channel_id = SessionId::generate();
        store
            .save_channel(&AgentChannel::new(channel_id.clone(), "Preview"))
            .unwrap();
        let mut artifacts = Vec::new();
        for (name, media_type) in [
            ("image.png", "image/png"),
            ("audio.wav", "audio/wav"),
            ("scene.glb", "model/gltf-binary"),
        ] {
            let path = evidence.join(name);
            fs::write(&path, b"saved artifact").unwrap();
            artifacts.push(
                sinomo_agent::AgentToolArtifactRef::new("file", path.display().to_string())
                    .with_media_type(media_type),
            );
        }
        artifacts.push(sinomo_agent::AgentToolArtifactRef::new(
            "file",
            evidence.join("missing.png").display().to_string(),
        ));
        let output = snapshot(
            &store,
            &data_dir,
            &channel_id,
            AgentToolProgressEvent::Artifacts {
                tool_call_id: None,
                step_index: 2,
                call_index: 1,
                call: AgentToolCall {
                    call_id: Some("native-call".into()),
                    tool: "generate_files".into(),
                    input: serde_json::json!({}),
                    r#async: false,
                    summary: String::new(),
                },
                status: AgentToolStatus::Succeeded,
                artifacts,
            },
        )
        .unwrap();
        assert_eq!(output.call.call_id.as_deref(), Some("native-call"));
        assert_eq!(output.call.step_index, 2);
        assert_eq!(output.call.call_index, Some(1));
        assert_eq!(output.files.len(), 4);
        for file in &output.files[..3] {
            assert!(file.error.is_none());
            assert_eq!(
                SifsBrowser::open(&data_dir)
                    .unwrap()
                    .read_file(&file.path)
                    .unwrap(),
                b"saved artifact"
            );
        }
        assert!(output.files[3].error.is_some());
        assert!(
            store
                .load_channel(&channel_id)
                .unwrap()
                .unwrap()
                .transcript
                .is_empty(),
            "inspecting tool files must not create assistant messages"
        );
        let outside = root.join("private.txt");
        fs::write(&outside, b"private").unwrap();
        assert!(
            read_artifact(&data_dir, outside.to_str().unwrap())
                .unwrap_err()
                .contains("outside")
        );
    }
}
