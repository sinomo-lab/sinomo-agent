//! Derive preview references from assistant Markdown without reading or copying files.

use std::collections::HashMap;

use percent_encoding::percent_decode_str;
use pulldown_cmark::{Event, Parser, Tag, TagEnd};
use sinomo_client::chat_contract::ChatToolOutputFile;

use super::files::FilesSourceKind;

const MAX_REFERENCES: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct MarkdownAttachment {
    pub source: FilesSourceKind,
    pub file: ChatToolOutputFile,
    pub image: bool,
}

pub(super) fn attachments(markdown: &str, workspaces: &[String]) -> Vec<MarkdownAttachment> {
    let mut pending: Vec<(String, String, bool)> = Vec::new();
    let mut references = Vec::new();
    for event in Parser::new(markdown) {
        match event {
            Event::Start(Tag::Link { dest_url, .. }) => {
                pending.push((dest_url.into_string(), String::new(), false));
            }
            Event::Start(Tag::Image { dest_url, .. }) => {
                pending.push((dest_url.into_string(), String::new(), true));
            }
            Event::Text(text) | Event::Code(text) => {
                for (_, label, _) in &mut pending {
                    label.push_str(&text);
                }
            }
            Event::End(TagEnd::Link | TagEnd::Image) => {
                if let Some((destination, label, image)) = pending.pop()
                    && let Some((source, path)) = resolve(&destination, workspaces)
                {
                    references.push((source, path, label, image));
                }
            }
            _ => {}
        }
    }
    let mut seen = HashMap::new();
    let mut attachments: Vec<MarkdownAttachment> = Vec::new();
    for (source, path, label, image) in references {
        let key = if cfg!(windows) && source == FilesSourceKind::Local {
            path.to_lowercase()
        } else {
            path.clone()
        };
        let key = (source == FilesSourceKind::Sifs, key);
        if let Some(index) = seen.get(&key).copied() {
            let existing: &mut MarkdownAttachment = &mut attachments[index];
            existing.image |= image;
            continue;
        }
        if attachments.len() >= MAX_REFERENCES {
            continue;
        }
        seen.insert(key, attachments.len());
        let file_name = path.rsplit('/').next().unwrap_or(&path).to_string();
        let label = if label.trim().is_empty() {
            file_name.clone()
        } else {
            label
        };
        attachments.push(MarkdownAttachment {
            source,
            image,
            file: ChatToolOutputFile {
                media_type: media_type(&file_name).map(str::to_string),
                file_name,
                path,
                label,
                size_bytes: 0,
                error: None,
            },
        });
    }
    attachments
}

fn resolve(destination: &str, workspaces: &[String]) -> Option<(FilesSourceKind, String)> {
    // Split URL decorations before decoding so an encoded '#' can remain part of a filename.
    let destination = destination.split(['#', '?']).next()?;
    let decoded = percent_decode_str(destination).decode_utf8().ok()?;
    let path = decoded.replace('\\', "/");
    if path.is_empty()
        || path.chars().any(char::is_control)
        || path.ends_with('/')
        || path.starts_with("//")
    {
        return None;
    }
    if let Some(path) = path.strip_prefix("sifs:") {
        return normalize_absolute(&format!("/{}", path.trim_start_matches('/')))
            .map(|path| (FilesSourceKind::Sifs, path));
    }
    if let Some(path) = path.strip_prefix("file:") {
        let path = if let Some(path) = path.strip_prefix("//localhost/") {
            format!("/{path}")
        } else if path.starts_with("///") {
            path[2..].to_string()
        } else if path.starts_with("//") {
            return None;
        } else {
            path.to_string()
        };
        let path = if path.starts_with('/') && windows_absolute(&path[1..]) {
            &path[1..]
        } else {
            &path
        };
        return normalize_absolute(path).map(|path| (FilesSourceKind::Local, path));
    }
    // These are the virtual paths used by the agent's workspace mounts.
    let sandbox = path.starts_with("sandbox:");
    let path = path.strip_prefix("sandbox:").unwrap_or(&path);
    for (index, workspace) in workspaces.iter().enumerate() {
        let mount = if index == 0 {
            "/work".to_string()
        } else {
            format!("/workspaces/{}", index + 1)
        };
        if let Some(relative) = path.strip_prefix(&format!("{mount}/")) {
            return join_workspace(workspace, relative);
        }
    }
    if sandbox || path == "/work" || path.starts_with("/work/") || path.starts_with("/workspaces/")
    {
        return None;
    }
    if path.starts_with('/') || windows_absolute(path) {
        return normalize_absolute(path).map(|path| (FilesSourceKind::Local, path));
    }
    // Other URI schemes (including web links and data URLs) never trigger file reads.
    if path.split('/').next()?.contains(':') {
        return None;
    }
    join_workspace(workspaces.first()?, path)
}

fn join_workspace(workspace: &str, relative: &str) -> Option<(FilesSourceKind, String)> {
    let workspace = workspace.replace('\\', "/");
    let (source, root) = if let Some(path) = workspace.strip_prefix("sifs:") {
        (
            FilesSourceKind::Sifs,
            format!("/{}", path.trim_start_matches('/')),
        )
    } else {
        (FilesSourceKind::Local, workspace)
    };
    let root = normalize_absolute(&root)?;
    let path = normalize_absolute(&format!("{}/{relative}", root.trim_end_matches('/')))?;
    // Relative references are scoped to the selected workspace, including sandbox aliases.
    if path != root && !path.starts_with(&format!("{}/", root.trim_end_matches('/'))) {
        return None;
    }
    Some((source, path))
}

fn windows_absolute(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1..3] == *b":/"
}

fn normalize_absolute(path: &str) -> Option<String> {
    let (prefix, rest) = if windows_absolute(path) {
        (&path[..3], &path[3..])
    } else if let Some(rest) = path.strip_prefix('/') {
        ("/", rest)
    } else {
        return None;
    };
    let mut parts = Vec::new();
    for part in rest.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            _ => parts.push(part),
        }
    }
    Some(format!("{prefix}{}", parts.join("/")))
}

fn media_type(file_name: &str) -> Option<&'static str> {
    match file_name.rsplit('.').next()?.to_ascii_lowercase().as_str() {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "webp" => Some("image/webp"),
        "gif" => Some("image/gif"),
        "svg" => Some("image/svg+xml"),
        "pdf" => Some("application/pdf"),
        "wav" => Some("audio/wav"),
        "mp3" => Some("audio/mpeg"),
        "ogg" => Some("audio/ogg"),
        "mp4" => Some("video/mp4"),
        "webm" => Some("video/webm"),
        "glb" => Some("model/gltf-binary"),
        "gltf" => Some("model/gltf+json"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_attachments_parse_links_images_and_reference_definitions() {
        let files = attachments(
            "![**Picture**](<images/my picture.PNG>) [report][r] [again](images/my%20picture.PNG#full)\n\n[r]: report.pdf",
            &["/project".into()],
        );
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].file.path, "/project/images/my picture.PNG");
        assert_eq!(files[0].file.label, "Picture");
        assert_eq!(files[0].file.media_type.as_deref(), Some("image/png"));
        assert_eq!(files[1].file.path, "/project/report.pdf");
    }

    #[test]
    fn markdown_attachments_ignore_code_web_links_and_incomplete_references() {
        let markdown = "`![inline](image.png)`\n\n```md\n![fenced](image.png)\n```\n\n![remote](https://example.com/image.png) [mail](mailto:a@b.com) ![cdn](//example.com/image.png) [jump](#section) ![unfinished](image.png";
        assert!(attachments(markdown, &["/project".into()]).is_empty());
        assert!(attachments("[relative](file.txt)", &[]).is_empty());
    }

    #[test]
    fn markdown_attachments_resolve_sifs_sandbox_and_local_file_uris() {
        let roots = vec!["sifs:/team/project".into(), "sifs:/team/assets".into()];
        let files = attachments(
            "![a](image.png) [b](sandbox:/work/video.mp4) [c](/workspaces/2/scene.glb) [d](sifs://other/report.pdf) [e](file:///C:/Images/my%20picture.png)",
            &roots,
        );
        assert_eq!(files.len(), 5);
        assert_eq!(files[0].source, FilesSourceKind::Sifs);
        assert_eq!(files[0].file.path, "/team/project/image.png");
        assert_eq!(files[1].file.path, "/team/project/video.mp4");
        assert_eq!(files[2].file.path, "/team/assets/scene.glb");
        assert_eq!(files[3].file.path, "/other/report.pdf");
        assert_eq!(files[4].source, FilesSourceKind::Local);
        assert_eq!(files[4].file.path, "C:/Images/my picture.png");
    }

    #[test]
    fn markdown_attachments_reject_relative_escape_and_network_file_uris() {
        assert!(attachments("[escape](../secret.png) [unc](file://server/share/image.png) [nul](image%00.png) [data](data:image/png;base64,abc)", &["/project".into()]).is_empty());
        let files = attachments("[file](file:///tmp/a%23b.png#page)", &[]);
        assert_eq!(files[0].file.path, "/tmp/a#b.png");
        assert!(
            attachments(
                "![sandbox](sandbox:/mnt/data/image.png) ![mount](/work/image.png)",
                &[]
            )
            .is_empty()
        );
    }

    #[test]
    fn markdown_attachments_bound_previews_and_keep_distinct_sources() {
        let markdown = (0..100)
            .map(|index| format!("[file {index}](sifs:/files/{index}.pdf) "))
            .collect::<String>();
        assert_eq!(attachments(&markdown, &[]).len(), MAX_REFERENCES);
        let files = attachments(
            "![local](file:///images/a.png) ![sifs](sifs:/images/a.png)",
            &[],
        );
        assert_eq!(files.len(), 2);
        assert_ne!(files[0].source, files[1].source);
        let files = attachments("[download](asset) ![preview](asset)", &["/project".into()]);
        assert_eq!(files.len(), 1);
        assert!(
            files[0].image,
            "image syntax can preview an extensionless file"
        );
        assert!(files[0].file.media_type.is_none());
    }
}
