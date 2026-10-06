use super::*;
use sinomo_runtime::dfs::*;
use sinomo_runtime::dfs_attachments::cli::DfsAttachmentCliOptions;
use std::io::{Read, Write};

pub(super) fn run(options: DfsAttachmentCliOptions) -> Result<(), CliError> {
    let fail = |detail: String| CliError::Parse { detail };
    let status = default_main_daemon_status()?
        .ok_or_else(|| fail("file attachments require the main daemon".into()))?;
    let browser = crate::app_api::SifsBrowser::open_daemon_with_timeout(
        status.address,
        Duration::from_secs(60),
    )
    .map_err(fail)?;
    let stat = browser.stat(&options.path).map_err(fail)?;
    let source = DfsAttachmentSource {
        path: options.path,
        file_system_id: DfsFileSystemId::new(&stat.file_system_id)
            .map_err(|e| fail(e.to_string()))?,
        object_id: DfsObjectId::new(&stat.object_id).map_err(|e| fail(e.to_string()))?,
    };
    if options.action == "list" {
        let response = browser
            .file_attachment(&DfsAttachmentRequest::List {
                source,
                cursor: options.cursor,
                limit: options.limit,
            })
            .map_err(fail)?;
        return print_json("<dfs-attachments>", &response);
    }
    let key = match options.action.as_str() {
        "tags" => DfsAttachmentKey::tags(),
        "transcript" => DfsAttachmentKey::transcript(&options.name),
        _ => DfsAttachmentKey {
            kind: options
                .kind
                .ok_or_else(|| fail("--kind is required".into()))?,
            name: options.name,
        },
    };
    key.validate().map_err(fail)?;
    let DfsAttachmentResponse::Descriptor { attachment } = browser
        .file_attachment(&DfsAttachmentRequest::Get {
            source: source.clone(),
            key: key.clone(),
        })
        .map_err(fail)?
    else {
        return Err(fail("unexpected attachment response".into()));
    };
    let operation_id = options
        .operation_id
        .unwrap_or_else(|| sinomo_runtime::core::Id::generate().to_string());
    if options.action == "delete" {
        let attachment = attachment.ok_or_else(|| fail("attachment does not exist".into()))?;
        let response = browser
            .file_attachment(&DfsAttachmentRequest::Delete {
                source,
                key,
                expected: attachment.revision,
                operation_id,
            })
            .map_err(fail)?;
        return print_json("<dfs-attachment>", &response);
    }
    if options.action == "put" || options.input.is_some() {
        let input = options
            .input
            .ok_or_else(|| fail("--input is required".into()))?;
        let mut file = std::fs::File::open(input).map_err(|e| fail(e.to_string()))?;
        let size = file.metadata().map_err(|e| fail(e.to_string()))?.len();
        if size > DFS_ATTACHMENT_MAX_PAYLOAD_BYTES as u64 {
            return Err(fail("attachment upload exceeds 64 MiB".into()));
        }
        let mut canonical_tags = None;
        let (schema, media_type, bind) = match options.action.as_str() {
            "tags" => {
                let mut bytes = Vec::new();
                (&mut file)
                    .take(64 * 1024 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|e| fail(e.to_string()))?;
                if bytes.len() > 64 * 1024 {
                    return Err(fail("tags exceed 64 KiB".into()));
                }
                canonical_tags = Some(
                    serde_json::from_slice::<DfsFileTags>(&bytes)
                        .map_err(|e| fail(e.to_string()))?
                        .encode()
                        .map_err(fail)?,
                );
                ("sinomo.tags/v1".into(), "application/json".into(), false)
            }
            "transcript" => (
                "sinomo.transcript/v1".into(),
                "application/x-ndjson".into(),
                true,
            ),
            _ => (
                options
                    .schema
                    .ok_or_else(|| fail("--schema is required".into()))?,
                options
                    .media_type
                    .ok_or_else(|| fail("--media-type is required".into()))?,
                options.bind_version,
            ),
        };
        let metadata = DfsAttachmentMetadata {
            key,
            schema,
            media_type,
            source_version: if bind {
                Some(DfsVersion::new(&stat.version).map_err(|e| fail(e.to_string()))?)
            } else {
                None
            },
            producer: "sinomo.cli.authored/v1".into(),
        };
        metadata.validate().map_err(fail)?;
        // Log the operation identity before dispatch so uncertain delivery can
        // be reconciled against the durable object, never retried blindly.
        eprintln!("Attachment operation: {operation_id}");
        let upload = DfsAttachmentUploadRequest {
            generation: false,
            source,
            metadata,
            expected: attachment.map(|a| a.revision),
            operation_id,
            payload_bytes: canonical_tags
                .as_ref()
                .map_or(size, |bytes| bytes.len() as u64),
        };
        let response = match canonical_tags {
            Some(bytes) => browser.upload_file_attachment(&upload, std::io::Cursor::new(bytes)),
            None => browser.upload_file_attachment(&upload, file),
        }
        .map_err(fail)?;
        return print_json("<dfs-attachment>", &response);
    }
    let attachment = attachment.ok_or_else(|| fail("attachment does not exist".into()))?;
    if !attachment.current {
        return Err(fail(
            "attachment is stale for the current source version".into(),
        ));
    }
    let mut output: Box<dyn Write> = match options.output {
        Some(path) => Box::new(std::fs::File::create(path).map_err(|e| fail(e.to_string()))?),
        None => Box::new(std::io::stdout().lock()),
    };
    let mut offset = 0;
    loop {
        let DfsAttachmentResponse::Data { bytes, eof } = browser
            .file_attachment(&DfsAttachmentRequest::Read {
                source: source.clone(),
                key: key.clone(),
                revision: attachment.revision.clone(),
                offset,
                max_bytes: DFS_ATTACHMENT_MAX_RANGE_BYTES,
            })
            .map_err(fail)?
        else {
            return Err(fail("unexpected attachment range response".into()));
        };
        output.write_all(&bytes).map_err(|e| fail(e.to_string()))?;
        offset += bytes.len() as u64;
        if eof {
            break;
        }
        if bytes.is_empty() {
            return Err(fail("attachment read made no progress".into()));
        }
    }
    Ok(())
}
