use super::*;
use sinomo_runtime::dfs::*;
#[cfg(test)]
use sinomo_runtime::dfs_attachments::DfsAttachmentsExt;

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_file_attachments(ctx) {
        id: "cluster.dfs.file-attachments",
        summary: "Verify file attachments through the daemon: large payload ranges, tags, transcripts, CAS, rename, and stale source fencing",
        tags: ["cluster-dfs", "dfs-file-attachments"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: { run_file_attachments(ctx) }
    }
}

fn run_file_attachments(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = scoped_dfs_test_base(ctx, "file-attachments")?;
    let _namespace = ScopedSifsTestNamespace::open(ctx, [base.clone()])?;
    let browser = open_local_daemon_sifs_browser(ctx)?;
    let fail = SinomoTestError::failed;
    let path = format!("{base}/source.bin");
    browser.write_file(&path, b"source-v1").map_err(fail)?;
    let stat = browser.stat(&path).map_err(fail)?;
    let mut source = DfsAttachmentSource {
        path,
        file_system_id: DfsFileSystemId::new(&stat.file_system_id)
            .map_err(|e| fail(e.to_string()))?,
        object_id: DfsObjectId::new(&stat.object_id).map_err(|e| fail(e.to_string()))?,
    };
    let version = DfsVersion::new(&stat.version).map_err(|e| fail(e.to_string()))?;
    let bytes = (0..2 * 1024 * 1024 + 71)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>();
    let mut records = Vec::new();
    for (metadata, payload) in [
        (DfsAttachmentMetadata { key: DfsAttachmentKey { kind: "org.example.binary".into(), name: "default".into() }, schema: "org.example.binary/v1".into(), media_type: "application/octet-stream".into(), source_version: Some(version.clone()), producer: "runtime-test".into() }, bytes.clone()),
        (DfsAttachmentMetadata { key: DfsAttachmentKey::tags(), schema: "sinomo.tags/v1".into(), media_type: "application/json".into(), source_version: None, producer: "runtime-test".into() }, DfsFileTags { tags: vec!["searchable".into(), "attachment".into()] }.encode().map_err(fail)?),
        (DfsAttachmentMetadata { key: DfsAttachmentKey::transcript("en"), schema: "sinomo.transcript/v1".into(), media_type: "application/x-ndjson".into(), source_version: Some(version), producer: "runtime-test".into() }, b"{\"start_millis\":0,\"end_millis\":1000,\"speaker\":null,\"text\":\"searchable transcript\"}\n".to_vec()),
    ] {
        let request = DfsAttachmentRequest::Put { source: source.clone(), metadata: metadata.clone(), expected: None, operation_id: sinomo_runtime::core::Id::generate().to_string(), bytes: payload.clone() };
        let response = if metadata.source_version.is_some() {
            let DfsAttachmentRequest::Put { operation_id, .. } = &request else { unreachable!() };
            let upload = DfsAttachmentUploadRequest { source: source.clone(), metadata: metadata.clone(), expected: None, operation_id: operation_id.clone(), payload_bytes: payload.len() as u64, generation: true };
            let claim = DfsAttachmentRequest::ClaimGeneration { upload: upload.clone() };
            let granted = browser.file_attachment(&claim).map_err(fail)?;
            if !matches!(granted, DfsAttachmentResponse::Generation { granted: true, .. }) || browser.file_attachment(&claim).map_err(fail)? != granted { return Err(fail("generation claim/replay failed".into())); }
            let mut competing = upload.clone(); competing.operation_id = sinomo_runtime::core::Id::generate().to_string();
            if !matches!(browser.file_attachment(&DfsAttachmentRequest::ClaimGeneration { upload: competing }).map_err(fail)?, DfsAttachmentResponse::Generation { granted: false, .. }) { return Err(fail("duplicate generation was admitted".into())); }
            browser.upload_file_attachment(&upload, std::io::Cursor::new(payload.clone())).map_err(fail)?
        } else { browser.file_attachment(&request).map_err(fail)? };
        let DfsAttachmentResponse::Stored { attachment } = response else { return Err(fail("missing commit descriptor".into())); };
        let replay = browser.file_attachment(&request).map_err(fail)?;
        if replay != (DfsAttachmentResponse::Stored { attachment: attachment.clone() }) { return Err(fail("exact retry changed the attachment revision".into())); }
        let mut offset = 0;
        loop {
            let response = browser.file_attachment(&DfsAttachmentRequest::Read { source: source.clone(), key: metadata.key.clone(), revision: attachment.revision.clone(), offset, max_bytes: DFS_ATTACHMENT_MAX_RANGE_BYTES }).map_err(fail)?;
            let DfsAttachmentResponse::Data { bytes, eof } = response else { return Err(fail("missing range".into())); };
            if bytes != payload[offset as usize..offset as usize + bytes.len()] { return Err(fail("attachment range differs".into())); }
            offset += bytes.len() as u64;
            if eof { break; }
            if bytes.is_empty() { return Err(fail("range made no progress".into())); }
        }
        records.push(attachment);
    }
    let batched = browser
        .file_attachment(&DfsAttachmentRequest::GetMany {
            source: source.clone(),
            keys: records.iter().map(|r| r.metadata.key.clone()).collect(),
        })
        .map_err(fail)?;
    if !matches!(batched, DfsAttachmentResponse::Descriptors { attachments } if attachments == records.iter().cloned().map(Some).collect::<Vec<_>>())
    {
        return Err(fail("batched descriptors differ".into()));
    }
    ctx.record("large_payload_bytes", bytes.len());
    let renamed = format!("{base}/renamed.bin");
    browser.rename(&source.path, &renamed).map_err(fail)?;
    source.path = renamed;
    let DfsAttachmentResponse::Page {
        attachments,
        next_cursor,
    } = browser
        .file_attachment(&DfsAttachmentRequest::List {
            source: source.clone(),
            cursor: None,
            limit: 2,
        })
        .map_err(fail)?
    else {
        return Err(fail("missing attachment page".into()));
    };
    if attachments.len() != 2 || next_cursor.is_none() {
        return Err(fail("attachment page was not bounded".into()));
    }
    let DfsAttachmentResponse::Page {
        attachments,
        next_cursor,
    } = browser
        .file_attachment(&DfsAttachmentRequest::List {
            source: source.clone(),
            cursor: next_cursor,
            limit: 2,
        })
        .map_err(fail)?
    else {
        return Err(fail("missing second page".into()));
    };
    if attachments.len() != 1 || next_cursor.is_some() {
        return Err(fail(
            "attachment pagination lost or duplicated records".into(),
        ));
    }
    browser
        .write_file(&source.path, b"source-v2")
        .map_err(fail)?;
    let old = &records[0];
    if browser
        .file_attachment(&DfsAttachmentRequest::Read {
            source: source.clone(),
            key: old.metadata.key.clone(),
            revision: old.revision.clone(),
            offset: 0,
            max_bytes: 1,
        })
        .is_ok()
    {
        return Err(fail("stale attachment was readable".into()));
    }
    for record in records {
        browser
            .file_attachment(&DfsAttachmentRequest::Delete {
                source: source.clone(),
                key: record.metadata.key,
                expected: record.revision,
                operation_id: sinomo_runtime::core::Id::generate().to_string(),
            })
            .map_err(fail)?;
    }
    ctx.record("attachment_count", 3);
    Ok(SinomoTestOutcome::passed(
        "attachments passed daemon publication, replay, pagination, range reads, rename and source-version fencing",
    ))
}
