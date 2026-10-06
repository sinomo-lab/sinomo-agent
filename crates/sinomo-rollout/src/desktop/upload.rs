//! Streamed cluster delivery. Publication receipts fence retries from replacing a newer offer.
use super::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

pub async fn receive_artifact(
    data_dir: PathBuf,
    request: AppUpdateUploadRequest,
    body: impl AsyncRead + Unpin,
) -> Result<AppUpdateUploadResponse> {
    validate_manifest(&request.manifest)?;
    let id = release_id(&request.manifest)?;
    let entry = request
        .manifest
        .files
        .get(request.file_index)
        .ok_or("unknown desktop artifact index")?;
    let releases = DesktopReleases::new(&data_dir);
    releases.ensure_root()?;
    let upload = releases.root.join("uploads").join(&id);
    tokio::fs::create_dir_all(&upload).await.map_err(err)?;
    let temporary = upload.join(format!(".receive-{}", sinomo_core::Id::generate()));
    let cleanup = sinomo_core::storage::scratch::ScratchPath::new(temporary.clone());
    let received = async {
        let mut file = tokio::fs::File::create(&temporary).await.map_err(err)?;
        let bytes = tokio::io::copy(&mut body.take(entry.size.saturating_add(1)), &mut file)
            .await
            .map_err(err)?;
        if bytes != entry.size {
            return Err("desktop upload length mismatch".into());
        }
        file.flush().await.map_err(err)?;
        file.sync_all().await.map_err(err)?;
        Ok(())
    }
    .await;
    if let Err(error) = received {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error);
    }
    tokio::task::spawn_blocking(move || {
        let _cleanup = cleanup;
        let result = finish_upload(&releases, &upload, &temporary, &id, &request);
        let _ = fs::remove_file(temporary);
        result
    })
    .await
    .map_err(err)?
}

fn finish_upload(
    releases: &DesktopReleases,
    upload: &Path,
    temporary: &Path,
    id: &str,
    request: &AppUpdateUploadRequest,
) -> Result<AppUpdateUploadResponse> {
    let entry = &request.manifest.files[request.file_index];
    let mut file = File::open(temporary).map_err(err)?;
    let mut hash = blake3::Hasher::new();
    hash.update_reader(&mut file).map_err(err)?;
    if hash.finalize().to_hex().as_str() != entry.content_hash {
        return Err("desktop upload digest mismatch".into());
    }
    drop(file);
    let _lock = releases.lock("upload.lock")?;
    let intent = upload.join(".publication.json");
    let current = releases.status()?;
    if current.candidate_release.as_deref() == Some(id)
        || current.active_release.as_deref() == Some(id)
    {
        atomic_json(&intent, &true)?;
        remove_published_payloads(upload, &request.manifest);
        return Ok(AppUpdateUploadResponse {
            release_id: id.into(),
            received_files: request.manifest.files.len(),
            expected_files: request.manifest.files.len(),
            status: Some(current),
        });
    }
    if intent.exists() {
        return Err("this desktop upload was already published or has an ambiguous publication; it cannot supersede another release on retry".into());
    }
    let destination = upload.join(&entry.path);
    if !destination.exists() {
        fs::create_dir_all(destination.parent().ok_or("artifact has no parent")?).map_err(err)?;
        fs::rename(temporary, &destination).map_err(err)?;
        sync_ancestors(
            destination.parent().ok_or("artifact has no parent")?,
            upload,
        )?;
    }
    atomic_json(&upload.join(APP_BUNDLE_MANIFEST), &request.manifest)?;
    let received_files = request
        .manifest
        .files
        .iter()
        .filter(|f| upload.join(&f.path).is_file())
        .count();
    let mut response = AppUpdateUploadResponse {
        release_id: id.into(),
        received_files,
        expected_files: request.manifest.files.len(),
        status: None,
    };
    if received_files != response.expected_files {
        return Ok(response);
    }
    verify_bundle_content(upload, &request.manifest)?;
    atomic_json(&intent, &false)?;
    response.status = Some(releases.stage(upload)?);
    atomic_json(&intent, &true)?;
    remove_published_payloads(upload, &request.manifest);
    Ok(response)
}

fn remove_published_payloads(upload: &Path, manifest: &AppBundleManifest) {
    for entry in &manifest.files {
        if let Ok(path) = checked_file(upload, &entry.path)
            && let Err(error) = sinomo_core::storage::scratch::remove_owned_scratch_path(&path)
        {
            tracing::warn!(path = %path.display(), %error, "could not reclaim published desktop upload payload");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn desktop_bundle_streams_over_real_loopback_carrier_before_becoming_ready() {
        use sinomo_core::network::*;
        let root = sinomo_testdir::TestDir::new("desktop-stream-carrier");
        let source = super::super::tests::bundle(&root, "source");
        fs::write(
            source.join(crate::executable_name("sinomo-app")),
            vec![b'x'; 2 * 1024 * 1024],
        )
        .unwrap();
        let files: Vec<_> = DESKTOP_BINARY_STEMS
            .into_iter()
            .map(crate::executable_name)
            .collect();
        let manifest = write_bundle_manifest(&source, "source".into(), &files).unwrap();
        let data_dir = sinomo_core::local::SinomoDataDir::new(root.join("receiver"));
        data_dir.ensure_root_layout().unwrap();
        ClusterPaths::new(&data_dir).ensure().unwrap();
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr_string().unwrap();
        let destination = data_dir.root();
        let server = tokio::spawn(async move {
            let mut carrier = listener.accept().await.unwrap().into_inner();
            let prelude = read_local_object_rpc_carrier_prelude(&mut carrier)
                .await
                .unwrap();
            let mut acceptor =
                accept_local_object_rpc_stream_carrier(Box::new(carrier), &prelude).unwrap();
            for _ in 0..DESKTOP_BINARY_STEMS.len() {
                let stream = acceptor.accept().await.unwrap().unwrap();
                let peer = RpcPeerContext::unauthenticated_local(
                    stream.session_id.clone(),
                    stream.channel_binding,
                );
                let destination = destination.clone();
                serve_service_rpc_peer_stream(
                    stream,
                    &peer,
                    ObjectFrameIoOptions::default(),
                    move |incoming, _| async move {
                        let request = incoming.request.decode_body::<AppUpdateUploadRequest>()?;
                        let response = receive_artifact(destination, request, incoming.body)
                            .await
                            .map_err(|detail| NetworkError::Serialization { detail })?;
                        Ok(ObjectRpcOutgoingResponse::value(
                            ObjectRpcResponse::encode_body(APP_UPDATE_UPLOAD_ROUTE, &response)?,
                        ))
                    },
                )
                .await
                .unwrap();
            }
        });
        for (file_index, file) in manifest.files.iter().enumerate() {
            let request = AppUpdateUploadRequest {
                manifest: manifest.clone(),
                file_index,
            };
            let call = ObjectRpcCall::new(
                ObjectRpcRequest::encode_body(APP_UPDATE_UPLOAD_ROUTE, "rollout", &request)
                    .unwrap(),
                ObjectRpcBodySource::file(source.join(&file.path)).unwrap(),
            );
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                sinomo_api::transport::call_daemon_address_rpc_async(
                    data_dir.clone(),
                    &address,
                    call,
                    ClusterObjectRpcOptions::default(),
                    &tokio::runtime::Handle::current(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
            let response: AppUpdateUploadResponse = response.decode_body().await.unwrap();
            assert_eq!(response.received_files, file_index + 1);
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            DesktopReleases::new(data_dir.root())
                .status()
                .unwrap()
                .phase,
            AppUpdatePhase::Ready
        );
    }
    #[tokio::test]
    async fn streaming_upload_checks_every_file_before_offering_and_does_not_reoffer_on_retry() {
        let root = sinomo_testdir::TestDir::new("desktop-upload");
        let source = super::super::tests::bundle(&root, "uploaded");
        let manifest = read_manifest(&source).unwrap();
        let data = root.join("data");
        let first = AppUpdateUploadRequest {
            manifest: manifest.clone(),
            file_index: 0,
        };
        assert!(
            receive_artifact(
                data.clone(),
                first.clone(),
                std::io::Cursor::new(b"truncated")
            )
            .await
            .is_err()
        );
        let result = receive_artifact(
            data.clone(),
            first.clone(),
            std::io::Cursor::new(fs::read(source.join(&manifest.files[0].path)).unwrap()),
        )
        .await
        .unwrap();
        assert!(result.status.is_none());
        assert!(
            DesktopReleases::new(&data)
                .status()
                .unwrap()
                .candidate_release
                .is_none()
        );
        let mut last = None;
        for file_index in 1..manifest.files.len() {
            let request = AppUpdateUploadRequest {
                manifest: manifest.clone(),
                file_index,
            };
            let bytes = fs::read(source.join(&manifest.files[file_index].path)).unwrap();
            let result = receive_artifact(
                data.clone(),
                request.clone(),
                std::io::Cursor::new(bytes.clone()),
            )
            .await
            .unwrap();
            last = Some((request, bytes, result));
        }
        let (second, bytes, result) = last.unwrap();
        let upload = DesktopReleases::new(&data)
            .root
            .join("uploads")
            .join(&result.release_id);
        assert!(upload.join(".publication.json").exists());
        for file in &manifest.files {
            assert!(!upload.join(&file.path).exists());
        }
        let replay = receive_artifact(
            data.clone(),
            second.clone(),
            std::io::Cursor::new(bytes.clone()),
        )
        .await
        .unwrap();
        assert_eq!(replay.received_files, manifest.files.len());
        for file in &manifest.files {
            assert!(!upload.join(&file.path).exists());
        }
        assert_eq!(
            result.status.unwrap().candidate_release.as_deref(),
            Some(result.release_id.as_str())
        );
        let newer = super::super::tests::bundle(&root, "newer");
        let current = DesktopReleases::new(&data).stage(&newer).unwrap();
        assert!(
            receive_artifact(data.clone(), second, std::io::Cursor::new(bytes))
                .await
                .is_err()
        );
        assert_eq!(DesktopReleases::new(&data).status().unwrap(), current);
    }
}
