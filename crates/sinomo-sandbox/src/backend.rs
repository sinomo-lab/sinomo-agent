//! Host-agnostic sandbox backend abstraction + the local filesystem backend.
//!
//! A [`SandboxBackend`] serves the sandbox control routes against some concrete store:
//! the local filesystem store ([`LocalSandboxBackend`], used when running daemonless) or
//! the clustered SIFS-over-DFS store (the daemon's backend). The seam's `SandboxService`
//! (in `sinomo-service`) dispatches each route to whichever backend the host/context
//! selects, so there is a single sandbox code path regardless of whether a cluster exists.
//!
//! The trait is intentionally at request→response granularity (one method per route): it
//! keeps the dispatching service thin and lets each backend own its specifics (policy
//! evaluation, event emission, replica placement) without forcing the local and clustered
//! stores to share a method signature.

use crate::internal::*;

/// Error surfaced by a [`SandboxBackend`] operation. Wraps the store/filesystem errors and
/// adds a policy-denial case (clustered backends evaluate node policy) plus an escape
/// hatch for host-specific failures a backend cannot express as the above.
#[derive(Debug, thiserror::Error)]
pub enum SandboxBackendError {
    #[error(transparent)]
    Store(#[from] SandboxStoreError),
    #[error(transparent)]
    Fs(#[from] SandboxFsError),
    #[error("sandbox policy denied: {0}")]
    PolicyDenied(String),
    #[error("{0}")]
    Other(String),
}

/// Serves the sandbox control routes against a concrete store. Implemented by the
/// host-agnostic [`LocalSandboxBackend`] and by the daemon's clustered SIFS backend.
///
/// Execution (`run/*`) and mediated HTTP (`http/*`) routes are served separately and are
/// not part of this trait.
pub trait SandboxBackend: Send {
    fn create(
        &mut self,
        request: SandboxCreateInstanceRequest,
    ) -> Result<SandboxCreateInstanceResponse, SandboxBackendError>;

    fn status(
        &mut self,
        request: SandboxStatusRequest,
    ) -> Result<SandboxStatusResponse, SandboxBackendError>;

    fn update(
        &mut self,
        request: SandboxUpdateInstanceRequest,
    ) -> Result<SandboxUpdateInstanceResponse, SandboxBackendError>;

    fn checkpoint(
        &mut self,
        request: SandboxCheckpointRequest,
    ) -> Result<SandboxCheckpointResponse, SandboxBackendError>;

    fn resume(
        &mut self,
        request: SandboxResumeRequest,
    ) -> Result<SandboxResumeResponse, SandboxBackendError>;

    fn reboot(
        &mut self,
        request: SandboxRebootRequest,
    ) -> Result<SandboxRebootResponse, SandboxBackendError>;

    fn destroy(
        &mut self,
        request: SandboxDestroyRequest,
    ) -> Result<SandboxDestroyResponse, SandboxBackendError>;

    fn list(
        &mut self,
        request: SandboxListRequest,
    ) -> Result<SandboxListResponse, SandboxBackendError>;

    fn read(
        &mut self,
        request: SandboxReadRequest,
    ) -> Result<SandboxReadResponse, SandboxBackendError>;

    fn write(
        &mut self,
        request: SandboxWriteRequest,
    ) -> Result<SandboxWriteResponse, SandboxBackendError>;

    fn http_fetch(
        &mut self,
        request: SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxBackendError>;

    fn http_fetch_to_file(
        &mut self,
        request: SandboxHttpFetchToFileRequest,
    ) -> Result<SandboxHttpFetchFileResult, SandboxBackendError>;

    fn import_busybox(
        &mut self,
        request: SandboxImportBusyBoxToolchainRequest,
    ) -> Result<SandboxImportBusyBoxToolchainResponse, SandboxBackendError>;

    fn toolchain_add_command(
        &mut self,
        request: SandboxAddToolchainCommandRequest,
    ) -> Result<SandboxAddToolchainCommandResponse, SandboxBackendError>;

    fn toolchain_add_artifact(
        &mut self,
        request: SandboxAddToolchainArtifactRequest,
    ) -> Result<SandboxAddToolchainArtifactResponse, SandboxBackendError>;

    fn import_toolchain_package(
        &mut self,
        request: SandboxImportToolchainPackageRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError>;

    fn import_toolchain_package_from_path(
        &mut self,
        request: SandboxImportToolchainPackageFromPathRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError>;
}

/// Daemonless sandbox backend backed by the local filesystem store. Works with no cluster
/// (and no daemon); resource limits recorded in the manifest are still enforced on writes.
/// Cluster-only concerns (node policy, cross-node replica placement, the cluster event
/// log) do not apply here.
pub struct LocalSandboxBackend {
    store: LocalSandboxInstanceStore,
}

impl LocalSandboxBackend {
    /// Build a local backend rooted at `runtime_root` (e.g. `<data-dir>/sandbox/runtime`).
    pub fn new(runtime_root: impl Into<PathBuf>) -> Self {
        Self {
            store: LocalSandboxInstanceStore::new(runtime_root),
        }
    }
}

/// Error for routes the local (daemonless) backend cannot serve — mediated HTTP and
/// toolchain management require a daemon.
fn local_mode_unsupported(operation: &str) -> SandboxBackendError {
    SandboxBackendError::Other(format!(
        "sandbox {operation} requires a daemon and is not available in local (daemonless) mode"
    ))
}

impl SandboxBackend for LocalSandboxBackend {
    fn create(
        &mut self,
        request: SandboxCreateInstanceRequest,
    ) -> Result<SandboxCreateInstanceResponse, SandboxBackendError> {
        let sandbox_id = request.sandbox_id.unwrap_or_else(SandboxId::generate);
        let (mut manifest, _layout) =
            self.store
                .create(sandbox_id, request.toolchain_id, OffsetDateTime::now_utc())?;
        manifest.temp_policy = request.temp_policy;
        if let Some(memory_mb) = request.memory_mb {
            manifest.limits.memory_mb = memory_mb;
        }
        if let Some(wall_time_ms) = request.wall_time_ms {
            manifest.limits.wall_time_ms = wall_time_ms;
        }
        if let Some(max_input_bytes) = request.max_input_bytes {
            manifest.limits.max_input_bytes = max_input_bytes;
        }
        if let Some(max_output_bytes) = request.max_output_bytes {
            manifest.limits.max_output_bytes = max_output_bytes;
        }
        if let Some(max_workspace_bytes) = request.max_workspace_bytes {
            manifest.limits.max_workspace_bytes = max_workspace_bytes;
        }
        if let Some(max_written_bytes_per_command) = request.max_written_bytes_per_command {
            manifest.limits.max_written_bytes_per_command = max_written_bytes_per_command;
        }
        if let Some(max_file_count) = request.max_file_count {
            manifest.limits.max_file_count = max_file_count;
        }
        manifest.mounts = request.mounts;
        self.store.save_manifest(&manifest)?;
        Ok(SandboxCreateInstanceResponse { manifest })
    }

    fn status(
        &mut self,
        request: SandboxStatusRequest,
    ) -> Result<SandboxStatusResponse, SandboxBackendError> {
        let (manifest, _layout) = self.store.load(&request.sandbox_id)?;
        Ok(SandboxStatusResponse { manifest })
    }

    fn update(
        &mut self,
        request: SandboxUpdateInstanceRequest,
    ) -> Result<SandboxUpdateInstanceResponse, SandboxBackendError> {
        let (mut manifest, layout) = self.store.load(&request.sandbox_id)?;
        for key in request.env_set.keys().chain(request.env_unset.iter()) {
            if !sandbox_env_name_is_valid(key) {
                return Err(SandboxBackendError::Other(format!(
                    "invalid sandbox env name `{key}`"
                )));
            }
        }
        for (key, value) in &request.env_set {
            if value.contains('\0') {
                return Err(SandboxBackendError::Other(format!(
                    "sandbox env `{key}` contains an embedded NUL byte"
                )));
            }
        }
        if let Some(cwd) = request.cwd.as_ref() {
            let fs = LocalSandboxFileSystem::new(layout);
            validate_sandbox_working_directory(&fs, cwd)?;
        }
        if let Some(cwd) = request.cwd {
            manifest.cwd = cwd;
        }
        for key in request.env_unset {
            manifest.env.remove(&key);
        }
        manifest.env.extend(request.env_set);
        manifest.updated_at = OffsetDateTime::now_utc();
        self.store.save_manifest(&manifest)?;
        Ok(SandboxUpdateInstanceResponse { manifest })
    }

    fn checkpoint(
        &mut self,
        request: SandboxCheckpointRequest,
    ) -> Result<SandboxCheckpointResponse, SandboxBackendError> {
        let checkpoint = self
            .store
            .create_checkpoint(&request.sandbox_id, OffsetDateTime::now_utc())?;
        Ok(SandboxCheckpointResponse { checkpoint })
    }

    fn resume(
        &mut self,
        request: SandboxResumeRequest,
    ) -> Result<SandboxResumeResponse, SandboxBackendError> {
        let (manifest, _layout) = self.store.resume_checkpoint(
            &request.sandbox_id,
            &request.checkpoint_id,
            OffsetDateTime::now_utc(),
        )?;
        Ok(SandboxResumeResponse { manifest })
    }

    fn reboot(
        &mut self,
        request: SandboxRebootRequest,
    ) -> Result<SandboxRebootResponse, SandboxBackendError> {
        let (manifest, _layout, cleared_tmp) = self
            .store
            .reboot_session(&request.sandbox_id, OffsetDateTime::now_utc())?;
        Ok(SandboxRebootResponse {
            manifest,
            cleared_tmp,
        })
    }

    fn destroy(
        &mut self,
        request: SandboxDestroyRequest,
    ) -> Result<SandboxDestroyResponse, SandboxBackendError> {
        self.store.destroy(&request.sandbox_id)?;
        Ok(SandboxDestroyResponse {
            sandbox_id: request.sandbox_id,
        })
    }

    fn list(
        &mut self,
        request: SandboxListRequest,
    ) -> Result<SandboxListResponse, SandboxBackendError> {
        let (_manifest, layout) = self.store.load(&request.sandbox_id)?;
        let fs = LocalSandboxFileSystem::new(layout);
        let entries = fs.list(&request.path)?;
        Ok(SandboxListResponse { entries })
    }

    fn read(
        &mut self,
        request: SandboxReadRequest,
    ) -> Result<SandboxReadResponse, SandboxBackendError> {
        let (_manifest, layout) = self.store.load(&request.sandbox_id)?;
        let fs = LocalSandboxFileSystem::new(layout);
        let bytes = fs.read(&request.path, request.range)?;
        Ok(SandboxReadResponse { bytes })
    }

    fn write(
        &mut self,
        request: SandboxWriteRequest,
    ) -> Result<SandboxWriteResponse, SandboxBackendError> {
        let (manifest, layout) = self.store.load(&request.sandbox_id)?;
        let mut fs = LocalSandboxFileSystem::new(layout);
        validate_sandbox_write_resource_limits(
            &fs,
            &request.path,
            request.bytes.len() as u64,
            &manifest.limits,
        )?;
        fs.write(&request.path, &request.bytes)?;
        let bytes_written = request.bytes.len() as u64;
        Ok(SandboxWriteResponse {
            path: request.path,
            bytes_written,
        })
    }

    fn http_fetch(
        &mut self,
        _request: SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxBackendError> {
        Err(local_mode_unsupported("http fetch"))
    }

    fn http_fetch_to_file(
        &mut self,
        _request: SandboxHttpFetchToFileRequest,
    ) -> Result<SandboxHttpFetchFileResult, SandboxBackendError> {
        Err(local_mode_unsupported("http fetch-to-file"))
    }

    fn import_busybox(
        &mut self,
        _request: SandboxImportBusyBoxToolchainRequest,
    ) -> Result<SandboxImportBusyBoxToolchainResponse, SandboxBackendError> {
        Err(local_mode_unsupported("toolchain import-busybox"))
    }

    fn toolchain_add_command(
        &mut self,
        _request: SandboxAddToolchainCommandRequest,
    ) -> Result<SandboxAddToolchainCommandResponse, SandboxBackendError> {
        Err(local_mode_unsupported("toolchain add-command"))
    }

    fn toolchain_add_artifact(
        &mut self,
        _request: SandboxAddToolchainArtifactRequest,
    ) -> Result<SandboxAddToolchainArtifactResponse, SandboxBackendError> {
        Err(local_mode_unsupported("toolchain add-artifact"))
    }

    fn import_toolchain_package(
        &mut self,
        _request: SandboxImportToolchainPackageRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        Err(local_mode_unsupported("toolchain import-package"))
    }

    fn import_toolchain_package_from_path(
        &mut self,
        _request: SandboxImportToolchainPackageFromPathRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        Err(local_mode_unsupported("toolchain import-package-from-path"))
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalSandboxBackend, SandboxBackend};
    use crate::internal::*;

    /// Full daemonless round trip: create a sandbox on the local filesystem store, write a
    /// file, read it back, see it in a directory listing, then destroy the instance — all
    /// with no cluster and no daemon.
    #[test]
    fn local_backend_round_trips_without_a_cluster() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-local-sandbox-backend-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let mut backend = LocalSandboxBackend::new(&root);

        let created = backend
            .create(SandboxCreateInstanceRequest {
                sandbox_id: None,
                toolchain_id: SandboxToolchainId::from_legacy_named("toolchain_test")
                    .expect("toolchain id"),
                temp_policy: SandboxTempPolicy::RetainAcrossSessions,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts: Vec::new(),
            })
            .expect("create");
        let sandbox_id = created.manifest.sandbox_id.clone();

        let status = backend
            .status(SandboxStatusRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .expect("status");
        assert_eq!(status.manifest.sandbox_id, sandbox_id);

        let path = SandboxPath::new("/work/hello.txt").expect("path");
        let write = backend
            .write(SandboxWriteRequest {
                sandbox_id: sandbox_id.clone(),
                path: path.clone(),
                bytes: b"hi".to_vec(),
            })
            .expect("write");
        assert_eq!(write.bytes_written, 2);

        let read = backend
            .read(SandboxReadRequest {
                sandbox_id: sandbox_id.clone(),
                path: path.clone(),
                range: None,
            })
            .expect("read");
        assert_eq!(read.bytes, b"hi".to_vec());

        let listing = backend
            .list(SandboxListRequest {
                sandbox_id: sandbox_id.clone(),
                path: SandboxPath::new("/work").expect("path"),
            })
            .expect("list");
        assert!(listing.entries.iter().any(|entry| entry.path == path));

        backend
            .destroy(SandboxDestroyRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .expect("destroy");
        assert!(
            backend.status(SandboxStatusRequest { sandbox_id }).is_err(),
            "status should fail after destroy"
        );

        let _ = std::fs::remove_dir_all(&root);
    }
}
