use super::*;

const MAX_PAGE_ENTRIES: usize = 256;
const MAX_READ_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CURSOR_BYTES: usize = 128 * 1024;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryCursor {
    root: String,
    path: String,
    object_id: String,
    version: String,
    cursor: crate::DfsDirectoryPageCursor,
}

fn metadata(stat: crate::DfsStat) -> crate::DaemonDfsBrowserStatResponse {
    daemon_dfs_browser_stat_response(stat, Vec::new(), None, None, None)
}

pub(super) fn validate_cluster_namespace(
    data_dir: &SinomoDataDir,
    expected: Option<&sinomo_core::Id>,
) -> Result<(), ServiceError> {
    if let Some(expected) = expected {
        let state =
            sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(data_dir))
                .map_err(dfs_browser_service_error)?;
        if &state.cluster_id != expected {
            return Err(dfs_browser_service_error(
                "filesystem source cluster namespace changed",
            ));
        }
    }
    Ok(())
}

fn failure(path: &str, error: crate::DfsPosixError) -> crate::DaemonDfsBrowserNoLinksResponse {
    use crate::DaemonDfsBrowserNoLinksFailure as F;
    let failure = match error {
        crate::DfsPosixError::NotFound { path } => F::NotFound { path },
        crate::DfsPosixError::AlreadyExists { path } => F::AlreadyExists { path },
        crate::DfsPosixError::NotDirectory { path } => F::NotDirectory { path },
        crate::DfsPosixError::IsDirectory { path } => F::IsDirectory { path },
        crate::DfsPosixError::InvalidPath { path } => F::InvalidPath {
            path,
            detail: "invalid or linked SIFS path".into(),
        },
        crate::DfsPosixError::Conflict { path, reason } => F::Conflict {
            path,
            detail: reason,
        },
        crate::DfsPosixError::Dfs(crate::DfsError::UnsupportedWritePrecondition { path, reason }) => F::Unsupported {
            path, detail: reason,
        },
        error => F::Unavailable {
            path: path.into(),
            detail: error.to_string(),
        },
    };
    crate::DaemonDfsBrowserNoLinksResponse::Failed { failure }
}

impl DfsBrowserBackend {
    /// Adapt the existing scoped filesystem primitives through the resident
    /// browser owner. There is no client-side check-then-open or second store.
    pub fn no_links(
        &self,
        request: crate::DaemonDfsBrowserNoLinksRequest,
    ) -> Result<crate::DaemonDfsBrowserNoLinksResponse, ServiceError> {
        match self.try_no_links(request)? {
            DfsBrowserMutationAttempt::Complete(response) => Ok(response),
            DfsBrowserMutationAttempt::ConfigurationPending(_) => Err(dfs_browser_service_error(
                "DFS write configuration is awaiting a coherent authoritative projection",
            )),
        }
    }

    pub(super) fn try_no_links(
        &self,
        request: crate::DaemonDfsBrowserNoLinksRequest,
    ) -> Result<
        DfsBrowserMutationAttempt<
            crate::DaemonDfsBrowserNoLinksRequest,
            crate::DaemonDfsBrowserNoLinksResponse,
        >,
        ServiceError,
    > {
        let expected = request.expected_cluster_id.clone();
        validate_cluster_namespace(&self.data_dir, expected.as_ref())?;
        use crate::DaemonDfsBrowserNoLinksOperation as O;
        let result = if matches!(
            &request.operation,
            O::CreateNew { .. } | O::ReplaceIfVersion { .. }
        ) {
            let root = normalize_sifs_browser_path(&request.root)?;
            let path = normalize_sifs_browser_path(&request.path)?;
            self.try_with_configured_mutation_service(
                &[&path],
                request,
                |service, configuration, request| {
                    let mut options = self.write_options_for_path_with_configuration(
                        service,
                        &path,
                        configuration,
                    )?;
                    options.foreground_replica_repair = true;
                    let result = match request.operation {
                        O::CreateNew { bytes } => service
                            .posix_create_file_beneath_no_links(&root, &path, &bytes, options),
                        O::ReplaceIfVersion {
                            expected_object_id,
                            expected_version,
                            bytes,
                        } => {
                            let object = crate::DfsObjectId::new(&expected_object_id)
                                .map_err(dfs_browser_service_error)?;
                            let version = crate::DfsVersion::new(&expected_version)
                                .map_err(dfs_browser_service_error)?;
                            service.posix_replace_file_if_version_beneath_no_links(
                                &root, &path, &object, &version, &bytes, options,
                            )
                        }
                        _ => unreachable!("only content writes use configured mutation planning"),
                    };
                    Ok(match result {
                        Ok(stat) => crate::DaemonDfsBrowserNoLinksResponse::Metadata {
                            metadata: metadata(stat),
                        },
                        Err(error) => failure(&path, error),
                    })
                },
            )?
        } else {
            DfsBrowserMutationAttempt::Complete(self.no_links_in_namespace(request)?)
        };
        validate_cluster_namespace(&self.data_dir, expected.as_ref())?;
        Ok(result)
    }

    fn no_links_in_namespace(
        &self,
        request: crate::DaemonDfsBrowserNoLinksRequest,
    ) -> Result<crate::DaemonDfsBrowserNoLinksResponse, ServiceError> {
        use crate::DaemonDfsBrowserNoLinksOperation as O;
        use crate::DaemonDfsBrowserNoLinksResponse as R;
        let root = normalize_sifs_browser_path(&request.root)?;
        let path = normalize_sifs_browser_path(&request.path)?;
        if matches!(&request.operation, O::CreateDirAll) {
            let _owner = self.host.operation_guard()?;
            let mut service = self.lock_mutation_for_paths(&[&path])?;
            self.refresh_peer_targets(&mut service);
            // Metadata-only mkdir keeps normal namespace admission; it does
            // not consume content providers, zone policy or write options.
            let result = service
                .posix_mkdir_all_beneath_no_links(&root, &path)
                .map(|_| R::DirectoryCreated);
            // BrowserMutation retains its ordinary publication, maintenance
            // and namespace-change owner even if the requesting client leaves.
            return Ok(result.unwrap_or_else(|error| failure(&path, error)));
        }

        let _owner = self.host.operation_guard()?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        match request.operation {
            O::Stat => Ok(
                match service.posix_open_beneath_no_links_distributed(&root, &path) {
                    Ok(stat) => R::Metadata {
                        metadata: metadata(stat),
                    },
                    Err(error) => failure(&path, error),
                },
            ),
            O::ReadRange { offset, max_bytes } => {
                if max_bytes > MAX_READ_BYTES {
                    return Err(dfs_browser_service_error(
                        "scoped SIFS read exceeds the 64 MiB limit",
                    ));
                }
                let max_bytes = usize::try_from(max_bytes).map_err(dfs_browser_service_error)?;
                let outcome = match service
                    .posix_read_file_range_beneath_no_links_distributed_outcome(
                        &root, &path, offset, max_bytes,
                    ) {
                    Ok(outcome) => outcome,
                    Err(error) => return Ok(failure(&path, error)),
                };
                Ok(match outcome {
                    crate::DfsPosixDistributedOpenedFileRangeReadOutcome::Found { stat, read } => {
                        R::Read {
                            metadata: metadata(stat),
                            bytes: read.bytes,
                            truncated: read.truncated,
                        }
                    }
                    crate::DfsPosixDistributedOpenedFileRangeReadOutcome::NotFound { path } => {
                        R::Failed {
                            failure: crate::DaemonDfsBrowserNoLinksFailure::NotFound { path },
                        }
                    }
                    crate::DfsPosixDistributedOpenedFileRangeReadOutcome::ContentUnavailable {
                        path,
                        detail,
                    } => R::Failed {
                        failure: crate::DaemonDfsBrowserNoLinksFailure::Unavailable {
                            path,
                            detail,
                        },
                    },
                })
            }
            O::ListPage {
                cursor,
                max_entries,
            } => {
                if max_entries == 0 || max_entries > MAX_PAGE_ENTRIES {
                    return Err(dfs_browser_service_error(
                        "scoped SIFS directory page must contain 1..=256 entries",
                    ));
                }
                let cursor = cursor
                    .map(|bytes| {
                        if bytes.len() > MAX_CURSOR_BYTES {
                            return Err(dfs_browser_service_error(
                                "scoped SIFS directory cursor exceeds its bound",
                            ));
                        }
                        sinomo_core::object::serde_ext::from_bytes::<DirectoryCursor>(&bytes)
                            .map_err(dfs_browser_service_error)
                    })
                    .transpose()?;
                if cursor
                    .as_ref()
                    .is_some_and(|cursor| cursor.root != root || cursor.path != path)
                {
                    return Err(dfs_browser_service_error(
                        "scoped SIFS directory cursor belongs to another root or path",
                    ));
                }
                let (directory, page) = match service
                    .posix_list_dir_beneath_no_links_distributed_page(
                        &root,
                        &path,
                        cursor.as_ref().map(|cursor| &cursor.cursor),
                        max_entries,
                    ) {
                    Ok(page) => page,
                    Err(error) => return Ok(failure(&path, error)),
                };
                if cursor.as_ref().is_some_and(|cursor| {
                    cursor.object_id != directory.object_id.as_str()
                        || cursor.version != directory.version.as_str()
                }) {
                    return Ok(R::Failed {
                        failure: crate::DaemonDfsBrowserNoLinksFailure::Conflict {
                            path,
                            detail: "scoped SIFS directory changed while paging".into(),
                        },
                    });
                }
                let next_cursor = page
                    .next_cursor
                    .map(|cursor| {
                        sinomo_core::object::serde_ext::to_bytes(
                            &DirectoryCursor {
                                root,
                                path,
                                object_id: directory.object_id.to_string(),
                                version: directory.version.to_string(),
                                cursor,
                            },
                            1,
                            sinomo_core::object::LengthMode::U32,
                        )
                        .map_err(dfs_browser_service_error)
                    })
                    .transpose()?;
                let entries = page
                    .entries
                    .into_iter()
                    .map(|entry| crate::DaemonDfsBrowserEntry {
                        name: entry.name,
                        path: entry.path,
                        kind: entry.kind,
                        size: entry.size_bytes,
                        metadata: None,
                    })
                    .collect();
                Ok(R::DirectoryPage {
                    directory: metadata(directory),
                    entries,
                    next_cursor,
                })
            }
            _ => unreachable!("read classification is exhaustive"),
        }
    }
}
