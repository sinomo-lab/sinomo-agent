use super::*;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct BuildLogOptions {
    job_id: Id,
    target: Option<String>,
    follow: bool,
}

impl BuildLogOptions {
    pub(super) fn parse(matches: &CliCommandMatches) -> Result<Self, CliParseError> {
        let value = matches.value("--job").ok_or_else(|| CliParseError::Parse {
            detail: "build-log requires --job".into(),
        })?;
        Ok(Self {
            job_id: Id::new(value).map_err(|error| CliParseError::Parse {
                detail: error.to_string(),
            })?,
            target: matches.value("--target").map(str::to_owned),
            follow: matches.contains("--follow"),
        })
    }
}

pub(super) fn run(options: BuildLogOptions) -> Result<(), CliError> {
    use sinomo_runtime::build::{
        BUILD_LOG_MAX_BYTES, BUILD_LOG_ROUTE, BUILD_RPC_CATEGORY, BuildLogRequest, BuildLogResponse,
    };
    let target = if let Some(target) = options.target {
        rollout_cluster_targets(&RolloutOptions {
            targets: vec![target],
            ..Default::default()
        })?
        .into_iter()
        .next()
        .ok_or_else(|| CliError::Parse {
            detail: "build server was not resolved".into(),
        })?
    } else {
        RolloutTarget::address(
            default_main_daemon_status()?
                .ok_or_else(|| CliError::DaemonNotRunning {
                    detail: "build-log requires a local daemon or --target".into(),
                })?
                .address,
        )
    };
    let mut request = BuildLogRequest {
        job_id: options.job_id,
        offset: 0,
        max_bytes: BUILD_LOG_MAX_BYTES,
        generation: None,
    };
    loop {
        let response: BuildLogResponse = send_rollout_object_rpc_value(
            &target,
            BUILD_LOG_ROUTE,
            BUILD_RPC_CATEGORY,
            &request,
            DAEMON_JOIN_TIMEOUT,
        )?;
        if response.job_id != request.job_id
            || response.next_offset != response.offset + response.bytes.len() as u64
        {
            return Err(CliError::Parse {
                detail: "build log response identity or cursor mismatch".into(),
            });
        }
        std::io::Write::write_all(&mut std::io::stdout(), &response.bytes).map_err(|error| {
            CliError::Io {
                path: "stdout".into(),
                detail: error.to_string(),
            }
        })?;
        request.offset = response.next_offset;
        request.generation = Some(response.generation);
        if response.complete {
            return if response.state == BuildJobState::Succeeded {
                Ok(())
            } else {
                Err(CliError::ExternalCommand {
                    command: "build job".into(),
                    detail: format!("job {} finished {:?}", request.job_id, response.state),
                })
            };
        }
        if response.bytes.len() < BUILD_LOG_MAX_BYTES {
            if !options.follow {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(500));
        }
    }
}
