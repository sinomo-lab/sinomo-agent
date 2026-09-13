use super::*;
use sinomo_runtime::rollout::{self as rollout, RolloutCoordinatorPlan, RolloutCoordinatorTarget};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RecoveryOptions {
    id: Option<String>,
    targets: Vec<String>,
    commit: Option<String>,
    repo: Option<PathBuf>,
}

impl RecoveryOptions {
    pub(super) fn parse(matches: &CliCommandMatches) -> Result<Self, CliParseError> {
        let options = Self {
            id: matches.value("--id").map(str::to_owned),
            targets: matches.values("--target").map(str::to_owned).collect(),
            commit: matches.value("--commit").map(str::to_owned),
            repo: matches.value("--repo").map(PathBuf::from),
        };
        if !options.targets.is_empty() && (options.commit.is_none() || options.id.is_some()) {
            return Err(CliParseError::Parse {
                detail: "verify --target requires --commit and cannot be combined with --id".into(),
            });
        }
        if options.targets.is_empty() && (options.commit.is_some() || options.repo.is_some()) {
            return Err(CliParseError::Parse {
                detail: "--commit and --repo require explicit verification targets".into(),
            });
        }
        Ok(options)
    }
}

pub(super) fn lock_for_new(paths: &SelfRolloutPaths) -> Result<fs::File, CliError> {
    let lock = rollout::lock_rollout_coordinator(paths)?;
    if let Some(previous_id) = rollout::unfinished_rollout_coordinator_plan_ids(paths)?.first() {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "rollout {previous_id} is unfinished; use `sinomo rollout resume --id {previous_id}`, or cancel a settled plan after proving node readiness"
            ),
        });
    }
    Ok(lock)
}

pub(super) fn start(
    paths: &SelfRolloutPaths,
    source_repo: PathBuf,
    source_revision: Option<String>,
    targets: Vec<(RolloutTarget, PreparedClusterBinaryRollout)>,
) -> Result<(), CliError> {
    let source_repo = if source_revision.is_some() {
        fs::canonicalize(&source_repo).map_err(|error| CliError::Io {
            path: source_repo.display().to_string(),
            detail: error.to_string(),
        })?
    } else {
        source_repo
    };
    let state = cluster::read_cluster_state_without_kv(&cluster_paths_from_defaults()?)?;
    let mut plan = RolloutCoordinatorPlan {
        schema_version: 1,
        cancelled: false,
        id: Id::generate().to_string(),
        cluster_id: state.cluster_id.clone(),
        source_repo,
        source_revision,
        targets: Vec::new(),
    };
    for (target, mut prepared) in targets {
        let preflight = read_rollout_target_status_before_cluster_activation(&target)?;
        // Each manifest is owned by exactly one node, even when artifacts are
        // shared by a platform group. Listener preservation follows that node.
        prepared.manifest.upload_id = format!("{}-{}", plan.id, preflight.cluster_target.node_id);
        prepared.manifest.port = preflight.status.port;
        let endpoints = rollout_restart_endpoints_from_state(
            &cluster_paths_from_defaults()?,
            &state,
            &preflight.cluster_target,
        );
        if let Some(port) = endpoints
            .iter()
            .find(|endpoint| {
                rollout_restart_endpoint_process(endpoint, &preflight.cluster_target)
                    == Some(sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE)
            })
            .and_then(|endpoint| endpoint.address.rsplit_once(':')?.1.parse().ok())
        {
            prepared.manifest.shadow_port = port;
        }
        let artifacts = prepared
            .artifacts
            .iter()
            .map(|artifact| {
                let spec = prepared
                    .manifest
                    .artifact(&artifact.artifact_id)
                    .ok_or_else(|| CliError::Parse {
                        detail: "prepared artifact is missing from its manifest".into(),
                    })?;
                Ok(rollout::RolloutArtifactSource {
                    artifact_id: artifact.artifact_id.clone(),
                    path: rollout::cache_rollout_coordinator_artifact(paths, &artifact.path, spec)?,
                })
            })
            .collect::<Result<Vec<_>, CliError>>()?;
        plan.targets.push(RolloutCoordinatorTarget {
            node_id: preflight.cluster_target.node_id,
            address: target.address,
            endpoint: target.endpoint,
            restart_endpoints: endpoints,
            was_voter: preflight.authority_target.is_some(),
            manifest: prepared.manifest,
            artifacts,
            submitted: false,
            verified: false,
        });
    }
    // Preserve the coordinator globally last, independent of platform grouping.
    plan.targets
        .sort_by_key(|target| target.node_id == state.local_node_id);
    rollout::save_rollout_coordinator_plan(paths, &plan)?;
    println!(
        "rollout {} saved; recover with `sinomo rollout resume --id {}`",
        plan.id, plan.id
    );
    run(paths, &mut plan)
}

pub(super) fn start_binary(options: RolloutOptions) -> Result<(), CliError> {
    validate_cluster_native_module_skip(&options)?;
    let paths = rollout_paths_from_defaults()?;
    let _lock = lock_for_new(&paths)?;
    let targets = rollout_cluster_targets(&options)?;
    if targets.is_empty() {
        return Err(CliError::Parse {
            detail: "no rollout targets".into(),
        });
    }
    let artifacts = rollout_prepare_artifacts(&options)?;
    let source_repo = rollout_source_repo_from_options(&options)?;
    let requested_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|error| CliError::Parse {
            detail: error.to_string(),
        })?;
    let mut prepared = Vec::new();
    for target in targets {
        let status = read_rollout_target_status(&target)?;
        ensure_rollout_target_platform_from_status(target.label(), &status)?;
        let mut candidate = prepare_cluster_binary_rollout_for_target(
            artifacts.built_binary.display().to_string(),
            &artifacts.built_binary,
            &artifacts.native_dylib_modules,
            options.skip_native_modules,
            status.target_os.expect("validated OS"),
            status.target_arch.expect("validated architecture"),
            options.host.clone(),
            status.port,
            options.shadow_port,
            requested_at.clone(),
        )?;
        candidate.manifest.host_override = options.host_explicit.then(|| options.host.clone());
        prepared.push((target, candidate));
    }
    start(&paths, source_repo, None, prepared)
}

pub(super) fn resume(options: RecoveryOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    let _lock = rollout::lock_rollout_coordinator(&paths)?;
    let mut plan = rollout::load_rollout_coordinator_plan(&paths, options.id.as_deref())?;
    if plan.cancelled {
        return Err(CliError::ClusterReadOnly {
            detail: "this rollout plan was cancelled; start a new plan".into(),
        });
    }
    run(&paths, &mut plan)
}

pub(super) fn cancel(options: RecoveryOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    let _lock = rollout::lock_rollout_coordinator(&paths)?;
    let mut plan = rollout::load_rollout_coordinator_plan(&paths, options.id.as_deref())?;
    let state = cluster::read_cluster_state_without_kv(&cluster_paths_from_defaults()?)?;
    if state.cluster_id != plan.cluster_id {
        return Err(CliError::ClusterReadOnly {
            detail: "saved rollout belongs to another cluster".into(),
        });
    }
    if !plan.cancelled {
        for saved in plan.targets.iter().filter(|target| target.submitted) {
            let target = target(saved);
            let preflight = read_rollout_target_status_before_cluster_activation(&target)?;
            if preflight.cluster_target.cluster_id != plan.cluster_id {
                return Err(CliError::ClusterReadOnly {
                    detail: "saved rollout target changed cluster".into(),
                });
            }
            let expected = preflight.cluster_target;
            let is_voter = preflight.authority_target.is_some();
            let manifest = saved.manifest.clone();
            let label = target.label().to_string();
            let node_id = expected.node_id.clone();
            run_rollout_authority_readiness_with_wall_clock_guard(
                &label,
                &node_id,
                ROLLOUT_AUTHORITY_READINESS_TIMEOUT
                    .saturating_add(ROLLOUT_AUTHORITY_READINESS_GUARD_GRACE),
                move || {
                    wait_for_rollout_readiness_with_validator(
                        target.label(),
                        &expected,
                        RolloutAuthorityReadinessTiming::production(),
                        |timeout| read_rollout_target_status_with_timeout(&target, timeout),
                        |timeout| {
                            let deadline = Instant::now() + timeout;
                            let upload: rollout::RolloutUploadStatus =
                                send_rollout_object_rpc_value(
                                    &target,
                                    rollout::ROLLOUT_UPLOAD_STATUS_ROUTE,
                                    rollout::ROLLOUT_RPC_CATEGORY,
                                    &manifest,
                                    if is_voter { timeout / 2 } else { timeout },
                                )?;
                            if !upload.can_retire() {
                                return Err(CliError::ClusterReadOnly { detail: "plan cannot be cancelled without a terminal receipt and proof that no replacements are pending".into() });
                            }
                            if is_voter {
                                let remaining = rollout_authority_remaining(
                                    deadline,
                                    Instant::now(),
                                    "cancellation proof exhausted its budget",
                                )?;
                                probe_rollout_target_cluster_kv_authority_readiness(
                                    &expected, remaining,
                                )?;
                            }
                            Ok(())
                        },
                        Instant::now,
                        thread::sleep,
                        rollout_main_status_pid,
                    )
                },
            )?;
        }
        plan.cancelled = true;
        rollout::save_rollout_coordinator_plan(&paths, &plan)?;
    }
    println!("rollout {}: settled plan cancelled", plan.id);
    Ok(())
}

fn target(saved: &RolloutCoordinatorTarget) -> RolloutTarget {
    RolloutTarget {
        address: saved.address.clone(),
        endpoint: saved.endpoint.clone(),
        rollout_node_id: Some(saved.node_id.clone()),
        restart_endpoints: saved.restart_endpoints.clone(),
    }
}

fn expectation(saved: &RolloutCoordinatorTarget) -> RolloutProgressExpectation {
    RolloutProgressExpectation {
        source: saved.manifest.source.clone(),
        requested_at: saved.manifest.requested_at.clone(),
        source_head: None,
    }
}

fn current_matches(saved: &RolloutCoordinatorTarget, status: &DaemonStatus) -> bool {
    status
        .rollout
        .as_ref()
        .is_some_and(|status| expectation(saved).matches(status))
}

fn verify_target(
    plan: &RolloutCoordinatorPlan,
    saved: &RolloutCoordinatorTarget,
) -> Result<(), CliError> {
    let target = target(saved);
    let preflight = read_rollout_target_status_before_cluster_activation(&target)?;
    if preflight.cluster_target.cluster_id != plan.cluster_id
        || preflight.cluster_target.node_id != saved.node_id
        || !current_matches(saved, &preflight.status)
    {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "{} does not report the saved rollout's target and operation identity",
                saved.node_id
            ),
        });
    }
    let expected = RolloutAuthorityTarget {
        cluster_id: plan.cluster_id.clone(),
        node_id: saved.node_id.clone(),
    };
    rollout_authority_status_pid(&expected, &preflight.status)
        .map_err(|detail| CliError::ClusterReadOnly { detail })?;
    let response = rollout_terminal_response(
        preflight.status.rollout.as_ref().expect("checked rollout"),
        &expectation(saved),
    )
    .ok_or_else(|| CliError::ClusterReadOnly {
        detail: format!("{} rollout has not finalized", saved.node_id),
    })?;
    let authority = if saved.was_voter {
        Some(&expected)
    } else {
        preflight.authority_target.as_ref()
    };
    ensure_rollout_target_authority_ready_after_acceptance(
        &target,
        authority,
        plan.source_revision
            .as_deref()
            .map(|revision| RolloutRuntimeSourceIdentity {
                repo: &plan.source_repo,
                revision,
            }),
        saved.manifest.host_override.as_deref(),
        saved.manifest.port,
        &response,
    )?;
    if preflight.status.port != saved.manifest.port
        || saved
            .manifest
            .host_override
            .as_ref()
            .is_some_and(|host| host != &preflight.status.host)
    {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "{} listener differs from its saved rollout settings",
                saved.node_id
            ),
        });
    }
    Ok(())
}

fn run(paths: &SelfRolloutPaths, plan: &mut RolloutCoordinatorPlan) -> Result<(), CliError> {
    let state = cluster::read_cluster_state_without_kv(&cluster_paths_from_defaults()?)?;
    if state.cluster_id != plan.cluster_id {
        return Err(CliError::ClusterReadOnly {
            detail: "saved rollout belongs to a different cluster".into(),
        });
    }
    for index in 0..plan.targets.len() {
        let saved = plan.targets[index].clone();
        let target = target(&saved);
        let expected = RolloutAuthorityTarget {
            cluster_id: plan.cluster_id.clone(),
            node_id: saved.node_id.clone(),
        };
        let status = read_rollout_target_status(&target);
        let matching = status
            .as_ref()
            .is_ok_and(|status| current_matches(&saved, status));
        if saved.verified {
            // Revalidate even completed voters before moving to an unfinished one.
            verify_target(plan, &saved)?;
            continue;
        }
        if matching {
            observe(&target, &saved, &expected)?;
        } else {
            if saved.submitted {
                // Only this typed route proves that replay is protected by the
                // receiver's durable activation claim. Old daemons fail closed.
                let upload: rollout::RolloutUploadStatus = send_rollout_object_rpc_value(
                    &target,
                    rollout::ROLLOUT_UPLOAD_STATUS_ROUTE,
                    rollout::ROLLOUT_RPC_CATEGORY,
                    &saved.manifest,
                    DAEMON_JOIN_TIMEOUT,
                )?;
                if upload.activation_started || upload.receipt.is_some() {
                    observe(&target, &saved, &expected)?;
                    verify_target(plan, &saved)?;
                    plan.targets[index].verified = true;
                    rollout::save_rollout_coordinator_plan(paths, plan)?;
                    continue;
                }
            }
            let preflight = read_rollout_target_status_before_cluster_activation(&target)?;
            if preflight.cluster_target.cluster_id != plan.cluster_id {
                return Err(CliError::ClusterReadOnly {
                    detail: "rollout target changed cluster".into(),
                });
            }
            plan.targets[index].submitted = true;
            rollout::save_rollout_coordinator_plan(paths, plan)?;
            let prepared = PreparedClusterBinaryRollout {
                manifest: saved.manifest.clone(),
                artifacts: saved
                    .artifacts
                    .iter()
                    .map(|artifact| RolloutUploadSource {
                        artifact_id: artifact.artifact_id.clone(),
                        path: artifact.path.clone(),
                    })
                    .collect(),
            };
            let response = match send_prepared_daemon_rollout_binary(&target, &prepared, &expected)
            {
                Ok(response) => response,
                Err(
                    error @ CliError::NetworkDelivery {
                        delivery_state: network::NetworkDeliveryState::DefinitelyNotSent,
                        ..
                    },
                ) => {
                    plan.targets[index].submitted = false;
                    rollout::save_rollout_coordinator_plan(paths, plan)?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            if response.status != "accepted" {
                return Err(CliError::ClusterReadOnly {
                    detail: format!(
                        "{} rollout {}: {}",
                        saved.node_id, response.status, response.detail
                    ),
                });
            }
        }
        verify_target(plan, &saved)?;
        plan.targets[index].verified = true;
        rollout::save_rollout_coordinator_plan(paths, plan)?;
        println!("{}: verified", saved.node_id);
    }
    println!(
        "rollout {}: all {} targets verified",
        plan.id,
        plan.targets.len()
    );
    Ok(())
}

fn observe(
    target: &RolloutTarget,
    saved: &RolloutCoordinatorTarget,
    expected: &RolloutAuthorityTarget,
) -> Result<(), CliError> {
    let restart = rollout_expected_restart_evidence(
        target,
        expected,
        saved.manifest.host_override.as_deref(),
        saved.manifest.port,
        saved.manifest.shadow_port,
    );
    let response = send_rollout_request_with_progress(
        target,
        "resumed rollout",
        expectation(saved),
        restart,
        |_| {
            Ok(ClusterRolloutBinaryResponse {
                status: "in-progress".into(),
                detail: "observing original request".into(),
                main_binary: PathBuf::new(),
                shadow_binary: PathBuf::new(),
                source_head: None,
            })
        },
    )?;
    if response.status != "accepted" {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "{} rollout {}: {}",
                saved.node_id, response.status, response.detail
            ),
        });
    }
    Ok(())
}

pub(super) fn verify(options: RecoveryOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    if options.targets.is_empty() {
        let plan = rollout::load_rollout_coordinator_plan(&paths, options.id.as_deref())?;
        for saved in &plan.targets {
            verify_target(&plan, saved)?;
            println!("{}: verified", saved.node_id);
        }
    } else {
        let repo = options.repo.unwrap_or(paths.source_repo);
        let commit = options.commit.expect("validated commit");
        let options = RolloutOptions {
            targets: options.targets,
            ..Default::default()
        };
        for target in rollout_cluster_targets(&options)? {
            let preflight = read_rollout_target_status_before_cluster_activation(&target)?;
            rollout_authority_status_pid(&preflight.cluster_target, &preflight.status)
                .map_err(|detail| CliError::ClusterReadOnly { detail })?;
            let response = ClusterRolloutBinaryResponse {
                status: "accepted".into(),
                detail: String::new(),
                main_binary: PathBuf::new(),
                shadow_binary: PathBuf::new(),
                source_head: None,
            };
            ensure_rollout_target_authority_ready_after_acceptance(
                &target,
                preflight.authority_target.as_ref(),
                Some(RolloutRuntimeSourceIdentity {
                    repo: &repo,
                    revision: &commit,
                }),
                None,
                preflight.status.port,
                &response,
            )?;
            println!(
                "{}: verified runtime {}",
                preflight.cluster_target.node_id, commit
            );
        }
    }
    Ok(())
}
