use super::*;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum ModuleCliCommand {
    Help,
    NativeDylib(ModuleNativeDylibCliCommand),
    Wasix(ModuleWasixCliCommand),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum ModuleNativeDylibCliCommand {
    Activate(ModuleNativeDylibActivateOptions),
    Unload(ModuleNativeDylibUnloadOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum ModuleWasixCliCommand {
    Activate(ModuleWasixActivateOptions),
    Unload,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct ModuleNativeDylibActivateOptions {
    pub(crate) source_path: PathBuf,
    pub(crate) expected_module_id: Option<sinomo_runtime::module::ModuleId>,
    pub(crate) artifact_id: Option<sinomo_runtime::module::ArtifactId>,
    pub(crate) expected_interface_version: Option<String>,
    pub(crate) cache_key: Option<String>,
    pub(crate) library_name: Option<String>,
    pub(crate) source: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ModuleWasixActivateOptions {
    pub(crate) source_path: PathBuf,
    pub(crate) artifact_id: Option<sinomo_runtime::module::ArtifactId>,
    pub(crate) library_name: Option<String>,
    pub(crate) source: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ModuleNativeDylibUnloadOptions {
    pub(crate) module_id: sinomo_runtime::module::ModuleId,
}

pub(crate) fn parse_module_command(args: &[String]) -> Result<ModuleCliCommand, CliError> {
    match args.split_first() {
        None => Ok(ModuleCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(ModuleCliCommand::Help)
        }
        Some((command, rest)) if command == "native-dylib" => {
            parse_module_native_dylib_command(rest)
        }
        Some((command, rest)) if command == "wasix" => parse_module_wasix_command(rest),
        Some((command, _)) => Err(CliError::Parse {
            detail: format!(
                "module command `{command}` is not implemented; run `sinomo module help`"
            ),
        }),
    }
}

fn parse_module_native_dylib_command(args: &[String]) -> Result<ModuleCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "activate" => Ok(ModuleCliCommand::NativeDylib(
            ModuleNativeDylibCliCommand::Activate(parse_module_native_dylib_activate_options(
                rest,
            )?),
        )),
        Some((command, rest)) if command == "unload" => Ok(ModuleCliCommand::NativeDylib(
            ModuleNativeDylibCliCommand::Unload(parse_module_native_dylib_unload_options(rest)?),
        )),
        _ => Err(CliError::Usage),
    }
}

fn parse_module_wasix_command(args: &[String]) -> Result<ModuleCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "activate" => Ok(ModuleCliCommand::Wasix(
            ModuleWasixCliCommand::Activate(parse_module_wasix_activate_options(rest)?),
        )),
        Some((command, [])) if command == "unload" => {
            Ok(ModuleCliCommand::Wasix(ModuleWasixCliCommand::Unload))
        }
        _ => Err(CliError::Usage),
    }
}

fn parse_module_native_dylib_activate_options(
    args: &[String],
) -> Result<ModuleNativeDylibActivateOptions, CliError> {
    let mut options = ModuleNativeDylibActivateOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--module" | "--module-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.expected_module_id = Some(parse_cli_module_id(value)?);
            }
            "--artifact" | "--artifact-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.artifact_id = Some(parse_cli_module_artifact_id(value)?);
            }
            "--interface" | "--interface-version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.expected_interface_version = Some(non_empty_value(value, "interface")?);
            }
            "--cache-key" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.cache_key = Some(validate_native_module_cache_key(value)?);
            }
            "--library-name" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.library_name = Some(validate_native_module_library_name(value)?);
            }
            "--source" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.source = Some(non_empty_value(value, "source")?);
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value => {
                if options.source_path.as_os_str().is_empty() {
                    options.source_path = PathBuf::from(value);
                } else {
                    return Err(CliError::Usage);
                }
            }
        }
        index += 1;
    }
    if options.source_path.as_os_str().is_empty() {
        return Err(CliError::Usage);
    }
    Ok(options)
}

fn parse_module_wasix_activate_options(
    args: &[String],
) -> Result<ModuleWasixActivateOptions, CliError> {
    let mut source_path = None;
    let mut artifact_id = None;
    let mut library_name = None;
    let mut source = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--artifact" | "--artifact-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                artifact_id = Some(parse_cli_module_artifact_id(value)?);
            }
            "--library-name" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                library_name = Some(validate_native_module_library_name(value)?);
            }
            "--source" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                source = Some(non_empty_value(value, "source")?);
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value => {
                if source_path.is_none() {
                    source_path = Some(PathBuf::from(value));
                } else {
                    return Err(CliError::Usage);
                }
            }
        }
        index += 1;
    }
    Ok(ModuleWasixActivateOptions {
        source_path: source_path.ok_or(CliError::Usage)?,
        artifact_id,
        library_name,
        source,
    })
}

fn parse_module_native_dylib_unload_options(
    args: &[String],
) -> Result<ModuleNativeDylibUnloadOptions, CliError> {
    let mut module_id = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--module" | "--module-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                module_id = Some(parse_cli_module_id(value)?);
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value => {
                if module_id.is_none() {
                    module_id = Some(parse_cli_module_id(value)?);
                } else {
                    return Err(CliError::Usage);
                }
            }
        }
        index += 1;
    }
    Ok(ModuleNativeDylibUnloadOptions {
        module_id: module_id.ok_or(CliError::Usage)?,
    })
}

pub(crate) fn run_module_cli_command(command: ModuleCliCommand) -> Result<(), CliError> {
    match command {
        ModuleCliCommand::Help => {
            print_module_help();
            Ok(())
        }
        ModuleCliCommand::NativeDylib(ModuleNativeDylibCliCommand::Activate(options)) => {
            activate_native_dylib_module(options)
        }
        ModuleCliCommand::NativeDylib(ModuleNativeDylibCliCommand::Unload(options)) => {
            unload_native_dylib_module(options)
        }
        ModuleCliCommand::Wasix(ModuleWasixCliCommand::Activate(options)) => {
            activate_wasix_native_module(options)
        }
        ModuleCliCommand::Wasix(ModuleWasixCliCommand::Unload) => unload_wasix_native_module(),
    }
}

fn activate_wasix_native_module(options: ModuleWasixActivateOptions) -> Result<(), CliError> {
    activate_native_dylib_module(ModuleNativeDylibActivateOptions {
        source_path: options.source_path,
        expected_module_id: Some(parse_cli_module_id(
            sinomo_runtime::sandbox_wasix_api::SINOMO_SANDBOX_WASIX_MODULE_ID,
        )?),
        artifact_id: options.artifact_id,
        expected_interface_version: Some(
            sinomo_runtime::sandbox_wasix_api::SINOMO_SANDBOX_WASIX_INTERFACE_VERSION.to_string(),
        ),
        cache_key: Some("module/sandbox/wasix".to_string()),
        library_name: options.library_name,
        source: options.source,
    })
}

fn unload_wasix_native_module() -> Result<(), CliError> {
    unload_native_dylib_module(ModuleNativeDylibUnloadOptions {
        module_id: parse_cli_module_id(
            sinomo_runtime::sandbox_wasix_api::SINOMO_SANDBOX_WASIX_MODULE_ID,
        )?,
    })
}

fn activate_native_dylib_module(options: ModuleNativeDylibActivateOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let result = sinomo_runtime::module::install_native_dylib_module_from_file(
        &data_dir.root(),
        &options.source_path,
        sinomo_runtime::module::NativeDylibModuleInstallRequest {
            expected_module_id: options.expected_module_id,
            artifact_id: options.artifact_id,
            expected_interface_version: options.expected_interface_version,
            cache_key: options.cache_key,
            library_name: options.library_name,
            source: options
                .source
                .unwrap_or_else(|| format!("local:{}", options.source_path.display())),
        },
    )
    .map_err(native_dylib_module_install_error_to_cli)?;

    println!("module: {}", result.descriptor_module_id);
    println!("module-state-id: {}", result.state.module_id);
    if let Some(artifact_id) = result.state.artifact_id.as_ref() {
        println!("artifact: {artifact_id}");
    }
    println!("interface: {}", result.state.interface_version);
    println!("platform: {}", result.state.platform_triple);
    println!("hash: {}", result.state.content_hash);
    println!("cache: {}", result.cache_path.display());
    println!("state: {}", result.state_path.display());
    Ok(())
}

fn unload_native_dylib_module(options: ModuleNativeDylibUnloadOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let module_id = options.module_id;
    let previous_state = sinomo_runtime::module::remove_native_dylib_module_loader_state(
        &data_dir.root(),
        &module_id,
    )
    .map_err(module_loader_store_error_to_cli)?;

    let mut artifact_id = None;
    let mut content_hash = None;
    let mut cache_key = None;
    let mut library_name = None;
    let mut runtime_unload_attempted = false;
    let mut runtime_unload_succeeded = previous_state.is_none();
    let mut runtime_unload_detail = "no active native-dylib loader state".to_string();
    let mut cache_path = None;

    if let Some(state) = previous_state.as_ref() {
        artifact_id = state.artifact_id.clone();
        content_hash = Some(state.content_hash.clone());
        cache_key = Some(state.cache_key.clone());
        library_name = Some(state.library_name.clone());
        let path = sinomo_runtime::module::native_module_cache_path(
            data_dir.root(),
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        cache_path = Some(path.clone());
        if path.exists() {
            runtime_unload_attempted = true;
            match unsafe {
                sinomo_runtime::module::NativeModule::load_from_content_addressed_path(
                    &path,
                    &state.content_hash,
                )
            } {
                Ok(module) => {
                    let report = module.unload();
                    runtime_unload_succeeded = report.shutdown_ok && report.library_handle_released;
                    runtime_unload_detail = format!(
                        "{}; library_handle_released={}",
                        report.detail, report.library_handle_released
                    );
                }
                Err(error) => {
                    runtime_unload_succeeded = false;
                    runtime_unload_detail =
                        format!("load cached native module for unload probe: {error}");
                }
            }
        } else {
            runtime_unload_detail = format!(
                "cached native module library not found at {}; active state was still removed",
                path.display()
            );
        }
    }

    let record = sinomo_runtime::module::ModuleUnloadRecord {
        module_id: module_id.clone(),
        backend: sinomo_runtime::core::ModuleKind::NativeDylib,
        artifact_id,
        content_hash,
        cache_key,
        library_name,
        state_removed: previous_state.is_some(),
        runtime_unload_attempted,
        runtime_unload_succeeded,
        runtime_unload_detail: runtime_unload_detail.clone(),
        unloaded_at: sinomo_runtime::module::module_loader_applied_at_now(),
        source: "local-cli".to_string(),
    };
    let record_path = sinomo_runtime::module::write_module_unload_record(&data_dir.root(), &record)
        .map_err(module_loader_store_error_to_cli)?;

    println!("module-state-id: {module_id}");
    println!(
        "state: {}",
        if previous_state.is_some() {
            "removed"
        } else {
            "not-active"
        }
    );
    if let Some(cache_path) = cache_path.as_ref() {
        println!("cache: {}", cache_path.display());
    }
    println!(
        "runtime-unload: {}",
        if runtime_unload_attempted {
            if runtime_unload_succeeded {
                "ok"
            } else {
                "failed"
            }
        } else {
            "skipped"
        }
    );
    println!("detail: {runtime_unload_detail}");
    println!("record: {}", record_path.display());
    Ok(())
}

fn validate_native_module_cache_key(value: &str) -> Result<String, CliError> {
    sinomo_runtime::module::validate_native_dylib_module_cache_key(value)
        .map_err(native_dylib_module_install_error_to_cli)
}

fn validate_native_module_library_name(value: &str) -> Result<String, CliError> {
    sinomo_runtime::module::validate_native_dylib_module_library_name(value)
        .map_err(native_dylib_module_install_error_to_cli)
}

fn non_empty_value(value: &str, label: &str) -> Result<String, CliError> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Err(CliError::Parse {
            detail: format!("{label} must be non-empty"),
        });
    }
    Ok(trimmed)
}

fn parse_cli_module_id(value: &str) -> Result<sinomo_runtime::module::ModuleId, CliError> {
    sinomo_runtime::module::ModuleId::new(value.to_string())
        .or_else(|_| sinomo_runtime::module::ModuleId::from_legacy_named(value))
        .map_err(|source| CliError::Parse {
            detail: format!("invalid module id `{value}`: {source}"),
        })
}

fn parse_cli_module_artifact_id(
    value: &str,
) -> Result<sinomo_runtime::module::ArtifactId, CliError> {
    sinomo_runtime::module::ArtifactId::new(value.to_string())
        .or_else(|_| sinomo_runtime::module::ArtifactId::from_legacy_named(value))
        .map_err(|source| CliError::Parse {
            detail: format!("invalid module artifact id `{value}`: {source}"),
        })
}

fn module_loader_store_error_to_cli(
    error: sinomo_runtime::module::ModuleLoaderStoreError,
) -> CliError {
    match error {
        sinomo_runtime::module::ModuleLoaderStoreError::Io { path, detail }
        | sinomo_runtime::module::ModuleLoaderStoreError::Object { path, detail } => {
            CliError::Io { path, detail }
        }
    }
}

fn native_dylib_module_install_error_to_cli(
    error: sinomo_runtime::module::NativeDylibModuleInstallError,
) -> CliError {
    match error {
        sinomo_runtime::module::NativeDylibModuleInstallError::Io { path, detail } => {
            CliError::Io { path, detail }
        }
        sinomo_runtime::module::NativeDylibModuleInstallError::Host(source) => {
            native_module_error_to_cli(source)
        }
        sinomo_runtime::module::NativeDylibModuleInstallError::Store(source) => {
            module_loader_store_error_to_cli(source)
        }
        sinomo_runtime::module::NativeDylibModuleInstallError::Descriptor { detail } => {
            CliError::Parse { detail }
        }
    }
}

fn native_module_error_to_cli(error: sinomo_runtime::module::NativeModuleHostError) -> CliError {
    match error {
        sinomo_runtime::module::NativeModuleHostError::Io { path, detail }
        | sinomo_runtime::module::NativeModuleHostError::Load { path, detail } => {
            CliError::Io { path, detail }
        }
        other => CliError::Parse {
            detail: other.to_string(),
        },
    }
}
