use super::*;
use std::path::Component;

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
    let bytes = fs::read(&options.source_path).map_err(|source| CliError::Io {
        path: options.source_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let content_hash = sinomo_runtime::module_host::native_module_content_hash(&bytes);
    let library_name = match options.library_name {
        Some(library_name) => library_name,
        None => source_library_name(&options.source_path)?,
    };
    let native_module = unsafe {
        sinomo_runtime::module_host::NativeModule::load_from_content_addressed_path(
            &options.source_path,
            &content_hash,
        )
    }
    .map_err(native_module_host_error_to_cli)?;
    let descriptor = native_module.descriptor().clone();
    drop(native_module);

    let descriptor_module_id = descriptor.module_id.clone();
    let descriptor_platform_triple = descriptor.platform_triple.clone();
    let descriptor_interface_version = descriptor.interface_version.clone();
    let module_id = parse_cli_module_id(&descriptor.module_id)?;
    if let Some(expected) = options.expected_module_id.as_ref() {
        if module_id != *expected {
            return Err(CliError::Parse {
                detail: format!(
                    "native module descriptor module_id `{}` does not match expected `{expected}`",
                    descriptor.module_id
                ),
            });
        }
    }
    if let Some(expected) = options.expected_interface_version.as_ref() {
        if descriptor.interface_version != *expected {
            return Err(CliError::Parse {
                detail: format!(
                    "native module descriptor interface `{}` does not match expected `{expected}`",
                    descriptor.interface_version
                ),
            });
        }
    }

    let cache_key = options
        .cache_key
        .unwrap_or_else(|| descriptor.module_id.replace('.', "/"));
    let cache_key = validate_native_module_cache_key(&cache_key)?;
    let cache_path = sinomo_runtime::module_host::native_module_cache_path(
        data_dir.root(),
        &cache_key,
        &content_hash,
        &library_name,
    );
    write_native_module_cache_file(&cache_path, &bytes, &options.source_path)?;

    let activated_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|source| CliError::Parse {
            detail: format!("format native module activation timestamp: {source}"),
        })?;
    let state = sinomo_runtime::module::NativeDylibModuleLoaderState {
        module_id: module_id.clone(),
        artifact_id: options.artifact_id.clone(),
        content_hash: content_hash.clone(),
        cache_key,
        library_name,
        platform_triple: descriptor_platform_triple,
        native_abi_version: descriptor.native_abi_version,
        interface_version: descriptor_interface_version,
        activated_at,
        source: options
            .source
            .unwrap_or_else(|| format!("local:{}", options.source_path.display())),
    };
    let state_path =
        sinomo_runtime::module::write_native_dylib_module_loader_state(&data_dir.root(), &state)
            .map_err(module_loader_store_error_to_cli)?;

    println!("module: {descriptor_module_id}");
    println!("module-state-id: {}", state.module_id);
    if let Some(artifact_id) = state.artifact_id.as_ref() {
        println!("artifact: {artifact_id}");
    }
    println!("interface: {}", state.interface_version);
    println!("platform: {}", state.platform_triple);
    println!("hash: {}", state.content_hash);
    println!("cache: {}", cache_path.display());
    println!("state: {}", state_path.display());
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
        let path = sinomo_runtime::module_host::native_module_cache_path(
            data_dir.root(),
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        cache_path = Some(path.clone());
        if path.exists() {
            runtime_unload_attempted = true;
            match unsafe {
                sinomo_runtime::module_host::NativeModule::load_from_content_addressed_path(
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

fn write_native_module_cache_file(
    path: &Path,
    bytes: &[u8],
    source_path: &Path,
) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    if path.exists() {
        let existing = fs::read(path).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        if sinomo_runtime::module_host::native_module_content_hash(&existing)
            == sinomo_runtime::module_host::native_module_content_hash(bytes)
        {
            return Ok(());
        }
        fs::remove_file(path).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let tmp_path = path.with_file_name(format!(
        ".{}.{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("native-module"),
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    fs::write(&tmp_path, bytes).map_err(|source| CliError::Io {
        path: tmp_path.display().to_string(),
        detail: source.to_string(),
    })?;
    if let Ok(metadata) = fs::metadata(source_path) {
        let _ = fs::set_permissions(&tmp_path, metadata.permissions());
    }
    fs::rename(&tmp_path, path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn source_library_name(path: &Path) -> Result<String, CliError> {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(CliError::Parse {
            detail: format!("native module path `{}` has no file name", path.display()),
        });
    };
    validate_native_module_library_name(file_name)
}

fn validate_native_module_cache_key(value: &str) -> Result<String, CliError> {
    let trimmed = non_empty_value(value, "native module cache key")?;
    let path = Path::new(&trimmed);
    if path.is_absolute() {
        return Err(CliError::Parse {
            detail: format!("native module cache key `{trimmed}` must be relative"),
        });
    }
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(CliError::Parse {
                    detail: format!(
                        "native module cache key `{trimmed}` must not contain `.` or `..`"
                    ),
                });
            }
        }
    }
    Ok(trimmed)
}

fn validate_native_module_library_name(value: &str) -> Result<String, CliError> {
    let trimmed = non_empty_value(value, "native module library name")?;
    let path = Path::new(&trimmed);
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(trimmed),
        _ => Err(CliError::Parse {
            detail: format!("native module library name `{trimmed}` must be a file name"),
        }),
    }
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

fn native_module_host_error_to_cli(
    error: sinomo_runtime::module_host::NativeModuleHostError,
) -> CliError {
    match error {
        sinomo_runtime::module_host::NativeModuleHostError::Io { path, detail }
        | sinomo_runtime::module_host::NativeModuleHostError::Load { path, detail } => {
            CliError::Io { path, detail }
        }
        other => CliError::Parse {
            detail: other.to_string(),
        },
    }
}
