use crate::internal::*;
use sinomo_module::{
    ModuleArtifactInvocation, ModuleArtifactRunner, ModuleInvocationLane, ModuleLoaderStoreError,
    ModuleReplayFixture, ModuleReplayReport, SINOMO_MODULE_V1_INTERFACE_VERSION,
    SinomoModuleV1Result, run_module_replay_fixture,
};
use thiserror::Error;
use wasmtime::component::{
    Component, Linker, Val,
    types::{ComponentItem, Type},
};

#[derive(Clone, Debug)]
pub struct SandboxWasmModuleArtifactRunner {
    registry: SandboxWasmModuleArtifactRegistry,
    runtime_root: PathBuf,
    artifact_root: PathBuf,
    default_toolchain_id: SandboxToolchainId,
}

impl SandboxWasmModuleArtifactRunner {
    pub fn new(
        registry: SandboxWasmModuleArtifactRegistry,
        runtime_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            registry,
            runtime_root: runtime_root.into(),
            artifact_root: PathBuf::new(),
            default_toolchain_id: SandboxToolchainId::from_legacy_named(
                BUSYBOX_TOOLCHAIN_LEGACY_ID,
            )
            .expect("valid bundled sandbox toolchain id"),
        }
    }

    pub fn with_artifact_root(mut self, artifact_root: impl Into<PathBuf>) -> Self {
        self.artifact_root = artifact_root.into();
        self
    }

    pub fn from_data_dir(data_dir_root: &Path) -> Result<Self, SandboxStoreError> {
        Ok(Self::new(
            read_sandbox_wasm_module_artifact_registry(data_dir_root)?.unwrap_or_default(),
            data_dir_root.join("sandbox").join("runtime"),
        )
        .with_artifact_root(data_dir_root))
    }
}

pub fn run_sandbox_wasm_module_replay_fixture(
    data_dir_root: &Path,
    fixture: &ModuleReplayFixture,
) -> Result<ModuleReplayReport, SandboxModuleReplayError> {
    let runner = SandboxWasmModuleArtifactRunner::from_data_dir(data_dir_root)?;
    run_module_replay_fixture(data_dir_root, fixture, &runner).map_err(Into::into)
}

#[derive(Debug, Error)]
pub enum SandboxModuleReplayError {
    #[error(transparent)]
    SandboxStore(#[from] SandboxStoreError),
    #[error(transparent)]
    ModuleStore(#[from] ModuleLoaderStoreError),
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SandboxModuleComponentContractError {
    #[error("sandbox WASM module is not a valid component: {detail}")]
    InvalidComponent { detail: String },
    #[error("sinomo-module-v1 component is missing export `{export}`")]
    MissingExport { export: String },
    #[error("sinomo-module-v1 component export `{export}` has wrong shape: {detail}")]
    InvalidExportShape { export: String, detail: String },
}

pub fn validate_sinomo_module_v1_component_contract(
    wasm: &[u8],
) -> Result<(), SandboxModuleComponentContractError> {
    let engine = sinomo_module_component_engine()?;
    let component = Component::new(&engine, wasm).map_err(|source| {
        SandboxModuleComponentContractError::InvalidComponent {
            detail: format!("{source:?}"),
        }
    })?;
    let export_name = "sinomo:module-v1/module";
    let (module_item, _) = component.get_export(None, export_name).ok_or_else(|| {
        SandboxModuleComponentContractError::MissingExport {
            export: export_name.to_string(),
        }
    })?;
    let module_instance = match module_item {
        ComponentItem::ComponentInstance(instance) => instance,
        other => {
            return Err(SandboxModuleComponentContractError::InvalidExportShape {
                export: export_name.to_string(),
                detail: format!("expected component instance, found {other:?}"),
            });
        }
    };
    let run_export = format!("{export_name}.run");
    let run_item = module_instance.get_export(&engine, "run").ok_or_else(|| {
        SandboxModuleComponentContractError::MissingExport {
            export: run_export.clone(),
        }
    })?;
    let run_func = match run_item {
        ComponentItem::ComponentFunc(func) => func,
        other => {
            return Err(SandboxModuleComponentContractError::InvalidExportShape {
                export: run_export,
                detail: format!("expected component function, found {other:?}"),
            });
        }
    };
    validate_sinomo_module_v1_run_signature(&run_export, &run_func)
}

fn sinomo_module_component_engine() -> Result<wasmtime::Engine, SandboxModuleComponentContractError>
{
    let mut config = wasmtime::Config::new();
    config.wasm_component_model(true);
    config.consume_fuel(true);
    wasmtime::Engine::new(&config).map_err(|source| {
        SandboxModuleComponentContractError::InvalidComponent {
            detail: format!("{source:?}"),
        }
    })
}

impl ModuleArtifactRunner for SandboxWasmModuleArtifactRunner {
    fn invoke(&self, invocation: &ModuleArtifactInvocation) -> Result<String, String> {
        let artifact = self
            .registry
            .artifact(&invocation.artifact_id)
            .ok_or_else(|| {
                format!(
                    "module artifact {} is not registered as a sandbox WASM module",
                    invocation.artifact_id
                )
            })?;
        run_sandbox_wasm_module_artifact(
            artifact,
            invocation,
            &self.runtime_root,
            &self.artifact_root,
            &self.default_toolchain_id,
        )
    }
}

fn run_sandbox_wasm_module_artifact(
    artifact: &SandboxWasmModuleArtifact,
    invocation: &ModuleArtifactInvocation,
    runtime_root: &Path,
    artifact_root: &Path,
    default_toolchain_id: &SandboxToolchainId,
) -> Result<String, String> {
    let wasm_path = if artifact.wasm_path.is_absolute() {
        artifact.wasm_path.clone()
    } else {
        artifact_root.join(&artifact.wasm_path)
    };
    let wasm = fs::read(&wasm_path).map_err(|source| {
        format!(
            "failed to read sandbox WASM artifact {} at {}: {source}",
            artifact.artifact_id,
            wasm_path.display()
        )
    })?;
    let actual_hash = sandbox_module_hash(&wasm);
    if actual_hash != artifact.content_hash {
        return Err(format!(
            "sandbox WASM artifact {} hash mismatch: expected {}, actual {actual_hash}",
            artifact.artifact_id, artifact.content_hash
        ));
    }

    if artifact.component_model {
        return run_sinomo_module_v1_component_artifact(artifact, invocation, &wasm);
    }

    let sandbox_id = artifact.sandbox_id.clone().unwrap_or_else(|| {
        SandboxId::derived(
            "sandbox-wasm-module-artifact",
            artifact.artifact_id.as_str().as_bytes(),
        )
    });
    let store = LocalSandboxInstanceStore::new(runtime_root);
    ensure_sandbox_instance(&store, &sandbox_id, default_toolchain_id).map_err(|source| {
        format!(
            "failed to prepare sandbox instance {sandbox_id} for artifact {}: {source}",
            artifact.artifact_id
        )
    })?;

    let mut env = artifact.env.clone();
    env.insert(
        "SINOMO_NODE_ID".to_string(),
        invocation.node_id.as_str().to_string(),
    );
    env.insert(
        "SINOMO_MODULE_ID".to_string(),
        invocation.module_id.as_str().to_string(),
    );
    env.insert(
        "SINOMO_ARTIFACT_ID".to_string(),
        invocation.artifact_id.as_str().to_string(),
    );
    env.insert(
        "SINOMO_INVOCATION_LANE".to_string(),
        invocation_lane_label(&invocation.lane).to_string(),
    );
    let argv = if artifact.argv.is_empty() {
        vec!["module.wasm".to_string()]
    } else {
        artifact.argv.clone()
    };
    let timeout_ms = artifact.timeout_ms.unwrap_or(artifact.limits.wall_time_ms);
    let runner = LocalStagedSandboxCommandRunner::new()
        .map_err(|source| format!("failed to initialize sandbox WASI runner: {source}"))?;
    let outcome = runner
        .run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
            &store,
            &sandbox_id,
            &wasm,
            argv,
            Some(invocation.input.as_bytes().to_vec()),
            timeout_ms,
            Some(artifact.limits.clone()),
            Vec::new(),
            None,
            artifact.abi.clone(),
            BTreeMap::new(),
            artifact.cwd.clone(),
            env,
            false,
            true,
            true,
            true,
            true,
            Vec::new(),
        )
        .map_err(|source| {
            format!(
                "sandbox WASM artifact {} failed during execution: {source}",
                artifact.artifact_id
            )
        })?;
    if outcome.response.exit_status == 0 {
        let output = String::from_utf8_lossy(&outcome.stdout).to_string();
        validate_module_artifact_output_contract(artifact, &output)?;
        Ok(output)
    } else {
        Err(format!(
            "sandbox WASM artifact {} exited with status {}: stdout={} stderr={}",
            artifact.artifact_id,
            outcome.response.exit_status,
            String::from_utf8_lossy(&outcome.stdout),
            String::from_utf8_lossy(&outcome.stderr)
        ))
    }
}

fn run_sinomo_module_v1_component_artifact(
    artifact: &SandboxWasmModuleArtifact,
    invocation: &ModuleArtifactInvocation,
    wasm: &[u8],
) -> Result<String, String> {
    if artifact.interface_version.as_deref() != Some(SINOMO_MODULE_V1_INTERFACE_VERSION) {
        return Err(format!(
            "sandbox WASM artifact {} requested component-model invocation without declaring {SINOMO_MODULE_V1_INTERFACE_VERSION}",
            artifact.artifact_id
        ));
    }
    validate_sinomo_module_v1_component_contract(wasm)
        .map_err(|source| format!("sandbox WASM artifact {}: {source}", artifact.artifact_id))?;

    let engine = sinomo_module_component_engine().map_err(|source| {
        format!("failed to create sinomo-module-v1 component engine: {source}")
    })?;
    let component = Component::new(&engine, wasm).map_err(|source| {
        format!(
            "failed to compile sinomo-module-v1 component artifact {}: {source:?}",
            artifact.artifact_id
        )
    })?;
    let budget_ms = artifact.timeout_ms.unwrap_or(artifact.limits.wall_time_ms);
    let mut store = wasmtime::Store::new(&engine, ());
    store
        .set_fuel(component_fuel_for_budget_ms(budget_ms))
        .map_err(|source| {
            format!(
                "failed to configure fuel for sinomo-module-v1 component artifact {}: {source:?}",
                artifact.artifact_id
            )
        })?;
    let linker = Linker::new(&engine);
    let instance = linker
        .instantiate(&mut store, &component)
        .map_err(|source| {
            format!(
                "failed to instantiate sinomo-module-v1 component artifact {}: {source:?}",
                artifact.artifact_id
            )
        })?;
    let module_export = instance
        .get_export_index(&mut store, None, "sinomo:module-v1/module")
        .ok_or_else(|| {
            format!(
                "sinomo-module-v1 component artifact {} is missing export `sinomo:module-v1/module`",
                artifact.artifact_id
            )
        })?;
    let run_export = instance
        .get_export_index(&mut store, Some(&module_export), "run")
        .ok_or_else(|| {
            format!(
                "sinomo-module-v1 component artifact {} is missing export `sinomo:module-v1/module.run`",
                artifact.artifact_id
            )
        })?;
    let run = instance.get_func(&mut store, &run_export).ok_or_else(|| {
        format!(
            "sinomo-module-v1 component artifact {} export `run` is not a function",
            artifact.artifact_id
        )
    })?;
    let params = [
        Val::Record(vec![
            (
                "node-id".to_string(),
                Val::String(invocation.node_id.as_str().to_string()),
            ),
            (
                "module-id".to_string(),
                Val::String(invocation.module_id.as_str().to_string()),
            ),
            (
                "module-version".to_string(),
                Val::String(invocation.artifact_id.as_str().to_string()),
            ),
            ("budget-ms".to_string(), Val::U64(budget_ms)),
            (
                "capabilities-json".to_string(),
                Val::String("{}".to_string()),
            ),
        ]),
        Val::String(invocation.input.clone()),
    ];
    let mut results = [Val::Record(vec![
        ("status".to_string(), Val::String(String::new())),
        ("events-json".to_string(), Val::List(Vec::new())),
        ("metrics-json".to_string(), Val::String("{}".to_string())),
    ])];
    run.call(&mut store, &params, &mut results)
        .map_err(|source| {
            format!(
                "sinomo-module-v1 component artifact {} failed during `run`: {source:?}",
                artifact.artifact_id
            )
        })?;
    run.post_return(&mut store).map_err(|source| {
        format!(
            "sinomo-module-v1 component artifact {} failed during post-return cleanup: {source:?}",
            artifact.artifact_id
        )
    })?;
    let result = sinomo_module_v1_result_from_component_val(&results[0]).map_err(|detail| {
        format!(
            "sinomo-module-v1 component artifact {} returned invalid module result: {detail}",
            artifact.artifact_id
        )
    })?;
    result.to_json_output().map_err(|source| {
        format!(
            "sinomo-module-v1 component artifact {} returned invalid module result: {source}",
            artifact.artifact_id
        )
    })
}

fn component_fuel_for_budget_ms(budget_ms: u64) -> u64 {
    budget_ms.saturating_mul(100_000).max(1_000_000)
}

fn sinomo_module_v1_result_from_component_val(value: &Val) -> Result<SinomoModuleV1Result, String> {
    let Val::Record(fields) = value else {
        return Err(format!("expected record result, found {value:?}"));
    };
    let status = component_record_string_field(fields, "status")?;
    let events_json = component_record_list_string_field(fields, "events-json")?;
    let metrics_json = component_record_string_field(fields, "metrics-json")?;
    Ok(SinomoModuleV1Result {
        status,
        events_json,
        metrics_json,
    })
}

fn component_record_string_field(fields: &[(String, Val)], name: &str) -> Result<String, String> {
    match fields.iter().find(|(field_name, _)| field_name == name) {
        Some((_, Val::String(value))) => Ok(value.clone()),
        Some((_, value)) => Err(format!("field `{name}` must be string, found {value:?}")),
        None => Err(format!("missing field `{name}`")),
    }
}

fn component_record_list_string_field(
    fields: &[(String, Val)],
    name: &str,
) -> Result<Vec<String>, String> {
    match fields.iter().find(|(field_name, _)| field_name == name) {
        Some((_, Val::List(values))) => values
            .iter()
            .enumerate()
            .map(|(index, value)| match value {
                Val::String(value) => Ok(value.clone()),
                other => Err(format!(
                    "field `{name}` item {index} must be string, found {other:?}"
                )),
            })
            .collect(),
        Some((_, value)) => Err(format!(
            "field `{name}` must be list<string>, found {value:?}"
        )),
        None => Err(format!("missing field `{name}`")),
    }
}

fn validate_module_artifact_output_contract(
    artifact: &SandboxWasmModuleArtifact,
    output: &str,
) -> Result<(), String> {
    if artifact.interface_version.as_deref() != Some(SINOMO_MODULE_V1_INTERFACE_VERSION) {
        return Ok(());
    }
    SinomoModuleV1Result::from_json_output(output)
        .map(|_| ())
        .map_err(|source| {
            format!(
                "sandbox WASM artifact {} declared {SINOMO_MODULE_V1_INTERFACE_VERSION} but returned invalid module result: {source}",
                artifact.artifact_id
            )
        })
}

fn validate_sinomo_module_v1_run_signature(
    export: &str,
    run_func: &wasmtime::component::types::ComponentFunc,
) -> Result<(), SandboxModuleComponentContractError> {
    if run_func.async_() {
        return Err(invalid_component_export(
            export,
            "run must be a synchronous function",
        ));
    }
    let params = run_func
        .params()
        .map(|(name, ty)| (name.to_string(), ty))
        .collect::<Vec<_>>();
    if params.len() != 2 {
        return Err(invalid_component_export(
            export,
            format!("expected 2 parameters, found {}", params.len()),
        ));
    }
    if params[0].0 != "context" {
        return Err(invalid_component_export(
            export,
            format!("first parameter must be `context`, found `{}`", params[0].0),
        ));
    }
    validate_record_type(
        export,
        "context",
        &params[0].1,
        &[
            ("node-id", ExpectedComponentType::Exact(Type::String)),
            ("module-id", ExpectedComponentType::Exact(Type::String)),
            ("module-version", ExpectedComponentType::Exact(Type::String)),
            ("budget-ms", ExpectedComponentType::Exact(Type::U64)),
            (
                "capabilities-json",
                ExpectedComponentType::Exact(Type::String),
            ),
        ],
    )?;
    if params[1].0 != "input-json" {
        return Err(invalid_component_export(
            export,
            format!(
                "second parameter must be `input-json`, found `{}`",
                params[1].0
            ),
        ));
    }
    if params[1].1 != Type::String {
        return Err(invalid_component_export(
            export,
            format!("`input-json` must be string, found {:?}", params[1].1),
        ));
    }

    let results = run_func.results().collect::<Vec<_>>();
    if results.len() != 1 {
        return Err(invalid_component_export(
            export,
            format!("expected 1 result, found {}", results.len()),
        ));
    }
    validate_record_type(
        export,
        "module-result",
        &results[0],
        &[
            ("status", ExpectedComponentType::Exact(Type::String)),
            ("events-json", ExpectedComponentType::ListString),
            ("metrics-json", ExpectedComponentType::Exact(Type::String)),
        ],
    )
}

#[derive(Clone, Debug)]
enum ExpectedComponentType {
    Exact(Type),
    ListString,
}

fn validate_record_type(
    export: &str,
    label: &str,
    ty: &Type,
    expected_fields: &[(&str, ExpectedComponentType)],
) -> Result<(), SandboxModuleComponentContractError> {
    let Type::Record(record) = ty else {
        return Err(invalid_component_export(
            export,
            format!("`{label}` must be record, found {ty:?}"),
        ));
    };
    let fields = record.fields().collect::<Vec<_>>();
    if fields.len() != expected_fields.len() {
        return Err(invalid_component_export(
            export,
            format!(
                "`{label}` must have {} fields, found {}",
                expected_fields.len(),
                fields.len()
            ),
        ));
    }
    for (index, (expected_name, expected_ty)) in expected_fields.iter().enumerate() {
        let field = &fields[index];
        if field.name != *expected_name {
            return Err(invalid_component_export(
                export,
                format!(
                    "`{label}` field {index} must be `{expected_name}`, found `{}`",
                    field.name
                ),
            ));
        }
        match expected_ty {
            ExpectedComponentType::Exact(expected_ty) => {
                if field.ty != *expected_ty {
                    return Err(invalid_component_export(
                        export,
                        format!(
                            "`{label}.{expected_name}` must be {expected_ty:?}, found {:?}",
                            field.ty
                        ),
                    ));
                }
            }
            ExpectedComponentType::ListString => {
                let Type::List(list) = &field.ty else {
                    return Err(invalid_component_export(
                        export,
                        format!(
                            "`{label}.{expected_name}` must be list<string>, found {:?}",
                            field.ty
                        ),
                    ));
                };
                if list.ty() != Type::String {
                    return Err(invalid_component_export(
                        export,
                        format!(
                            "`{label}.{expected_name}` must be list<string>, found list<{:?}>",
                            list.ty()
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn invalid_component_export(
    export: &str,
    detail: impl Into<String>,
) -> SandboxModuleComponentContractError {
    SandboxModuleComponentContractError::InvalidExportShape {
        export: export.to_string(),
        detail: detail.into(),
    }
}

fn ensure_sandbox_instance(
    store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
    toolchain_id: &SandboxToolchainId,
) -> Result<(), SandboxStoreError> {
    match store.load(sandbox_id) {
        Ok(_) => Ok(()),
        Err(SandboxStoreError::NotFound { .. }) => store
            .create(
                sandbox_id.clone(),
                toolchain_id.clone(),
                OffsetDateTime::now_utc(),
            )
            .map(|_| ()),
        Err(error) => Err(error),
    }
}

fn invocation_lane_label(lane: &ModuleInvocationLane) -> &'static str {
    match lane {
        ModuleInvocationLane::Active => "active",
        ModuleInvocationLane::Candidate => "candidate",
        ModuleInvocationLane::Shadow => "shadow",
    }
}
