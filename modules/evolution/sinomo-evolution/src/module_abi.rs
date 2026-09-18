use sinomo_module_api::cli as cli_contract;
use sinomo_module_api::{
    ComponentDescriptor, ModuleSettingsExport, NativeModuleBackend,
    NativeModuleCapabilitiesResponse, NativeModuleDescriptor, NativeModuleErrorResponse,
    NativeModuleInvocationRequest, NativeModuleInvocationResponse, NativeModuleLifecycle,
    NativeModuleObjectRpcRoute, NativeModuleRequest, NativeModuleResponse, NativeModuleSurfaces,
    SINOMO_MODULE_STATUS_INVALID_ARGUMENT, SINOMO_MODULE_STATUS_OK,
    SINOMO_MODULE_STATUS_RUNTIME_ERROR, SINOMO_MODULE_STATUS_UNSUPPORTED_ABI,
    SINOMO_NATIVE_MODULE_ABI_VERSION, ServiceOffer, ServiceOperationOffer, SinomoModuleByteBuffer,
    SinomoModuleByteSlice, SinomoModuleHostApiV2, SinomoModuleStatusCode, decode_sobj, encode_sobj,
};
use sinomo_runtime::api::app::evolution as contract;
use std::path::Path;
use std::sync::Mutex;

use crate::agent_package::{evolution_agent_exports, evolution_agent_resources};
use crate::{
    app_evolution_dashboard, evolution_config_definition, evolution_data_dir, run_app_control_goal,
    run_app_create_goal, run_app_evolution_decision, run_app_evolution_job,
    run_app_record_revision, run_app_update_workflow, run_evolve_command_captured,
    with_evolution_data_dir,
};

const CAPABILITY_IMPORTS: &[&str] = &[
    "storage.local.read",
    "storage.local.write",
    "cluster.kv.read",
    "cluster.kv.write",
    "lm.invoke",
    "agent.coding.run",
    "sandbox.command.run",
    "rollout.status.read",
];

static MODULE_LIFECYCLE: NativeModuleLifecycle = NativeModuleLifecycle::new();
static EVOLUTION_MUTATION_LOCK: Mutex<()> = Mutex::new(());

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_api_version() -> u32 {
    SINOMO_NATIVE_MODULE_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_descriptor(
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    write_buffer(out, encode_sobj(&evolution_native_module_descriptor()))
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV2) -> SinomoModuleStatusCode {
    if host.abi_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
        return SINOMO_MODULE_STATUS_UNSUPPORTED_ABI;
    }
    if MODULE_LIFECYCLE.initialize() {
        SINOMO_MODULE_STATUS_OK
    } else {
        SINOMO_MODULE_STATUS_RUNTIME_ERROR
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let Some(_invocation) = MODULE_LIFECYCLE.begin_invocation() else {
        return write_native_response(
            response_sobj,
            NativeModuleResponse::Error(native_error(
                "module-unavailable",
                "evolution module is not initialized or is shutting down",
            )),
        );
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        invoke_unchecked(request_sobj, response_sobj)
    }));
    match result {
        Ok(status) => status,
        Err(_) => write_native_response(
            response_sobj,
            NativeModuleResponse::Error(native_error(
                "panic",
                "evolution module panicked while handling an invocation",
            )),
        ),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
    if MODULE_LIFECYCLE.begin_shutdown() {
        SINOMO_MODULE_STATUS_OK
    } else {
        SINOMO_MODULE_STATUS_RUNTIME_ERROR
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_free_buffer(buffer: SinomoModuleByteBuffer) {
    unsafe {
        drop(SinomoModuleByteBuffer::into_vec(buffer));
    }
}

pub fn evolution_native_module_descriptor() -> NativeModuleDescriptor {
    let settings = evolution_config_definition().expect("evolution config definition");
    NativeModuleDescriptor {
        module_id: contract::EVOLUTION_MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        owner: contract::EVOLUTION_MODULE_OWNER.to_string(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: contract::EVOLUTION_MODULE_INTERFACE_VERSION.to_string(),
        platform_triple: platform_triple(),
        supported_operations: contract::EVOLUTION_OPERATIONS
            .iter()
            .chain(std::iter::once(&cli_contract::CLI_INVOKE_OPERATION))
            .map(|operation| (*operation).to_string())
            .collect(),
        capabilities: contract::EVOLUTION_OPERATIONS
            .iter()
            .chain(std::iter::once(&cli_contract::CLI_INVOKE_OPERATION))
            .map(|operation| (*operation).to_string())
            .collect(),
        surfaces: NativeModuleSurfaces {
            object_rpc_routes: vec![
                NativeModuleObjectRpcRoute::new(
                    "/evolution/cli/command-v1",
                    "module.cli",
                    cli_contract::CLI_INVOKE_OPERATION,
                    "component",
                )
                .with_timeout_ms(cli_contract::CLI_MAX_DEADLINE_MS),
                route(
                    contract::EVOLUTION_DASHBOARD_ROUTE,
                    contract::EVOLUTION_DASHBOARD_OPERATION,
                ),
                long_running_route(
                    contract::EVOLUTION_START_ROUTE,
                    contract::EVOLUTION_START_OPERATION,
                ),
                long_running_route(
                    contract::EVOLUTION_DECISION_ROUTE,
                    contract::EVOLUTION_DECISION_OPERATION,
                ),
                long_running_route(
                    contract::EVOLUTION_GOAL_CREATE_ROUTE,
                    contract::EVOLUTION_GOAL_CREATE_OPERATION,
                ),
                long_running_route(
                    contract::EVOLUTION_REVISION_RECORD_ROUTE,
                    contract::EVOLUTION_REVISION_RECORD_OPERATION,
                ),
                long_running_route(
                    contract::EVOLUTION_GOAL_CONTROL_ROUTE,
                    contract::EVOLUTION_GOAL_CONTROL_OPERATION,
                ),
                long_running_route(
                    contract::EVOLUTION_WORKFLOW_UPDATE_ROUTE,
                    contract::EVOLUTION_WORKFLOW_UPDATE_OPERATION,
                ),
                long_running_route(
                    contract::EVOLUTION_CLI_ROUTE,
                    contract::EVOLUTION_CLI_OPERATION,
                ),
            ],
            extension_contributions: vec![
                crate::cli_runtime::evolution_cli_descriptor()
                    .contribution()
                    .expect("CLI contribution"),
            ],
            agent_definitions: evolution_agent_exports(),
            declarative_resources: evolution_agent_resources(),
            storage_owners: vec![contract::EVOLUTION_MODULE_OWNER.to_string()],
            capability_imports: CAPABILITY_IMPORTS
                .iter()
                .map(|capability| (*capability).to_string())
                .collect(),
            capability_exports: contract::EVOLUTION_OPERATIONS
                .iter()
                .chain(std::iter::once(&cli_contract::CLI_INVOKE_OPERATION))
                .map(|operation| (*operation).to_string())
                .collect(),
            settings: vec![ModuleSettingsExport::new(
                settings.key.as_str(),
                encode_sobj(&settings).expect("encode evolution config definition"),
            )],
            ..NativeModuleSurfaces::default()
        },
        component: ComponentDescriptor::new(
            contract::EVOLUTION_MODULE_ID,
            env!("CARGO_PKG_VERSION"),
        )
        .with_offer(
            ServiceOffer::new(
                contract::EVOLUTION_SERVICE_ID,
                contract::EVOLUTION_INTERFACE_ID,
                contract::EVOLUTION_INTERFACE_VERSION,
            )
            .with_operation(operation_offer(
                contract::EVOLUTION_DASHBOARD_OPERATION,
                contract::EVOLUTION_DASHBOARD_ROUTE,
            ))
            .with_operation(operation_offer(
                contract::EVOLUTION_START_OPERATION,
                contract::EVOLUTION_START_ROUTE,
            ))
            .with_operation(operation_offer(
                contract::EVOLUTION_DECISION_OPERATION,
                contract::EVOLUTION_DECISION_ROUTE,
            ))
            .with_operation(operation_offer(
                contract::EVOLUTION_GOAL_CREATE_OPERATION,
                contract::EVOLUTION_GOAL_CREATE_ROUTE,
            ))
            .with_operation(operation_offer(
                contract::EVOLUTION_REVISION_RECORD_OPERATION,
                contract::EVOLUTION_REVISION_RECORD_ROUTE,
            ))
            .with_operation(operation_offer(
                contract::EVOLUTION_GOAL_CONTROL_OPERATION,
                contract::EVOLUTION_GOAL_CONTROL_ROUTE,
            ))
            .with_operation(operation_offer(
                contract::EVOLUTION_WORKFLOW_UPDATE_OPERATION,
                contract::EVOLUTION_WORKFLOW_UPDATE_ROUTE,
            ))
            .with_operation(operation_offer(
                contract::EVOLUTION_CLI_OPERATION,
                contract::EVOLUTION_CLI_ROUTE,
            )),
        )
        .with_offer(
            ServiceOffer::new(
                "evolution-cli",
                cli_contract::CLI_INTERFACE_ID,
                cli_contract::CLI_INTERFACE_VERSION,
            )
            .with_operation(
                ServiceOperationOffer::new(cli_contract::CLI_INVOKE_OPERATION)
                    .at_route("/evolution/cli/command-v1")
                    .require_caller_capability(cli_contract::CLI_INVOKE_OPERATION),
            ),
        ),
    }
}

fn route(route_path: &str, operation: &str) -> NativeModuleObjectRpcRoute {
    NativeModuleObjectRpcRoute::new(
        route_path,
        contract::EVOLUTION_RPC_CATEGORY,
        operation,
        contract::EVOLUTION_ROUTE_AUTH_SCOPE,
    )
}

fn long_running_route(route_path: &str, operation: &str) -> NativeModuleObjectRpcRoute {
    route(route_path, operation)
        .with_concurrency_limit(1)
        .with_timeout_ms(60 * 60 * 1_000)
}

fn operation_offer(operation: &str, route: &str) -> ServiceOperationOffer {
    ServiceOperationOffer::new(operation)
        .at_route(route)
        .require_caller_capability(operation)
}

fn invoke_unchecked(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let request = unsafe { request_sobj.as_slice() };
    let response = match decode_sobj::<NativeModuleRequest>(request) {
        Ok(NativeModuleRequest::ProbeCapabilities(_)) => {
            NativeModuleResponse::Capabilities(NativeModuleCapabilitiesResponse {
                descriptor: evolution_native_module_descriptor(),
            })
        }
        Ok(NativeModuleRequest::Invoke(request)) => invoke_operation(request),
        Err(error) => NativeModuleResponse::Error(native_error(
            "invalid-request",
            &format!("decode evolution module request: {error}"),
        )),
    };
    write_native_response(response_sobj, response)
}

fn invoke_operation(mut request: NativeModuleInvocationRequest) -> NativeModuleResponse {
    let host_data_dir_root = request.host_data_dir_root.take();
    match host_data_dir_root {
        Some(root) => with_evolution_data_dir(Path::new(&root), || invoke_operation_inner(request)),
        None => invoke_operation_inner(request),
    }
}

fn invoke_operation_inner(request: NativeModuleInvocationRequest) -> NativeModuleResponse {
    match request.operation.as_str() {
        contract::EVOLUTION_DASHBOARD_OPERATION => {
            invoke_dashboard(&request.operation, &request.payload_sobj)
        }
        contract::EVOLUTION_START_OPERATION => {
            invoke_serialized(|| invoke_start(&request.operation, &request.payload_sobj))
        }
        contract::EVOLUTION_DECISION_OPERATION => {
            invoke_serialized(|| invoke_decision(&request.operation, &request.payload_sobj))
        }
        contract::EVOLUTION_GOAL_CREATE_OPERATION => {
            invoke_serialized(|| invoke_goal_create(&request.operation, &request.payload_sobj))
        }
        contract::EVOLUTION_REVISION_RECORD_OPERATION => {
            invoke_serialized(|| invoke_revision_record(&request.operation, &request.payload_sobj))
        }
        contract::EVOLUTION_GOAL_CONTROL_OPERATION => {
            invoke_serialized(|| invoke_goal_control(&request.operation, &request.payload_sobj))
        }
        contract::EVOLUTION_WORKFLOW_UPDATE_OPERATION => {
            invoke_serialized(|| invoke_workflow_update(&request.operation, &request.payload_sobj))
        }
        cli_contract::CLI_INVOKE_OPERATION => {
            match decode_sobj::<cli_contract::CliModuleRequest>(&request.payload_sobj) {
                Ok(payload) => {
                    let run = || match encode_sobj(
                        &crate::cli_runtime::invoke_evolution_module_cli(&payload),
                    ) {
                        Ok(body) => invocation_response(cli_contract::CLI_INVOKE_OPERATION, body),
                        Err(error) => NativeModuleResponse::Error(native_error(
                            "cli-response-encoding",
                            &error.to_string(),
                        )),
                    };
                    if payload.action == cli_contract::CliAction::Invoke {
                        invoke_serialized(run)
                    } else {
                        run()
                    }
                }
                Err(error) => NativeModuleResponse::Error(native_error(
                    "invalid-cli-request",
                    &error.to_string(),
                )),
            }
        }
        contract::EVOLUTION_CLI_OPERATION => {
            invoke_serialized(|| invoke_cli(&request.operation, &request.payload_sobj))
        }
        other => NativeModuleResponse::Error(native_error(
            "unsupported-operation",
            &format!("unsupported evolution operation `{other}`"),
        )),
    }
}

fn invoke_serialized(run: impl FnOnce() -> NativeModuleResponse) -> NativeModuleResponse {
    let Ok(_guard) = EVOLUTION_MUTATION_LOCK.lock() else {
        return NativeModuleResponse::Error(native_error(
            "evolution-state-lock-poisoned",
            "evolution module state mutation lock is unavailable",
        ));
    };
    run()
}

fn invoke_dashboard(operation: &str, payload: &[u8]) -> NativeModuleResponse {
    let request = decode_sobj::<contract::EvolutionDashboardRequest>(payload);
    match request.and_then(|_| {
        let data_dir = evolution_data_dir()
            .map_err(|error| sinomo_module_api::NativeModuleApiError::Runtime(error.to_string()))?;
        encode_sobj(&contract::EvolutionDashboardResponse {
            dashboard: app_evolution_dashboard(&data_dir),
        })
    }) {
        Ok(payload_sobj) => invocation_response(operation, payload_sobj),
        Err(error) => NativeModuleResponse::Error(native_error(
            "dashboard-failed",
            &format!("read evolution dashboard: {error}"),
        )),
    }
}

fn invoke_start(operation: &str, payload: &[u8]) -> NativeModuleResponse {
    invoke_mutation::<contract::EvolutionStartRequest, _>(operation, payload, |request| {
        run_app_evolution_job(request)
            .map(|()| contract::EvolutionMutationResponse { accepted: true })
    })
}

fn invoke_decision(operation: &str, payload: &[u8]) -> NativeModuleResponse {
    invoke_mutation::<contract::EvolutionDecisionRequest, _>(operation, payload, |request| {
        run_app_evolution_decision(request)
            .map(|()| contract::EvolutionMutationResponse { accepted: true })
    })
}

fn invoke_goal_create(operation: &str, payload: &[u8]) -> NativeModuleResponse {
    invoke_mutation::<contract::EvolutionGoalCreateRequest, _>(operation, payload, |request| {
        let data_dir = evolution_data_dir()?;
        run_app_create_goal(&data_dir, request)
    })
}

fn invoke_revision_record(operation: &str, payload: &[u8]) -> NativeModuleResponse {
    invoke_mutation::<contract::EvolutionRevisionRecordRequest, _>(operation, payload, |request| {
        let data_dir = evolution_data_dir()?;
        run_app_record_revision(&data_dir, request)
    })
}

fn invoke_goal_control(operation: &str, payload: &[u8]) -> NativeModuleResponse {
    invoke_mutation::<contract::EvolutionGoalControlRequest, _>(operation, payload, |request| {
        let data_dir = evolution_data_dir()?;
        run_app_control_goal(&data_dir, request)
    })
}

fn invoke_workflow_update(operation: &str, payload: &[u8]) -> NativeModuleResponse {
    invoke_mutation::<contract::EvolutionWorkflowUpdateRequest, _>(operation, payload, |request| {
        let data_dir = evolution_data_dir()?;
        run_app_update_workflow(&data_dir, request)
    })
}

fn invoke_mutation<Request, Response>(
    operation: &str,
    payload: &[u8],
    run: impl FnOnce(Request) -> Result<Response, crate::CliError>,
) -> NativeModuleResponse
where
    Request: serde::de::DeserializeOwned,
    Response: serde::Serialize,
{
    match decode_sobj::<Request>(payload) {
        Ok(request) => match run(request) {
            Ok(response) => match encode_sobj(&response) {
                Ok(payload_sobj) => invocation_response(operation, payload_sobj),
                Err(error) => NativeModuleResponse::Error(native_error(
                    "response-encoding-failed",
                    &error.to_string(),
                )),
            },
            Err(error) => NativeModuleResponse::Error(native_error(
                "evolution-mutation-failed",
                &error.to_string(),
            )),
        },
        Err(error) => NativeModuleResponse::Error(native_error(
            "invalid-request",
            &format!("decode evolution mutation request: {error}"),
        )),
    }
}

fn invoke_cli(operation: &str, payload: &[u8]) -> NativeModuleResponse {
    match decode_sobj::<contract::EvolutionCliRequest>(payload) {
        Ok(request) => match run_evolve_command_captured(&request.args) {
            Ok(stdout) => match encode_sobj(&contract::EvolutionCliResponse { stdout }) {
                Ok(payload_sobj) => invocation_response(operation, payload_sobj),
                Err(error) => NativeModuleResponse::Error(native_error(
                    "response-encoding-failed",
                    &error.to_string(),
                )),
            },
            Err(error) => NativeModuleResponse::Error(native_error(
                "evolution-cli-failed",
                &error.to_string(),
            )),
        },
        Err(error) => NativeModuleResponse::Error(native_error(
            "invalid-request",
            &format!("decode evolution CLI request: {error}"),
        )),
    }
}

fn invocation_response(operation: &str, payload_sobj: Vec<u8>) -> NativeModuleResponse {
    NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
        operation: operation.to_string(),
        payload_sobj,
        response_body: None,
    })
}

fn native_error(code: &str, detail: &str) -> NativeModuleErrorResponse {
    NativeModuleErrorResponse {
        code: code.to_string(),
        detail: detail.to_string(),
    }
}

fn write_native_response(
    out: *mut SinomoModuleByteBuffer,
    response: NativeModuleResponse,
) -> SinomoModuleStatusCode {
    write_buffer(out, encode_sobj(&response))
}

fn write_buffer(
    out: *mut SinomoModuleByteBuffer,
    bytes: Result<Vec<u8>, sinomo_module_api::NativeModuleApiError>,
) -> SinomoModuleStatusCode {
    let Ok(bytes) = bytes else {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    };
    if out.is_null() {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    }
    unsafe {
        *out = SinomoModuleByteBuffer::from_vec(bytes);
    }
    SINOMO_MODULE_STATUS_OK
}

fn platform_triple() -> String {
    format!(
        "{}-{}-{}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::env::consts::FAMILY
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_owns_evolution_routes_settings_and_lifecycle() {
        let descriptor = evolution_native_module_descriptor();
        assert_eq!(descriptor.module_id, contract::EVOLUTION_MODULE_ID);
        assert_eq!(descriptor.backend, NativeModuleBackend::NativeDylib);
        assert_eq!(
            descriptor.supported_operations,
            contract::EVOLUTION_OPERATIONS
                .iter()
                .copied()
                .chain([cli_contract::CLI_INVOKE_OPERATION])
                .collect::<Vec<_>>()
        );
        assert_eq!(
            descriptor.surfaces.object_rpc_routes.len(),
            contract::EVOLUTION_RPC_ROUTES.len() + 1
        );
        assert_eq!(descriptor.surfaces.settings.len(), 1);
        assert_eq!(descriptor.surfaces.agent_definitions.len(), 4);
        assert_eq!(
            descriptor.surfaces.agent_definitions[0].id,
            crate::EVOLUTION_AGENT_ID
        );
        assert_eq!(descriptor.surfaces.declarative_resources.len(), 3);
        assert!(
            descriptor
                .surfaces
                .declarative_resources
                .iter()
                .all(|resource| {
                    resource.kind == "skill"
                        && resource.target_agents.is_empty()
                        && resource.embedded_text.is_some()
                })
        );
        assert_eq!(
            descriptor
                .surfaces
                .object_rpc_routes
                .iter()
                .find(|route| route.route == contract::EVOLUTION_START_ROUTE)
                .unwrap()
                .timeout_ms,
            Some(60 * 60 * 1_000)
        );
        assert_eq!(
            descriptor.component.component_id,
            contract::EVOLUTION_MODULE_ID
        );
        descriptor
            .component
            .validate()
            .expect("component descriptor");
    }

    #[test]
    fn module_owned_cli_exports_help_completion_and_parse_errors_without_effects() {
        let root = sinomo_testdir::TestDir::new("evolution-cli-contract");
        let descriptor = evolution_native_module_descriptor();
        let command: cli_contract::CliCommandDescriptor =
            decode_sobj(&descriptor.surfaces.extension_contributions[0].payload_sobj).unwrap();
        command.validate().unwrap();
        assert_eq!(command.name, "evolve");
        assert!(command.help_on_empty);
        assert!(
            descriptor
                .surfaces
                .capability_exports
                .iter()
                .any(|operation| operation == cli_contract::CLI_INVOKE_OPERATION)
        );
        assert!(
            descriptor
                .component
                .offers
                .iter()
                .any(|offer| offer.service_id == command.service_id)
        );
        for (action, args, code) in [
            (cli_contract::CliAction::Help, vec![], 0),
            (cli_contract::CliAction::Complete, vec!["prop".into()], 0),
            (
                cli_contract::CliAction::Invoke,
                vec!["unknown-command".into()],
                2,
            ),
        ] {
            let response = invoke_operation(NativeModuleInvocationRequest {
                operation: cli_contract::CLI_INVOKE_OPERATION.into(),
                payload_sobj: encode_sobj(&cli_contract::CliModuleRequest {
                    command_id: command.command_id.clone(),
                    invocation_id: "test-cli".into(),
                    action,
                    args,
                    stdin: Vec::new(),
                    current_dir: None,
                })
                .unwrap(),
                host_data_dir_root: Some(root.path().display().to_string()),
                cancellation_wait_handle: None,
                request_body: None,
                response_body_path: None,
            });
            let NativeModuleResponse::Invocation(response) = response else {
                panic!("{response:?}");
            };
            let response: cli_contract::CliModuleResponse =
                decode_sobj(&response.payload_sobj).unwrap();
            assert_eq!(response.exit_code, code);
            if action == cli_contract::CliAction::Help {
                assert!(
                    String::from_utf8(response.stdout)
                        .unwrap()
                        .contains("sinomo evolve propose")
                );
            }
            if action == cli_contract::CliAction::Complete {
                assert!(response.completions.contains(&"propose".into()));
            }
        }
        assert!(
            !root.path().join("evolution").exists(),
            "help, completion, and invalid syntax must not create authority state"
        );
    }

    #[test]
    fn invocation_uses_the_host_data_directory_and_captures_cli_output() {
        let root = sinomo_testdir::TestDir::new("evolution-native-host-context");
        let dashboard = invoke_operation(NativeModuleInvocationRequest {
            operation: contract::EVOLUTION_DASHBOARD_OPERATION.to_string(),
            payload_sobj: encode_sobj(&contract::EvolutionDashboardRequest)
                .expect("dashboard request"),
            host_data_dir_root: Some(root.path().display().to_string()),
            cancellation_wait_handle: None,
            request_body: None,
            response_body_path: None,
        });
        let NativeModuleResponse::Invocation(dashboard) = dashboard else {
            panic!("dashboard invocation failed: {dashboard:?}");
        };
        let dashboard: contract::EvolutionDashboardResponse =
            decode_sobj(&dashboard.payload_sobj).expect("dashboard response");
        assert_eq!(
            dashboard.dashboard.source,
            root.path().join("evolution").display().to_string()
        );

        let create = invoke_operation(NativeModuleInvocationRequest {
            operation: contract::EVOLUTION_GOAL_CREATE_OPERATION.to_string(),
            payload_sobj: encode_sobj(&contract::EvolutionGoalCreateRequest {
                objective: "improve a generic artifact".to_string(),
                evolution_agent_id: None,
                metrics: vec![contract::EvolutionMetricDefinitionInput {
                    id: "quality".to_string(),
                    label: "Quality".to_string(),
                    direction: "maximize".to_string(),
                    unit: "points".to_string(),
                    target: None,
                    required: true,
                }],
            })
            .expect("goal create request"),
            host_data_dir_root: Some(root.path().display().to_string()),
            cancellation_wait_handle: None,
            request_body: None,
            response_body_path: None,
        });
        let NativeModuleResponse::Invocation(create) = create else {
            panic!("goal creation failed: {create:?}");
        };
        let create: contract::EvolutionGoalCreateResponse =
            decode_sobj(&create.payload_sobj).expect("goal create response");
        assert!(create.accepted);

        let cli = invoke_operation(NativeModuleInvocationRequest {
            operation: contract::EVOLUTION_CLI_OPERATION.to_string(),
            payload_sobj: encode_sobj(&contract::EvolutionCliRequest {
                args: vec!["help".to_string()],
            })
            .expect("CLI request"),
            host_data_dir_root: Some(root.path().display().to_string()),
            cancellation_wait_handle: None,
            request_body: None,
            response_body_path: None,
        });
        let NativeModuleResponse::Invocation(cli) = cli else {
            panic!("CLI invocation failed: {cli:?}");
        };
        let cli: contract::EvolutionCliResponse =
            decode_sobj(&cli.payload_sobj).expect("CLI response");
        assert!(cli.stdout.contains("sinomo evolve propose"));
    }
}
