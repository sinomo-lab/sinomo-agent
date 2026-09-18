use sinomo_cli_registry::{
    CliCommandParseOutcome, CliCommandSpec, CliPositionalSpec, CliValueSpec,
};
use sinomo_module_api::cli::*;
use std::collections::BTreeMap;

use sinomo_module_api::observe::{
    OBSERVE_METRIC_EXPORTER_CATEGORY, OBSERVE_METRIC_EXPORTER_INTERFACE_ID,
    OBSERVE_METRIC_EXPORTER_INTERFACE_VERSION, OBSERVE_METRIC_EXPORTER_OPERATION,
    ObserveMetricDescriptor, ObserveMetricExportRequest, ObserveMetricExportResponse,
    ObserveMetricKind, ObserveMetricRecord, ObserveMetricUnit,
};
use sinomo_module_api::{
    ComponentDescriptor, NativeModuleBackend, NativeModuleCapabilitiesResponse,
    NativeModuleDescriptor, NativeModuleErrorResponse, NativeModuleInvocationResponse,
    NativeModuleObjectRpcRoute, NativeModuleRequest, NativeModuleResponse, NativeModuleSurfaces,
    SINOMO_MODULE_STATUS_INVALID_ARGUMENT, SINOMO_MODULE_STATUS_OK,
    SINOMO_MODULE_STATUS_RUNTIME_ERROR, SINOMO_MODULE_STATUS_UNSUPPORTED_ABI,
    SINOMO_NATIVE_MODULE_ABI_VERSION, ServiceOffer, ServiceOperationOffer, SinomoModuleByteBuffer,
    SinomoModuleByteSlice, SinomoModuleHostApiV2, SinomoModuleStatusCode, decode_sobj, encode_sobj,
};

const MODULE_ID: &str = "fixture.external.echo";
const INTERFACE_ID: &str = "sinomo.fixture.external-echo";
const INTERFACE_VERSION: &str = "1.0.0";
const OPERATION: &str = "fixture.external.echo";
const METRIC_EXPORT_ROUTE: &str = "/fixture/external/metrics";
thread_local! {
    static HOST: std::cell::Cell<SinomoModuleHostApiV2> = std::cell::Cell::new(SinomoModuleHostApiV2::empty());
}

fn descriptor() -> NativeModuleDescriptor {
    NativeModuleDescriptor {
        module_id: MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        owner: "external-fixture".to_string(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: INTERFACE_VERSION.to_string(),
        platform_triple: format!(
            "{}-{}-{}",
            std::env::consts::ARCH,
            std::env::consts::OS,
            std::env::consts::FAMILY
        ),
        supported_operations: vec![
            OPERATION.to_string(),
            CLI_INVOKE_OPERATION.to_string(),
            OBSERVE_METRIC_EXPORTER_OPERATION.to_string(),
        ],
        capabilities: Vec::new(),
        surfaces: NativeModuleSurfaces {
            capability_exports: vec![
                CLI_INVOKE_OPERATION.into(),
                OBSERVE_METRIC_EXPORTER_OPERATION.into(),
            ],
            extension_contributions: vec![
                cli_descriptor().contribution().expect("CLI contribution"),
            ],
            object_rpc_routes: vec![
                NativeModuleObjectRpcRoute::new(
                    "/fixture/cli",
                    "module.cli",
                    CLI_INVOKE_OPERATION,
                    "component",
                ),
                NativeModuleObjectRpcRoute::new(
                    METRIC_EXPORT_ROUTE,
                    OBSERVE_METRIC_EXPORTER_CATEGORY,
                    OBSERVE_METRIC_EXPORTER_OPERATION,
                    "component",
                ),
            ],
            ..NativeModuleSurfaces::default()
        },
        component: ComponentDescriptor::new(MODULE_ID, env!("CARGO_PKG_VERSION"))
            .with_offer(
                ServiceOffer::new(
                    "external-fixture-cli",
                    CLI_INTERFACE_ID,
                    CLI_INTERFACE_VERSION,
                )
                .with_operation(
                    ServiceOperationOffer::new(CLI_INVOKE_OPERATION)
                        .at_route("/fixture/cli")
                        .require_caller_capability(CLI_INVOKE_OPERATION),
                ),
            )
            .with_offer(
                ServiceOffer::new("external-echo", INTERFACE_ID, INTERFACE_VERSION)
                    .with_provider_capability("echo.bytes"),
            )
            .with_offer(
                ServiceOffer::new(
                    "external-fixture-metrics",
                    OBSERVE_METRIC_EXPORTER_INTERFACE_ID,
                    OBSERVE_METRIC_EXPORTER_INTERFACE_VERSION,
                )
                .with_operation(
                    ServiceOperationOffer::new(OBSERVE_METRIC_EXPORTER_OPERATION)
                        .at_route(METRIC_EXPORT_ROUTE)
                        .require_caller_capability(OBSERVE_METRIC_EXPORTER_OPERATION),
                ),
            ),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_api_version() -> u32 {
    SINOMO_NATIVE_MODULE_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_descriptor(
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    write_buffer(out, encode_sobj(&descriptor()))
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV2) -> SinomoModuleStatusCode {
    if host.abi_version == SINOMO_NATIVE_MODULE_ABI_VERSION {
        HOST.set(host);
        SINOMO_MODULE_STATUS_OK
    } else {
        SINOMO_MODULE_STATUS_UNSUPPORTED_ABI
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let request = unsafe { request_sobj.as_slice() };
    let response = match decode_sobj::<NativeModuleRequest>(request) {
        Ok(NativeModuleRequest::ProbeCapabilities(_)) => {
            NativeModuleResponse::Capabilities(NativeModuleCapabilitiesResponse {
                descriptor: descriptor(),
            })
        }
        Ok(NativeModuleRequest::Invoke(request)) if request.operation == CLI_INVOKE_OPERATION => {
            match decode_sobj::<CliModuleRequest>(&request.payload_sobj)
                .map(|request| cli_invoke(request))
                .and_then(|response| encode_sobj(&response))
            {
                Ok(payload_sobj) => {
                    NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
                        operation: request.operation,
                        payload_sobj,
                        response_body: None,
                    })
                }
                Err(error) => NativeModuleResponse::Error(NativeModuleErrorResponse {
                    code: "invalid-cli-request".into(),
                    detail: error.to_string(),
                }),
            }
        }
        Ok(NativeModuleRequest::Invoke(request)) if request.operation == OPERATION => {
            NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
                operation: request.operation,
                payload_sobj: request.payload_sobj,
                response_body: None,
            })
        }
        Ok(NativeModuleRequest::Invoke(request))
            if request.operation == OBSERVE_METRIC_EXPORTER_OPERATION =>
        {
            let response = decode_sobj::<ObserveMetricExportRequest>(&request.payload_sobj)
                .map(|request| metric_export(request))
                .and_then(|response| encode_sobj(&response));
            match response {
                Ok(payload_sobj) => {
                    NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
                        operation: request.operation,
                        payload_sobj,
                        response_body: None,
                    })
                }
                Err(error) => NativeModuleResponse::Error(NativeModuleErrorResponse {
                    code: "invalid-metric-export".to_string(),
                    detail: error.to_string(),
                }),
            }
        }
        Ok(NativeModuleRequest::Invoke(request)) => {
            NativeModuleResponse::Error(NativeModuleErrorResponse {
                code: "unsupported-operation".to_string(),
                detail: format!("unsupported operation `{}`", request.operation),
            })
        }
        Err(error) => NativeModuleResponse::Error(NativeModuleErrorResponse {
            code: "invalid-request".to_string(),
            detail: error.to_string(),
        }),
    };
    match encode_sobj(&response) {
        Ok(bytes) => write_buffer(response_sobj, Ok(bytes)),
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

fn metric_export(request: ObserveMetricExportRequest) -> ObserveMetricExportResponse {
    let descriptor = ObserveMetricDescriptor::new(
        "fixture.external.echo.invocations",
        ObserveMetricKind::Counter,
        Some(ObserveMetricUnit::Count),
        "Synthetic counter proving independently built metric exports.",
    );
    let record = ObserveMetricRecord::counter(descriptor.name.clone(), BTreeMap::new(), 1);
    let matches = request
        .name_prefix
        .as_ref()
        .is_none_or(|prefix| record.name.starts_with(prefix));
    ObserveMetricExportResponse {
        descriptors: matches.then_some(descriptor).into_iter().collect(),
        metrics: matches.then_some(record).into_iter().collect(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
    SINOMO_MODULE_STATUS_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_free_buffer(buffer: SinomoModuleByteBuffer) {
    unsafe {
        drop(SinomoModuleByteBuffer::into_vec(buffer));
    }
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

fn cli_spec() -> CliCommandSpec {
    CliCommandSpec::new("fixture-cli", "An independently installed command")
        .with_aliases(["fx"])
        .with_children([
            CliCommandSpec::new("echo", "Echo argument tokens and stdin").with_positionals([
                CliPositionalSpec::optional(CliValueSpec::new("text", "literal tokens"))
                    .repeatable(),
            ]),
            CliCommandSpec::new("fail", "Return a module-defined exit status"),
            CliCommandSpec::new("wait", "Wait for cancellation and finish cleanup"),
        ])
}
fn cli_descriptor() -> CliCommandDescriptor {
    let spec = cli_spec();
    let help = spec.help();
    CliCommandDescriptor {
        command_id: "fixture.cli".into(),
        name: spec.name.clone(),
        aliases: spec.aliases.clone(),
        summary: spec.about.clone(),
        help: std::iter::once(help.title).chain(help.lines).collect(),
        autocomplete_sobj: encode_sobj(&spec.autocomplete()).expect("autocomplete"),
        service_id: "external-fixture-cli".into(),
        max_deadline_ms: 5000,
        accepts_stdin: true,
        help_on_empty: false,
        requires_cluster_state: false,
    }
}
fn cli_invoke(request: CliModuleRequest) -> CliModuleResponse {
    if let Err(error) = request.validate() {
        return CliModuleResponse::failure(2, error);
    }
    if request.command_id != "fixture.cli" {
        return CliModuleResponse::failure(2, "unknown command");
    }
    let spec = cli_spec();
    match request.action {
        CliAction::Help => CliModuleResponse {
            stdout: cli_descriptor().help.join("\n").into_bytes(),
            ..Default::default()
        },
        CliAction::Complete => CliModuleResponse {
            completions: sinomo_cli_registry::complete_module_words(
                &spec.autocomplete(),
                &request.args,
            ),
            ..Default::default()
        },
        CliAction::Invoke => match spec.parse(&request.args) {
            Ok(CliCommandParseOutcome::Help(help)) => CliModuleResponse {
                stdout: help.lines.join("\n").into_bytes(),
                ..Default::default()
            },
            Ok(CliCommandParseOutcome::Matches(matches)) if matches.command_name() == "echo" => {
                let mut stdout = matches.positionals().join("|").into_bytes();
                stdout.extend(request.stdin);
                CliModuleResponse {
                    stdout,
                    ..Default::default()
                }
            }
            Ok(CliCommandParseOutcome::Matches(matches)) if matches.command_name() == "wait" => {
                let started = std::time::Instant::now();
                loop {
                    let cancelled = HOST.with(|host| {
                        let host = host.get();
                        host.is_cancelled
                            .is_some_and(|callback| unsafe { callback(host.user_data) != 0 })
                    });
                    if cancelled {
                        // Exercise the interval between cancellation and physical completion.
                        std::thread::sleep(std::time::Duration::from_millis(150));
                        break CliModuleResponse::failure(130, "fixture cancelled");
                    }
                    if started.elapsed() > std::time::Duration::from_secs(3) {
                        break CliModuleResponse::failure(1, "fixture cancellation did not arrive");
                    }
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            }
            Ok(_) => CliModuleResponse::failure(7, "fixture failure"),
            Err(error) => CliModuleResponse::failure(2, error.to_string()),
        },
    }
}
