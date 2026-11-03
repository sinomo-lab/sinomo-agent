//! Tool policy and command translation. Execution stays in the host agent runtime.
use serde::Deserialize;
use sinomo_module_api::{agent::*, *};

const PROMPT: &str = "You are a temporary subagent working for a parent agent. Complete the assigned work, coordinate shared resources, and return concrete findings and verification. You may message your parent using the subagents tool with id parent. Your final answer is delivered to your parent.";
static LIFECYCLE: NativeModuleLifecycle = NativeModuleLifecycle::new();

pub fn definition() -> SessionToolDefinition {
    SessionToolDefinition {
        name: "subagents".into(),
        toolsets: vec!["core".into(), "module".into(), "delegation".into()],
        description: "Spawn reusable subagents with inherited configuration, message agents, inspect sessions, wait, cancel, or discard. Completion notifies the parent. Idle children archive after 24 hours.".into(),
        input_schema: serde_json::json!({
            "type":"object", "properties": {
                "action":{"type":"string","enum":["spawn","list","inspect","message","wait","cancel","discard","dispose"]},
                "name":{"type":"string"}, "task":{"type":"string"}, "prompt":{"type":"string"},
                "id":{"type":"string","description":"Agent id or parent"}, "message":{"type":"string"},
                "session":{"type":"boolean"}, "archived":{"type":"boolean","default":false},
                "timeout_ms":{"type":"integer","minimum":0,"maximum":60000}
            }, "required":["action"], "additionalProperties":false
        }),
        skill_id: "subagents".into(),
        skill: include_str!("../skills/subagents/SKILL.md").into(),
    }
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Spawn {
        task: String,
        #[serde(default)]
        name: String,
        #[serde(default)]
        prompt: String,
    },
    List {
        #[serde(default)]
        archived: bool,
    },
    Inspect {
        id: String,
        #[serde(default)]
        session: bool,
    },
    Message {
        id: String,
        message: String,
    },
    Wait {
        id: String,
        #[serde(default = "wait_default")]
        timeout_ms: u64,
    },
    Cancel {
        id: String,
    },
    #[serde(alias = "dispose")]
    Discard {
        id: String,
    },
}
fn wait_default() -> u64 {
    1000
}

pub fn translate(input: serde_json::Value) -> Result<SessionCommand, String> {
    if serde_json::to_vec(&input).map_err(|e| e.to_string())?.len() > 128 * 1024 {
        return Err("session tool input exceeds 128 KiB".into());
    }
    let action: Action = serde_json::from_value(input).map_err(|e| e.to_string())?;
    Ok(match action {
        Action::Spawn { name, task, prompt } => {
            if task.trim().is_empty() {
                return Err("task must not be empty".into());
            }
            SessionCommand::Create {
                name,
                instructions: task,
                prompt: format!("{PROMPT}\n\n{prompt}"),
                idle_timeout_secs: 86400,
            }
        }
        Action::List { archived } => SessionCommand::List { archived },
        Action::Inspect { id, session } => SessionCommand::Inspect { id, session },
        Action::Message { id, message } => {
            if message.trim().is_empty() {
                return Err("message must not be empty".into());
            }
            SessionCommand::Message { id, message }
        }
        Action::Wait { id, timeout_ms } => SessionCommand::Wait {
            id,
            timeout_ms: timeout_ms.min(60000),
        },
        Action::Cancel { id } => SessionCommand::Cancel { id },
        Action::Discard { id } => SessionCommand::Archive { id },
    })
}

pub fn descriptor() -> NativeModuleDescriptor {
    let operations = [
        SESSION_TOOLS_DESCRIBE,
        SESSION_TOOLS_TRANSLATE,
        SESSION_TOOLS_EXECUTE,
    ];
    NativeModuleDescriptor {
        module_id: "module.subagents".into(),
        module_version: env!("CARGO_PKG_VERSION").into(),
        owner: "module.subagents".into(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: "sinomo.agent.session-tools.v2".into(),
        platform_triple: format!(
            "{}-{}-{}",
            std::env::consts::ARCH,
            std::env::consts::OS,
            std::env::consts::FAMILY
        ),
        supported_operations: operations.iter().map(|s| (*s).into()).collect(),
        capabilities: operations.iter().map(|s| (*s).into()).collect(),
        surfaces: NativeModuleSurfaces {
            declarative_resources: vec![NativeModuleDeclarativeResource {
                id: "subagents".into(),
                kind: "skill".into(),
                path: "skills/subagents/SKILL.md".into(),
                description: "Delegate focused work to reusable temporary agents and coordinate their sessions.".into(),
                target_agents: vec![],
                model_families: vec![],
                activation_tags: vec!["delegation".into()],
                required_capabilities: vec![],
                embedded_text: Some(definition().skill),
            }],
            capability_exports: operations.iter().map(|s| (*s).into()).collect(),
            ..Default::default()
        },
        component: ComponentDescriptor::new("module.subagents", env!("CARGO_PKG_VERSION"))
            .with_display_name("Subagents")
            .require(ServiceRequirement::new(sinomo_module_api::sessions::SESSION_INTERFACE,"1.0.0").with_operation(sinomo_module_api::sessions::SESSION_ACCESS))
            .with_offer(
                ServiceOffer::new(
                    "agent-session-tools",
                    SESSION_TOOLS_INTERFACE,
                    SESSION_TOOLS_VERSION,
                )
                .with_operation(ServiceOperationOffer::new(SESSION_TOOLS_DESCRIBE))
                .with_operation(ServiceOperationOffer::new(SESSION_TOOLS_TRANSLATE))
                .with_operation(ServiceOperationOffer::new(SESSION_TOOLS_EXECUTE)),
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
    write(out, &descriptor())
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV2) -> SinomoModuleStatusCode {
    if host.abi_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
        return SINOMO_MODULE_STATUS_UNSUPPORTED_ABI;
    }
    if LIFECYCLE.initialize() {
        SINOMO_MODULE_STATUS_OK
    } else {
        SINOMO_MODULE_STATUS_RUNTIME_ERROR
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
    if LIFECYCLE.begin_shutdown() {
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
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    request: SinomoModuleByteSlice,
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let Some(_lease) = LIFECYCLE.begin_invocation() else {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    };
    let response = std::panic::catch_unwind(|| {
        match decode_sobj::<NativeModuleRequest>(unsafe { request.as_slice() }) {
            Ok(NativeModuleRequest::ProbeCapabilities(_)) => {
                NativeModuleResponse::Capabilities(NativeModuleCapabilitiesResponse {
                    descriptor: descriptor(),
                })
            }
            Ok(NativeModuleRequest::Invoke(request)) => {
                let result = match request.operation.as_str() {
                    SESSION_TOOLS_DESCRIBE => encode_sobj(&definition()).map_err(|e| e.to_string()),
                    SESSION_TOOLS_TRANSLATE => decode_sobj(&request.payload_sobj)
                        .map_err(|e| e.to_string())
                        .and_then(translate)
                        .and_then(|command| encode_sobj(&command).map_err(|e| e.to_string())),
                    _ => Err("unsupported session tool operation".into()),
                };
                match result {
                    Ok(payload_sobj) => {
                        NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
                            operation: request.operation,
                            payload_sobj,
                            response_body: None,
                        })
                    }
                    Err(detail) => error(detail),
                }
            }
            Err(e) => error(e.to_string()),
        }
    })
    .unwrap_or_else(|_| error("session tool provider panicked".into()));
    write(out, &response)
}
fn error(detail: String) -> NativeModuleResponse {
    NativeModuleResponse::Error(NativeModuleErrorResponse {
        code: "session-tool".into(),
        detail,
    })
}
fn write<T: serde::Serialize>(
    out: *mut SinomoModuleByteBuffer,
    value: &T,
) -> SinomoModuleStatusCode {
    if out.is_null() {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    }
    match encode_sobj(value) {
        Ok(bytes) => {
            unsafe {
                *out = SinomoModuleByteBuffer::from_vec(bytes);
            }
            SINOMO_MODULE_STATUS_OK
        }
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke_with_host_v1(
    request: SinomoModuleByteSlice,
    host: sinomo_module_api::host::InvocationHost,
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let decoded = decode_sobj::<NativeModuleRequest>(unsafe { request.as_slice() });
    if let Ok(NativeModuleRequest::Invoke(invocation)) = decoded
        && invocation.operation == SESSION_TOOLS_EXECUTE
    {
        let Some(_lease) = LIFECYCLE.begin_invocation() else {
            return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
        };
        let result = std::panic::catch_unwind(|| -> Result<NativeModuleResponse, String> {
            let command =
                translate(decode_sobj(&invocation.payload_sobj).map_err(|e| e.to_string())?)?;
            let client = unsafe { sinomo_module_api::sessions::SessionClient::new(&host) };
            let value: serde_json::Value = if let SessionCommand::Wait { id, timeout_ms } = command
            {
                let deadline =
                    std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
                loop {
                    if unsafe { host.cancelled() } {
                        return Err("session invocation cancelled".into());
                    }
                    let remaining = deadline
                        .saturating_duration_since(std::time::Instant::now())
                        .as_millis()
                        .min(100) as u64;
                    let value: serde_json::Value = client
                        .request(&sinomo_module_api::sessions::SessionRequest::Command {
                            command: SessionCommand::Wait {
                                id: id.clone(),
                                timeout_ms: remaining,
                            },
                        })
                        .map_err(|e| e.to_string())?;
                    if value["subagent"]["status"] != "running"
                        || std::time::Instant::now() >= deadline
                    {
                        break value;
                    }
                }
            } else {
                client
                    .request(&sinomo_module_api::sessions::SessionRequest::Command { command })
                    .map_err(|e| e.to_string())?
            };
            Ok(NativeModuleResponse::Invocation(
                NativeModuleInvocationResponse {
                    operation: invocation.operation,
                    payload_sobj: encode_sobj(&value).map_err(|e| e.to_string())?,
                    response_body: None,
                },
            ))
        })
        .unwrap_or_else(|_| Err("session provider panicked".into()));
        return write(out, &result.unwrap_or_else(error));
    }
    sinomo_module_invoke(request, out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn contract_and_policy() {
        descriptor().component.validate().unwrap();
        assert!(
            translate(serde_json::json!({"action":"spawn","task":"work","permissions":[]}))
                .is_err()
        );
        assert!(matches!(
            translate(serde_json::json!({"action":"spawn","task":"work"})).unwrap(),
            SessionCommand::Create {
                idle_timeout_secs: 86400,
                ..
            }
        ));
        assert!(matches!(
            translate(serde_json::json!({"action":"list"})).unwrap(),
            SessionCommand::List { archived: false }
        ));
    }
}
