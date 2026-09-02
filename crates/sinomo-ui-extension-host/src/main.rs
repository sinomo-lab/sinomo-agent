//! Isolated host for one module-contributed UI component.
//!
//! The parent owns stdin/stdout and may terminate this process when a request
//! exceeds its wall-time budget. The guest has no WASI or other host imports;
//! all file reads and application actions are represented as typed requests in
//! the returned UI frame and are mediated by the parent.

use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use sinomo_module_api::{
    MAX_UI_COMPONENT_ARTIFACT_BYTES, UiWidgetRequest, UiWidgetResponse, UiWidgetResponseKind,
    decode_sobj, encode_sobj,
};
use thiserror::Error;
use wasmtime::component::{Component, Func, Instance, Linker, Val, types::ComponentItem};
use wasmtime::{Engine, Store, StoreLimits, StoreLimitsBuilder};

const DEFAULT_ENTRYPOINT: &str = "sinomo:ui-widget/widget";
const HANDLE_EXPORT: &str = "handle";
const MAX_IPC_FRAME_BYTES: usize = 72 * 1024 * 1024;
const MAX_COMPONENT_MEMORY_BYTES: usize = 128 * 1024 * 1024;
const COMPONENT_FUEL_PER_REQUEST: u64 = 25_000_000;

fn main() -> ExitCode {
    match run_main(std::env::args().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sinomo-ui-extension-host: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run_main(args: impl IntoIterator<Item = String>) -> Result<(), HostError> {
    let options = HostOptions::parse(args)?;
    let mut runner = UiComponentRunner::load(
        &options.artifact,
        &options.content_hash,
        &options.entrypoint,
    )?;
    if options.check_only {
        return Ok(());
    }
    serve_stdio(&mut runner)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HostOptions {
    artifact: PathBuf,
    content_hash: String,
    entrypoint: String,
    check_only: bool,
}

impl HostOptions {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, HostError> {
        let mut artifact = None;
        let mut content_hash = None;
        let mut entrypoint = DEFAULT_ENTRYPOINT.to_string();
        let mut check_only = false;
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--artifact" => {
                    artifact = Some(PathBuf::from(args.next().ok_or_else(|| {
                        HostError::Options("--artifact requires a path".into())
                    })?));
                }
                "--content-hash" => {
                    content_hash = Some(args.next().ok_or_else(|| {
                        HostError::Options("--content-hash requires a value".into())
                    })?);
                }
                "--entrypoint" => {
                    entrypoint = args.next().ok_or_else(|| {
                        HostError::Options("--entrypoint requires a value".into())
                    })?;
                }
                "--check" => check_only = true,
                other => return Err(HostError::Options(format!("unknown argument `{other}`"))),
            }
        }
        Ok(Self {
            artifact: artifact
                .ok_or_else(|| HostError::Options("--artifact is required".into()))?,
            content_hash: content_hash
                .ok_or_else(|| HostError::Options("--content-hash is required".into()))?,
            entrypoint,
            check_only,
        })
    }
}

struct UiComponentRunner {
    store: Store<StoreLimits>,
    _instance: Instance,
    handle: Func,
}

impl UiComponentRunner {
    fn load(path: &Path, expected_hash: &str, entrypoint: &str) -> Result<Self, HostError> {
        let metadata = fs::symlink_metadata(path).map_err(|source| HostError::Io {
            operation: format!("inspect UI component {}", path.display()),
            source,
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(HostError::Contract(format!(
                "UI component {} must be a regular non-symlink file",
                path.display()
            )));
        }
        if metadata.len() > MAX_UI_COMPONENT_ARTIFACT_BYTES {
            return Err(HostError::ArtifactTooLarge {
                actual: metadata.len(),
                limit: MAX_UI_COMPONENT_ARTIFACT_BYTES,
            });
        }
        let wasm = fs::read(path).map_err(|source| HostError::Io {
            operation: format!("read UI component {}", path.display()),
            source,
        })?;
        let actual_hash = format!("blake3:{}", blake3::hash(&wasm).to_hex());
        if actual_hash != expected_hash {
            return Err(HostError::HashMismatch {
                expected: expected_hash.to_string(),
                actual: actual_hash,
            });
        }

        let mut config = wasmtime::Config::new();
        config.wasm_component_model(true);
        config.consume_fuel(true);
        let engine = Engine::new(&config).map_err(component_error("create Wasmtime engine"))?;
        let component =
            Component::new(&engine, &wasm).map_err(component_error("compile UI component"))?;
        validate_component_contract(&engine, &component, entrypoint)?;
        let limits = StoreLimitsBuilder::new()
            .memory_size(MAX_COMPONENT_MEMORY_BYTES)
            .instances(4)
            .memories(4)
            .tables(8)
            .build();
        let mut store = Store::new(&engine, limits);
        store.limiter(|limits| limits);
        store
            .set_fuel(COMPONENT_FUEL_PER_REQUEST)
            .map_err(component_error("configure UI component fuel"))?;
        let linker = Linker::new(&engine);
        let instance = linker
            .instantiate(&mut store, &component)
            .map_err(component_error("instantiate UI component"))?;
        let instance_export = instance
            .get_export_index(&mut store, None, entrypoint)
            .ok_or_else(|| HostError::Contract(format!("missing export `{entrypoint}`")))?;
        let handle_export = instance
            .get_export_index(&mut store, Some(&instance_export), HANDLE_EXPORT)
            .ok_or_else(|| {
                HostError::Contract(format!("missing export `{entrypoint}.{HANDLE_EXPORT}`"))
            })?;
        let handle = instance
            .get_func(&mut store, handle_export)
            .ok_or_else(|| {
                HostError::Contract(format!(
                    "export `{entrypoint}.{HANDLE_EXPORT}` is not a function"
                ))
            })?;
        Ok(Self {
            store,
            _instance: instance,
            handle,
        })
    }

    fn dispatch(&mut self, request_bytes: &[u8]) -> Result<Vec<u8>, HostError> {
        if request_bytes.len() > MAX_IPC_FRAME_BYTES {
            return Err(HostError::FrameTooLarge {
                actual: request_bytes.len(),
                limit: MAX_IPC_FRAME_BYTES,
            });
        }
        self.store
            .set_fuel(COMPONENT_FUEL_PER_REQUEST)
            .map_err(component_error("refill UI component fuel"))?;
        let params = [Val::List(
            request_bytes.iter().copied().map(Val::U8).collect(),
        )];
        let mut results = [Val::List(Vec::new())];
        self.handle
            .call(&mut self.store, &params, &mut results)
            .map_err(component_error("invoke UI component"))?;
        let Val::List(values) = &results[0] else {
            return Err(HostError::Contract(
                "UI component handle result is not list<u8>".to_string(),
            ));
        };
        if values.len() > MAX_IPC_FRAME_BYTES {
            return Err(HostError::FrameTooLarge {
                actual: values.len(),
                limit: MAX_IPC_FRAME_BYTES,
            });
        }
        values
            .iter()
            .map(|value| match value {
                Val::U8(byte) => Ok(*byte),
                other => Err(HostError::Contract(format!(
                    "UI component returned non-byte list item {other:?}"
                ))),
            })
            .collect()
    }
}

fn validate_component_contract(
    engine: &Engine,
    component: &Component,
    entrypoint: &str,
) -> Result<(), HostError> {
    let (item, _) = component
        .get_export(None, entrypoint)
        .ok_or_else(|| HostError::Contract(format!("missing export `{entrypoint}`")))?;
    let instance = match item {
        ComponentItem::ComponentInstance(instance) => instance,
        other => {
            return Err(HostError::Contract(format!(
                "export `{entrypoint}` must be an instance, found {other:?}"
            )));
        }
    };
    let handle = instance
        .get_export(engine, HANDLE_EXPORT)
        .ok_or_else(|| HostError::Contract(format!("missing export `{entrypoint}.handle`")))?;
    let function = match handle.ty {
        ComponentItem::ComponentFunc(function) => function,
        other => {
            return Err(HostError::Contract(format!(
                "export `{entrypoint}.handle` must be a function, found {other:?}"
            )));
        }
    };
    if function.async_() {
        return Err(HostError::Contract(
            "UI component handle must be synchronous inside its isolated worker".to_string(),
        ));
    }
    let params = function.params().collect::<Vec<_>>();
    let results = function.results().collect::<Vec<_>>();
    if params.len() != 1 || params[0].0 != "request-sobj" || !is_byte_list(&params[0].1) {
        return Err(HostError::Contract(
            "UI component handle must take `request-sobj: list<u8>`".to_string(),
        ));
    }
    if results.len() != 1 || !is_byte_list(&results[0]) {
        return Err(HostError::Contract(
            "UI component handle must return `list<u8>`".to_string(),
        ));
    }
    Ok(())
}

fn is_byte_list(ty: &wasmtime::component::types::Type) -> bool {
    match ty {
        wasmtime::component::types::Type::List(list) => {
            list.ty() == wasmtime::component::types::Type::U8
        }
        _ => false,
    }
}

fn serve_stdio(runner: &mut UiComponentRunner) -> Result<(), HostError> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = stdin.lock();
    let mut writer = stdout.lock();
    serve_streams(runner, &mut reader, &mut writer)
}

fn serve_streams(
    runner: &mut UiComponentRunner,
    reader: &mut impl Read,
    writer: &mut impl Write,
) -> Result<(), HostError> {
    while let Some(request_bytes) = read_frame(reader)? {
        let request: UiWidgetRequest = decode_sobj(&request_bytes)
            .map_err(|source| HostError::Protocol(format!("decode widget request: {source}")))?;
        let response_bytes = match runner.dispatch(&request_bytes) {
            Ok(bytes) => match decode_sobj::<UiWidgetResponse>(&bytes) {
                Ok(response)
                    if response.instance_id == request.instance_id
                        && response.request_id == request.request_id =>
                {
                    if let UiWidgetResponseKind::Frame { frame } = &response.result {
                        frame.validate().map_err(|source| {
                            HostError::Protocol(format!("validate widget frame: {source}"))
                        })?;
                    }
                    bytes
                }
                Ok(_) => encode_error_response(
                    &request,
                    "response-mismatch",
                    "UI component response identity does not match its request",
                )?,
                Err(source) => encode_error_response(
                    &request,
                    "invalid-response",
                    &format!("decode UI component response: {source}"),
                )?,
            },
            Err(error) => encode_error_response(&request, "component-failed", &error.to_string())?,
        };
        write_frame(writer, &response_bytes)?;
        writer.flush().map_err(|source| HostError::Io {
            operation: "flush stdout".to_string(),
            source,
        })?;
    }
    Ok(())
}

fn encode_error_response(
    request: &UiWidgetRequest,
    code: &str,
    detail: &str,
) -> Result<Vec<u8>, HostError> {
    encode_sobj(&UiWidgetResponse {
        instance_id: request.instance_id.clone(),
        request_id: request.request_id,
        result: UiWidgetResponseKind::Error {
            code: code.to_string(),
            detail: detail.to_string(),
        },
    })
    .map_err(|source| HostError::Protocol(format!("encode widget error response: {source}")))
}

fn read_frame(reader: &mut impl Read) -> Result<Option<Vec<u8>>, HostError> {
    let mut length = [0u8; 4];
    let mut read = 0;
    while read < length.len() {
        match reader.read(&mut length[read..]) {
            Ok(0) if read == 0 => return Ok(None),
            Ok(0) => {
                return Err(HostError::Protocol(
                    "stdin ended inside frame length".to_string(),
                ));
            }
            Ok(count) => read += count,
            Err(source) if source.kind() == io::ErrorKind::Interrupted => {}
            Err(source) => {
                return Err(HostError::Io {
                    operation: "read frame length".to_string(),
                    source,
                });
            }
        }
    }
    let length = u32::from_le_bytes(length) as usize;
    if length > MAX_IPC_FRAME_BYTES {
        return Err(HostError::FrameTooLarge {
            actual: length,
            limit: MAX_IPC_FRAME_BYTES,
        });
    }
    let mut bytes = vec![0; length];
    reader
        .read_exact(&mut bytes)
        .map_err(|source| HostError::Io {
            operation: "read frame body".to_string(),
            source,
        })?;
    Ok(Some(bytes))
}

fn write_frame(writer: &mut impl Write, bytes: &[u8]) -> Result<(), HostError> {
    if bytes.len() > MAX_IPC_FRAME_BYTES || bytes.len() > u32::MAX as usize {
        return Err(HostError::FrameTooLarge {
            actual: bytes.len(),
            limit: MAX_IPC_FRAME_BYTES,
        });
    }
    writer
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .and_then(|_| writer.write_all(bytes))
        .map_err(|source| HostError::Io {
            operation: "write frame".to_string(),
            source,
        })
}

fn component_error(operation: &'static str) -> impl FnOnce(wasmtime::Error) -> HostError {
    move |source| HostError::Component {
        operation: operation.to_string(),
        detail: format!("{source:?}"),
    }
}

#[derive(Debug, Error)]
enum HostError {
    #[error("invalid options: {0}")]
    Options(String),
    #[error("{operation}: {source}")]
    Io {
        operation: String,
        #[source]
        source: io::Error,
    },
    #[error("UI component hash mismatch: expected {expected}, actual {actual}")]
    HashMismatch { expected: String, actual: String },
    #[error("invalid UI component contract: {0}")]
    Contract(String),
    #[error("{operation}: {detail}")]
    Component { operation: String, detail: String },
    #[error("UI extension protocol error: {0}")]
    Protocol(String),
    #[error("UI extension frame is {actual} bytes; limit is {limit}")]
    FrameTooLarge { actual: usize, limit: usize },
    #[error("UI component artifact is {actual} bytes; limit is {limit}")]
    ArtifactTooLarge { actual: u64, limit: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_require_verified_artifact_identity() {
        let options = HostOptions::parse([
            "--artifact".to_string(),
            "/tmp/widget.wasm".to_string(),
            "--content-hash".to_string(),
            format!("blake3:{}", "0".repeat(64)),
            "--check".to_string(),
        ])
        .expect("options");
        assert_eq!(options.entrypoint, DEFAULT_ENTRYPOINT);
        assert!(options.check_only);
        assert!(HostOptions::parse(["--artifact".to_string()]).is_err());
    }

    #[test]
    fn framed_transport_roundtrips_and_rejects_oversize() {
        let mut encoded = Vec::new();
        write_frame(&mut encoded, b"widget-frame").expect("write frame");
        assert_eq!(
            read_frame(&mut encoded.as_slice()).expect("read frame"),
            Some(b"widget-frame".to_vec())
        );
        let mut oversized = Vec::new();
        oversized.extend_from_slice(&((MAX_IPC_FRAME_BYTES as u32) + 1).to_le_bytes());
        assert!(matches!(
            read_frame(&mut oversized.as_slice()),
            Err(HostError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn contract_rejects_component_without_widget_instance() {
        let mut config = wasmtime::Config::new();
        config.wasm_component_model(true);
        let engine = Engine::new(&config).expect("engine");
        let bytes = wat::parse_str("(component)").expect("component wat");
        let component = Component::new(&engine, bytes).expect("component");
        let error = validate_component_contract(&engine, &component, DEFAULT_ENTRYPOINT)
            .expect_err("missing export");
        assert!(error.to_string().contains("missing export"));
    }

    #[test]
    fn component_contract_executes_byte_list_handler() {
        let component_wat = r#"
            (component
                (core module $module
                    (memory (export "memory") 1)
                    (global $heap (mut i32) (i32.const 1024))
                    (func $realloc (export "realloc")
                        (param i32 i32 i32 i32) (result i32)
                        (local $result i32)
                        global.get $heap
                        i32.const 7
                        i32.add
                        i32.const -8
                        i32.and
                        local.tee $result
                        local.get 3
                        i32.add
                        global.set $heap
                        local.get $result)
                    (func (export "handle") (param i32 i32) (result i32)
                        (local $result i32)
                        global.get $heap
                        i32.const 7
                        i32.add
                        i32.const -8
                        i32.and
                        local.tee $result
                        i32.const 8
                        i32.add
                        global.set $heap
                        local.get $result
                        local.get 0
                        i32.store
                        local.get $result
                        local.get 1
                        i32.store offset=4
                        local.get $result))
                (core instance $instance (instantiate $module))
                (type $handle-type
                    (func (param "request-sobj" (list u8)) (result (list u8))))
                (func $handle (type $handle-type)
                    (canon lift (core func $instance "handle")
                        (memory (core memory $instance "memory"))
                        (realloc (core func $instance "realloc"))))
                (instance $widget
                    (export "handle" (func $handle)))
                (export "sinomo:ui-widget/widget" (instance $widget))
            )
        "#;
        let bytes = wat::parse_str(component_wat).expect("component wat");
        let root = std::env::temp_dir().join(format!(
            "sinomo-ui-extension-host-{}-{}.wasm",
            std::process::id(),
            monotonic_test_nonce()
        ));
        fs::write(&root, &bytes).expect("write component");
        let hash = format!("blake3:{}", blake3::hash(&bytes).to_hex());
        let mut runner = UiComponentRunner::load(&root, &hash, DEFAULT_ENTRYPOINT)
            .expect("load fixture component");
        let request = b"sobj-frame";
        assert_eq!(runner.dispatch(request).expect("dispatch"), request);
        let _ = fs::remove_file(root);
    }

    #[test]
    fn stdio_protocol_returns_validated_component_frame() {
        let request = UiWidgetRequest {
            instance_id: "fixture".to_string(),
            request_id: 1,
            message: sinomo_module_api::UiWidgetRequestKind::Render {
                viewport: sinomo_module_api::UiViewport {
                    width: 320.0,
                    height: 180.0,
                    scale_factor: 1.0,
                },
                theme: fixture_theme(),
                monotonic_time_ms: 0,
            },
        };
        let expected = UiWidgetResponse {
            instance_id: request.instance_id.clone(),
            request_id: request.request_id,
            result: UiWidgetResponseKind::Frame {
                frame: sinomo_module_api::UiWidgetFrame {
                    generation: 1,
                    commands: vec![sinomo_module_api::UiPaintCommand::FillRect {
                        rect: sinomo_module_api::UiRect {
                            x: 0.0,
                            y: 0.0,
                            width: 320.0,
                            height: 180.0,
                        },
                        color: sinomo_module_api::UiColor {
                            red: 24,
                            green: 48,
                            blue: 96,
                            alpha: 255,
                        },
                    }],
                    ..Default::default()
                },
            },
        };
        let response_bytes = encode_sobj(&expected).expect("encode response");
        let data = response_bytes
            .iter()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let component_wat = static_response_component_wat(&data, response_bytes.len());
        let wasm = wat::parse_str(component_wat).expect("static response component");
        let root = std::env::temp_dir().join(format!(
            "sinomo-ui-extension-host-response-{}-{}.wasm",
            std::process::id(),
            monotonic_test_nonce()
        ));
        fs::write(&root, &wasm).expect("write response component");
        let hash = format!("blake3:{}", blake3::hash(&wasm).to_hex());
        let mut runner = UiComponentRunner::load(&root, &hash, DEFAULT_ENTRYPOINT)
            .expect("load response component");
        let mut input = Vec::new();
        write_frame(&mut input, &encode_sobj(&request).expect("encode request"))
            .expect("frame request");
        let mut output = Vec::new();
        serve_streams(&mut runner, &mut input.as_slice(), &mut output).expect("serve fixture");
        let encoded = read_frame(&mut output.as_slice())
            .expect("read response")
            .expect("response frame");
        let actual: UiWidgetResponse = decode_sobj(&encoded).expect("decode response");
        assert_eq!(actual, expected);
        let _ = fs::remove_file(root);
    }

    fn fixture_theme() -> sinomo_module_api::UiThemeSnapshot {
        use sinomo_module_api::{UiColor, UiThemeMode, UiThemeSnapshot};
        UiThemeSnapshot {
            mode: UiThemeMode::Dark,
            accent: UiColor::default(),
            foreground: UiColor::default(),
            muted_foreground: UiColor::default(),
            background: UiColor::default(),
            surface: UiColor::default(),
            danger: UiColor::default(),
        }
    }

    fn static_response_component_wat(data: &str, length: usize) -> String {
        format!(
            r#"
            (component
                (core module $module
                    (memory (export "memory") 1)
                    (data (i32.const 32768) "{data}")
                    (global $heap (mut i32) (i32.const 1024))
                    (func $realloc (export "realloc")
                        (param i32 i32 i32 i32) (result i32)
                        (local $result i32)
                        global.get $heap
                        i32.const 7
                        i32.add
                        i32.const -8
                        i32.and
                        local.tee $result
                        local.get 3
                        i32.add
                        global.set $heap
                        local.get $result)
                    (func (export "handle") (param i32 i32) (result i32)
                        (local $result i32)
                        global.get $heap
                        i32.const 7
                        i32.add
                        i32.const -8
                        i32.and
                        local.tee $result
                        i32.const 8
                        i32.add
                        global.set $heap
                        local.get $result
                        i32.const 32768
                        i32.store
                        local.get $result
                        i32.const {length}
                        i32.store offset=4
                        local.get $result))
                (core instance $instance (instantiate $module))
                (type $handle-type
                    (func (param "request-sobj" (list u8)) (result (list u8))))
                (func $handle (type $handle-type)
                    (canon lift (core func $instance "handle")
                        (memory (core memory $instance "memory"))
                        (realloc (core func $instance "realloc"))))
                (instance $widget
                    (export "handle" (func $handle)))
                (export "sinomo:ui-widget/widget" (instance $widget))
            )
            "#
        )
    }

    fn monotonic_test_nonce() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    }
}
