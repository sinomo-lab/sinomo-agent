use crate::SandboxWasiError;

pub(super) fn validate_wasix_module_network_imports(wasm: &[u8]) -> Result<(), SandboxWasiError> {
    if !wasm.starts_with(b"\0asm") {
        return Ok(());
    }
    for payload in wasmparser::Parser::new(0).parse_all(wasm) {
        let payload = payload.map_err(|error| SandboxWasiError::Runtime {
            detail: format!("parse WASIX module imports: {error}"),
        })?;
        let wasmparser::Payload::ImportSection(imports) = payload else {
            continue;
        };
        for import in imports.into_imports() {
            let import = import.map_err(|error| SandboxWasiError::Runtime {
                detail: format!("parse WASIX import: {error}"),
            })?;
            if wasix_import_is_ambient_network(import.module) {
                return Err(SandboxWasiError::InvalidRequest {
                    detail: format!(
                        "WASIX module imports ambient network module `{}`; use mediated sandbox fetch instead",
                        import.module
                    ),
                });
            }
        }
    }
    Ok(())
}

fn wasix_import_is_ambient_network(module: &str) -> bool {
    module == "wasix_http_client_v1"
}
