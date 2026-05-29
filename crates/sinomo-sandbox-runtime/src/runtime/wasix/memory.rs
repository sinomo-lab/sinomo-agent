use crate::SandboxWasiError;

pub(super) fn validate_wasix_module_memory_budget(
    wasm: &[u8],
    memory_limit_bytes: usize,
) -> Result<(), SandboxWasiError> {
    if !wasm.starts_with(b"\0asm") {
        return Ok(());
    }
    let memory_limit_bytes = memory_limit_bytes as u64;
    for payload in wasmparser::Parser::new(0).parse_all(wasm) {
        match payload.map_err(|error| SandboxWasiError::Runtime {
            detail: format!("parse WASIX module memory declarations: {error}"),
        })? {
            wasmparser::Payload::ImportSection(imports) => {
                for import in imports.into_imports() {
                    let import = import.map_err(|error| SandboxWasiError::Runtime {
                        detail: format!("parse WASIX memory import: {error}"),
                    })?;
                    if let wasmparser::TypeRef::Memory(memory) = import.ty {
                        validate_wasix_memory_type(memory, memory_limit_bytes)?;
                    }
                }
            }
            wasmparser::Payload::MemorySection(memories) => {
                for memory in memories {
                    validate_wasix_memory_type(
                        memory.map_err(|error| SandboxWasiError::Runtime {
                            detail: format!("parse WASIX memory section: {error}"),
                        })?,
                        memory_limit_bytes,
                    )?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_wasix_memory_type(
    memory: wasmparser::MemoryType,
    memory_limit_bytes: u64,
) -> Result<(), SandboxWasiError> {
    let page_size = 1u64
        .checked_shl(memory.page_size_log2.unwrap_or(16))
        .ok_or_else(|| SandboxWasiError::Runtime {
            detail: "WASIX module declares an invalid memory page size".to_string(),
        })?;
    let initial_bytes = memory.initial.checked_mul(page_size).ok_or_else(|| {
        SandboxWasiError::MemoryLimitExceeded {
            limit: memory_limit_bytes as usize,
        }
    })?;
    if initial_bytes > memory_limit_bytes {
        return Err(SandboxWasiError::MemoryLimitExceeded {
            limit: memory_limit_bytes as usize,
        });
    }
    let Some(max_pages) = memory.maximum else {
        return Err(SandboxWasiError::Runtime {
            detail: format!(
                "WASIX module memory is unbounded; declare a maximum memory no larger than {memory_limit_bytes} bytes"
            ),
        });
    };
    let maximum_bytes =
        max_pages
            .checked_mul(page_size)
            .ok_or_else(|| SandboxWasiError::MemoryLimitExceeded {
                limit: memory_limit_bytes as usize,
            })?;
    if maximum_bytes > memory_limit_bytes {
        return Err(SandboxWasiError::MemoryLimitExceeded {
            limit: memory_limit_bytes as usize,
        });
    }
    Ok(())
}
