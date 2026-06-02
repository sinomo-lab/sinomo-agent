use crate::{SandboxWasiError, sandbox_env_name_is_valid};
use std::collections::BTreeMap;

const SINOMO_SANDBOX_WASI_MAX_CHILD_STRING_BYTES: usize = 4096;

pub(super) fn read_guest_env_array(
    caller: &mut wasmtime::Caller<'_, super::state::LocalWasiState>,
    memory: &wasmtime::Memory,
    ptr: usize,
) -> Result<BTreeMap<String, String>, SandboxWasiError> {
    let entries = read_guest_cstring_array(caller, memory, ptr)?;
    let mut env = BTreeMap::new();
    for entry in entries {
        let Some((key, value)) = entry.split_once('=') else {
            return Err(SandboxWasiError::InvalidRequest {
                detail: "sandbox child process environment entry is missing `=`".to_string(),
            });
        };
        if !sandbox_env_name_is_valid(key) {
            return Err(SandboxWasiError::InvalidRequest {
                detail: format!(
                    "sandbox child process environment variable name `{key}` is invalid"
                ),
            });
        }
        env.insert(key.to_string(), value.to_string());
    }
    Ok(env)
}

pub(super) fn read_guest_cstring_array(
    caller: &mut wasmtime::Caller<'_, super::state::LocalWasiState>,
    memory: &wasmtime::Memory,
    ptr: usize,
) -> Result<Vec<String>, SandboxWasiError> {
    let mut values = Vec::new();
    for index in 0..4096usize {
        let element_ptr = ptr.saturating_add(index.saturating_mul(4));
        let string_ptr = read_guest_u32(caller, memory, element_ptr)?;
        if string_ptr == 0 {
            return Ok(values);
        }
        values.push(read_guest_cstring(caller, memory, string_ptr as usize)?);
    }
    Err(SandboxWasiError::InvalidRequest {
        detail: "sandbox child process argv/env array is not null terminated".to_string(),
    })
}

pub(super) fn read_guest_optional_string(
    caller: &mut wasmtime::Caller<'_, super::state::LocalWasiState>,
    memory: &wasmtime::Memory,
    ptr: i32,
    len: i32,
) -> Result<Option<String>, SandboxWasiError> {
    if len == 0 {
        return Ok(None);
    }
    if ptr <= 0 || len < 0 {
        return Err(SandboxWasiError::InvalidRequest {
            detail: "optional child process string has invalid pointer or length".to_string(),
        });
    }
    read_guest_string(caller, memory, ptr as usize, len as usize).map(Some)
}

pub(super) fn read_guest_string(
    caller: &mut wasmtime::Caller<'_, super::state::LocalWasiState>,
    memory: &wasmtime::Memory,
    ptr: usize,
    len: usize,
) -> Result<String, SandboxWasiError> {
    if len > SINOMO_SANDBOX_WASI_MAX_CHILD_STRING_BYTES {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!(
                "sandbox child process string exceeds {SINOMO_SANDBOX_WASI_MAX_CHILD_STRING_BYTES} byte limit"
            ),
        });
    }
    let mut bytes = vec![0u8; len];
    memory
        .read(&mut *caller, ptr, &mut bytes)
        .map_err(|_| SandboxWasiError::InvalidRequest {
            detail: "sandbox child process string is outside guest memory".to_string(),
        })?;
    std::str::from_utf8(&bytes)
        .map(ToString::to_string)
        .map_err(|_| SandboxWasiError::InvalidRequest {
            detail: "sandbox child process string is not utf-8".to_string(),
        })
}

fn read_guest_u32(
    caller: &mut wasmtime::Caller<'_, super::state::LocalWasiState>,
    memory: &wasmtime::Memory,
    ptr: usize,
) -> Result<u32, SandboxWasiError> {
    let mut bytes = [0u8; 4];
    memory
        .read(&mut *caller, ptr, &mut bytes)
        .map_err(|_| SandboxWasiError::InvalidRequest {
            detail: "sandbox child process pointer is outside guest memory".to_string(),
        })?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_guest_cstring(
    caller: &mut wasmtime::Caller<'_, super::state::LocalWasiState>,
    memory: &wasmtime::Memory,
    ptr: usize,
) -> Result<String, SandboxWasiError> {
    let data = memory.data(&mut *caller);
    let Some(tail) = data.get(ptr..) else {
        return Err(SandboxWasiError::InvalidRequest {
            detail: "sandbox child process string is outside guest memory".to_string(),
        });
    };
    let scan_len = tail
        .len()
        .min(SINOMO_SANDBOX_WASI_MAX_CHILD_STRING_BYTES + 1);
    let Some(end) = tail[..scan_len].iter().position(|byte| *byte == 0) else {
        if tail.len() > SINOMO_SANDBOX_WASI_MAX_CHILD_STRING_BYTES {
            return Err(SandboxWasiError::InvalidRequest {
                detail: format!(
                    "sandbox child process string exceeds {SINOMO_SANDBOX_WASI_MAX_CHILD_STRING_BYTES} byte limit"
                ),
            });
        }
        return Err(SandboxWasiError::InvalidRequest {
            detail: "sandbox child process string is not null terminated".to_string(),
        });
    };
    std::str::from_utf8(&tail[..end])
        .map(ToString::to_string)
        .map_err(|_| SandboxWasiError::InvalidRequest {
            detail: "sandbox child process string is not utf-8".to_string(),
        })
}
