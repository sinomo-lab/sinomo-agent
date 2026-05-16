#![allow(clippy::result_large_err)]
use std::{
    collections::HashMap,
    future::Future,
    ops::Deref,
    path::Path,
    pin::Pin,
    sync::{Arc, RwLock},
};

use anyhow::Context;
use shared_buffer::OwnedBuffer;
use virtual_fs::{AsyncReadExt, FileSystem};
use wasmer::FunctionEnvMut;
use wasmer_package::utils::from_bytes;

mod binary_package;
mod exec;

const SINOMO_WASIX_PINNED_CHILD_EXEC_ONLY: &str = "SINOMO_WASIX_PINNED_CHILD_EXEC_ONLY";

pub use self::{
    binary_package::*,
    exec::{
        package_command_by_name, run_exec, spawn_exec, spawn_exec_module, spawn_exec_wasm,
        spawn_load_module, spawn_union_fs,
    },
};
use crate::{
    Runtime, SpawnError, WasiEnv,
    os::{command::Commands, task::TaskJoinHandle},
    runtime::module_cache::HashedModuleData,
};

#[derive(Debug, Clone)]
pub struct BinFactory {
    pub(crate) commands: Commands,
    runtime: Arc<dyn Runtime + Send + Sync + 'static>,
    pub(crate) local: Arc<RwLock<HashMap<String, Option<Arc<BinaryPackage>>>>>,
}

impl BinFactory {
    pub fn new(runtime: Arc<dyn Runtime + Send + Sync + 'static>) -> BinFactory {
        BinFactory {
            commands: Commands::new_with_builtins(runtime.clone()),
            runtime,
            local: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn runtime(&self) -> &(dyn Runtime + Send + Sync) {
        self.runtime.deref()
    }

    pub fn set_binary(&self, name: &str, binary: &Arc<BinaryPackage>) {
        let mut cache = self.local.write().unwrap();
        cache.insert(name.to_string(), Some(binary.clone()));
    }

    #[allow(clippy::await_holding_lock)]
    pub async fn get_binary(
        &self,
        name: &str,
        fs: Option<&dyn FileSystem>,
    ) -> Option<Arc<BinaryPackage>> {
        self.get_executable(name, fs)
            .await
            .and_then(|executable| match executable {
                Executable::Wasm(_) | Executable::Script { .. } => None,
                Executable::BinaryPackage(pkg) => Some(pkg),
            })
    }

    pub fn spawn<'a>(
        &'a self,
        name: String,
        env: WasiEnv,
    ) -> Pin<Box<dyn Future<Output = Result<TaskJoinHandle, SpawnError>> + 'a>> {
        Box::pin(async move {
            if sinomo_pinned_child_exec_only(&env) && !sinomo_child_exec_path_is_pinned(&name) {
                return Err(SpawnError::BinaryNotFound {
                    binary: name.clone(),
                });
            }

            // Find the binary (or die trying) and make the spawn type
            let res = self
                .get_executable(name.as_str(), Some(env.fs_root()))
                .await
                .ok_or_else(|| SpawnError::BinaryNotFound {
                    binary: name.clone(),
                });
            let executable = res?;

            // Execute
            match executable {
                Executable::Script {
                    interpreter,
                    interpreter_arg,
                } => {
                    if sinomo_pinned_child_exec_only(&env)
                        && !sinomo_child_exec_path_is_pinned(&interpreter)
                    {
                        return Err(SpawnError::BinaryNotFound {
                            binary: interpreter.clone(),
                        });
                    }
                    {
                        let mut args = env.state.args.lock().unwrap();
                        let mut script_args = Vec::with_capacity(
                            args.len() + 1 + usize::from(interpreter_arg.is_some()),
                        );
                        script_args.push(interpreter.clone());
                        if let Some(interpreter_arg) = interpreter_arg {
                            script_args.push(interpreter_arg);
                        }
                        script_args.push(name.clone());
                        script_args.extend(args.iter().skip(1).cloned());
                        *args = script_args;
                    }
                    self.spawn(interpreter, env).await
                }
                Executable::Wasm(bytes) => {
                    apply_sinomo_command_metadata(name.as_str(), &env);
                    let data = HashedModuleData::new(bytes.clone());
                    spawn_exec_wasm(data, name.as_str(), env, &self.runtime).await
                }
                Executable::BinaryPackage(pkg) => {
                    {
                        let cmd = package_command_by_name(&pkg, name.as_str())?;
                        env.prepare_spawn(cmd);
                    }

                    spawn_exec(pkg.as_ref().clone(), name.as_str(), env, &self.runtime).await
                }
            }
        })
    }

    pub fn try_built_in(
        &self,
        name: String,
        parent_ctx: Option<&FunctionEnvMut<'_, WasiEnv>>,
        builder: &mut Option<WasiEnv>,
    ) -> Result<TaskJoinHandle, SpawnError> {
        // We check for built in commands
        if let Some(parent_ctx) = parent_ctx {
            if self.commands.exists(name.as_str()) {
                return self.commands.exec(parent_ctx, name.as_str(), builder);
            }
        } else if self.commands.exists(name.as_str()) {
            tracing::warn!("builtin command without a parent ctx - {}", name);
        }
        Err(SpawnError::BinaryNotFound { binary: name })
    }

    // TODO: remove allow once BinFactory is refactored
    // currently fine because a BinFactory is only used by a single process tree
    #[allow(clippy::await_holding_lock)]
    pub async fn get_executable(
        &self,
        name: &str,
        fs: Option<&dyn FileSystem>,
    ) -> Option<Executable> {
        let name = name.to_string();

        // Return early if the path is already cached
        {
            let cache = self.local.read().unwrap();
            if let Some(data) = cache.get(&name) {
                data.clone().map(Executable::BinaryPackage);
            }
        }

        let mut cache = self.local.write().unwrap();

        // Check the cache again to avoid a race condition where the cache was populated inbetween the fast path and here
        if let Some(data) = cache.get(&name) {
            return data.clone().map(Executable::BinaryPackage);
        }

        // Check the filesystem for the file
        if name.starts_with('/')
            && let Some(fs) = fs
        {
            match load_executable_from_filesystem(fs, name.as_ref(), self.runtime()).await {
                Ok(executable) => {
                    if let Executable::BinaryPackage(pkg) = &executable {
                        cache.insert(name, Some(pkg.clone()));
                    }

                    return Some(executable);
                }
                Err(e) => {
                    tracing::warn!(
                        path = name,
                        error = &*e,
                        "Unable to load the package from disk"
                    );
                }
            }
        }

        // NAK
        cache.insert(name, None);
        None
    }
}

fn apply_sinomo_command_metadata(name: &str, env: &WasiEnv) {
    let Some(command_name) = name.rsplit('/').next().filter(|value| !value.is_empty()) else {
        return;
    };
    let argv_prefix_key = format!("SINOMO_WASIX_COMMAND_ARGV_PREFIX_{command_name}");
    let exec_name_key = format!("SINOMO_WASIX_COMMAND_EXEC_NAME_{command_name}");
    let mut argv_prefix = None;
    let mut exec_name = None;
    for entry in env.state.envs.lock().unwrap().iter() {
        let Some((key, value)) = split_env_entry(entry) else {
            continue;
        };
        if key == argv_prefix_key {
            argv_prefix = Some(
                value
                    .split('\u{1f}')
                    .filter(|arg| !arg.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>(),
            );
        } else if key == exec_name_key {
            exec_name = Some(value.to_string());
        }
    }
    if argv_prefix.as_ref().is_none_or(Vec::is_empty) && exec_name.is_none() {
        return;
    }

    let mut args = env.state.args.lock().unwrap();
    if args.is_empty() {
        return;
    }
    if args
        .get(1)
        .is_some_and(|arg| matches!(arg.as_str(), "-cc1" | "-cc1as"))
    {
        return;
    }
    if let Some(prefix) = argv_prefix {
        args.splice(1..1, prefix);
    }
    if let Some(exec_name) = exec_name {
        args[0] = exec_name;
    }
}

fn split_env_entry(entry: &[u8]) -> Option<(&str, &str)> {
    let split = entry.iter().position(|byte| *byte == b'=')?;
    let key = std::str::from_utf8(&entry[..split]).ok()?;
    let value = std::str::from_utf8(&entry[split + 1..]).ok()?;
    Some((key, value))
}

fn sinomo_pinned_child_exec_only(env: &WasiEnv) -> bool {
    env.state.envs.lock().unwrap().iter().any(|entry| {
        split_env_entry(entry)
            .is_some_and(|(key, value)| key == SINOMO_WASIX_PINNED_CHILD_EXEC_ONLY && value == "1")
    })
}

fn sinomo_child_exec_path_is_pinned(path: &str) -> bool {
    let Some(path) = path.strip_prefix('/') else {
        return false;
    };
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return false;
                }
            }
            part => parts.push(part),
        }
    }
    matches!(parts.as_slice(), ["bin", _])
}

pub enum Executable {
    Wasm(OwnedBuffer),
    BinaryPackage(Arc<BinaryPackage>),
    Script {
        interpreter: String,
        interpreter_arg: Option<String>,
    },
}

async fn load_executable_from_filesystem(
    fs: &dyn FileSystem,
    path: &Path,
    rt: &(dyn Runtime + Send + Sync),
) -> Result<Executable, anyhow::Error> {
    let mut f = fs
        .new_open_options()
        .read(true)
        .open(path)
        .context("Unable to open the file")?;

    // Fast path if the file is fully available in memory.
    // Prevents redundant copying of the file data.
    if let Some(buf) = f.as_owned_buffer() {
        if wasmer_package::utils::is_container(buf.as_slice()) {
            let bytes = buf.clone().into_bytes();
            if let Ok(container) = from_bytes(bytes.clone()) {
                let pkg = BinaryPackage::from_webc(&container, rt)
                    .await
                    .context("Unable to load the package")?;

                return Ok(Executable::BinaryPackage(Arc::new(pkg)));
            }
        }

        if let Some(script) = parse_shebang(buf.as_slice()) {
            return Ok(script);
        }

        Ok(Executable::Wasm(buf))
    } else {
        let mut data = Vec::with_capacity(f.size() as usize);
        f.read_to_end(&mut data).await.context("Read failed")?;

        let bytes: bytes::Bytes = data.into();

        if let Ok(container) = from_bytes(bytes.clone()) {
            let pkg = BinaryPackage::from_webc(&container, rt)
                .await
                .context("Unable to load the package")?;

            Ok(Executable::BinaryPackage(Arc::new(pkg)))
        } else if let Some(script) = parse_shebang(&bytes) {
            Ok(script)
        } else {
            Ok(Executable::Wasm(OwnedBuffer::from_bytes(bytes)))
        }
    }
}

fn parse_shebang(bytes: &[u8]) -> Option<Executable> {
    let rest = bytes.strip_prefix(b"#!")?;
    let line_end = rest
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(rest.len());
    let line = std::str::from_utf8(&rest[..line_end]).ok()?.trim();
    if line.is_empty() {
        return None;
    }

    let mut parts = line.split_whitespace();
    let interpreter = parts.next()?.to_string();
    let interpreter_arg = parts.next().map(ToOwned::to_owned);
    Some(Executable::Script {
        interpreter,
        interpreter_arg,
    })
}
