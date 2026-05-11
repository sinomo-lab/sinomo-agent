use sinomo_pytorch::{
    MINICPM_DEFAULT_MODEL, PythonPytorchRuntime, PytorchDevice, PytorchRequest,
    PytorchRuntimeConfig, default_bridge_script,
};
use std::env;
use std::io::{self, Read};
use std::path::PathBuf;

fn env_os_any(primary: &str, fallback: &str) -> Option<std::ffi::OsString> {
    env::var_os(primary).or_else(|| env::var_os(fallback))
}

fn env_any(primary: &str, fallback: &str) -> Option<String> {
    env::var(primary).ok().or_else(|| env::var(fallback).ok())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let request: PytorchRequest = serde_json::from_str(&input)?;
    let config = PytorchRuntimeConfig {
        python: env_os_any("SINOMO_PYTORCH_PYTHON", "SINOMO_MINICPM_PYTHON")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("python3")),
        bridge_script: env_os_any("SINOMO_PYTORCH_BRIDGE", "SINOMO_MINICPM_BRIDGE")
            .map(PathBuf::from)
            .unwrap_or_else(default_bridge_script),
        model: env_any("SINOMO_PYTORCH_MODEL", "SINOMO_MINICPM_MODEL")
            .unwrap_or_else(|| MINICPM_DEFAULT_MODEL.to_string()),
        device: match env_any("SINOMO_PYTORCH_DEVICE", "SINOMO_MINICPM_DEVICE").as_deref() {
            Some("cpu") => PytorchDevice::Cpu,
            Some("cuda") => PytorchDevice::Cuda,
            _ => PytorchDevice::Auto,
        },
        timeout_seconds: env_any(
            "SINOMO_PYTORCH_TIMEOUT_SECONDS",
            "SINOMO_MINICPM_TIMEOUT_SECONDS",
        )
        .and_then(|value| value.parse().ok())
        .unwrap_or(300),
        mock: env_any("SINOMO_PYTORCH_MOCK", "SINOMO_MINICPM_MOCK").as_deref() == Some("1"),
    };
    let runtime = PythonPytorchRuntime::new(config);
    let response = runtime.complete(request)?;
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}
