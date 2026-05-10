use sinomo_minicpm::{
    MINICPM_DEFAULT_MODEL, MiniCpmDevice, MiniCpmRequest, MiniCpmRuntimeConfig,
    PythonMiniCpmRuntime, default_bridge_script,
};
use std::env;
use std::io::{self, Read};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let request: MiniCpmRequest = serde_json::from_str(&input)?;
    let config = MiniCpmRuntimeConfig {
        python: env::var_os("SINOMO_MINICPM_PYTHON")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("python3")),
        bridge_script: env::var_os("SINOMO_MINICPM_BRIDGE")
            .map(PathBuf::from)
            .unwrap_or_else(default_bridge_script),
        model: env::var("SINOMO_MINICPM_MODEL")
            .unwrap_or_else(|_| MINICPM_DEFAULT_MODEL.to_string()),
        device: match env::var("SINOMO_MINICPM_DEVICE").as_deref() {
            Ok("cpu") => MiniCpmDevice::Cpu,
            Ok("cuda") => MiniCpmDevice::Cuda,
            _ => MiniCpmDevice::Auto,
        },
        timeout_seconds: env::var("SINOMO_MINICPM_TIMEOUT_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(300),
        mock: env::var("SINOMO_MINICPM_MOCK").as_deref() == Ok("1"),
    };
    let runtime = PythonMiniCpmRuntime::new(config);
    let response = runtime.complete(request)?;
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}
