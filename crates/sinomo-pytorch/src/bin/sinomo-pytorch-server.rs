use sinomo_pytorch::{MINICPM_DEFAULT_MODEL, PytorchDevice};
use std::env;
use std::path::PathBuf;
use std::process::Command;

fn env_os_any(primary: &str, fallback: &str) -> Option<std::ffi::OsString> {
    env::var_os(primary).or_else(|| env::var_os(fallback))
}

fn env_any(primary: &str, fallback: &str) -> Option<String> {
    env::var(primary).ok().or_else(|| env::var(fallback).ok())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut bind = "127.0.0.1".to_string();
    let mut port = "32153".to_string();
    let mut model = env_any("SINOMO_PYTORCH_MODEL", "SINOMO_MINICPM_MODEL")
        .unwrap_or_else(|| MINICPM_DEFAULT_MODEL.to_string());
    let mut device = env_any("SINOMO_PYTORCH_DEVICE", "SINOMO_MINICPM_DEVICE")
        .unwrap_or_else(|| "auto".to_string());
    let mut mode = env_any("SINOMO_PYTORCH_SERVER_MODE", "SINOMO_MINICPM_SERVER_MODE")
        .unwrap_or_else(|| "vision".to_string());
    let mut auth_token: Option<String> = None;
    let mut auth_token_file: Option<String> = None;
    let mut mock = env_any("SINOMO_PYTORCH_MOCK", "SINOMO_MINICPM_MOCK").as_deref() == Some("1");

    let args: Vec<String> = env::args().skip(1).collect();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--bind" => {
                index += 1;
                bind = args.get(index).ok_or("--bind requires a value")?.clone();
            }
            "--port" => {
                index += 1;
                port = args.get(index).ok_or("--port requires a value")?.clone();
            }
            "--model" => {
                index += 1;
                model = args.get(index).ok_or("--model requires a value")?.clone();
            }
            "--device" => {
                index += 1;
                device = args.get(index).ok_or("--device requires a value")?.clone();
            }
            "--mode" => {
                index += 1;
                mode = args.get(index).ok_or("--mode requires a value")?.clone();
            }
            "--auth-token" => {
                index += 1;
                auth_token = Some(
                    args.get(index)
                        .ok_or("--auth-token requires a value")?
                        .clone(),
                );
            }
            "--auth-token-file" => {
                index += 1;
                auth_token_file = Some(
                    args.get(index)
                        .ok_or("--auth-token-file requires a value")?
                        .clone(),
                );
            }
            "--mock" => mock = true,
            "--help" | "-h" => {
                println!(
                    "usage: sinomo-pytorch-server [--bind 127.0.0.1|localhost|::1] [--port PORT] [--model MODEL] [--device auto|cpu|cuda] [--mode text|vision|audio|duplex] [--auth-token TOKEN|--auth-token-file PATH] [--mock]"
                );
                return Ok(());
            }
            value => return Err(format!("unexpected argument `{value}`").into()),
        }
        index += 1;
    }
    match device.as_str() {
        "auto" | "cpu" | "cuda" => {}
        value => return Err(format!("unsupported device `{value}`").into()),
    }
    match mode.as_str() {
        "text" | "vision" | "audio" | "duplex" => {}
        value => return Err(format!("unsupported mode `{value}`").into()),
    }
    if !matches!(bind.as_str(), "127.0.0.1" | "localhost" | "::1" | "[::1]") {
        return Err(
            "sinomo-pytorch-server only binds loopback addresses; cluster traffic must use sinomo-network"
                .into(),
        );
    }
    let _device_marker = match device.as_str() {
        "cpu" => PytorchDevice::Cpu,
        "cuda" => PytorchDevice::Cuda,
        _ => PytorchDevice::Auto,
    };

    let python = env_os_any("SINOMO_PYTORCH_PYTHON", "SINOMO_MINICPM_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("python3"));
    let server_script = env_os_any("SINOMO_PYTORCH_SERVER", "SINOMO_MINICPM_SERVER")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/pytorch_server.py")
        });

    let mut command = Command::new(python);
    command
        .arg(server_script)
        .arg("--bind")
        .arg(bind)
        .arg("--port")
        .arg(port)
        .arg("--model")
        .arg(model)
        .arg("--device")
        .arg(device)
        .arg("--mode")
        .arg(mode);
    if let Some(auth_token) = auth_token {
        command.arg("--auth-token").arg(auth_token);
    }
    if let Some(auth_token_file) = auth_token_file {
        command.arg("--auth-token-file").arg(auth_token_file);
    }
    if mock {
        command.arg("--mock");
    }
    let status = command.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("PyTorch model server exited with status {status}").into())
    }
}
