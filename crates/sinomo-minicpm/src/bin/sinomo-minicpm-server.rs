use sinomo_minicpm::{MINICPM_DEFAULT_MODEL, MiniCpmDevice};
use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut bind = "127.0.0.1".to_string();
    let mut port = "32153".to_string();
    let mut model =
        env::var("SINOMO_MINICPM_MODEL").unwrap_or_else(|_| MINICPM_DEFAULT_MODEL.to_string());
    let mut device = env::var("SINOMO_MINICPM_DEVICE").unwrap_or_else(|_| "auto".to_string());
    let mut mode = env::var("SINOMO_MINICPM_SERVER_MODE").unwrap_or_else(|_| "vision".to_string());
    let mut mock = env::var("SINOMO_MINICPM_MOCK").as_deref() == Ok("1");

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
            "--mock" => mock = true,
            "--help" | "-h" => {
                println!(
                    "usage: sinomo-minicpm-server [--bind HOST] [--port PORT] [--model MODEL] [--device auto|cpu|cuda] [--mode text|vision|audio|duplex] [--mock]"
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
    let _device_marker = match device.as_str() {
        "cpu" => MiniCpmDevice::Cpu,
        "cuda" => MiniCpmDevice::Cuda,
        _ => MiniCpmDevice::Auto,
    };

    let python = env::var_os("SINOMO_MINICPM_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("python3"));
    let server_script = env::var_os("SINOMO_MINICPM_SERVER")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/minicpm_server.py")
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
    if mock {
        command.arg("--mock");
    }
    let status = command.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("MiniCPM server exited with status {status}").into())
    }
}
