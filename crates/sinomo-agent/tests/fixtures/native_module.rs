use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub fn build(package: &str) -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let output = Command::new(env!("CARGO"))
        .current_dir(manifest.parent().unwrap().parent().unwrap())
        .args([
            "build",
            "--locked",
            "--message-format=json-render-diagnostics",
            "-p",
            package,
        ])
        .stderr(Stdio::inherit())
        .output()
        .expect("build native test module");
    assert!(output.status.success(), "failed to build {package}");

    let name = package.replace('-', "_");
    for line in output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let message: serde_json::Value = serde_json::from_slice(line).expect("Cargo build message");
        if message["reason"] == "compiler-artifact" && message["target"]["name"] == name {
            for file in message["filenames"].as_array().expect("artifact filenames") {
                let file = file.as_str().expect("artifact path");
                if file.ends_with(std::env::consts::DLL_SUFFIX) {
                    return PathBuf::from(file);
                }
            }
        }
    }
    panic!("Cargo reported no dynamic library for {package}");
}
