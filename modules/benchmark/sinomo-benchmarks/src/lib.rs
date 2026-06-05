//! Optional native-dylib benchmark module for validating Sinomo's agent loop.
//!
//! These adapters deliberately live outside the production runtime. They can be
//! built as an opt-in native-dylib module for catalog/inspection purposes, and
//! they still expose explicit research harness scripts for Terminal-Bench/Harbor,
//! SWE-bench, MCU, and MineStudio.

use serde::{Deserialize, Serialize};
use sinomo_agent::{
    AgentToolKind, AgentToolSpec, default_coding_agent_tools, default_main_agent_tools,
    main_agent_system_prompt,
};
use sinomo_pytorch::{MINICPM_DEFAULT_MODEL, PytorchDevice, PytorchRuntimeConfig};
use sinomo_runtime::module_api::{
    NativeModuleBackend, NativeModuleCapabilitiesResponse, NativeModuleDescriptor,
    NativeModuleErrorResponse, NativeModuleInvocationRequest, NativeModuleInvocationResponse,
    NativeModuleRequest, NativeModuleResponse, SINOMO_MODULE_STATUS_INVALID_ARGUMENT,
    SINOMO_MODULE_STATUS_OK, SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    SINOMO_MODULE_STATUS_UNSUPPORTED_ABI, SINOMO_NATIVE_MODULE_ABI_VERSION, SinomoModuleByteBuffer,
    SinomoModuleByteSlice, SinomoModuleHostApiV1, SinomoModuleStatusCode, decode_sobj, encode_sobj,
};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use thiserror::Error;

pub const SINOMO_BENCHMARKS_MODULE_ID: &str = "module.benchmarks";
pub const SINOMO_BENCHMARKS_INTERFACE_VERSION: &str = "sinomo.benchmarks.v1";
pub const SINOMO_BENCHMARKS_OPERATION: &str = "benchmarks.catalog";
pub const SINOMO_BENCHMARKS_MODULE_ROOT: &str = "modules/benchmark/sinomo-benchmarks";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BenchmarkHarnessPlan {
    pub name: String,
    pub setup_commands: Vec<String>,
    pub run_command: String,
    pub required_tools: Vec<AgentToolKind>,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum BenchmarkFamily {
    Coding,
    Terminal,
    VisionAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BenchmarkSuiteDescriptor {
    pub name: String,
    pub family: BenchmarkFamily,
    pub task_count: usize,
    pub dataset: Option<String>,
    pub split: Option<String>,
    pub runner: String,
    pub baseline: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SweBenchSuite {
    pub name: String,
    pub dataset: String,
    pub split: String,
    pub selection: String,
    pub instances: Vec<SweBenchInstance>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SweBenchInstance {
    pub instance_id: String,
    pub repo: String,
    pub base_commit: String,
    pub difficulty: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum BenchmarkModuleRequest {
    Catalog,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum BenchmarkModuleResponse {
    Catalog(BenchmarkModuleCatalog),
    Error(BenchmarkModuleError),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BenchmarkModuleCatalog {
    pub module_root: String,
    pub suites: Vec<BenchmarkSuiteDescriptor>,
    pub plans: Vec<BenchmarkHarnessPlan>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BenchmarkModuleError {
    pub code: String,
    pub detail: String,
}

pub fn benchmark_module_root() -> &'static str {
    SINOMO_BENCHMARKS_MODULE_ROOT
}

pub fn benchmark_module_path(relative_path: &str) -> String {
    format!(
        "{}/{}",
        SINOMO_BENCHMARKS_MODULE_ROOT,
        relative_path.trim_start_matches('/')
    )
}

pub fn benchmark_module_catalog() -> BenchmarkModuleCatalog {
    BenchmarkModuleCatalog {
        module_root: SINOMO_BENCHMARKS_MODULE_ROOT.to_string(),
        suites: vec![swebench_verified_10_descriptor()],
        plans: vec![
            swebench_verified_10_plan(),
            terminalbench_harbor_plan(),
            mcu_minestudio_plan(),
            minestudio_plan(),
            minestudio_vision_action_loop_plan(),
            minestudio_live_duplex_loop_plan(),
        ],
    }
}

pub fn benchmarks_module_descriptor() -> NativeModuleDescriptor {
    NativeModuleDescriptor {
        module_id: SINOMO_BENCHMARKS_MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: SINOMO_BENCHMARKS_INTERFACE_VERSION.to_string(),
        platform_triple: platform_triple(),
        supported_operations: vec![SINOMO_BENCHMARKS_OPERATION.to_string()],
        capabilities: vec![
            "benchmark.catalog.read".to_string(),
            "benchmark.swebench.plan".to_string(),
            "benchmark.minestudio.plan".to_string(),
            "benchmark.terminalbench.plan".to_string(),
        ],
    }
}

pub fn swebench_verified_10_suite() -> SweBenchSuite {
    serde_json::from_str(include_str!("../swebench/verified_10.json"))
        .expect("checked-in SWE-bench Verified suite is valid JSON")
}

pub fn swebench_verified_10_descriptor() -> BenchmarkSuiteDescriptor {
    let suite = swebench_verified_10_suite();
    BenchmarkSuiteDescriptor {
        name: suite.name,
        family: BenchmarkFamily::Coding,
        task_count: suite.instances.len(),
        dataset: Some(suite.dataset),
        split: Some(suite.split),
        runner: benchmark_module_path("swebench/run_swebench_verified.py"),
        baseline: Some("mini-extra swebench".to_string()),
    }
}

pub fn terminalbench_harbor_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "terminalbench-harbor".to_string(),
        setup_commands: vec![
            "python -m pip install harbor".to_string(),
            "harbor run --help".to_string(),
        ],
        run_command: "harbor run -d terminal-bench/terminal-bench-2 --agent-import-path sinomo_harbor_agent:SinomoHarborAgent"
            .to_string(),
        required_tools: default_main_agent_tools()
            .into_iter()
            .filter(|tool| {
                matches!(
                    tool.kind,
                    AgentToolKind::RunShellCommand
                        | AgentToolKind::StartAsyncTask
                        | AgentToolKind::ReadSifs
                        | AgentToolKind::WriteSifs
                )
            })
            .map(|tool| tool.kind)
            .collect(),
        notes: vec![
            "Harbor custom agents implement BaseAgent or BaseInstalledAgent; current Terminal-Bench 2 dataset id is terminal-bench/terminal-bench-2.".to_string(),
            "The generated adapter uses an external agent so Sinomo can keep tools outside the task container."
                .to_string(),
        ],
    }
}

pub fn swebench_verified_10_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "swebench-verified-10".to_string(),
        setup_commands: vec![
            "python3 -m pip install mini-swe-agent swebench".to_string(),
            "cargo build -p sinomo-benchmarks --bin sinomo-swebench-agent".to_string(),
            "sinomo --local providers test deepseek".to_string(),
        ],
        run_command: format!(
            "python3 {} --mode both --provider deepseek --model deepseek-v4-flash --baseline-model deepseek/deepseek-v4-flash --evaluate --summary-out benchmark-results/swebench/summary.json --progress-out benchmark-results/swebench/progress.jsonl",
            benchmark_module_path("swebench/run_swebench_verified.py")
        ),
        required_tools: default_coding_agent_tools()
            .into_iter()
            .filter(|tool| {
                matches!(
                    tool.kind,
                    AgentToolKind::ReadFile
                        | AgentToolKind::SearchFiles
                        | AgentToolKind::ApplyPatch
                        | AgentToolKind::RunShellCommand
                        | AgentToolKind::StartCommandSession
                        | AgentToolKind::InspectCommandSession
                        | AgentToolKind::WriteCommandStdin
                        | AgentToolKind::CancelCommandSession
                        | AgentToolKind::RequestUserInput
                )
            })
            .map(|tool| tool.kind)
            .collect(),
        notes: vec![
            "The Sinomo path runs the Rust coding Agent with local workspace tools and writes SWE-bench prediction JSONL.".to_string(),
            "The baseline path delegates to mini-SWE-agent with a regex filter over the same 10 Verified instance ids.".to_string(),
            "Official scoring remains the SWE-bench harness over princeton-nlp/SWE-bench_Verified.".to_string(),
        ],
    }
}

pub fn minestudio_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "minestudio-smoke".to_string(),
        setup_commands: vec![
            "python -m pip install minestudio".to_string(),
            "python -m minestudio.simulator.entry".to_string(),
        ],
        run_command: "python minestudio/sinomo_minestudio_smoke.py".to_string(),
        required_tools: vec![AgentToolKind::ObserveVision, AgentToolKind::RunShellCommand],
        notes: vec![
            "MineStudio requires JDK 8 plus Xvfb or VirtualGL for rendering.".to_string(),
            "The smoke path captures one observation frame and sends it through the MiniCPM vision mode."
                .to_string(),
            "The Docker runtime under minestudio/docker starts a real MinecraftSim behind an HTTP reset/observe/step wrapper."
                .to_string(),
        ],
    }
}

pub fn mcu_minestudio_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "mcu-minestudio".to_string(),
        setup_commands: vec![
            "git clone --depth 1 https://github.com/CraftJarvis/MCU.git /tmp/mcu".to_string(),
            format!(
                "docker compose -f {} up -d --build minestudio",
                benchmark_module_path("minestudio/docker/compose.yml")
            ),
            format!(
                "python {} --help",
                benchmark_module_path("mcu/mcu_live_duplex_runner.py")
            ),
        ],
        run_command: format!(
            "MCU_ROOT=/tmp/mcu SINOMO_MINESTUDIO_ENV_URL=http://127.0.0.1:18080 SINOMO_PYTORCH_ENDPOINT=http://127.0.0.1:32153 python {}",
            benchmark_module_path("mcu/mcu_live_duplex_runner.py")
        ),
        required_tools: vec![AgentToolKind::ObserveVision, AgentToolKind::RunShellCommand],
        notes: vec![
            "MCU-turbo task configs are consumed from MCU_benchmark/task_configs and executed through the MineStudio HTTP wrapper.".to_string(),
            "The full-duplex runner sends frame sequences to MiniCPM-o with stream=true and records MineStudio RecordCallback MP4s for every task.".to_string(),
        ],
    }
}

pub fn mcu_game_tool_specs() -> Vec<AgentToolSpec> {
    vec![
        AgentToolSpec::new(
            AgentToolKind::ObserveVision,
            "observe_vision",
            "Attach the current Minecraft camera frame for multimodal reasoning.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::RunShellCommand,
            "inspect_game_state",
            "Read structured Minecraft state provided by the benchmark harness: task text, inventory, nearby blocks, entities, health, hunger, and position.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::RunShellCommand,
            "send_game_action",
            "Issue one bounded Minecraft action macro through the benchmark harness: look, move, jump, attack, mine, place, craft, use, select_hotbar, or wait.",
            true,
        ),
    ]
}

pub fn minestudio_vision_action_loop_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "minestudio-vision-action-loop".to_string(),
        setup_commands: vec![
            "python -m pip install minestudio".to_string(),
            format!(
                "docker compose -f {} build",
                benchmark_module_path("minestudio/docker/compose.yml")
            ),
            format!(
                "python {} --help",
                benchmark_module_path("minestudio/minestudio_vision_action_loop.py")
            ),
        ],
        run_command: format!(
            "SINOMO_PYTORCH_ENDPOINT=http://127.0.0.1:32153 python {}",
            benchmark_module_path("minestudio/minestudio_vision_action_loop.py")
        ),
        required_tools: vec![AgentToolKind::ObserveVision],
        notes: vec![
            "The loop starts a real MineStudio MinecraftSim environment and sends task text plus rendered pov frames to MiniCPM-o.".to_string(),
            "It does not include structured game-state text such as inventory, position, nearby blocks, health, reward, or simulator info dictionaries.".to_string(),
            "Recovered action macros are mapped through the benchmark-local MineStudio action bridge and recorded as replay trajectory artifacts.".to_string(),
        ],
    }
}

pub fn minestudio_live_duplex_loop_plan() -> BenchmarkHarnessPlan {
    BenchmarkHarnessPlan {
        name: "minestudio-live-duplex-loop".to_string(),
        setup_commands: vec![
            format!(
                "docker compose -f {} up -d minestudio",
                benchmark_module_path("minestudio/docker/compose.yml")
            ),
            format!(
                "python {} --help",
                benchmark_module_path("minestudio/minestudio_live_duplex_loop.py")
            ),
        ],
        run_command: format!(
            "SINOMO_MINESTUDIO_ENV_URL=http://127.0.0.1:18080 SINOMO_PYTORCH_ENDPOINT=http://127.0.0.1:32153 python {}",
            benchmark_module_path("minestudio/minestudio_live_duplex_loop.py")
        ),
        required_tools: vec![AgentToolKind::ObserveVision],
        notes: vec![
            "The loop sends chronological MineStudio rendered frame sequences to MiniCPM-o with stream=true and consumes streaming response chunks.".to_string(),
            "It applies the first recovered action macro back to the paused MineStudio environment, so wall-clock speed does not affect benchmark correctness.".to_string(),
            "Each run asks MineStudio's RecordCallback to write high-frame-rate episode MP4s next to the JSON and observation PNG artifacts.".to_string(),
        ],
    }
}

pub fn harbor_external_agent_python() -> &'static str {
    r#"import json
import os
import shlex
import subprocess
from pathlib import Path

from harbor.agents.base import BaseAgent
from harbor.environments.base import BaseEnvironment
from harbor.models.agent.context import AgentContext


def run_sinomo_agent(prompt: str) -> str:
    default_provider = "minicpm-o" if os.environ.get("SINOMO_PYTORCH_ENDPOINT", os.environ.get("SINOMO_MINICPM_ENDPOINT")) else "codex"
    default_model = (
        "openbmb/MiniCPM-o-4_5"
        if default_provider == "minicpm-o"
        else "gpt-5.2-codex"
    )
    command = [
        os.environ.get("SINOMO_BIN", "sinomo"),
        "agent",
        "run",
        "--provider",
        os.environ.get("SINOMO_HARBOR_PROVIDER", default_provider),
        "--model",
        os.environ.get("SINOMO_HARBOR_MODEL", default_model),
        "--prompt",
        prompt,
    ]
    proc = subprocess.run(command, text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        return (
            "Sinomo host agent failed; fall back to direct shell inspection. "
            f"stderr={proc.stderr.strip()}"
        )
    return proc.stdout.strip()


def exec_output_text(result, field: str) -> str:
    value = getattr(result, field, None)
    return value if value is not None else ""


class SinomoHarborAgent(BaseAgent):
    @staticmethod
    def name() -> str:
        return "sinomo"

    def version(self) -> str | None:
        return "0.1.0"

    async def setup(self, environment: BaseEnvironment) -> None:
        host = subprocess.run(
            [os.environ.get("SINOMO_BIN", "sinomo"), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        if host.returncode != 0:
            raise RuntimeError(f"sinomo host binary is unavailable: {host.stderr}")
        await environment.exec("pwd && ls -la")

    async def run(
        self,
        instruction: str,
        environment: BaseEnvironment,
        context: AgentContext,
    ) -> None:
        first_probe = await environment.exec("pwd && find . -maxdepth 2 -type f | sort | head -80")
        first_probe_stdout = exec_output_text(first_probe, "stdout")
        prompt = json.dumps(
            {
                "benchmark": "terminalbench-harbor",
                "instruction": instruction,
                "environment_probe": first_probe_stdout,
                "available_action": "Return a concise shell plan. The adapter will execute one bounded shell command if it is safe.",
            },
            indent=2,
        )
        decision = run_sinomo_agent(prompt)
        safe_command = "true"
        if "pytest" in decision:
            safe_command = "pytest -q"
        elif "test.sh" in first_probe_stdout:
            safe_command = "bash test.sh"
        elif "ls" in decision:
            safe_command = "ls -la"
        result = await environment.exec(safe_command)
        stdout = exec_output_text(result, "stdout")
        stderr = exec_output_text(result, "stderr")
        output = (
            "sinomo_decision:\n"
            + decision
            + "\n\nexecuted:\n"
            + shlex.quote(safe_command)
            + "\n\nstdout:\n"
            + stdout
            + "\nstderr:\n"
            + stderr
        )
        logs_dir = Path(self.logs_dir)
        logs_dir.mkdir(parents=True, exist_ok=True)
        output_path = logs_dir / "sinomo-output.txt"
        output_path.write_text(output)
        context.metadata = {
            **(context.metadata or {}),
            "sinomo_output": str(output_path),
            "sinomo_command": safe_command,
        }
"#
}

pub fn minestudio_smoke_python() -> &'static str {
    r#"import base64
import json
import os
import subprocess
import tempfile
import urllib.request
from pathlib import Path

from PIL import Image


def main():
    frame = Image.new("RGB", (64, 64), color=(90, 160, 90))
    with tempfile.TemporaryDirectory() as tmp:
        image_path = Path(tmp) / "frame.png"
        frame.save(image_path)
        request = {
            "mode": "vision",
            "system_prompt": "Describe the Minecraft observation and choose the next high-level action.",
            "messages": [{"role": "user", "content": "What should the agent do next?"}],
            "image_path": str(image_path),
            "stream": False,
            "metadata": {"benchmark": "minestudio-smoke"},
        }
        endpoint = os.environ.get("SINOMO_PYTORCH_ENDPOINT", os.environ.get("SINOMO_MINICPM_ENDPOINT"))
        if endpoint:
            request.pop("image_path", None)
            request["image_name"] = image_path.name
            request["image_base64"] = base64.b64encode(image_path.read_bytes()).decode("ascii")
            http_request = urllib.request.Request(
                endpoint.rstrip("/") + "/complete",
                data=json.dumps(request).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(http_request, timeout=300) as response:
                print(response.read().decode("utf-8"))
            return 0
        proc = subprocess.run(
            ["sinomo-pytorch-bridge"],
            input=json.dumps(request),
            text=True,
            capture_output=True,
            check=False,
        )
        print(proc.stdout)
        return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
"#
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentEnvironmentConfig {
    pub pytorch: PytorchRuntimeConfig,
    pub prompt: String,
}

impl Default for AgentEnvironmentConfig {
    fn default() -> Self {
        Self {
            pytorch: PytorchRuntimeConfig {
                model: MINICPM_DEFAULT_MODEL.to_string(),
                device: PytorchDevice::Auto,
                ..PytorchRuntimeConfig::default()
            },
            prompt: main_agent_system_prompt(&default_main_agent_tools()),
        }
    }
}

#[derive(Debug, Error)]
pub enum BenchmarkError {
    #[error("benchmark environment `{name}` is disabled; rebuild with feature `env-bench`")]
    Disabled { name: String },
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

#[cfg(feature = "env-bench")]
pub fn docker_smoke_command(plan: &BenchmarkHarnessPlan) -> String {
    match plan.name.as_str() {
        "terminalbench-harbor" => "docker run --rm python:3.12 bash -lc 'pip install harbor && harbor run --help'".to_string(),
        "minestudio-smoke" => "docker run --rm python:3.10 bash -lc 'pip install minestudio && python - <<\\'PY\\'\nimport minestudio\nprint(\"minestudio import ok\")\nPY'".to_string(),
        "mcu-minestudio" => "docker run --rm python:3.10 bash -lc 'pip install minestudio && python - <<\\'PY\\'\nimport minestudio\nprint(\"mcu minestudio import ok\")\nPY'".to_string(),
        "minestudio-vision-action-loop" => "docker run --rm python:3.10 bash -lc 'pip install minestudio && python - <<\\'PY\\'\nfrom minestudio.simulator import MinecraftSim\nprint(\"real minestudio env import ok\", MinecraftSim)\nPY'".to_string(),
        "minestudio-live-duplex-loop" => format!(
            "python3 {} --help",
            benchmark_module_path("minestudio/minestudio_live_duplex_loop.py")
        ),
        _ => "docker --version".to_string(),
    }
}

#[cfg(not(feature = "env-bench"))]
pub fn docker_smoke_command(plan: &BenchmarkHarnessPlan) -> Result<String, BenchmarkError> {
    Err(BenchmarkError::Disabled {
        name: plan.name.clone(),
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_api_version() -> u32 {
    SINOMO_NATIVE_MODULE_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_descriptor(
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    write_buffer(out, encode_sobj(&benchmarks_module_descriptor()))
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV1) -> SinomoModuleStatusCode {
    if host.abi_version == SINOMO_NATIVE_MODULE_ABI_VERSION {
        SINOMO_MODULE_STATUS_OK
    } else {
        SINOMO_MODULE_STATUS_UNSUPPORTED_ABI
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let result = catch_unwind(AssertUnwindSafe(|| {
        invoke_unchecked(request_sobj, response_sobj)
    }));
    match result {
        Ok(status) => status,
        Err(_) => write_response(
            response_sobj,
            NativeModuleResponse::Error(native_error_response(
                "panic",
                "benchmark module panicked while handling invocation",
            )),
            SINOMO_MODULE_STATUS_RUNTIME_ERROR,
        ),
    }
}

fn invoke_unchecked(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let response = match decode_native_module_request(request_sobj) {
        Ok(NativeModuleRequest::ProbeCapabilities(_)) => {
            NativeModuleResponse::Capabilities(NativeModuleCapabilitiesResponse {
                descriptor: benchmarks_module_descriptor(),
            })
        }
        Ok(NativeModuleRequest::Invoke(request)) => invoke_benchmark_operation(request),
        Err(error) => NativeModuleResponse::Error(native_error_response(
            "invalid-request",
            &format!("decode benchmark module request: {error}"),
        )),
    };
    write_response(response_sobj, response, SINOMO_MODULE_STATUS_OK)
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
    SINOMO_MODULE_STATUS_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_free_buffer(buffer: SinomoModuleByteBuffer) {
    unsafe {
        drop(SinomoModuleByteBuffer::into_vec(buffer));
    }
}

fn invoke_benchmark_operation(request: NativeModuleInvocationRequest) -> NativeModuleResponse {
    if request.operation != SINOMO_BENCHMARKS_OPERATION {
        return NativeModuleResponse::Error(native_error_response(
            "unsupported-operation",
            &format!(
                "unsupported benchmark module operation `{}`",
                request.operation
            ),
        ));
    }
    let benchmark_request = match decode_sobj::<BenchmarkModuleRequest>(&request.payload_sobj) {
        Ok(request) => request,
        Err(error) => {
            return NativeModuleResponse::Error(native_error_response(
                "invalid-benchmark-request",
                &format!("decode benchmark catalog request: {error}"),
            ));
        }
    };
    let benchmark_response = match benchmark_request {
        BenchmarkModuleRequest::Catalog => {
            BenchmarkModuleResponse::Catalog(benchmark_module_catalog())
        }
    };
    match encode_sobj(&benchmark_response) {
        Ok(payload_sobj) => NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
            operation: SINOMO_BENCHMARKS_OPERATION.to_string(),
            payload_sobj,
        }),
        Err(error) => NativeModuleResponse::Error(native_error_response(
            "encode-benchmark-response",
            &format!("encode benchmark catalog response: {error}"),
        )),
    }
}

fn decode_native_module_request(
    request_sobj: SinomoModuleByteSlice,
) -> Result<NativeModuleRequest, sinomo_runtime::module_api::NativeModuleApiError> {
    let request_bytes = unsafe { request_sobj.as_slice() };
    decode_sobj(request_bytes)
}

fn native_error_response(code: &str, detail: &str) -> NativeModuleErrorResponse {
    NativeModuleErrorResponse {
        code: code.to_string(),
        detail: detail.to_string(),
    }
}

fn write_response(
    out: *mut SinomoModuleByteBuffer,
    response: NativeModuleResponse,
    success_status: SinomoModuleStatusCode,
) -> SinomoModuleStatusCode {
    match encode_sobj(&response) {
        Ok(bytes) => {
            let status = write_buffer(out, Ok(bytes));
            if status == SINOMO_MODULE_STATUS_OK {
                success_status
            } else {
                status
            }
        }
        Err(error) => write_buffer(
            out,
            encode_sobj(&NativeModuleResponse::Error(native_error_response(
                "encode-native-response",
                &format!("encode native benchmark module response: {error}"),
            ))),
        ),
    }
}

fn write_buffer(
    out: *mut SinomoModuleByteBuffer,
    bytes: Result<Vec<u8>, sinomo_runtime::module_api::NativeModuleApiError>,
) -> SinomoModuleStatusCode {
    match bytes {
        Ok(bytes) => {
            if out.is_null() {
                SINOMO_MODULE_STATUS_INVALID_ARGUMENT
            } else {
                unsafe {
                    *out = SinomoModuleByteBuffer::from_vec(bytes);
                }
                SINOMO_MODULE_STATUS_OK
            }
        }
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

fn platform_triple() -> String {
    format!(
        "{}-{}-{}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::env::consts::FAMILY
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn terminalbench_plan_uses_harbor_custom_agent_shape() {
        let plan = terminalbench_harbor_plan();
        assert!(plan.run_command.contains("--agent-import-path"));
        assert!(harbor_external_agent_python().contains("BaseAgent"));
        assert!(harbor_external_agent_python().contains("run_sinomo_agent"));
        assert!(harbor_external_agent_python().contains("environment.exec"));
        assert!(
            plan.required_tools
                .contains(&AgentToolKind::RunShellCommand)
        );
    }

    #[test]
    fn swebench_verified_suite_is_small_diverse_and_stable() {
        let suite = swebench_verified_10_suite();
        assert_eq!(suite.dataset, "princeton-nlp/SWE-bench_Verified");
        assert_eq!(suite.split, "test");
        assert_eq!(suite.instances.len(), 10);
        let instance_ids: BTreeSet<_> = suite
            .instances
            .iter()
            .map(|instance| instance.instance_id.as_str())
            .collect();
        let repos: BTreeSet<_> = suite
            .instances
            .iter()
            .map(|instance| instance.repo.as_str())
            .collect();
        let mut difficulties = BTreeMap::new();
        for instance in &suite.instances {
            *difficulties
                .entry(instance.difficulty.as_str())
                .or_insert(0) += 1;
        }
        assert_eq!(instance_ids.len(), 10);
        assert_eq!(repos.len(), 10);
        assert_eq!(difficulties.get("<15 min fix"), Some(&4));
        assert_eq!(difficulties.get("15 min - 1 hour"), Some(&5));
        assert_eq!(difficulties.get("1-4 hours"), Some(&1));
        assert!(instance_ids.contains("django__django-14752"));
        assert!(instance_ids.contains("sympy__sympy-19495"));
        assert!(instance_ids.contains("sphinx-doc__sphinx-7462"));
        assert!(instance_ids.contains("pylint-dev__pylint-8898"));
    }

    #[test]
    fn swebench_plan_wires_sinomo_and_mini_swe_agent() {
        let descriptor = swebench_verified_10_descriptor();
        assert_eq!(descriptor.family, BenchmarkFamily::Coding);
        assert_eq!(descriptor.task_count, 10);
        assert_eq!(descriptor.baseline.as_deref(), Some("mini-extra swebench"));
        let plan = swebench_verified_10_plan();
        assert!(plan.run_command.contains("run_swebench_verified.py"));
        assert!(plan.run_command.contains("--mode both"));
        assert!(plan.run_command.contains("deepseek-v4-flash"));
        assert!(plan.run_command.contains("--progress-out"));
        assert!(plan.required_tools.contains(&AgentToolKind::ReadFile));
        assert!(plan.required_tools.contains(&AgentToolKind::ApplyPatch));
        assert!(
            plan.required_tools
                .contains(&AgentToolKind::RunShellCommand)
        );
        assert!(include_str!("../swebench/run_swebench_verified.py").contains("mini-extra"));
        assert!(
            include_str!("../swebench/run_swebench_verified.py")
                .contains("swebench.harness.run_evaluation")
        );
        assert!(
            include_str!("../swebench/run_swebench_verified.py")
                .contains("official_evaluation_prerequisite_issues")
        );
        assert!(
            include_str!("../swebench/run_swebench_verified.py").contains("evaluation-blocked")
        );
    }

    #[test]
    fn swebench_runner_rebuilds_default_agent_and_records_binary_provenance() {
        let runner = include_str!("../swebench/run_swebench_verified.py");
        assert!(runner.contains("DEFAULT_AGENT_BIN"));
        assert!(runner.contains("sinomo_agent_bin_explicit"));
        assert!(runner.contains("rebuilt-default-before-run"));
        assert!(runner.contains("explicit-binary-no-build"));
        assert!(runner.contains("agent_binary"));
        assert!(runner.contains("sha256"));
        assert!(runner.contains("\"cargo\", \"build\""));
    }

    #[test]
    fn minestudio_plan_keeps_vision_path_explicit() {
        let plan = minestudio_plan();
        assert!(plan.required_tools.contains(&AgentToolKind::ObserveVision));
        assert!(minestudio_smoke_python().contains("\"mode\": \"vision\""));
    }

    #[test]
    fn mcu_plan_uses_game_environment_tools() {
        let plan = mcu_minestudio_plan();
        assert!(plan.run_command.contains("mcu_live_duplex_runner.py"));
        assert!(plan.run_command.contains("SINOMO_MINESTUDIO_ENV_URL"));
        assert!(plan.required_tools.contains(&AgentToolKind::ObserveVision));
        let tools = mcu_game_tool_specs();
        assert!(tools.iter().any(|tool| tool.name == "inspect_game_state"));
        assert!(tools.iter().any(|tool| tool.name == "send_game_action"));
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("MCU_benchmark/task_configs"))
        );
        assert!(include_str!("../mcu/mcu_live_duplex_runner.py").contains("call_minicpm_stream"));
    }

    #[test]
    fn minestudio_vision_loop_is_vision_only() {
        let plan = minestudio_vision_action_loop_plan();
        assert!(
            plan.run_command
                .contains("minestudio_vision_action_loop.py")
        );
        assert!(
            plan.setup_commands
                .iter()
                .any(|command| command.contains("minestudio"))
        );
        assert!(
            plan.setup_commands
                .iter()
                .any(|command| command.contains("compose.yml build"))
        );
        assert!(plan.required_tools.contains(&AgentToolKind::ObserveVision));
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("real MineStudio MinecraftSim"))
        );
        assert!(
            include_str!("../minestudio/minestudio_vision_action_loop.py").contains("MinecraftSim")
        );
        assert!(
            !include_str!("../minestudio/minestudio_vision_action_loop.py").contains("ImageDraw")
        );
    }

    #[test]
    fn minestudio_live_duplex_loop_uses_streaming_frames() {
        let plan = minestudio_live_duplex_loop_plan();
        assert!(plan.run_command.contains("SINOMO_MINESTUDIO_ENV_URL"));
        assert!(plan.run_command.contains("minestudio_live_duplex_loop.py"));
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("RecordCallback"))
        );
        assert!(
            include_str!("../minestudio/minestudio_live_duplex_loop.py")
                .contains("\"image_sequence_base64\"")
        );
        assert!(
            include_str!("../minestudio/minestudio_live_duplex_loop.py")
                .contains("\"stream\": True")
        );
        assert!(
            include_str!("../minestudio/minestudio_live_duplex_loop.py")
                .contains("collect_minestudio_recordings")
        );
    }

    #[test]
    fn default_environment_prompt_mentions_async_and_tools() {
        let config = AgentEnvironmentConfig::default();
        assert!(config.prompt.contains("async"));
        assert!(config.prompt.contains("run_shell_command"));
    }

    #[test]
    fn benchmark_module_descriptor_exports_catalog_operation() {
        let descriptor = benchmarks_module_descriptor();

        assert_eq!(descriptor.module_id, SINOMO_BENCHMARKS_MODULE_ID);
        assert_eq!(
            descriptor.interface_version,
            SINOMO_BENCHMARKS_INTERFACE_VERSION
        );
        assert_eq!(descriptor.backend, NativeModuleBackend::NativeDylib);
        assert!(
            descriptor
                .supported_operations
                .contains(&SINOMO_BENCHMARKS_OPERATION.to_string())
        );
        assert!(
            descriptor
                .capabilities
                .contains(&"benchmark.catalog.read".to_string())
        );
    }

    #[test]
    fn benchmark_module_catalog_lists_research_harnesses() {
        let catalog = benchmark_module_catalog();

        assert_eq!(catalog.module_root, SINOMO_BENCHMARKS_MODULE_ROOT);
        assert_eq!(catalog.suites.len(), 1);
        assert_eq!(catalog.suites[0].name, "swebench-verified-10");
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "swebench-verified-10")
        );
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "minestudio-live-duplex-loop")
        );
        assert!(
            catalog
                .plans
                .iter()
                .all(|plan| !plan.run_command.contains("crates/sinomo-benchmarks"))
        );
    }

    #[test]
    fn benchmark_module_native_abi_returns_catalog() {
        let payload_sobj = encode_sobj(&BenchmarkModuleRequest::Catalog).expect("payload");
        let request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
            operation: SINOMO_BENCHMARKS_OPERATION.to_string(),
            payload_sobj,
        });
        let request_sobj = encode_sobj(&request).expect("request");
        let mut response_buffer = SinomoModuleByteBuffer::null();

        let status = sinomo_module_invoke(
            SinomoModuleByteSlice::from_slice(&request_sobj),
            &mut response_buffer,
        );

        assert_eq!(status, SINOMO_MODULE_STATUS_OK);
        let response_bytes = unsafe { SinomoModuleByteBuffer::into_vec(response_buffer) };
        let native_response: NativeModuleResponse =
            decode_sobj(&response_bytes).expect("native response");
        let NativeModuleResponse::Invocation(response) = native_response else {
            panic!("expected invocation response");
        };
        assert_eq!(response.operation, SINOMO_BENCHMARKS_OPERATION);
        let benchmark_response: BenchmarkModuleResponse =
            decode_sobj(&response.payload_sobj).expect("benchmark response");
        let BenchmarkModuleResponse::Catalog(catalog) = benchmark_response else {
            panic!("expected catalog response");
        };
        assert!(
            catalog
                .plans
                .iter()
                .any(|plan| plan.name == "swebench-verified-10")
        );
    }
}
