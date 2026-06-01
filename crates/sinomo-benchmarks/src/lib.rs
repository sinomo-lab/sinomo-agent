//! Optional benchmark adapters for validating Sinomo's agent loop.
//!
//! These adapters deliberately live outside the production runtime. They
//! generate harness code and smoke-test plans for Terminal-Bench/Harbor and
//! MineStudio without making either benchmark a production dependency.

use serde::{Deserialize, Serialize};
use sinomo_agent::{
    AgentToolKind, AgentToolSpec, default_coding_agent_tools, default_main_agent_tools,
    main_agent_system_prompt,
};
use sinomo_pytorch::{MINICPM_DEFAULT_MODEL, PytorchDevice, PytorchRuntimeConfig};
use std::path::PathBuf;
use thiserror::Error;

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

pub fn swebench_verified_10_suite() -> SweBenchSuite {
    serde_json::from_str(include_str!(
        "../../sinomo-benchmarks/swebench/verified_10.json"
    ))
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
        runner: "crates/sinomo-benchmarks/swebench/run_swebench_verified.py".to_string(),
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
        run_command: "python3 crates/sinomo-benchmarks/swebench/run_swebench_verified.py --mode both --provider deepseek --model deepseek-v4-flash --baseline-model deepseek/deepseek-v4-flash --evaluate --summary-out benchmark-results/swebench/summary.json"
            .to_string(),
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
            "docker compose -f crates/sinomo-benchmarks/minestudio/docker/compose.yml up -d --build minestudio".to_string(),
            "python crates/sinomo-benchmarks/mcu/mcu_live_duplex_runner.py --help".to_string(),
        ],
        run_command: "MCU_ROOT=/tmp/mcu SINOMO_MINESTUDIO_ENV_URL=http://127.0.0.1:18080 SINOMO_PYTORCH_ENDPOINT=http://127.0.0.1:32153 python crates/sinomo-benchmarks/mcu/mcu_live_duplex_runner.py"
            .to_string(),
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
            "docker compose -f crates/sinomo-benchmarks/minestudio/docker/compose.yml build".to_string(),
            "python crates/sinomo-benchmarks/minestudio/minestudio_vision_action_loop.py --help"
                .to_string(),
        ],
        run_command: "SINOMO_PYTORCH_ENDPOINT=http://127.0.0.1:32153 python crates/sinomo-benchmarks/minestudio/minestudio_vision_action_loop.py"
            .to_string(),
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
            "docker compose -f crates/sinomo-benchmarks/minestudio/docker/compose.yml up -d minestudio".to_string(),
            "python crates/sinomo-benchmarks/minestudio/minestudio_live_duplex_loop.py --help"
                .to_string(),
        ],
        run_command: "SINOMO_MINESTUDIO_ENV_URL=http://127.0.0.1:18080 SINOMO_PYTORCH_ENDPOINT=http://127.0.0.1:32153 python crates/sinomo-benchmarks/minestudio/minestudio_live_duplex_loop.py"
            .to_string(),
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
        "minestudio-live-duplex-loop" => "python3 crates/sinomo-benchmarks/minestudio/minestudio_live_duplex_loop.py --help".to_string(),
        _ => "docker --version".to_string(),
    }
}

#[cfg(not(feature = "env-bench"))]
pub fn docker_smoke_command(plan: &BenchmarkHarnessPlan) -> Result<String, BenchmarkError> {
    Err(BenchmarkError::Disabled {
        name: plan.name.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

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
        assert_eq!(instance_ids.len(), 10);
        assert_eq!(repos.len(), 10);
        assert!(instance_ids.contains("astropy__astropy-12907"));
        assert!(instance_ids.contains("django__django-10097"));
        assert!(instance_ids.contains("scikit-learn__scikit-learn-10297"));
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
        assert!(plan.required_tools.contains(&AgentToolKind::ReadFile));
        assert!(plan.required_tools.contains(&AgentToolKind::ApplyPatch));
        assert!(
            plan.required_tools
                .contains(&AgentToolKind::RunShellCommand)
        );
        assert!(
            include_str!("../../sinomo-benchmarks/swebench/run_swebench_verified.py")
                .contains("mini-extra")
        );
        assert!(
            include_str!("../../sinomo-benchmarks/swebench/run_swebench_verified.py")
                .contains("swebench.harness.run_evaluation")
        );
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
        assert!(
            include_str!("../../sinomo-benchmarks/mcu/mcu_live_duplex_runner.py")
                .contains("call_minicpm_stream")
        );
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
            include_str!("../../sinomo-benchmarks/minestudio/minestudio_vision_action_loop.py")
                .contains("MinecraftSim")
        );
        assert!(
            !include_str!("../../sinomo-benchmarks/minestudio/minestudio_vision_action_loop.py")
                .contains("ImageDraw")
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
            include_str!("../../sinomo-benchmarks/minestudio/minestudio_live_duplex_loop.py")
                .contains("\"image_sequence_base64\"")
        );
        assert!(
            include_str!("../../sinomo-benchmarks/minestudio/minestudio_live_duplex_loop.py")
                .contains("\"stream\": True")
        );
        assert!(
            include_str!("../../sinomo-benchmarks/minestudio/minestudio_live_duplex_loop.py")
                .contains("collect_minestudio_recordings")
        );
    }

    #[test]
    fn default_environment_prompt_mentions_async_and_tools() {
        let config = AgentEnvironmentConfig::default();
        assert!(config.prompt.contains("async"));
        assert!(config.prompt.contains("run_shell_command"));
    }
}
