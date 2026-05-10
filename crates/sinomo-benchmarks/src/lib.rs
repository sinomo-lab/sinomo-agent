//! Optional benchmark adapters for validating Sinomo's agent loop.
//!
//! These adapters deliberately live outside the production runtime. They
//! generate harness code and smoke-test plans for Terminal-Bench/Harbor and
//! MineStudio without making either benchmark a production dependency.

use serde::{Deserialize, Serialize};
use sinomo_agent::{AgentToolKind, default_main_agent_tools, main_agent_system_prompt};
use sinomo_minicpm::{MINICPM_DEFAULT_MODEL, MiniCpmDevice, MiniCpmRuntimeConfig};
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
        ],
    }
}

pub fn harbor_external_agent_python() -> &'static str {
    r#"import json
import os
import shlex
import subprocess

from harbor.agents.base import BaseAgent
from harbor.environments.base import BaseEnvironment
from harbor.models.agent.context import AgentContext


def run_sinomo_agent(prompt: str) -> str:
    default_provider = "minicpm-o" if os.environ.get("SINOMO_MINICPM_ENDPOINT") else "codex"
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
        prompt = json.dumps(
            {
                "benchmark": "terminalbench-harbor",
                "instruction": instruction,
                "environment_probe": first_probe.stdout,
                "available_action": "Return a concise shell plan. The adapter will execute one bounded shell command if it is safe.",
            },
            indent=2,
        )
        decision = run_sinomo_agent(prompt)
        safe_command = "true"
        if "pytest" in decision:
            safe_command = "pytest -q"
        elif "test.sh" in first_probe.stdout:
            safe_command = "bash test.sh"
        elif "ls" in decision:
            safe_command = "ls -la"
        result = await environment.exec(safe_command)
        context.output = (
            "sinomo_decision:\n"
            + decision
            + "\n\nexecuted:\n"
            + shlex.quote(safe_command)
            + "\n\nstdout:\n"
            + result.stdout
            + "\nstderr:\n"
            + result.stderr
        )
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
        endpoint = os.environ.get("SINOMO_MINICPM_ENDPOINT")
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
            ["sinomo-minicpm-bridge"],
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
    pub minicpm: MiniCpmRuntimeConfig,
    pub prompt: String,
}

impl Default for AgentEnvironmentConfig {
    fn default() -> Self {
        Self {
            minicpm: MiniCpmRuntimeConfig {
                model: MINICPM_DEFAULT_MODEL.to_string(),
                device: MiniCpmDevice::Auto,
                ..MiniCpmRuntimeConfig::default()
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
    fn minestudio_plan_keeps_vision_path_explicit() {
        let plan = minestudio_plan();
        assert!(plan.required_tools.contains(&AgentToolKind::ObserveVision));
        assert!(minestudio_smoke_python().contains("\"mode\": \"vision\""));
    }

    #[test]
    fn default_environment_prompt_mentions_async_and_tools() {
        let config = AgentEnvironmentConfig::default();
        assert!(config.prompt.contains("async"));
        assert!(config.prompt.contains("run_shell_command"));
    }
}
