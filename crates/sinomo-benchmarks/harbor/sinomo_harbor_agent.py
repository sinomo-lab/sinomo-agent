import json
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
