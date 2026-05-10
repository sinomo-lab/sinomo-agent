import json
import os
import subprocess
import tempfile
from pathlib import Path

from PIL import Image


def main() -> int:
    frame = Image.new("RGB", (96, 64), color=(92, 150, 84))
    with tempfile.TemporaryDirectory() as tmp:
        image_path = Path(tmp) / "synthetic-minecraft-frame.png"
        frame.save(image_path)
        request = {
            "mode": "vision",
            "system_prompt": (
                "Describe the Minecraft observation and choose one safe next "
                "high-level action."
            ),
            "messages": [
                {
                    "role": "user",
                    "content": "What should the Minecraft agent do next?",
                }
            ],
            "image_path": str(image_path),
            "stream": False,
            "metadata": {"benchmark": "minestudio-smoke"},
        }
        env = dict(os.environ)
        env.setdefault("SINOMO_MINICPM_MOCK", "1")
        proc = subprocess.run(
            ["sinomo-minicpm-bridge"],
            input=json.dumps(request),
            text=True,
            capture_output=True,
            check=False,
            env=env,
        )
        print(proc.stdout)
        if proc.stderr:
            print(proc.stderr)
        return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
