# MCU MineStudio Runner

This folder contains Sinomo's lightweight MCU-turbo integration on top of MineStudio task semantics. It consumes the upstream MCU YAML task configs from `MCU_benchmark/task_configs`, sends a small task subset through the Sinomo main agent with MiniCPM-o, and writes per-task JSON plus case-study markdown under the repo-local gitignored `benchmark-results/` directory.

Typical usage:

```sh
git clone --depth 1 https://github.com/CraftJarvis/MCU.git /tmp/mcu
SINOMO_BIN=target/debug/sinomo \
SINOMO_PYTORCH_ENDPOINT=http://<minicpm-host>:32153 \
python3 crates/sinomo-benchmarks/mcu/sinomo_mcu_runner.py --mcu-root /tmp/mcu
```

The runner is intentionally benchmark-specific. Minecraft control affordances such as `inspect_game_state` and `send_game_action` are described and scored in this crate rather than added to the general Sinomo agent crate.

The generic event contracts for interactive state packets, action macros, and trajectory artifacts live in `sinomo-events`. MineStudio-specific conversion is implemented by `../minestudio/minestudio_action_bridge.py`, which maps recovered action macros into MineStudio/VPT-style action dictionaries and writes trajectory JSONL under `benchmark-results/_trajectories`.

For vision-only evaluation, use `../minestudio/minestudio_vision_action_loop.py`. That runner starts a real MineStudio `MinecraftSim`, sends rendered `pov` frames to MiniCPM-o instead of structured game-state text, applies recovered actions through `env.step`, then uses the same action bridge and trajectory recorder.

For full-duplex MCU-turbo evaluation, start the MineStudio Docker wrapper and run:

```sh
docker compose -f crates/sinomo-benchmarks/minestudio/docker/compose.yml up -d --build minestudio
MCU_ROOT=/tmp/mcu \
SINOMO_MINESTUDIO_ENV_URL=http://127.0.0.1:18080 \
SINOMO_PYTORCH_ENDPOINT=http://<minicpm-host>:32153 \
python3 crates/sinomo-benchmarks/mcu/mcu_live_duplex_runner.py
```

This enumerates every YAML task under `MCU_benchmark/task_configs`, resets a real MineStudio environment for each task with its `custom_init_commands`, sends chronological rendered frames to MiniCPM-o with `mode=duplex` and `stream=true`, applies the recovered action, and saves MineStudio `RecordCallback` MP4s under each task result directory. Each cycle writes `cycle-####-model-input.json` and `cycle-####-model-output.json` next to the PNG frames. The model input log records the instruction text, image sequence file paths, and a benchmark-only MineStudio inspection snapshot. The inspection snapshot comes from compact `obs`/`info` fields and optional `/data get ...` command probes through MineStudio's `env.execute_cmd`; it is for analysis and is not passed as structured state to the model.
