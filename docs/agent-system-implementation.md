# Agent System Implementation Notes

This document tracks the implementation entry points for [agent-system-architecture.md](agent-system-architecture.md).

## PyTorch Model Bridge

The Rust contract lives in `crates/sinomo-pytorch`. It owns stable request and response structs, process invocation, status reporting, and a `LanguageModel` provider adapter. PyTorch and Transformers stay isolated in `crates/sinomo-pytorch/python/pytorch_bridge.py`.

The checked-in bridge supports:

- text requests;
- vision requests with an image path or base64 image payload;
- audio and duplex request modes in the stable schema;
- a probe mode for dependency diagnostics;
- a mock mode for CI and benchmark harness smoke tests without downloading the 9B model.

MiniCPM-o remains the first configured backend (`--provider minicpm-o`), but the bridge is intentionally named around the implementation boundary: any local PyTorch/Transformers model that can satisfy the request schema can be hosted behind `sinomo-pytorch-bridge` or `sinomo-pytorch-server`. Product runtime configuration is cluster-scoped and stored in cluster KV at `/config/lm/runtime`; use `sinomo models runtime set` instead of environment variables for model paths, device choices, and token budgets.

The local model path follows the MiniCPM-o 4.5 PyTorch reference path. Current upstream docs recommend a Linux NVIDIA GPU with more than 28 GB VRAM for the full PyTorch demo. CPU mode is useful for dependency and wiring checks, but it is not expected to be fast enough for interactive full-duplex operation.

MiniCPM-o 4.5 currently depends on trusted remote model code that uses the Transformers 4.53 cache API. Use `transformers==4.53.3`; newer 4.x builds can load the checkpoint but fail at generation with `DynamicCache.seen_tokens` compatibility errors, and Transformers 5.x fails earlier in model loading. The Windows CUDA smoke also required `minicpmo`, `onnx`, `onnxruntime`, `hyperpyyaml`, and `diffusers` in addition to the base `torch`, `PIL`, and `accelerate` dependencies.

Local probe status from this implementation pass:

- an isolated Python 3.11 venv at `/tmp/sinomo-pytorch-venv` can import `torch`, `transformers`, `PIL`, and `accelerate`;
- `sinomo-pytorch-bridge --python /tmp/sinomo-pytorch-venv/bin/python` reports MiniCPM dependencies are importable;
- `torch.cuda.is_available()` is false on the local host, so the full 9B PyTorch model was not loaded here.
- a Windows node with an RTX 3090 was able to load `openbmb/MiniCPM-o-4_5` through the long-running managed PyTorch server with CUDA after pinning `transformers==4.53.3`; text and vision smoke requests completed through the local loopback runtime.

The PyTorch HTTP server is a private implementation detail of the Sinomo node that spawned it. The Rust supervisor binds it to loopback only, adds a per-process bearer token, writes stdout and stderr logs under `.sinomo/logs/pytorch`, observes readiness through `/health`, and restarts it when the process exits or stops answering. It must not be exposed on a LAN or used as a cross-node model endpoint.

Cluster model traffic must go through `sinomo-network`. A configured `pytorch_bridge.endpoint` is accepted only when it is a loopback endpoint for the same node. Clear older LAN endpoints with:

```sh
sinomo models runtime set --pytorch-endpoint ""
```

## Main Agent Loop

`crates/sinomo-agent` now exposes:

- `main_agent_system_prompt`;
- `maid_agent_system_prompt`;
- `default_main_agent_tools`;
- `AsyncTaskUpdate`;
- `AgentLoopInput`;
- `AgentLoopDecision`;
- `MainAgentLoop`.

The loop asks the model for a JSON decision with user-visible text, optional tool calls, optional maid handoff, and the next interaction state. Quiet async updates can be consumed without calling the model.

## CLI Smoke Entry

`sinomo agent run --prompt <text>` runs one main-agent decision with the configured provider. It defaults to the stored Codex provider so development can use existing local credentials.

Examples:

```sh
sinomo agent run --prompt "Check cluster health later and tell me if anything is wrong."
sinomo agent run --provider codex --model gpt-5.2-codex --prompt "Summarize the current repo status."
```

## Benchmark Harnesses

`crates/sinomo-benchmarks` is an application-level optional harness crate. It does not participate in production runtime dependencies.

Terminal-Bench 2.0 is represented through Harbor:

- current install smoke: `pip install harbor`;
- command smoke: `harbor run --help`;
- current dataset id from Harbor docs: `terminal-bench/terminal-bench-2`;
- adapter template: `crates/sinomo-benchmarks/harbor/sinomo_harbor_agent.py`.

The Harbor adapter is an external agent. It runs the Sinomo agent loop on the host side with stored Codex credentials and uses Harbor's `environment.exec` only for bounded task-container commands. This keeps Sinomo tools and credentials out of the benchmark container.

MineStudio is represented through a vision-path smoke:

- current install smoke: `pip install minestudio`;
- import smoke: `import minestudio`;
- synthetic request-path smoke: `crates/sinomo-benchmarks/minestudio/sinomo_minestudio_smoke.py`.

The smoke script has a local mock mode so it validates the vision request path without requiring the full MiniCPM-o model. Benchmark harnesses may still accept environment variables because they are local experiment launchers, but Sinomo runtime commands should read model runtime settings from cluster KV. The Docker runtime in `crates/sinomo-benchmarks/minestudio/docker/compose.yml` builds a headless Xvfb/Mesa/OpenJDK 8 MineStudio container with an HTTP wrapper for `reset`/`observe`/`step` control from the host. The real action-loop runner is `crates/sinomo-benchmarks/minestudio/minestudio_vision_action_loop.py`; it starts `minestudio.simulator.MinecraftSim`, reads rendered frames from `info["pov"]` or `obs["image"]`, asks MiniCPM-o for one action macro, applies the action with `env.step`, and repeats. It can also use the Docker wrapper by setting `SINOMO_MINESTUDIO_ENV_URL=http://127.0.0.1:18080`. `crates/sinomo-benchmarks/minestudio/minestudio_live_duplex_loop.py` uses the same wrapper for a live-style duplex workflow: it samples chronological frame sequences, sends them to MiniCPM-o as streaming duplex requests, consumes incremental output chunks, and applies the recovered action back to MineStudio.

MCU-turbo is represented through a MineStudio-compatible task-config runner:

- clone the benchmark configs from `https://github.com/CraftJarvis/MCU` into `/tmp/mcu` or set `MCU_ROOT`;
- configure the product runtime with `sinomo models runtime set --pytorch-endpoint <server-or-proxy-url>`, then run `crates/sinomo-benchmarks/mcu/sinomo_mcu_runner.py`;
- results are written under the gitignored `benchmark-results/` folder as per-task JSON, `summary.json`, and `case_studies.md`.

The MCU runner uses the benchmark YAML files from `MCU_benchmark/task_configs` and drives Sinomo's main-agent loop with Minecraft task text, init commands, a MineStudio-style observation summary, and an expected `send_game_action` macro. This is a lightweight integration benchmark for the Sinomo agent interface; full MCU scoring still requires running MineStudio simulator episodes and VLM video evaluation.

## Interactive Environment Contracts

Reusable interactive-environment contracts live in `sinomo-events`:

- `InteractiveStatePacket` describes environment kind, adapter, task, inventory, pose, nearby blocks/entities, health, hunger, and metadata;
- `InteractiveActionMacro` describes a bounded action in environment-neutral terms: verb, target, duration, stop condition, and parameters;
- `InteractiveTrajectoryStep` links state, action, and artifact references;
- event constructors emit `interactive.state.observed`, `interactive.action.requested`, and `interactive.trajectory.step.recorded`;
- `InteractiveArtifactStore` writes replay artifacts under the registered `events/interactive/trajectories` data-dir subtree.

The runtime storage layout registers interactive trajectory and screenshot subtrees through `sinomo-events::register_storage_paths`. These contracts are not Minecraft-specific and can be reused for other games, remote desktops, UI automation, and robotics-style environments.

MineStudio-specific code stays in `sinomo-benchmarks`:

- `minestudio/minestudio_action_bridge.py` maps generic action macros to a MineStudio/VPT-style action dictionary and can call `env.step` repeatedly;
- the same bridge records benchmark trajectory JSONL and optional screenshots;
- `mcu/sinomo_mcu_runner.py` uses the bridge to attach MineStudio action mappings and trajectory records to MCU case-study output.
- `minestudio/minestudio_vision_action_loop.py` runs a vision-only action loop against a real `MinecraftSim`: MiniCPM-o receives task text plus rendered `pov` frames, no structured game-state text, and the resulting action is mapped through the same bridge before calling `env.step`.
- `minestudio/minestudio_live_duplex_loop.py` emulates live full-duplex control without requiring wall-clock realtime execution: MineStudio frame sequences flow into MiniCPM-o while the model response is consumed as a stream, then the first recoverable action is stepped back into the environment. It also enables MineStudio's `RecordCallback` so high-frame-rate episode MP4s are saved next to the JSON and lower-rate observation PNG artifacts for visual review.

The Minecraft improvement workflow, benchmark failure detectors, evaluation criteria, and automation shape are tracked in [`minecraft-agent-improvement-workflow.md`](minecraft-agent-improvement-workflow.md).

Live smoke status from this implementation pass:

- `docker run python:3.12 ... pip install harbor && harbor run --help` completed successfully;
- `docker run python:3.10 ... pip install minestudio && import minestudio` completed successfully;
- `PATH=target/debug:$PATH python3 crates/sinomo-benchmarks/minestudio/sinomo_minestudio_smoke.py` completed and returned a MiniCPM vision-mode response from the mock bridge;
- `sinomo models runtime set --pytorch-endpoint http://127.0.0.1:32154` followed by `sinomo agent run --provider minicpm-o ...` completed through the Windows RTX 3090 server;
- `docker run --rm --network host ... python /tmp/sinomo_minestudio_smoke.py` completed through the same live MiniCPM endpoint and reported about 7.5 estimated output tokens/sec for the short vision smoke;
- a longer text completion through the Windows RTX 3090 endpoint reported 79 estimated output tokens in 4272 ms, or about 18.5 estimated output tokens/sec;
- the current `minestudio_vision_action_loop.py` path now requires a real MineStudio simulator runtime. It intentionally rejects the earlier synthetic frame approach for action-loop evaluation;
- `harbor run -d terminal-bench/terminal-bench-2 -l 1 -n 1 --agent-import-path sinomo_harbor_agent:SinomoHarborAgent --disable-verification` completed one Terminal-Bench task with zero adapter exceptions. The sampled task was not solved by the current minimal adapter, so this is an integration smoke rather than a performance result.
