# Agent System Implementation Notes

This document tracks the implementation entry points for
[agent-system-architecture.md](agent-system-architecture.md).

## MiniCPM-o Runtime

The Rust contract lives in `crates/sinomo-minicpm`. It owns stable request and
response structs, process invocation, status reporting, and a `LanguageModel`
provider adapter. PyTorch and Transformers stay isolated in
`crates/sinomo-minicpm/python/minicpm_bridge.py`.

The checked-in bridge supports:

- text requests;
- vision requests with an image path or base64 image payload;
- audio and duplex request modes in the stable schema;
- a probe mode for dependency diagnostics;
- `SINOMO_MINICPM_MOCK=1` for CI and benchmark harness smoke tests without
  downloading the 9B model.

The local model path follows the MiniCPM-o 4.5 PyTorch reference path. Current
upstream docs recommend a Linux NVIDIA GPU with more than 28 GB VRAM for the
full PyTorch demo. CPU mode is useful for dependency and wiring checks, but it
is not expected to be fast enough for interactive full-duplex operation.

MiniCPM-o 4.5 currently depends on trusted remote model code that uses the
Transformers 4.53 cache API. Use `transformers==4.53.3`; newer 4.x builds can
load the checkpoint but fail at generation with `DynamicCache.seen_tokens`
compatibility errors, and Transformers 5.x fails earlier in model loading.
The Windows CUDA smoke also required `minicpmo`, `onnx`, `onnxruntime`,
`hyperpyyaml`, and `diffusers` in addition to the base `torch`, `PIL`, and
`accelerate` dependencies.

Local probe status from this implementation pass:

- an isolated Python 3.11 venv at `/tmp/sinomo-minicpm-venv` can import
  `torch`, `transformers`, `PIL`, and `accelerate`;
- `SINOMO_MINICPM_PYTHON=/tmp/sinomo-minicpm-venv/bin/python
  sinomo-minicpm-bridge` reports MiniCPM dependencies are importable;
- `torch.cuda.is_available()` is false on the local host, so the full 9B
  PyTorch model was not loaded here.
- a Windows node with an RTX 3090 was able to load `openbmb/MiniCPM-o-4_5`
  through the long-running `sinomo-minicpm-server` with CUDA after pinning
  `transformers==4.53.3`; text and vision smoke requests completed through
  the remote HTTP runtime.

When the Windows firewall blocks direct inbound traffic to the MiniCPM server,
run the local MCP forwarding proxy and point benchmark tools at it:

```sh
SINOMO_WINDOWS_MCP_URL=http://<windows-host>:39999/mcp \
SINOMO_WINDOWS_MCP_SESSION=<mcp-session-id> \
python3 crates/sinomo-benchmarks/minicpm_mcp_proxy.py --bind 127.0.0.1 --port 32154
SINOMO_MINICPM_ENDPOINT=http://127.0.0.1:32154 sinomo agent run \
  --provider minicpm-o \
  --model openbmb/MiniCPM-o-4_5 \
  --prompt "Return a short acknowledgement."
```

The proxy forwards `/health` and `/complete` through the Windows MCP
PowerShell channel to the MiniCPM server's Windows-local endpoint. This keeps
Docker benchmarks on the Linux host independent of Windows firewall state.

## Main Agent Loop

`crates/sinomo-agent` now exposes:

- `main_agent_system_prompt`;
- `maid_agent_system_prompt`;
- `default_main_agent_tools`;
- `AsyncTaskUpdate`;
- `AgentLoopInput`;
- `AgentLoopDecision`;
- `MainAgentLoop`.

The loop asks the model for a JSON decision with user-visible text, optional
tool calls, optional maid handoff, and the next interaction state. Quiet async
updates can be consumed without calling the model.

## CLI Smoke Entry

`sinomo agent run --prompt <text>` runs one main-agent decision with the
configured provider. It defaults to the stored Codex provider so development
can use existing local credentials.

Examples:

```sh
sinomo agent run --prompt "Check cluster health later and tell me if anything is wrong."
sinomo agent run --provider codex --model gpt-5.2-codex --prompt "Summarize the current repo status."
```

## Benchmark Harnesses

`crates/sinomo-benchmarks` is an application-level optional harness crate. It
does not participate in production runtime dependencies.

Terminal-Bench 2.0 is represented through Harbor:

- current install smoke: `pip install harbor`;
- command smoke: `harbor run --help`;
- current dataset id from Harbor docs: `terminal-bench/terminal-bench-2`;
- adapter template:
  `crates/sinomo-benchmarks/harbor/sinomo_harbor_agent.py`.

The Harbor adapter is an external agent. It runs the Sinomo agent loop on the
host side with stored Codex credentials and uses Harbor's `environment.exec`
only for bounded task-container commands. This keeps Sinomo tools and
credentials out of the benchmark container.

MineStudio is represented through a vision-path smoke:

- current install smoke: `pip install minestudio`;
- import smoke: `import minestudio`;
- synthetic frame bridge:
  `crates/sinomo-benchmarks/minestudio/sinomo_minestudio_smoke.py`.

The smoke script uses `SINOMO_MINICPM_MOCK=1` by default so it validates the
vision request path without requiring the full MiniCPM-o model. Set
`SINOMO_MINICPM_ENDPOINT` to run it against a live remote MiniCPM server.

Live smoke status from this implementation pass:

- `docker run python:3.12 ... pip install harbor && harbor run --help`
  completed successfully;
- `docker run python:3.10 ... pip install minestudio && import minestudio`
  completed successfully;
- `PATH=target/debug:$PATH SINOMO_MINICPM_MOCK=1 python3
  crates/sinomo-benchmarks/minestudio/sinomo_minestudio_smoke.py` completed
  and returned a MiniCPM vision-mode response from the mock bridge;
- `SINOMO_MINICPM_ENDPOINT=http://127.0.0.1:32154 sinomo agent run
  --provider minicpm-o ...` completed through the Windows RTX 3090 server;
- `docker run --rm --network host ... python
  /tmp/sinomo_minestudio_smoke.py` completed through the same live MiniCPM
  endpoint and reported about 7.5 estimated output tokens/sec for the short
  vision smoke;
- a longer text completion through the Windows RTX 3090 endpoint reported 79
  estimated output tokens in 4272 ms, or about 18.5 estimated output
  tokens/sec;
- `harbor run -d terminal-bench/terminal-bench-2 -l 1 -n 1
  --agent-import-path sinomo_harbor_agent:SinomoHarborAgent
  --disable-verification` completed one Terminal-Bench task with zero
  adapter exceptions. The sampled task was not solved by the current minimal
  adapter, so this is an integration smoke rather than a performance result.
