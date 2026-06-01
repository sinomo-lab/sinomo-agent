# Agent System Implementation Notes

This document tracks the implementation entry points for [agent-system-architecture.md](agent-system-architecture.md).

## PyTorch Model Bridge

The Rust contract lives in `crates/sinomo-pytorch`. It owns stable request and response structs, process invocation, status reporting, and a `LanguageModel` provider adapter. PyTorch and Transformers stay isolated in `crates/sinomo-pytorch/python/pytorch_bridge.py`.

The checked-in bridge supports:

- text requests;
- vision requests with an image path or base64 image payload;
- audio and duplex request modes in the stable schema;
- a probe mode for dependency diagnostics;
- a mock mode for local smoke tests and benchmark harness checks without downloading the 9B model.

MiniCPM-o remains the first configured backend (`--provider minicpm-o`), but the bridge is intentionally named around the implementation boundary: any local PyTorch/Transformers model that can satisfy the request schema can be hosted behind `sinomo-pytorch-bridge` or `sinomo-pytorch-server`. Product runtime configuration is cluster-scoped and stored in cluster KV at `/config/lm/runtime`; use `sinomo models runtime set` instead of environment variables for model paths, device choices, and token budgets. During feature testing, prefix commands with `sinomo --local` to force local file-backed settings even on a clustered node; model runtime settings then live at `$SINOMO_DATA_DIR/config/models-runtime.json`.

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

For a local-only test override, use:

```sh
sinomo --local models runtime set --pytorch-endpoint ""
```

## Main Agent Loop

`crates/sinomo-agent` now exposes:

- `main_agent_system_prompt`;
- `maid_agent_system_prompt`;
- `default_main_agent_tools`;
- `AsyncTaskUpdate`;
- `AgentLoopInput`;
- `AgentLoopDecision`;
- `MainAgentLoop`;
- `Agent`;
- `AgentSession`;
- `AgentTranscript`;
- `AgentItem`;
- `AgentSessionProjection`;
- `AgentWorkingState`;
- `TurnContext`;
- `ContextAssembler`;
- `ContextAssemblyReport`;
- `ContextBudgetLane`;
- `AgentModelEvent`;
- `AgentCompactor`;
- `AgentCompactionSummary`;
- `AgentPolicyBundle`;
- `AgentToolPolicy`;
- `AgentToolCapability`;
- `AgentToolEvidence`;
- `AgentSessionStore`;
- `FileAgentSessionStore`;
- `AgentSessionSnapshot`;
- `AgentSessionStoreRecord`;
- `AgentTraceStore`;
- `FileAgentTraceStore`;
- `AgentStoragePaths`;
- `AgentTraceRecord`;
- `AgentToolRegistry`;
- `LocalWorkspaceAgentConfig`;
- `default_coding_agent_tools`;
- `default_agent_tool_descriptors`.

The loop asks the model for a JSON decision with user-visible text, optional tool calls, optional maid handoff, and the next interaction state. Quiet async updates can be consumed without calling the model.

`Agent` is the first coding-agent style loop for deliberation and self-evolution work. It normalizes provider calls through `LanguageModelRequest`, keeps both a model-visible transcript and provider-neutral typed `AgentItem` evidence, executes requested tools through a registry/orchestrator boundary, returns tool results as follow-up user messages, and emits `agent.loop.*` events. By default, agent steps, per-step tool calls, model repair attempts, model output tokens, prompt-reference expansion, context size, and compaction are not hard-capped. Runtime layers can tighten those values when a concrete model or session policy provides real limits; model-window inference belongs to `sinomo-lm` provider implementations, and `AgentContextBudget::for_loaded_model` converts loaded model metadata into an optional byte budget plus ratio-based compaction threshold.

The current architecture surface now has a session/turn split: `Agent::start_turn`, `continue_turn`, `steer`, `follow_up`, `compact`, and `abort` operate on `AgentSession`, while `Agent::run` remains the compatibility helper for one-shot runs. `AgentSession` keeps a model-facing projection transcript plus an append-only raw transcript so compaction does not destroy source history. `start_turn` projects typed session transcript into model-visible history, and queued steering/follow-up messages are consumed into the next turn as an `agent_queued_turn_context` user message. Every model step assembles a `TurnContext` and records a `ContextAssemblyReport` with included sources, omitted context, and byte-budget estimates. The model request includes a structured `turn_context` message with workspace facts, current session projection/working state, async updates, workspace instruction files such as `AGENTS.md` as authored by the user, and prompt-reference expansions for `@file` and `@directory` mentions when `AgentConfig::workspace_root` is configured. `ContextBudgetEstimate` now includes budget lanes for fixed prompt, current work, recent tail, retrieved memories, old summaries, prompt references, generic context attachments, and agent policy so future memory retrieval and projection pruning can be managed deliberately. `sinomo-lm` exposes provider-neutral `LanguageModelStreamEvent` values for text deltas, reasoning summaries, tool-call start/delta/completion, usage, finish, error, and abort events. `Agent` converts those into `AgentModelEvent` records, while providers that only return a completed response fall back to one completed-response delta.

Self-evolution is an R2 caller of the generic agent loop rather than a built-in agent mode. It passes `AgentLoopInput::context_attachments` that summarize an evolution job, mutation packet, candidate bundle, or prior evidence as generic typed JSON. `ContextAssembler` records these as `TurnContext::context_attachments`, includes their byte cost in `ContextBudgetEstimate`, and renders them into the model-facing `turn_context` message. When attachments are present, `AgentRun::candidate_evidence_bundle` collects the final response plus patch snapshots and verification results from the run, giving the caller a provider-neutral evidence summary before any evolution-specific durable packaging is attached.

`Agent` treats malformed deliberation-model output as recoverable. The loop records the unparseable response as `AgentItem::Error` plus `AgentModelEvent::Error`, then sends a focused JSON repair message to the model. `AgentConfig::max_model_repair_attempts` is optional: when a runtime policy sets it, exhaustion fails with `AgentError::ModelResponse` after preserving the failed attempts in trace state; otherwise repair attempts are left unbounded for the session runtime to cancel or steer.

`AgentSessionStore` is the reloadable session-state boundary. `FileAgentSessionStore` stores a pretty JSON snapshot at `events/agent/sessions/<session>/snapshot.json`, appends session lifecycle records to `events/agent/sessions/<session>/events.jsonl`, appends raw typed items to `items.jsonl`, and appends session projections to `projections.jsonl`. `Agent::with_session_store` attaches it to the runtime; `start_turn`, `steer`, `follow_up`, `compact`, and `abort` update the snapshot and append session records, while `Agent::load_session` and `Agent::save_session` expose explicit reload/save hooks. The snapshot is the canonical resume projection; item and projection streams are the append-only audit trail; traces remain observability/evidence.

`AgentTraceStore` is the append-only trace-store boundary. `FileAgentTraceStore` writes JSONL records for run boundaries, context reports, provider-neutral model events, typed transcript items, tool outcomes, compactions, and emitted runtime events. `Agent` uses no-op session and trace stores by default, and callers opt into durable session state and traces independently.

`AgentStoragePaths` derives the Sinomo-owned agent subtree from `SinomoDataDir`: session snapshots, lifecycle records, raw item records, and projection records live under `events/agent/sessions/<session>/`, traces live under `events/agent/traces/<session>.jsonl`, and full tool evidence lives under `events/agent/evidence/<session>/...`. `FileAgentSessionStore::for_data_dir`, `FileAgentTraceStore::for_data_dir_session`, and `LocalWorkspaceAgentConfig::with_data_dir_evidence` wire those paths into the first local implementation. `sinomo-agent::register_storage_paths` registers `events/agent` as the R2 escape subtree used by the CLI storage-contract layout.

`AgentPolicyBundle` is the first session availability bundle. It can include or exclude tools by name and by declared `AgentToolCapability` values such as `ReadWorkspace`, `WriteWorkspace`, `ExecuteProcess`, `InspectVersionControl`, `AccessNetwork`, `PromptUser`, `Delegate`, `ObserveEnvironment`, `CallModel`, `ManageBackgroundTask`, and `InspectClusterState`. This is not a human approval model: a tool that is visible in the session is available to the agent. Strictly forbidden resources should be absent from the registry and cut off by the sandbox or tool execution environment. The policy filters model-visible tool schemas before context assembly and keeps a runtime guard for hallucinated or stale hidden-tool calls. Hidden tools are recorded in the `ContextAssemblyReport` omitted-context list, and blocked hidden-tool calls return model-visible blocked tool results with `AgentToolEvidence.policy.allowed = false`.

Tool contracts and implementations now live under `crates/sinomo-agent/src/tools/`. The shared `AgentTool` trait, descriptors, outcomes, evidence, and dynamic `AgentToolRegistry` are separated from the loop. The local workspace built-ins live in `tools/local_workspace/`, with each tool in its own file (`read_file`, `search`, `apply_patch`, `run_command`, command-session tools, and `request_user_input`). `AgentToolRegistry` is an interior-mutable handle: callers can register a tool, replace an existing tool with the same name, remove a tool, clear the registry, or clone the registry handle for runtime extension and future MCP/plugin registration. `Agent::tool_registry`, `Agent::register_tool`, and `Agent::remove_tool` expose that dynamic surface without changing the main turn loop.

Compaction is represented by typed `AgentCompactionSummary` items, `AgentSessionProjection`, `AgentWorkingState`, and `agent.compaction.completed` events. The first compactor is intentionally deterministic: it records source item ids, retained item ids, previous projection lineage, current user request, active objective, pending tool state, file/evidence facts, verification state, blockers, and open risks. Explicit `Agent::compact` rewrites only the in-memory/model-facing projection while preserving raw transcript history, then persists the projection when a session store is attached. Automatic budget compaction records the same projection shape and uses it for subsequent model turns, but it does not yet run an auxiliary summarizer or garbage-collect old evidence from durable storage.

The OpenAI Responses implementation path currently lives in `sinomo-lm` through the Codex provider's Responses renderer and stream parser. The Codex parser emits `LanguageModelStreamEvent` records from Responses SSE blocks for text, reasoning, tool calls, usage, completion, and failure. The durable `sinomo-agent` transcript is provider-neutral so future Chat Completions-compatible local servers, Anthropic, Google, process-backed coding agents, and smaller interactive models can render from the same `TurnContext`.

The bundled local workspace registry is intentionally conservative: `read_file` and `search` are available under the configured workspace root with bounded outputs and path-escape checks. `apply_patch` is a standalone workspace edit tool: it accepts Codex-style `*** Begin Patch` freeform patches or structured JSON edit operations, applies add/update/delete/move changes directly inside the workspace root, records changed files, preserves the submitted patch as evidence when an evidence directory is configured, and emits an `AgentItem::PatchSnapshot` for review/rollback context. It is blocked unless the caller explicitly enables file writes on `LocalWorkspaceAgentConfig`. `run_command` and command sessions are blocked unless shell commands are enabled. Higher-level git, formatting, and test workflows are intentionally left to the model through `run_command` or to future session-specific extensions. `request_user_input` is visible in the model contract but blocked until an interactive runtime bridge is attached.

Tool results now carry a generic `AgentToolEvidence` envelope with the runtime availability decision, model-visible output size and truncation marker, durable artifact references, optional execution evidence, typed evidence records, and free-form metadata. Coding tools currently attach a `kind = "coding"` record whose payload is `AgentCodingToolEvidence` for patch refs, changed files, diff summaries, and verification state; non-coding tools can add their own record kinds without changing the envelope. `LocalWorkspaceAgentConfig::with_evidence_dir` lets local tools preserve truncated read or command output behind a `full_output` artifact reference; `with_data_dir_evidence` uses the standard data-dir layout. Wiring those handles to SIFS remains a storage integration point.

## CLI Smoke Entry

`sinomo agent run --prompt <text>` runs one main-agent decision with the configured provider. It defaults to the stored Codex provider so development can use existing local credentials.

Examples:

```sh
sinomo agent run --prompt "Check cluster health later and tell me if anything is wrong."
sinomo agent run --provider codex --model gpt-5.2-codex --prompt "Summarize the current repo status."
sinomo --local agent run --provider deepseek --model deepseek-v4-flash --prompt "Summarize the current repo status."
```

`--local` is a global CLI flag. It bypasses cluster quorum/orphan guards for local-capable commands and reads or writes local config instead of cluster KV. This is useful for testing provider authentication, default model selection, runtime model settings, sandbox policy, registry trusted signer records, and local-owned DFS data on a node that may already be part of a cluster. Local model/provider files are under `$SINOMO_DATA_DIR/config/` (`models.json`, `models-runtime.json`, and `sandbox-policy.json`), registry trusted signer records live at `$SINOMO_DATA_DIR/registry/trusted-artifact-signers.json`, and provider API-key credentials remain under the local provider credentials directory.

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
