# Realtime computer-use and gaming action options

## Question

What options are available if Sinomo targets realtime computer-use actions such as gaming, not just audio/video conversation?

## Short answer

For realtime gaming, do not use a frontier LLM as the direct controller. Current computer-use models are useful for GUI workflows, menus, setup screens, inventory management, and slow desktop tasks, but they are too slow and unreliable for frame-rate gameplay control.

Sinomo should use a hierarchical control stack:

```text
Screen / game telemetry / audio
  -> low-latency observation adapters
  -> local realtime policy/controller at 20-120 Hz
  -> action arbiter and safety limits
  -> keyboard/mouse/gamepad/game API actions

LLM / VLM planner
  -> goals, strategies, skill selection, explanations
  -> updates every 0.5-10+ seconds
```

The realtime actor should be a local model, rule system, behavior tree, imitation/RL policy, or game-specific skill controller. The LLM should supervise, plan, explain, learn skills, and choose modes.

## Latency classes

| Layer | Target | Suitable options |
|---|---:|---|
| Reflex control | 8-50 ms | rules, PID/control code, behavior tree, tiny ONNX policy |
| Gameplay policy | 16-100 ms | CNN/ViT + RNN/Transformer policy, imitation learning, RL policy |
| Tactical decisions | 100-500 ms | small local VLM/LLM, object/state parser, action classifier |
| Strategy/planning | 0.5-10+ s | larger LLM/VLM, retrieval, skill library, tree search |
| GUI computer use | 1-10+ s | OpenAI/Claude computer use, UI-TARS, ShowUI, OmniParser + VLM |

## Available option categories

### 1. Hosted computer-use agents

Examples:

- OpenAI Computer Use / `computer-use-preview`
- Anthropic Claude computer use

Strengths:

- Can see screenshots and issue mouse/keyboard actions.
- Good for desktop workflows, browsers, installers, menus, and slow multi-step UI tasks.
- Useful as a high-level fallback when native APIs are unavailable.

Limitations for gaming:

- Not realtime. Anthropic explicitly warns current latency may be too slow for regular human-directed computer actions.
- OpenAI's `computer-use-preview` model page lists no audio/video support and no streaming support.
- Screenshot-action-screenshot loops miss short-lived visual events and are fragile for fast games.
- Coordinate grounding errors are still common enough to require sandboxing and confirmation layers.

Recommended Sinomo role:

- Use for launcher setup, settings menus, mod managers, web UIs, account-free test environments, and non-twitch tasks.
- Do not use as the direct combat/movement/gameplay controller.

### 2. Open GUI vision-language-action models

Examples:

- UI-TARS / UI-TARS Desktop
- ShowUI
- ShowUI-pi for continuous drag trajectories
- OmniParser + general VLM/LLM

Strengths:

- Can run locally or semi-locally.
- Better fit for screen grounding than general VLMs.
- UI-TARS explicitly targets desktop/browser GUI automation and has updates claiming stronger game/code/tool capabilities.
- ShowUI is lightweight and built as a vision-language-action GUI model.
- OmniParser is a strong component for converting screenshots into structured UI elements.

Limitations for gaming:

- Mostly trained/evaluated on GUI workflows, not high-speed game physics.
- Action spaces are usually clicks, typing, scrolls, and short drags, not continuous joystick/mouse-look policies.
- Model-call latency is still generally too high for 60 FPS control.

Recommended Sinomo role:

- UI parsing and grounding for slow surfaces.
- Inventory, map, menu, settings, launcher, and strategy-game UI actions.
- Data collection and labeling for Sinomo's own action traces.
- A fallback "visual desktop operator" for non-realtime tasks.

### 3. Game-engine-native agents

Examples:

- Unity ML-Agents
- Unreal Engine Learning Agents
- Custom plugins/mods for games Sinomo is allowed to control

Strengths:

- Best option when the game or simulation is instrumentable.
- Access to structured observations, reward, state, and legal action APIs.
- Supports imitation learning and reinforcement learning.
- Can run policies inside or adjacent to the engine at frame rate.

Limitations:

- Requires control over the game/sim or a mod/plugin API.
- Not generally available for closed commercial games.
- Training and environment design take real engineering time.

Recommended Sinomo role:

- Primary path for self-hosted game worlds, robotics-like simulations, custom game agents, testing environments, and research sandboxes.
- Best route if Sinomo eventually has her own interactive world or training arena.

### 4. RL / imitation-learning environments

Examples:

- Gymnasium and Farama ecosystem.
- PettingZoo for multi-agent settings.
- Arcade Learning Environment for Atari.
- ViZDoom for visual Doom agents.
- MineDojo / MineRL-style Minecraft environments.

Strengths:

- Mature tooling for observation-action loops.
- Good for training local policies that can act quickly.
- ViZDoom supports visual input, depth/object labels, audio buffer, off-screen rendering, and very high simulation FPS.
- Minecraft research has strong precedents for video-to-action learning, especially OpenAI VPT and Voyager-style skill libraries.

Limitations:

- These are research/training environments, not a universal bridge to arbitrary desktop games.
- Policies often overfit to the environment unless deliberately trained for robustness.
- Reward design and data collection are hard.

Recommended Sinomo role:

- Train and evaluate fast local controllers.
- Build benchmark suites for Sinomo's realtime action architecture.
- Learn reusable skills that the LLM can later select or compose.

### 5. Direct screen capture plus virtual input

Observation options on Windows:

- DXGI Desktop Duplication API for frame-by-frame desktop images.
- Windows.Graphics.Capture for window/display capture.
- OBS plugin/source integration for capture pipelines.
- NVIDIA Capture SDK on supported Linux/NVIDIA capture setups.

Action options:

- Win32 `SendInput` for keyboard/mouse injection.
- Raw gamepad or virtual gamepad paths where permitted.
- Game-specific APIs, mods, or accessibility interfaces when available.

Strengths:

- Works with many desktop apps and games without engine integration.
- Can be kept local and low-latency.
- Useful for observation/action trace collection.

Limitations:

- Screen pixels are lower quality state than game telemetry.
- Input injection can be blocked by OS integrity levels, anti-cheat systems, permissions, or game policies.
- Online competitive games often prohibit automation. Sinomo should avoid ToS-violating or unfair play contexts.

Recommended Sinomo role:

- Local sandbox/offline game control.
- Data collection from user demonstrations.
- Assistive overlays and coaching.
- Closed-loop control only where permitted and safe.

## Model architectures to consider

### A. Hierarchical game agent

```text
LLM strategist
  -> objective / tactic / skill choice
  -> every 0.5-10 s

Tactical policy
  -> target selection, route choice, mode switching
  -> 2-10 Hz

Realtime controller
  -> movement, aiming, timing, interactions
  -> 20-120 Hz
```

This is the recommended Sinomo architecture.

Why:

- Keeps fast actions local.
- Lets the LLM reason without holding the frame loop hostage.
- Makes skills inspectable and replaceable.
- Works with both game APIs and screen-only control.

### B. Vision-action policy from demonstrations

Train a model from recordings of screen frames plus keyboard/mouse/gamepad actions.

Useful techniques:

- Behavioral cloning.
- Inverse dynamics labeling, as in OpenAI VPT.
- Dataset aggregation after mistakes.
- Recurrent policy state for temporal context.
- Later RL fine-tuning in instrumented environments.

Recommended for:

- Repeated gameplay mechanics.
- Games where Sinomo can observe human play.
- Building a reusable "motor prior" for computer use.

### C. RL policy in an instrumented environment

Use a game/sim API to train policy networks directly.

Recommended for:

- Custom games.
- Unity/Unreal simulations.
- ViZDoom/ALE/MineDojo training tasks.
- Any environment where reward and reset are available.

Not enough by itself for:

- Arbitrary commercial desktop games.
- Long-horizon semantic goals unless paired with a planner/skill library.

### D. GUI action model

Use UI-TARS/ShowUI/OmniParser-like models to map screenshot + instruction to a UI action.

Recommended for:

- Menus.
- Inventory management.
- Turn-based games.
- Desktop/browser automation.
- Assistive control panels.

Not recommended for:

- First-person aiming.
- Platformer timing.
- Fighting game reaction loops.
- Any action requiring frame-perfect response.

### E. Skill library plus LLM planner

Represent reusable actions as named skills:

- `open_inventory`
- `equip_item`
- `kite_enemy`
- `navigate_to_marker`
- `hold_angle`
- `farm_resource`
- `recover_health`

Each skill owns its own observation needs, action space, timeout, and success/failure criteria. The LLM chooses or composes skills; the local controller executes them.

This matches the strongest lesson from Voyager-style agents: store reusable executable skills, not just text memories.

## Recommended MVP for Sinomo

### MVP 1: Realtime desktop action harness

Build a local harness with:

- window/screen capture adapter,
- action injector abstraction,
- hot observation state,
- action arbiter,
- trace recorder,
- replay viewer,
- safety kill switch,
- one simple test game or toy environment.

Use this to collect synchronized:

- frame timestamps,
- screen crops,
- OCR/UI observations,
- keyboard/mouse/gamepad actions,
- game state when available,
- Sinomo decisions,
- outcome events.

### MVP 2: Slow GUI computer-use integration

Add one GUI action backend:

- OpenAI/Claude computer use for hosted testing, or
- UI-TARS/ShowUI/OmniParser for local/open testing.

Scope it to:

- menus,
- browser tasks,
- launchers,
- settings screens,
- turn-based UIs.

### MVP 3: Local realtime policy

Train or hand-build a fast controller for one sandbox:

- simple 2D game,
- ViZDoom scenario,
- Unity ML-Agents scene,
- MineDojo/Minecraft task,
- or a custom Sinomo test environment.

The LLM should set goals and choose skills. The policy should act every tick.

## Sinomo crate implications

Add or plan these capabilities:

- `sinomo-screen`: capture frames, windows, crops, dirty regions, and cursor state.
- `sinomo-input`: keyboard, mouse, controller, and game/action API abstraction.
- `sinomo-control`: high-rate local control loop and action arbiter.
- `sinomo-policy`: local model/runtime wrapper for ONNX/TensorRT/torch policies.
- `sinomo-trace`: synchronized observation/action/outcome recording.
- `sinomo-sim`: test environments and replayable sandboxes.
- `sinomo-skills`: executable game/computer-use skill library.

The existing `sinomo-trigger`, `sinomo-agent`, and `sinomo-deliberation` split still holds, but gaming needs a new high-rate action plane below the interactive loop.

## Practical recommendation

Use this architecture:

```text
LLM/VLM planner
  - decides goals, strategy, skill choice
  - reads event summaries and state packets
  - updates slowly

Skill executive
  - decomposes goals into active skills
  - monitors success/failure
  - can swap or cancel skills

Realtime controller
  - local policy/rules/behavior tree
  - acts at frame/tick rate
  - never waits on a cloud LLM

Observation/action harness
  - screen/game capture
  - telemetry when available
  - safe input injection
  - trace recording and replay
```

This gives Sinomo a credible path from normal computer use to realtime game-like action without pretending that today's general computer-use agents can play twitch games directly.

## Source notes

- OpenAI Computer Use guide: https://platform.openai.com/docs/guides/tools-computer-use/
- OpenAI `computer-use-preview` model page: https://platform.openai.com/docs/models/computer-use-preview
- Anthropic Claude computer use docs: https://docs.anthropic.com/en/docs/build-with-claude/computer-use
- Microsoft OmniParser: https://www.microsoft.com/en-us/research/publication/omniparser-for-pure-vision-based-gui-agent/
- OmniParser GitHub: https://github.com/microsoft/OmniParser
- UI-TARS GitHub: https://github.com/bytedance/UI-TARS
- UI-TARS Desktop: https://github.com/bytedance/UI-TARS-desktop
- ShowUI GitHub: https://github.com/showlab/ShowUI
- ShowUI-pi project: https://showlab.github.io/showui-pi
- Unity ML-Agents docs: https://unity-technologies.github.io/ml-agents/ML-Agents-Toolkit-Documentation/
- Unreal Learning Agents docs: https://dev.epicgames.com/documentation/en-us/unreal-engine/API/PluginIndex/LearningAgents
- Gymnasium docs: https://gymnasium.farama.org/
- PettingZoo docs: https://pettingzoo.farama.org/
- ViZDoom: https://vizdoom.cs.put.edu.pl/
- Arcade Learning Environment: https://github.com/Farama-Foundation/Arcade-Learning-Environment
- MineDojo: https://minedojo.org/
- OpenAI VPT: https://openai.com/index/vpt/
- Voyager: https://voyager.minedojo.org/
- Windows Desktop Duplication API: https://learn.microsoft.com/en-us/windows/win32/direct3ddxgi/desktop-dup-api
- Windows.Graphics.Capture overview: https://learn.microsoft.com/en-us/windows/uwp/audio-video-camera/screen-capture-video
- Win32 SendInput: https://learn.microsoft.com/en-us/windows/win32/api/winuser/nf-winuser-sendinput
- NVIDIA Capture SDK: https://developer.nvidia.com/capture-sdk
