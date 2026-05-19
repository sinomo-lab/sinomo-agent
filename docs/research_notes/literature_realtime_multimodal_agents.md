# Literature research: real-time multimodal interactive AI agents

## Scope
Focused on end-to-end or near-end-to-end agent/system literature for assistants that process streaming audio/video, react to live events, and still support slower deliberative reasoning when needed. Emphasis: real-time multimodal assistants, embodied/conversational agents, voice/video systems, event-driven architectures, and benchmarks for streaming or interactive evaluation.

## Recommended core reading list (11 items)

| Year | Paper / system | Venue / source | Contribution | Why it matters architecturally |
|---|---|---|---|---|
| 1992 | **ATLANTIS / “Integrating Planning and Reacting in a Heterogeneous Asynchronous Architecture for Controlling Real-World Mobile Robots”** (Erann Gat) | AAAI 1992 | Classic hybrid architecture: fast reactive controller + sequencer + slower planner, all running asynchronously for noisy real-world control. | Still the cleanest template for a live multimodal agent: keep low-latency perception/action loops separate from slower, interruptible planning/reasoning loops. |
| 2022 | **SayCan: Do As I Can, Not As I Say** | CoRL 2022 / PMLR 2023 | Grounds LM planning with robot affordance/value functions; language suggests useful next skills, value models score what is feasible now. | Important for agent design because it explicitly separates **semantic planning** from **state-conditioned executability**—a strong pattern for event-driven multimodal assistants. |
| 2022 | **TEACh: Task-Driven Embodied Agents That Chat** | AAAI 2022 | Human-human embodied dialogue benchmark where a follower executes household tasks, asks clarifying questions, and recovers from mistakes via conversation. | Good evaluation target for assistants that must mix live perception, dialogue, clarification, and long-horizon task completion instead of single-turn QA. |
| 2023 | **PaLM-E: An Embodied Multimodal Language Model** | ICML 2023 | Injects continuous sensor observations directly into an LLM as multimodal sentences, trained end-to-end across robotics, VQA, and captioning. | Architecturally important because it shows a unified token-space interface for text + perception + state, instead of brittle late fusion across separate stacks. |
| 2023 | **RT-2: Vision-Language-Action Models Transfer Web Knowledge to Robotic Control** | CoRL 2023 | Co-fine-tunes internet-scale VLMs on robot trajectories by representing robot actions as text tokens; enables semantic generalization and rudimentary reasoning in control. | Key pattern: treat **actions as tokens** in the same autoregressive model as perception/language, enabling one model to bridge sensing, reasoning, and actuation. |
| 2024 | **Moshi: a speech-text foundation model for real-time dialogue** | arXiv / Kyutai, 2024 | Full-duplex speech-to-speech model with separate user/self streams and ~200 ms practical latency; supports overlap, interruptions, and “inner monologue.” | One of the best references for low-latency voice agents: always-listening/full-duplex interaction beats turn-based ASR→LLM→TTS pipelines for natural barge-in and latency. |
| 2024 | **GPT-4o System Card** | OpenAI system card / arXiv 2024 | Describes an end-to-end omni model handling text, audio, image, and video in a single network, with audio responses as low as 232 ms. | Important as the clearest production-scale example of an end-to-end omni stack; supports the case for unified multimodal inference instead of chained modality-specific services. |
| 2024 | **VITA: Towards Open-Source Interactive Omni Multimodal LLM** | arXiv 2024 | Open-source omni model for simultaneous video/image/text/audio understanding, with explicit interaction features like non-awakening interaction and audio interruption. | Useful practical reference for open implementations of interruptible multimodal UX, even though it still mixes model and engineering components rather than being fully end-to-end. |
| 2024 | **StreamingBench: Assessing the Gap for MLLMs to Achieve Streaming Video Understanding** | arXiv 2024 | First broad benchmark focused on streaming video understanding with 18 tasks, 900 videos, and time-stamped questions during the stream. | Important because it measures what most offline video benchmarks miss: causal, partial-context reasoning under live temporal constraints. |
| 2025 | **OmniMMI: A Comprehensive Multi-modal Interaction Benchmark in Streaming Video Contexts** | CVPR 2025 | Streaming benchmark for OmniLLMs covering both streaming understanding and proactive reasoning/turn-taking; also proposes M4 to see/listen while generating. | Architecturally valuable because it elevates **proactivity** and **turn-taking** to first-class evaluation targets, not just passive QA accuracy. |
| 2026 | **ROMA: Real-time Omni-Multimodal Assistant with Interactive Streaming Understanding** | arXiv 2026 | Unified reactive + proactive omni assistant for streaming audio/video; aligns dense audio with sparse frames and adds a lightweight **speak head** to decide when to respond. | Probably the most directly relevant recent systems paper for your use case: it explicitly decouples **response triggering** from **response generation**, which is a strong systems pattern for live assistants. |

## Cross-paper architectural takeaways

1. **Use a layered reactive/deliberative architecture, not a single monolithic loop.** ATLANTIS and later embodied systems strongly support a three-speed design:
   - fast sensor/event loop for interruption handling and safety,
   - mid-level sequencer/executive for dialogue/task state,
   - slow planner/reasoner for longer-horizon inference.

2. **Decouple “should I respond now?” from “what should I say/do?”** ROMA’s speak head and OmniMMI’s proactive tasks make this explicit. For streaming assistants, response timing is its own prediction problem.

3. **Prefer full-duplex voice over turn-based voice pipelines when natural interaction matters.** Moshi and GPT-4o-style systems show why: lower latency, overlap handling, interruptions, interjections, and preservation of non-textual vocal cues.

4. **Ground planning with feasibility checks.** SayCan remains the clearest example. For agentic assistants, planner outputs should be filtered/scored by current-world executability, permissions, or confidence from perception tools.

5. **A unified token/action space is powerful, but you still need system-level orchestration.** PaLM-E and RT-2 show the upside of end-to-end multimodal/action modeling, but real assistants still benefit from explicit memory, schedulers, interrupt routing, and tool gating.

6. **Streaming evaluation is different from offline multimodal QA.** StreamingBench and OmniMMI show that strong offline models can still fail on causal, proactive, long-context interaction.

## Practical design patterns suggested by the literature

- **Event bus + multimodal state store:** ingest ASR/audio events, VAD, visual detections, trackers, and dialogue acts into a shared state layer.
- **Three-loop runtime:**
  - `fast loop` (50–200 ms): VAD, keywording, wake/no-wake policy, interrupts, safety triggers, urgent event detection
  - `interactive loop` (200 ms–2 s): grounded response generation, short tool calls, acknowledgment, clarification
  - `deliberative loop` (2 s+): planning, summarization, retrieval, long-horizon reasoning
- **Separate trigger model from content model:** use a small policy/head to decide whether to speak, alert, ask, or stay silent.
- **Persistent rolling memory:** maintain compressed summaries + recent raw cache for causal streaming video/audio context.
- **Fallback hierarchy:** reactive shortcuts for urgent conditions, richer reasoning only when latency budget permits.

## Best “start here” subset if time is short

1. ATLANTIS (1992) — architecture pattern
2. SayCan (2022) — grounding planner with executability
3. PaLM-E (2023) — unified sensor-language model
4. Moshi (2024) — low-latency full-duplex voice
5. GPT-4o System Card (2024) — production end-to-end omni reference
6. OmniMMI (2025) + ROMA (2026) — proactive streaming multimodal evaluation/system design

## Useful adjacent papers/benchmarks not in the core 11

- **BEHAVIOR** (CoRL 2022): long-horizon embodied household benchmark with realistic activities and human-referenced metrics.
- **SVBench** (ICLR 2025 Spotlight): long-context streaming video benchmark with temporal multi-turn dialogue chains.
- **OVO-Bench** (CVPR 2025): emphasizes time-aware online video understanding and when to answer.
- **LiViBench** (AAAI 2026): interactive livestream benchmark with comments/audio/speech, useful if your target is live social video.

## Source notes used
Primary sources gathered from AAAI / PMLR / CVPR Open Access / arXiv / project pages for the papers above.
