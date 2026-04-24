# Long-horizon reasoning, memory, and deliberation for streaming agents

## Focus
Literature relevant to agents that must process streaming audio/video in real time, react quickly, and escalate into slower/longer reasoning when uncertainty or task difficulty warrants it.

## Key papers / systems

1. **Transformer-XL: Attentive Language Models beyond a Fixed-Length Context** (2019, **ACL**)
   - Introduces segment-level recurrence and relative positional encoding so a transformer can reuse prior hidden states across segments, reducing context fragmentation and extending effective dependency length.
   - **Architectural relevance:** foundational pattern for maintaining a rolling working memory instead of reprocessing full history; useful for any streaming stack that needs bounded-latency updates.

2. **Mamba: Linear-Time Sequence Modeling with Selective State Spaces** (2024, **COLM**)
   - Replaces attention with selective state-space dynamics that can selectively remember/forget based on input, with linear-time scaling and reported strong performance up to million-length sequences.
   - **Architectural relevance:** strong candidate backbone for low-latency recurrent perception/memory modules where full attention is too expensive, especially for long audio/video streams.

3. **Efficient Streaming Language Models with Attention Sinks (StreamingLLM)** (2024, **ICLR**)
   - Shows that preserving a few initial “attention sink” KV states plus a recent-token window lets LLMs stably process effectively infinite streams without full recomputation; reports up to 22.2x speedup over sliding-window recomputation.
   - **Architectural relevance:** practical recipe for bounded-memory, always-on context maintenance; directly reusable for text-side dialogue state and as inspiration for multimodal KV-cache policies.

4. **ReAct: Synergizing Reasoning and Acting in Language Models** (2022, **arXiv**)
   - Interleaves reasoning traces with environment/tool actions so the model can ground itself, update plans, and retrieve missing information during execution.
   - **Architectural relevance:** foundational fast-loop agent pattern: act-observe-think rather than think-only, which is critical for live systems that must respond under partial observability.

5. **Generative Agents: Interactive Simulacra of Human Behavior** (2023, **UIST**)
   - Stores a natural-language record of experience, synthesizes reflections over time, and retrieves memories for planning; the observation-reflection-planning loop produced coherent multi-agent social behavior.
   - **Architectural relevance:** canonical event-memory design: raw events -> reflective summaries -> plan-conditioned retrieval. Useful for long-horizon episodic memory over user/environment events.

6. **Reflexion: Language Agents with Verbal Reinforcement Learning** (2023, **NeurIPS**)
   - Agents store linguistic self-critiques from trial-and-error in an episodic memory buffer rather than updating model weights; achieved strong gains across coding, reasoning, and sequential decision tasks.
   - **Architectural relevance:** good template for post-hoc learning in deployed agents: keep short “lessons learned” memories from failures and feed them back only when relevant.

7. **Voyager: An Open-Ended Embodied Agent with Large Language Models** (2023, **arXiv / NeurIPS workshop system**)
   - Uses an automatic curriculum, iterative self-improvement, and an ever-growing executable skill library in Minecraft; skills are stored as reusable code rather than just text summaries.
   - **Architectural relevance:** strong precedent for **procedural memory** alongside episodic memory. For real-time agents, reusable skills/macros reduce deliberation cost on recurring situations.

8. **MemGPT: Towards LLMs as Operating Systems** (2023, **arXiv**)
   - Frames memory as hierarchical virtual context management, paging information between limited working context and larger archival stores, with explicit control-flow “interrupts.”
   - **Architectural relevance:** directly useful for designing memory tiers: hot working memory, warm summaries, cold archives, and policies for promotion/eviction/retrieval.

9. **Quiet-STaR: Language Models Can Teach Themselves to Think Before Speaking** (2024, **COLM**)
   - Trains models to generate internal token-level rationales that improve future-token prediction, showing zero-shot reasoning gains without task-specific fine-tuning.
   - **Architectural relevance:** supports a dual-process design where some reasoning stays latent/internal; for streaming agents, this suggests lightweight hidden deliberation before expensive outward tool use or response.

10. **s1: Simple test-time scaling** (2025, **EMNLP**)
    - Demonstrates a simple recipe for stronger reasoning via small high-quality reasoning data plus **budget forcing**, which explicitly lengthens or truncates test-time thinking.
    - **Architectural relevance:** valuable for an adaptive compute controller: spend extra decoding budget only on hard/ambiguous moments, while keeping a cheap fast path for routine live reactions.

11. **InternLM-XComposer2.5-OmniLive** (2024, **arXiv**)
    - End-to-end multimodal streaming system with disentangled **streaming perception**, **long memory**, and **reasoning** modules for real-time video+audio interaction.
    - **Architectural relevance:** one of the clearest direct blueprints for audio-video agents: separate always-on perception from query-triggered reasoning and compress short-term memory into retrievable long-term memory.

12. **Thinking in Streaming Video (ThinkStream)** (2026, **arXiv**)
    - Proposes a **Watch-Think-Speak** loop plus **Reasoning-Compressed Streaming Memory (RCSM)**, where intermediate reasoning traces replace old dense visual tokens while preserving context under latency constraints.
    - **Architectural relevance:** especially relevant for live video agents that need intermittent deeper reasoning; reasoning traces themselves become memory, not just outputs.

## Cross-cutting architectural takeaways

1. **Use memory tiers, not one giant context.**
   - Working memory / KV cache for the recent stream.
   - Mid-term episodic/event summaries.
   - Long-term semantic or archival store.
   - Procedural skill library for reusable behaviors.

2. **Separate fast and slow loops.**
   - Fast loop: perception, event detection, policy execution, short responses.
   - Slow loop: reflective reasoning, uncertainty resolution, planning, retrospective summarization.
   - Papers most aligned: ReAct, Quiet-STaR, s1, ThinkStream, OmniLive.

3. **Compress by semantics, not only by truncation.**
   - Strongest recurring idea is to replace old raw tokens/frames with summaries, reflections, reasoning traces, or skills.
   - Papers most aligned: Generative Agents, MemGPT, StreamingLLM, ThinkStream, OmniLive.

4. **Maintain explicit event memory.**
   - For streaming AV, store time-stamped events, entities, changes, and unresolved questions rather than raw frame history alone.
   - Papers most aligned: Generative Agents, Reflexion, ThinkStream.

5. **Add adaptive compute control.**
   - Most live inputs do not deserve long deliberation. Trigger slow reasoning on novelty, uncertainty, safety risk, contradiction, or user escalation.
   - Papers most aligned: Quiet-STaR, s1, ThinkStream.

## Suggested architecture for the target system

- **Perception front-end:** recurrent/streaming visual+audio encoders; bounded KV/state cache (Transformer-XL / Mamba / StreamingLLM ideas).
- **Event layer:** convert raw stream into time-stamped events, entities, salience scores, and short summaries.
- **Memory system:**
  - hot: recent multimodal window
  - warm: episodic event memory + reflections
  - cold: searchable semantic/archive store
  - procedural: tools/skills/macros
- **Controller:** ReAct-style action loop for real-time interaction.
- **Deliberation manager:** adaptive test-time compute budget (s1-style) with optional internal latent reasoning (Quiet-STaR-style).
- **Long-horizon compression:** periodically replace dense old context with event summaries or reasoning-compressed memory (ThinkStream / OmniLive).

## Sources consulted
- ACL Anthology: Transformer-XL, s1
- OpenReview: Reflexion, Mamba, StreamingLLM, StreamingVLM
- ACM UIST: Generative Agents
- arXiv/OpenReview/project pages: ReAct, MemGPT, Voyager, Quiet-STaR, InternLM-XComposer2.5-OmniLive, ThinkStream, Think While Watching
