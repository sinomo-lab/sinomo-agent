# Streaming Multimodal Perception for Live Audio/Video AI Systems

## Scope
Literature shortlist for agentic systems that must process streaming audio/video, react to live events, and still support longer-horizon reasoning and memory.

## Key papers/systems

1. **Online Action Detection** (2016, ECCV)
   - **Contribution:** Formalized online action detection as a causal, per-frame problem and introduced the TVSeries benchmark/evaluation protocol. It made explicit the constraint that models must decide from current and past frames only.
   - **Why it matters architecturally:** This is the cleanest early statement of the *causal perception* problem. It anchors design choices around bounded-latency inference, incremental state updates, and “no future context” deployment.

2. **Temporal Recurrent Networks for Online Action Detection** (2019, ICCV)
   - **Contribution:** TRN improved causal action detection by jointly recognizing the current action and anticipating the immediate future, using predicted future context as a training/inference aid.
   - **Why it matters architecturally:** Useful pattern for live agents: a streaming perception model can maintain a small predictive world model, not just a passive state tracker. Anticipation helps disambiguate partial observations.

3. **Long Short-Term Transformer for Online Action Detection (LSTR)** (2021, NeurIPS Spotlight)
   - **Contribution:** Introduced a long-/short-term memory transformer, with a coarse long-history encoder and a fine short-window decoder, achieving strong online action detection results.
   - **Why it matters architecturally:** Very relevant template for live video stacks: compress distant history aggressively, but preserve high-resolution short-term state for current decisions.

4. **Look, Listen and Learn** (2017, ICCV)
   - **Contribution:** Introduced Audio-Visual Correspondence (AVC) self-supervision from unlabeled video, learning aligned visual/audio representations and cross-modal localization.
   - **Why it matters architecturally:** Foundational for building shared audio-video encoders before downstream streaming tasks. It shows that temporal co-occurrence itself is a strong alignment signal.

5. **Audio-Visual Event Localization in Unconstrained Videos** (2018, ECCV)
   - **Contribution:** Introduced the AVE dataset and a family of AV event localization tasks, with audio-guided visual attention and dual multimodal residual fusion.
   - **Why it matters architecturally:** A key reference for event-centric AV fusion: align audio and vision in time, let one modality guide attention over the other, and fuse only after temporal grounding.

6. **UniAV: Unified Audio-Visual Perception for Multi-Task Video Event Localization** (2025, TPAMI; arXiv 2024)
   - **Contribution:** Unified temporal action localization, sound event detection, and audio-visual event localization with a shared AV encoder, task-specific experts, and a language-aware classifier.
   - **Why it matters architecturally:** Strong argument against siloed pipelines. For live systems, a shared streaming backbone plus lightweight task experts can support multiple event heads without fully separate models.

7. **Emformer: Efficient Memory Transformer Based Acoustic Model for Low Latency Streaming Speech Recognition** (2021, ICASSP)
   - **Contribution:** Proposed a memory transformer for streaming ASR with augmented memory banks, cached left-context computation, and parallel block training; it reduced latency and compute while retaining strong accuracy.
   - **Why it matters architecturally:** One of the clearest blueprints for low-latency speech frontends. The memory-bank + chunked-processing recipe is directly reusable for always-on audio perception.

8. **DCTX-Conformer: Dynamic context carry-over for low latency unified streaming and non-streaming Conformer ASR** (2023, Interspeech)
   - **Contribution:** Added dynamic context carry-over to unified Conformer ASR, reducing the gap between streaming and full-context decoding with minimal latency cost.
   - **Why it matters architecturally:** Important if you want one speech stack for both live interaction and offline reprocessing. It suggests a unified deployment model rather than maintaining separate streaming/offline recognizers.

9. **VideoLLM-online: Online Video Large Language Model for Streaming Video** (2024, CVPR)
   - **Contribution:** One of the first dedicated streaming Video-LLM systems; the LIVE framework includes a streaming language-modeling objective, offline-to-streaming data conversion, and an optimized real-time inference pipeline.
   - **Why it matters architecturally:** Important systems paper for turning offline video-language models into live assistants. Especially relevant for temporally aligned dialogue over continuous video.

10. **Flash-VStream: Memory-Based Real-Time Understanding for Long Video Streams** (2024, arXiv; later ICCV 2025 project update)
   - **Contribution:** Proposed a memory-based real-time video-language model for very long streams, with separate frame-handling and question-handling processes and a persistent STAR memory.
   - **Why it matters architecturally:** Excellent reference for *decoupling ingestion from QA*. Live agents should not block frame ingestion while reasoning; this split-process design is highly practical.

11. **Streaming Long Video Understanding with Large Language Models (VideoStreaming)** (2024, NeurIPS)
   - **Contribution:** Introduced memory-propagated streaming encoding and adaptive memory selection so arbitrarily long video can be processed with a constant token budget.
   - **Why it matters architecturally:** Strong design for long-horizon reasoning under tight context limits. The main lesson is to separate continual encoding from query-time memory retrieval.

12. **InternLM-XComposer2.5-OmniLive** (2024, arXiv)
   - **Contribution:** Built a full streaming audio-video system with explicitly disentangled modules for streaming perception, multimodal long memory, and reasoning.
   - **Why it matters architecturally:** Probably the most directly relevant system paper for an *agentic* live AV assistant. It argues that perception, memory, and reasoning should be concurrent cooperating modules, not one monolithic seq2seq loop.

## Most useful architectural patterns that recur

- **Causal perception with no future access:** Online Action Detection, TRN, LSTR.
- **Predict-while-perceiving:** TRN, PreFM-style ideas, anticipation heads.
- **Hierarchical memory:** LSTR, Flash-VStream, VideoStreaming, VideoChat-Online/OmniLive family.
- **Decoupled ingestion vs reasoning:** Flash-VStream, VideoStreaming, OmniLive.
- **Shared AV backbones with specialized heads/experts:** Look Listen Learn, AVE, UniAV.
- **Chunked audio models with carry-over memory:** Emformer, DCTX-Conformer.
- **Offline-to-online training conversion:** VideoLLM-online, VideoChat-Online, LiveCC.
- **Speech as streaming supervision/context for video:** LiveCC.

## High-value honorable mentions

- **LiveCC: Learning Video LLM with Streaming Speech Transcription at Scale** (CVPR 2025)
  - Densely interleaves ASR words and frames by timestamp; especially relevant for sports, meetings, tutorials, and any speech-rich live video.
- **Online Video Understanding: OVBench and VideoChat-Online** (CVPR 2025)
  - Strong benchmark + Pyramid Memory Bank architecture for online video perception/memory/reasoning.
- **VITA-1.5: Towards GPT-4o Level Real-Time Vision and Speech Interaction** (NeurIPS 2025)
  - End-to-end near-real-time vision/speech interaction without separate ASR/TTS modules; relevant if the agent will answer in speech.

## Practical takeaways for a system design

1. **Use separate streaming encoders for audio and video**, each causal and chunked.
2. **Maintain at least two memory tiers:** short-term high-resolution state + long-term compressed episodic memory.
3. **Decouple perception from reasoning** so ingestion never stalls while the agent thinks/responds.
4. **Add event/action heads before the LLM layer** for low-latency triggers and interrupts.
5. **Treat speech transcripts/timestamps as first-class streaming signals**, not just post-hoc metadata.
6. **Retrieve compressed memories into the reasoner on demand** instead of replaying raw streams into the LLM.
