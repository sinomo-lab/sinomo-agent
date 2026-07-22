# tau3-bench voice integration

This adapter evaluates Sinomo's realtime audio path with tau3-bench's full-duplex customer-service simulations. tau3-bench remains an external, revision-pinned dependency; no dataset or upstream source is vendored into Sinomo.

The integration consists of:

- `sinomo-tau3-voice-bridge`, a JSON-lines Rust process backed by `sinomo_lm::RealtimeAudioSession`;
- `sinomo_adapter.py`, which converts tau3's 8 kHz mu-law ticks to Sinomo's 24 kHz PCM input/output and maps tool calls, VAD, and interruption truncation;
- `run_tau3_voice.py`, which verifies the upstream revision, selects a fixed suite, and writes a reproducibility manifest and results under `benchmark-results/tau3-voice/`.

## Setup

Use Python 3.12 because tau3-bench requires Python `>=3.12,<3.14` and its telephony converter uses the standard-library `audioop` module.

```bash
git clone https://github.com/sierra-research/tau2-bench.git /tmp/tau3-bench
git -C /tmp/tau3-bench checkout f0927a4b9cdfa7b374269efaa29ff7fdac90d8fc
cd /tmp/tau3-bench
uv sync --python 3.12 --extra voice
```

Follow tau3-bench's voice-persona setup for the user simulator. A live run generally needs OpenAI credentials for the realtime agent and configured user-simulator LLM, ElevenLabs, and Deepgram credentials. The Sinomo bridge resolves OpenAI credentials from the normal `SINOMO_OPENAI_API_KEY`/`OPENAI_API_KEY` environment or Sinomo's node-local credential store.

Build the opt-in bridge:

```bash
cargo build -p sinomo-benchmarks \
  --features tau3-voice-bench \
  --bin sinomo-tau3-voice-bridge
```

## Plan and run

Planning is offline and makes no model calls:

```bash
uv run --python 3.12 \
  modules/benchmark/sinomo-benchmarks/tau3/run_tau3_voice.py \
  --mode plan \
  --tau3-root /tmp/tau3-bench
```

A smoke run executes the first pinned base task in each domain. It makes paid realtime, user-simulator, speech-synthesis, and transcription calls:

```bash
cd /tmp/tau3-bench
uv run python \
  /path/to/sinomo-agent-app/modules/benchmark/sinomo-benchmarks/tau3/run_tau3_voice.py \
  --mode smoke \
  --tau3-root /tmp/tau3-bench
```

Run the checked-in 12-task development suite with `--mode run`. Add `--full-base` for tau3's complete base split. `--domain airline` (repeatable) narrows either run. The default is realistic 20 ms packet pacing; `--send-audio-instant` is an explicit latency-ablation mode.

Each domain retains tau3's directory-format results. The run root also contains `manifest.json` with the exact upstream revision, selected task IDs, bridge hash, Python version, and non-secret configuration, plus a compact `summary.json`. These generated files must not be committed.

No live run is part of default CI. Deterministic validation is:

```bash
cargo test -p sinomo-lm realtime_audio
cargo test -p sinomo-benchmarks \
  --features tau3-voice-bench \
  --bin sinomo-tau3-voice-bridge
python3 -m unittest discover \
  -s modules/benchmark/sinomo-benchmarks/tau3 -p 'test_*.py'
```
