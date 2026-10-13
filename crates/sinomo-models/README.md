# sinomo-models

Rust-native model runtimes for Sinomo.

This crate is the home for model implementations that run directly inside the Sinomo Rust process. It provides model-specific loaders, preprocessing, inference paths, decode state, and validation utilities for native runtimes. The goal is to make production Sinomo components able to depend on local model execution without going through a Python bridge or an external model server.

Each model module owns the details of its upstream checkpoint format and exposes Rust APIs that higher-level crates can use without knowing those checkpoint internals. Benchmark and validation binaries live here as support tooling for the native runtimes; they are not the purpose of the crate.

## Kokoro-82M (text to speech)

The `kokoro` module targets [`hexgrad/Kokoro-82M`](https://huggingface.co/hexgrad/Kokoro-82M), an
Apache-licensed StyleTTS2 model with an ISTFTNet vocoder. It runs entirely in Rust on Candle: no
Python bridge, no model server, no ONNX runtime.

The model is **language-blind and takes phonemes, not text**. Grapheme-to-phoneme conversion is a
separate concern and deliberately lives outside this crate, so `synthesize` takes a phoneme string
and the checkpoint's own phoneme table is exposed as `PhonemeVocab`.

Implemented pieces:

- `config.json` parsing, including the ALBERT fields the file leaves at their defaults.
- Weight loading straight from the upstream `kokoro-v1_0.pth`, reconstructing `weight_g`/`weight_v`
  weight-normalized convolutions, and voice packs from the bare-tensor `voices/*.pt` files.
- The phoneme-level ALBERT encoder, with the single shared layer group applied 12 times.
- Bidirectional LSTMs, style-conditioned instance and layer norms, and both StyleTTS2 residual
  blocks (`AdainResBlk1d` with its transposed-convolution upsample, `AdaInResBlock1` with learned
  Snake activations).
- The duration encoder and prosody predictor: per-phoneme durations, pitch and energy contours.
- The ISTFTNet vocoder: harmonic-plus-noise excitation, the upsampling stack, and a centered STFT
  pair implemented as a real-DFT matmul so it runs on any Candle device.
- `atan2` and `align_corners=False` linear interpolation, neither of which Candle provides.

Runtime commands:

```sh
# Fetch the checkpoint and one voice into the model cache (about 328 MB).
cargo run -p sinomo-models --release --bin sinomo-models-kokoro -- --fetch

# Synthesize phonemes to a WAV file.
cargo run -p sinomo-models --release --bin sinomo-models-kokoro -- \
  --phonemes "hɛlˈO fɹʌm sɪnˈOmO" --voice af_heart --output /tmp/speech.wav

# Report what the runtime sees.
cargo run -p sinomo-models --release --bin sinomo-models-kokoro -- --list-voices --json
```

Live end-to-end synthesis also runs through the runtime test registry. The cases are registered only
when the CLI is built with `--features native-models`, and each reports what the synthesis cost in its
own diagnostics:

```sh
sinomo test run models.kokoro.tts --live --json
sinomo test run models.kokoro.tts-sentence --live --json
sinomo test run models.kokoro.voices --live --json
```

Measured on this server (Core i9-12900H, CPU, `af_heart`):

| Case | Result |
| --- | --- |
| `models.kokoro.tts` | 1.88 s of audio from 20 tokens in 2.04 s (0.92x real time) |
| `models.kokoro.tts-sentence` | 4.93 s of audio from 79 tokens in 5.11 s (0.96x real time) |
| `models.kokoro.voices` | 4 voices load, synthesize and differ in prosody |

Each case also emits `sinomo.models.tts.*` metrics through `sinomo-observe`. Those reach
`sinomo observe metrics` only when synthesis runs inside the daemon: that surface is served by
daemon-registered exporters reading persisted state, so a short-lived `sinomo test run` process
reports through its diagnostics instead.

### Validation status

`tests/kokoro_reference.rs` compares this runtime against the reference PyTorch implementation on the
real checkpoint, stage by stage. Measured on the phonemes `hɛlˈO fɹʌm sɪnˈOmO` with the `af_heart`
voice:

| Stage | Max difference vs reference |
| --- | ---: |
| ALBERT last hidden state | 1.1e-6 relative |
| Prosody conditioning (`bert_encoder`) | 4.5e-6 relative |
| Pitch contour | 1.1e-5 relative |
| Energy contour | 2.1e-6 relative |
| Text encoder features | 1.6e-6 relative |
| Aligned features | 1.6e-6 relative |
| Decoder trunk (vocoder input) | 1.1e-5 relative |
| Waveform | correlation 0.985, RMS within 0.7%, band energies within 8% |

Predicted per-phoneme durations and the token sequence match exactly.

The waveform is compared spectrally rather than sample by sample, and that is a property of the
architecture rather than a weakened test. The vocoder's excitation integrates the pitch contour into
a harmonic phase, and that integral amplifies single-precision differences without bound: perturbing
the pitch contour by one part in 10^5 moves the excitation by several percent. That is the same
magnitude as the difference between this runtime and the reference — and as the difference between
two PyTorch runs on differently-vectorized CPUs. Two correct implementations therefore agree on what
the audio sounds like, not on its individual samples. Where the two do differ in the interpolation
itself, this runtime is the more accurate one: PyTorch computes `interpolate` source indices in
`float32`, which is off by an ulp at large indices, while this implementation is exact.

Reproducing the comparison needs a fixture set dumped from the reference with its stochastic
excitation disabled; `SynthesisRequest::deterministic()` disables the same terms here.

Current limitations:

- Phonemes only; there is no grapheme-to-phoneme conversion in this crate.
- One utterance at a time. Batched synthesis would work but the masking that padding requires is not
  implemented, since inference never needs it.
- CPU throughput is roughly real time for the whole model, so it is fast enough for offline synthesis
  but leaves no headroom for streaming. The vocoder dominates: 108 dilated convolutions over one STFT
  frame per five output samples.
- Kokoro's phoneme table uses IPA letters, so the voiced velar stop is `ɡ` (U+0261) and not an ASCII
  `g`. Phonemes outside the table are dropped and reported in `SynthesizedSpeech::unknown_phonemes`
  rather than refused.

## MiniCPM-V 4.6

The `minicpm_v46` module targets `openbmb/MiniCPM-V-4_6`.

Implemented pieces:

- Upstream `config.json` parsing.
- Upstream `preprocessor_config.json` parsing and RGB image preprocessing to BCHW `f32`.
- Safetensors header/full-file inspection.
- A native Candle MiniCPM NaViT vision tower, including patch embeddings, packed vision attention, encoder layers, insert-layer window merger, and final post layer norm.
- A Candle wrapper for the raw SigLIP-compatible `model.vision_tower` checkpoint prefix, kept for comparison while the native path matures.
- MiniCPM `vision_tower.vit_merger` window-indexing, local attention, residual restore, and 2x2 MLP merge path.
- MiniCPM final 2x2 downsample MLP merger.
- Qwen3.5 centered RMSNorm, SwiGLU MLP, Gated DeltaNet linear attention, full causal attention, decoder layers, text model, and tied LM-head handling.
- MiniCPM wrapper for image/video feature extraction, text-only logits, image placeholder replacement, and safetensors loading from the upstream checkpoint layout.

Runtime and validation commands:

```sh
cargo run -p sinomo-models --bin sinomo-models-minicpm-v46 -- --iterations 100 --json
cargo run -p sinomo-models --bin sinomo-models-minicpm-v46 -- \
  --config "$HOME/.cache/sinomo-models/openbmb--MiniCPM-V-4_6/config.json" \
  --safetensors "$HOME/.cache/sinomo-models/openbmb--MiniCPM-V-4_6/model.safetensors" \
  --text-forward-check --iterations 1 --json
cargo run -p sinomo-models --bin sinomo-models-minicpm-v46 -- \
  --config "$HOME/.cache/sinomo-models/openbmb--MiniCPM-V-4_6/config.json" \
  --safetensors "$HOME/.cache/sinomo-models/openbmb--MiniCPM-V-4_6/model.safetensors" \
  --image-forward-check --iterations 1 --json
cargo run -p sinomo-models --bin sinomo-models-minicpm-v46 -- \
  --config "$HOME/.cache/sinomo-models/openbmb--MiniCPM-V-4_6/config.json" \
  --safetensors "$HOME/.cache/sinomo-models/openbmb--MiniCPM-V-4_6/model.safetensors" \
  --cached-text-forward-check --iterations 1 --json
```

Current limitations:

- The image placeholder replacement helper currently materializes embeddings on CPU.
- CPU inference is correct but not yet as fast as the PyTorch fallback path for the same model on this server.

Validation status:

- `merger_matches_pytorch_fixture` compares the Rust merger against a PyTorch fixture generated from `transformers.models.minicpmv4_6.modeling_minicpmv4_6.MiniCPMV4_6Merger` math.
- `vit_window_merger_matches_pytorch_zero_projection_fixture` checks the Rust ViT window merger against a PyTorch-derived fixture for the window restore and 2x2 residual merge path.
- `vision_encoder_layer_matches_pytorch_fixture` compares one Rust MiniCPM vision encoder layer against the upstream Transformers implementation with nonzero attention and MLP weights.
- `vision_position_ids_match_pytorch_bucketizing` checks NaViT position-id bucketing against the PyTorch bucketize behavior.
- `qwen3_5_rms_norm_matches_pytorch_fixture` compares centered RMSNorm against the PyTorch Qwen3.5 implementation.
- `qwen3_5_mlp_matches_pytorch_fixture` compares the SwiGLU MLP against the PyTorch Qwen3.5 implementation.
- `qwen3_5_gated_delta_net_matches_pytorch_fixture` compares the Rust Gated DeltaNet fallback recurrence against the upstream PyTorch fallback.
- `qwen3_5_full_attention_matches_pytorch_fixture` compares the Rust full-attention path against upstream PyTorch with RoPE and causal masking.

## Performance Snapshot

The measurements below were captured on this server with a 12th Gen Intel Core i9-12900H, CPU-only execution, and `float32` weights. Rust uses the `target/release/sinomo-models-minicpm-v46` binary with Candle 0.10.2; these figures predate the crate's move to Candle 0.11.0. PyTorch uses `torch 2.11.0+cu130` and `transformers 5.8.0` on CPU; CUDA was not available. Transformers reported that the MiniCPM fast path was unavailable because `flash-linear-attention` and `causal-conv1d` were not installed, so the PyTorch numbers are from its torch fallback implementation.

| Check | Rust Candle | PyTorch/Transformers |
| --- | ---: | ---: |
| Load local `model.safetensors` | 2508 ms | 463 ms |
| Text forward, `input_ids=[[0]]` | 365 ms | 241 ms |
| Synthetic image forward, one `image_token_id`, zero `1x3x28x28` image, `target_sizes=[[2,2]]`, `downsample_mode=4x` | 418 ms | 266 ms |
| Cached text decode, token-by-token ids `[0,1,2]` | 1074 ms | 392 ms |

The Rust benchmark also measured preprocessing a synthetic 980x980 RGB image at 2.88 ms/frame and the small synthetic MiniCPM vision encoder layer at 0.56 ms/iteration.

Rust and PyTorch logits match within normal floating-point tolerance for the same checkpoint and synthetic inputs. Incremental text decode caches are implemented for both Qwen3.5 mixer types: Gated DeltaNet stores the depthwise-conv rolling state plus recurrent delta-rule state, and full attention stores RoPE-applied key/value tensors. On the real checkpoint, Rust token-by-token cached decode for input ids `[0,1,2]` matches full-prefix logits with max absolute difference around `2e-5`.
