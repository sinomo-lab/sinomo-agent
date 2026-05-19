# sinomo-models

Rust-native model runtimes for Sinomo.

This crate is the home for model implementations that run directly inside the Sinomo Rust process. It provides model-specific loaders, preprocessing, inference paths, decode state, and validation utilities for native runtimes. The goal is to make production Sinomo components able to depend on local model execution without going through a Python bridge or an external model server.

Each model module owns the details of its upstream checkpoint format and exposes Rust APIs that higher-level crates can use without knowing those checkpoint internals. Benchmark and validation binaries live here as support tooling for the native runtimes; they are not the purpose of the crate.

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

The measurements below were captured on this server with a 12th Gen Intel Core i9-12900H, CPU-only execution, and `float32` weights. Rust uses the `target/release/sinomo-models-minicpm-v46` binary with Candle 0.10.2. PyTorch uses `torch 2.11.0+cu130` and `transformers 5.8.0` on CPU; CUDA was not available. Transformers reported that the MiniCPM fast path was unavailable because `flash-linear-attention` and `causal-conv1d` were not installed, so the PyTorch numbers are from its torch fallback implementation.

| Check | Rust Candle | PyTorch/Transformers |
| --- | ---: | ---: |
| Load local `model.safetensors` | 2508 ms | 463 ms |
| Text forward, `input_ids=[[0]]` | 365 ms | 241 ms |
| Synthetic image forward, one `image_token_id`, zero `1x3x28x28` image, `target_sizes=[[2,2]]`, `downsample_mode=4x` | 418 ms | 266 ms |
| Cached text decode, token-by-token ids `[0,1,2]` | 1074 ms | 392 ms |

The Rust benchmark also measured preprocessing a synthetic 980x980 RGB image at 2.88 ms/frame and the small synthetic MiniCPM vision encoder layer at 0.56 ms/iteration.

Rust and PyTorch logits match within normal floating-point tolerance for the same checkpoint and synthetic inputs. Incremental text decode caches are implemented for both Qwen3.5 mixer types: Gated DeltaNet stores the depthwise-conv rolling state plus recurrent delta-rule state, and full attention stores RoPE-applied key/value tensors. On the real checkpoint, Rust token-by-token cached decode for input ids `[0,1,2]` matches full-prefix logits with max absolute difference around `2e-5`.
