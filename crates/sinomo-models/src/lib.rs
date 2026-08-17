//! Native model runtime experiments.
//!
//! `sinomo-models` is where model-specific Rust/Candle ports live. The first
//! target is MiniCPM-V 4.6. That model is currently a mixed architecture:
//! SigLIP-like vision encoder, MiniCPM projector/packer, and a Qwen3.5 text
//! backbone with linear-attention layers. The crate exposes the supported
//! pieces directly and returns explicit errors for pieces that still need a
//! native implementation.

pub mod kokoro;
pub mod minicpm_v46;
