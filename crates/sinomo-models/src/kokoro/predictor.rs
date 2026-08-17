//! Phoneme encoding and prosody prediction.
//!
//! Two encoders read the same phoneme sequence. `TextEncoder` produces the acoustic content that the
//! decoder turns into sound, while `ProsodyPredictor` decides how long each phoneme lasts and what
//! pitch and energy it carries. Everything here runs on a single un-padded utterance, so the masking
//! and sequence packing the upstream training code performs is a no-op and is left out.

use super::blocks::AdainResBlk1d;
use super::nn::{AdaLayerNorm, BiLstm, ChannelLayerNorm, WeightNormConv1d, leaky_relu};
use super::{KokoroConfig, Result};
use candle_core::{D, Module, Tensor};
use candle_nn::{Embedding, Linear, VarBuilder, embedding, linear};

/// The slope StyleTTS2 uses wherever it applies a leaky rectifier.
const LEAKY_RELU_SLOPE: f64 = 0.2;

/// Encodes phonemes into the acoustic features the decoder consumes.
#[derive(Debug)]
pub struct TextEncoder {
    embedding: Embedding,
    cnn: Vec<(WeightNormConv1d, ChannelLayerNorm)>,
    lstm: BiLstm,
}

impl TextEncoder {
    pub fn load(config: &KokoroConfig, vb: VarBuilder) -> Result<Self> {
        let channels = config.hidden_dim;
        let kernel_size = config.text_encoder_kernel_size;
        let mut cnn = Vec::with_capacity(config.n_layer);
        for index in 0..config.n_layer {
            // Each stage is a `Sequential`, so the convolution is at `.0` and the norm at `.1`.
            let stage = vb.pp("cnn").pp(index.to_string());
            cnn.push((
                WeightNormConv1d::load(
                    channels,
                    channels,
                    kernel_size,
                    candle_nn::Conv1dConfig {
                        padding: (kernel_size - 1) / 2,
                        ..Default::default()
                    },
                    /*bias*/ true,
                    stage.pp("0"),
                )?,
                ChannelLayerNorm::load(channels, stage.pp("1"))?,
            ));
        }
        Ok(Self {
            embedding: embedding(config.n_token, channels, vb.pp("embedding"))?,
            cnn,
            lstm: BiLstm::load(channels, channels / 2, vb.pp("lstm"))?,
        })
    }

    /// `input_ids` is `[B, T]`; the result is `[B, hidden_dim, T]`.
    pub fn forward(&self, input_ids: &Tensor) -> Result<Tensor> {
        let mut hidden = self
            .embedding
            .forward(input_ids)?
            .transpose(1, 2)?
            .contiguous()?;
        for (conv, norm) in &self.cnn {
            hidden = conv.forward(&hidden)?;
            hidden = norm.forward(&hidden)?;
            hidden = leaky_relu(&hidden, LEAKY_RELU_SLOPE)?;
        }
        let hidden = self.lstm.forward(&hidden.transpose(1, 2)?)?;
        Ok(hidden.transpose(1, 2)?.contiguous()?)
    }
}

/// Interleaved recurrent layers and style-conditioned norms that build the duration features.
///
/// The style vector is re-appended after every normalization step, so each recurrent layer sees the
/// voice again rather than having to carry it through.
#[derive(Debug)]
struct DurationEncoder {
    layers: Vec<DurationLayer>,
}

#[derive(Debug)]
enum DurationLayer {
    /// Boxed because a recurrent layer is far larger than a norm, and these sit in one list.
    Lstm(Box<BiLstm>),
    Norm(AdaLayerNorm),
}

impl DurationEncoder {
    fn load(config: &KokoroConfig, vb: VarBuilder) -> Result<Self> {
        let width = config.hidden_dim + config.style_dim;
        let mut layers = Vec::with_capacity(config.n_layer * 2);
        for index in 0..config.n_layer {
            // Upstream appends the recurrent layer and the norm to one list, so their indices
            // interleave: even entries are recurrent, odd entries are norms.
            let lstm = vb.pp("lstms").pp((index * 2).to_string());
            let norm = vb.pp("lstms").pp((index * 2 + 1).to_string());
            layers.push(DurationLayer::Lstm(Box::new(BiLstm::load(
                width,
                config.hidden_dim / 2,
                lstm,
            )?)));
            layers.push(DurationLayer::Norm(AdaLayerNorm::load(
                config.style_dim,
                config.hidden_dim,
                norm,
            )?));
        }
        Ok(Self { layers })
    }

    /// `xs` is `[B, hidden_dim, T]` and the result is `[B, T, hidden_dim + style_dim]`.
    fn forward(&self, xs: &Tensor, style: &Tensor) -> Result<Tensor> {
        let tokens = xs.dim(D::Minus1)?;
        // `[B, style_dim]` broadcast along time, the layout the concatenations below expect.
        let style_over_time = style
            .unsqueeze(D::Minus1)?
            .broadcast_as((style.dim(0)?, style.dim(1)?, tokens))?
            .contiguous()?;
        let mut hidden = Tensor::cat(&[xs, &style_over_time], 1)?.contiguous()?;
        for layer in &self.layers {
            hidden = match layer {
                DurationLayer::Lstm(lstm) => lstm
                    .forward(&hidden.transpose(1, 2)?)?
                    .transpose(1, 2)?
                    .contiguous()?,
                DurationLayer::Norm(norm) => {
                    let normalized = norm.forward(&hidden.transpose(1, 2)?, style)?;
                    let normalized = normalized.transpose(1, 2)?.contiguous()?;
                    Tensor::cat(&[&normalized, &style_over_time], 1)?.contiguous()?
                }
            };
        }
        Ok(hidden.transpose(1, 2)?.contiguous()?)
    }
}

/// Predicted prosody for one utterance.
pub struct Prosody {
    /// Per-phoneme frame counts.
    pub durations: Vec<usize>,
    /// Pitch contour, `[B, 2 * frames]`.
    pub f0: Tensor,
    /// Energy contour, `[B, 2 * frames]`.
    pub energy: Tensor,
    /// The `[1, tokens, frames]` one-hot matrix that stretched the durations onto the frame grid.
    ///
    /// The acoustic features have to be stretched by the very same matrix, so it is returned rather
    /// than rebuilt from `durations` by the caller.
    pub alignment: Tensor,
}

#[derive(Debug)]
pub struct ProsodyPredictor {
    text_encoder: DurationEncoder,
    lstm: BiLstm,
    duration_proj: Linear,
    shared: BiLstm,
    f0_blocks: Vec<AdainResBlk1d>,
    n_blocks: Vec<AdainResBlk1d>,
    f0_proj: candle_nn::Conv1d,
    n_proj: candle_nn::Conv1d,
}

impl ProsodyPredictor {
    pub fn load(config: &KokoroConfig, vb: VarBuilder) -> Result<Self> {
        let hidden = config.hidden_dim;
        let style = config.style_dim;
        let width = hidden + style;
        let conv_config = candle_nn::Conv1dConfig::default();
        // The pitch and energy heads have the same shape: keep width, halve it while doubling
        // length, then keep width again.
        let contour_blocks = |vb: VarBuilder| -> Result<Vec<AdainResBlk1d>> {
            Ok(vec![
                AdainResBlk1d::load(hidden, hidden, style, /*upsample*/ false, vb.pp("0"))?,
                AdainResBlk1d::load(
                    hidden,
                    hidden / 2,
                    style,
                    /*upsample*/ true,
                    vb.pp("1"),
                )?,
                AdainResBlk1d::load(
                    hidden / 2,
                    hidden / 2,
                    style,
                    /*upsample*/ false,
                    vb.pp("2"),
                )?,
            ])
        };
        Ok(Self {
            text_encoder: DurationEncoder::load(config, vb.pp("text_encoder"))?,
            lstm: BiLstm::load(width, hidden / 2, vb.pp("lstm"))?,
            duration_proj: linear(
                hidden,
                config.max_dur,
                vb.pp("duration_proj").pp("linear_layer"),
            )?,
            shared: BiLstm::load(width, hidden / 2, vb.pp("shared"))?,
            f0_blocks: contour_blocks(vb.pp("F0"))?,
            n_blocks: contour_blocks(vb.pp("N"))?,
            f0_proj: candle_nn::conv1d(hidden / 2, 1, 1, conv_config, vb.pp("F0_proj"))?,
            n_proj: candle_nn::conv1d(hidden / 2, 1, 1, conv_config, vb.pp("N_proj"))?,
        })
    }

    /// Predicts durations and contours from the encoder features `xs` (`[B, hidden_dim, T]`).
    ///
    /// `speed` divides the predicted durations, so a larger value produces shorter audio.
    pub fn forward(&self, xs: &Tensor, style: &Tensor, speed: f32) -> Result<Prosody> {
        let features = self.text_encoder.forward(xs, style)?;
        let durations = self.durations(&features, speed)?;
        let alignment = alignment_matrix(&durations, features.dim(1)?, xs.device())?;
        // Expanding the duration features onto the frame grid is a matrix product with the
        // one-hot alignment rather than an explicit repeat.
        let aligned = features.transpose(1, 2)?.contiguous()?.matmul(&alignment)?;
        let (f0, energy) = self.contours(&aligned, style)?;
        Ok(Prosody {
            durations,
            f0,
            energy,
            alignment,
        })
    }

    fn durations(&self, features: &Tensor, speed: f32) -> Result<Vec<usize>> {
        let hidden = self.lstm.forward(features)?;
        let projected = self.duration_proj.forward(&hidden)?;
        // Each of the `max_dur` outputs is an independent "lasts at least this long" probability, so
        // the expected duration is their sum rather than an argmax.
        let expected = candle_nn::ops::sigmoid(&projected)?.sum(D::Minus1)?;
        let expected = (expected / f64::from(speed))?;
        let frames = expected.flatten_all()?.to_vec1::<f32>()?;
        Ok(frames
            .into_iter()
            .map(|frames| (frames.round() as i64).max(1) as usize)
            .collect())
    }

    fn contours(&self, aligned: &Tensor, style: &Tensor) -> Result<(Tensor, Tensor)> {
        let shared = self
            .shared
            .forward(&aligned.transpose(1, 2)?.contiguous()?)?
            .transpose(1, 2)?
            .contiguous()?;
        let mut f0 = shared.clone();
        for block in &self.f0_blocks {
            f0 = block.forward(&f0, style)?;
        }
        let f0 = self.f0_proj.forward(&f0)?.squeeze(1)?;
        let mut energy = shared;
        for block in &self.n_blocks {
            energy = block.forward(&energy, style)?;
        }
        let energy = self.n_proj.forward(&energy)?.squeeze(1)?;
        Ok((f0, energy))
    }
}

/// Builds the `[tokens, frames]` one-hot matrix that stretches each phoneme over its frames.
fn alignment_matrix(
    durations: &[usize],
    tokens: usize,
    device: &candle_core::Device,
) -> Result<Tensor> {
    let frames: usize = durations.iter().sum();
    let mut matrix = vec![0_f32; tokens * frames];
    let mut frame = 0;
    for (token, duration) in durations.iter().copied().enumerate() {
        for _ in 0..duration {
            matrix[token * frames + frame] = 1.0;
            frame += 1;
        }
    }
    Ok(Tensor::from_vec(matrix, (1, tokens, frames), device)?)
}
