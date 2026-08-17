//! Layers Kokoro needs that Candle does not provide.
//!
//! Everything here is shaped by the upstream checkpoint rather than by taste: weight-normalized
//! convolutions store `weight_g`/`weight_v` instead of `weight`, StyleTTS2's `LayerNorm` normalizes
//! the channel axis of a `[B, C, T]` tensor, and the adaptive norms take their scale and shift from
//! a style vector instead of from learned parameters.

use super::{KokoroError, Result};
use candle_core::{D, IndexOp, Module, Tensor};
use candle_nn::{Conv1dConfig, ConvTranspose1dConfig, Linear, VarBuilder, linear};

/// A convolution whose checkpoint stores the weight in normalized form.
///
/// PyTorch's `weight_norm` splits a weight into a direction `weight_v` and a per-output-channel
/// magnitude `weight_g`. Inference only ever needs the product, so it is reconstructed once at load
/// time and the layer behaves like any other convolution afterwards.
#[derive(Debug)]
pub struct WeightNormConv1d {
    weight: Tensor,
    bias: Option<Tensor>,
    config: Conv1dConfig,
}

impl WeightNormConv1d {
    pub fn load(
        in_channels: usize,
        out_channels: usize,
        kernel_size: usize,
        config: Conv1dConfig,
        bias: bool,
        vb: VarBuilder,
    ) -> Result<Self> {
        let weight_v = vb.get(
            (out_channels, in_channels / config.groups, kernel_size),
            "weight_v",
        )?;
        let weight_g = vb.get((out_channels, 1, 1), "weight_g")?;
        let weight = denormalize_weight(&weight_v, &weight_g)?;
        let bias = if bias {
            Some(vb.get(out_channels, "bias")?)
        } else {
            None
        };
        Ok(Self {
            weight,
            bias,
            config,
        })
    }

    pub fn forward(&self, xs: &Tensor) -> Result<Tensor> {
        let xs = xs.conv1d(
            &self.weight,
            self.config.padding,
            self.config.stride,
            self.config.dilation,
            self.config.groups,
        )?;
        match &self.bias {
            None => Ok(xs),
            Some(bias) => Ok(xs.broadcast_add(&bias.reshape((1, (), 1))?)?),
        }
    }
}

/// A transposed convolution whose checkpoint stores the weight in normalized form.
#[derive(Debug)]
pub struct WeightNormConvTranspose1d {
    weight: Tensor,
    bias: Option<Tensor>,
    config: ConvTranspose1dConfig,
}

impl WeightNormConvTranspose1d {
    pub fn load(
        in_channels: usize,
        out_channels: usize,
        kernel_size: usize,
        config: ConvTranspose1dConfig,
        bias: bool,
        vb: VarBuilder,
    ) -> Result<Self> {
        // A transposed convolution stores its weight as `[in, out/groups, k]`, the transpose of the
        // forward layout, and `weight_g` is shaped to match that first axis.
        let weight_v = vb.get(
            (in_channels, out_channels / config.groups, kernel_size),
            "weight_v",
        )?;
        let weight_g = vb.get((in_channels, 1, 1), "weight_g")?;
        let weight = denormalize_weight(&weight_v, &weight_g)?;
        let bias = if bias {
            Some(vb.get(out_channels, "bias")?)
        } else {
            None
        };
        Ok(Self {
            weight,
            bias,
            config,
        })
    }

    pub fn forward(&self, xs: &Tensor) -> Result<Tensor> {
        let xs = xs.conv_transpose1d(
            &self.weight,
            self.config.padding,
            self.config.output_padding,
            self.config.stride,
            self.config.dilation,
            self.config.groups,
        )?;
        match &self.bias {
            None => Ok(xs),
            Some(bias) => Ok(xs.broadcast_add(&bias.reshape((1, (), 1))?)?),
        }
    }
}

/// Rebuilds `weight = g * v / ||v||`, with the norm taken over every axis but the first.
fn denormalize_weight(weight_v: &Tensor, weight_g: &Tensor) -> Result<Tensor> {
    let dims = weight_v.dims().to_vec();
    let flattened = weight_v.reshape((dims[0], ()))?;
    let norm = flattened
        .sqr()?
        .sum_keepdim(D::Minus1)?
        .sqrt()?
        .reshape((dims[0], 1, 1))?;
    Ok(weight_v
        .broadcast_div(&norm)?
        .broadcast_mul(weight_g)?
        .contiguous()?)
}

/// StyleTTS2's channel-axis layer norm over a `[B, C, T]` tensor.
///
/// It is not Candle's `LayerNorm`: the statistics are taken over the channel axis of a
/// channels-first tensor, and the checkpoint names the affine parameters `gamma`/`beta`.
#[derive(Debug)]
pub struct ChannelLayerNorm {
    gamma: Tensor,
    beta: Tensor,
    eps: f64,
}

impl ChannelLayerNorm {
    pub fn load(channels: usize, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            gamma: vb.get(channels, "gamma")?,
            beta: vb.get(channels, "beta")?,
            eps: 1e-5,
        })
    }

    pub fn forward(&self, xs: &Tensor) -> Result<Tensor> {
        let xs = xs.transpose(1, 2)?.contiguous()?;
        let normalized = layer_norm_last_dim(&xs, self.eps)?;
        Ok(normalized
            .broadcast_mul(&self.gamma)?
            .broadcast_add(&self.beta)?
            .transpose(1, 2)?
            .contiguous()?)
    }
}

/// Normalizes the last axis to zero mean and unit variance, without affine parameters.
pub fn layer_norm_last_dim(xs: &Tensor, eps: f64) -> Result<Tensor> {
    let mean = xs.mean_keepdim(D::Minus1)?;
    let centered = xs.broadcast_sub(&mean)?;
    let variance = centered.sqr()?.mean_keepdim(D::Minus1)?;
    Ok(centered.broadcast_div(&(variance + eps)?.sqrt()?)?)
}

/// Normalizes each `[B, C]` channel over time, the inference behaviour of `InstanceNorm1d`.
///
/// The upstream module is built with `affine=True`, but the checkpoint predates that and carries no
/// affine parameters, so they stay at their identity initialization and are omitted here.
pub fn instance_norm(xs: &Tensor, eps: f64) -> Result<Tensor> {
    let mean = xs.mean_keepdim(D::Minus1)?;
    let centered = xs.broadcast_sub(&mean)?;
    let variance = centered.sqr()?.mean_keepdim(D::Minus1)?;
    Ok(centered.broadcast_div(&(variance + eps)?.sqrt()?)?)
}

/// Instance norm whose scale and shift come from a style vector.
#[derive(Debug)]
pub struct AdaIn1d {
    fc: Linear,
}

impl AdaIn1d {
    pub fn load(style_dim: usize, num_features: usize, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            fc: linear(style_dim, num_features * 2, vb.pp("fc"))?,
        })
    }

    /// `xs` is `[B, C, T]` and `style` is `[B, style_dim]`.
    pub fn forward(&self, xs: &Tensor, style: &Tensor) -> Result<Tensor> {
        let (gamma, beta) = self.scale_and_shift(style)?;
        let normalized = instance_norm(xs, 1e-5)?;
        Ok(normalized
            .broadcast_mul(&(gamma + 1.0)?)?
            .broadcast_add(&beta)?)
    }

    fn scale_and_shift(&self, style: &Tensor) -> Result<(Tensor, Tensor)> {
        let h = self.fc.forward(style)?;
        let channels = h.dim(D::Minus1)? / 2;
        let h = h.reshape(((), channels * 2, 1))?;
        let gamma = h.narrow(1, 0, channels)?;
        let beta = h.narrow(1, channels, channels)?;
        Ok((gamma, beta))
    }
}

/// Layer norm whose scale and shift come from a style vector.
///
/// The upstream module transposes twice on entry and twice on exit; for the rank-3 tensors used here
/// those transposes cancel, so this normalizes the last axis of a `[B, T, C]` tensor directly.
#[derive(Debug)]
pub struct AdaLayerNorm {
    fc: Linear,
}

impl AdaLayerNorm {
    pub fn load(style_dim: usize, channels: usize, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            fc: linear(style_dim, channels * 2, vb.pp("fc"))?,
        })
    }

    /// `xs` is `[B, T, C]` and `style` is `[B, style_dim]`.
    pub fn forward(&self, xs: &Tensor, style: &Tensor) -> Result<Tensor> {
        let h = self.fc.forward(style)?;
        let channels = h.dim(D::Minus1)? / 2;
        let h = h.reshape(((), channels * 2, 1))?;
        let gamma = h.narrow(1, 0, channels)?.transpose(1, 2)?;
        let beta = h.narrow(1, channels, channels)?.transpose(1, 2)?;
        let normalized = layer_norm_last_dim(&xs.contiguous()?, 1e-5)?;
        Ok(normalized
            .broadcast_mul(&(gamma + 1.0)?)?
            .broadcast_add(&beta)?)
    }
}

/// A bidirectional single-layer LSTM.
///
/// Candle supplies one direction at a time and always walks the sequence forwards, so the reverse
/// pass runs over a flipped sequence and its output is flipped back before the two halves are
/// concatenated — the layout PyTorch's `bidirectional=True` produces.
#[derive(Debug)]
pub struct BiLstm {
    forward: candle_nn::LSTM,
    backward: candle_nn::LSTM,
}

impl BiLstm {
    pub fn load(in_dim: usize, hidden_dim: usize, vb: VarBuilder) -> Result<Self> {
        let forward = candle_nn::lstm(
            in_dim,
            hidden_dim,
            candle_nn::LSTMConfig {
                direction: candle_nn::rnn::Direction::Forward,
                ..Default::default()
            },
            vb.clone(),
        )?;
        let backward = candle_nn::lstm(
            in_dim,
            hidden_dim,
            candle_nn::LSTMConfig {
                direction: candle_nn::rnn::Direction::Backward,
                ..Default::default()
            },
            vb,
        )?;
        Ok(Self { forward, backward })
    }

    /// `xs` is `[B, T, in_dim]`; the result is `[B, T, 2 * hidden_dim]`.
    pub fn forward(&self, xs: &Tensor) -> Result<Tensor> {
        use candle_nn::RNN;
        let xs = xs.contiguous()?;
        let forward = self.forward.seq(&xs)?;
        let forward = self.forward.states_to_tensor(&forward)?;
        let reversed = flip_sequence(&xs)?;
        let backward = self.backward.seq(&reversed)?;
        let backward = flip_sequence(&self.backward.states_to_tensor(&backward)?)?;
        Ok(Tensor::cat(&[forward, backward], D::Minus1)?.contiguous()?)
    }
}

/// Reverses the time axis of a `[B, T, C]` tensor.
fn flip_sequence(xs: &Tensor) -> Result<Tensor> {
    let steps = xs.dim(1)?;
    let indices = (0..steps)
        .rev()
        .map(|index| index as u32)
        .collect::<Vec<_>>();
    let indices = Tensor::from_vec(indices, steps, xs.device())?;
    Ok(xs.index_select(&indices, 1)?.contiguous()?)
}

/// Repeats each element of the last axis `factor` times.
///
/// This is `Upsample(scale_factor=factor)` in its default nearest mode for an integer factor.
pub fn upsample_nearest_last_dim(xs: &Tensor, factor: usize) -> Result<Tensor> {
    if factor == 1 {
        return Ok(xs.clone());
    }
    let dims = xs.dims().to_vec();
    let last = *dims.last().expect("a tensor has at least one axis");
    let mut expanded = dims.clone();
    expanded.push(factor);
    let mut collapsed = dims.clone();
    let collapsed_last = collapsed.len() - 1;
    collapsed[collapsed_last] = last * factor;
    Ok(xs
        .unsqueeze(D::Minus1)?
        .broadcast_as(expanded)?
        .contiguous()?
        .reshape(collapsed)?)
}

/// Resamples the last axis to `out_len` by linear interpolation.
///
/// This matches `interpolate(mode="linear")` with the default `align_corners=False`: sample centers
/// are mapped by `(index + 0.5) * scale - 0.5` and clamped at the edges.
pub fn interpolate_linear_last_dim(xs: &Tensor, out_len: usize) -> Result<Tensor> {
    let in_len = xs.dim(D::Minus1)?;
    if in_len == out_len {
        return Ok(xs.clone());
    }
    if in_len == 0 || out_len == 0 {
        return Err(KokoroError::InvalidInput {
            detail: "linear interpolation requires non-empty axes".to_string(),
        });
    }
    if in_len == 1 {
        return upsample_nearest_last_dim(xs, out_len);
    }
    let scale = in_len as f64 / out_len as f64;
    let mut left = Vec::with_capacity(out_len);
    let mut right = Vec::with_capacity(out_len);
    let mut weight = Vec::with_capacity(out_len);
    for index in 0..out_len {
        let source = ((index as f64 + 0.5) * scale - 0.5).max(0.0);
        let low = source.floor();
        let low_index = (low as usize).min(in_len - 1);
        let high_index = (low_index + 1).min(in_len - 1);
        left.push(low_index as u32);
        right.push(high_index as u32);
        weight.push((source - low) as f32);
    }
    let device = xs.device();
    let axis = xs.rank() - 1;
    let low = xs.index_select(&Tensor::from_vec(left, out_len, device)?, axis)?;
    let high = xs.index_select(&Tensor::from_vec(right, out_len, device)?, axis)?;
    let weight = Tensor::from_vec(weight, out_len, device)?.to_dtype(xs.dtype())?;
    let delta = (high - &low)?.broadcast_mul(&weight)?;
    Ok((low + delta)?.contiguous()?)
}

/// Pads the last axis by reflecting across the edge samples, excluding the edges themselves.
///
/// This is `pad_mode="reflect"`, the padding `torch.stft` applies when centering frames.
pub fn reflect_pad_last_dim(xs: &Tensor, pad: usize) -> Result<Tensor> {
    if pad == 0 {
        return Ok(xs.clone());
    }
    let len = xs.dim(D::Minus1)?;
    if pad >= len {
        return Err(KokoroError::InvalidInput {
            detail: format!("reflection padding {pad} needs an axis longer than {len}"),
        });
    }
    let axis = xs.rank() - 1;
    let mut indices = Vec::with_capacity(len + 2 * pad);
    for offset in (1..=pad).rev() {
        indices.push(offset as u32);
    }
    indices.extend((0..len).map(|index| index as u32));
    for offset in 1..=pad {
        indices.push((len - 1 - offset) as u32);
    }
    let count = indices.len();
    Ok(xs
        .index_select(&Tensor::from_vec(indices, count, xs.device())?, axis)?
        .contiguous()?)
}

/// Pads the last axis by repeating the first element, matching `ReflectionPad1d((1, 0))` for a
/// single left-hand element.
pub fn reflection_pad_left_one(xs: &Tensor) -> Result<Tensor> {
    let len = xs.dim(D::Minus1)?;
    if len < 2 {
        return Err(KokoroError::InvalidInput {
            detail: "single-element reflection padding needs at least two samples".to_string(),
        });
    }
    let axis = xs.rank() - 1;
    let mut indices = Vec::with_capacity(len + 1);
    indices.push(1_u32);
    indices.extend((0..len).map(|index| index as u32));
    let count = indices.len();
    Ok(xs
        .index_select(&Tensor::from_vec(indices, count, xs.device())?, axis)?
        .contiguous()?)
}

/// `LeakyReLU`, which Candle exposes only through its activation enum.
pub fn leaky_relu(xs: &Tensor, negative_slope: f64) -> Result<Tensor> {
    Ok(candle_nn::ops::leaky_relu(xs, negative_slope)?)
}

/// The Snake activation used by the ISTFTNet residual blocks: `x + sin(a * x)^2 / a`.
///
/// `alpha` is a learned per-channel `[1, C, 1]` parameter, so the periodic term has a different
/// frequency in every channel.
pub fn snake(xs: &Tensor, alpha: &Tensor) -> Result<Tensor> {
    let scaled = xs.broadcast_mul(alpha)?;
    let periodic = scaled.sin()?.sqr()?.broadcast_div(alpha)?;
    Ok((xs + periodic)?)
}

/// Coefficients of the standard minimax polynomial for `atan` on `[-1, 1]`.
///
/// Evaluated in Horner form, this is accurate to about one unit in the last place for `f32`, so the
/// phase it produces is exact at the precision the model runs at.
const ATAN_POLY: [f64; 8] = [
    0.002_866_225_7,
    -0.016_165_736_7,
    0.042_909_613_8,
    -0.075_289_640_0,
    0.106_562_639_3,
    -0.142_088_994_4,
    0.199_935_508_5,
    -0.333_331_452_8,
];

/// `atan` for arguments in `[0, 1]`.
fn atan_unit_interval(xs: &Tensor) -> Result<Tensor> {
    let squared = xs.sqr()?;
    let mut poly = Tensor::full(ATAN_POLY[0], xs.shape(), xs.device())?.to_dtype(xs.dtype())?;
    for coefficient in &ATAN_POLY[1..] {
        poly = ((poly * &squared)? + *coefficient)?;
    }
    Ok((xs + (xs * &squared)?.mul(&poly)?)?)
}

/// The two-argument arctangent, which Candle does not provide.
///
/// The angle is built from the ratio of the smaller to the larger magnitude, which keeps the
/// polynomial inside the interval it is accurate on and avoids dividing by a vanishing component.
/// Quadrant and sign are then restored with selections, following the conventions of
/// `torch.atan2` — including `atan2(0, x)` being `0` for positive `x` and `pi` for negative `x`.
pub fn atan2(y: &Tensor, x: &Tensor) -> Result<Tensor> {
    let abs_y = y.abs()?;
    let abs_x = x.abs()?;
    let larger = abs_y.maximum(&abs_x)?;
    let smaller = abs_y.minimum(&abs_x)?;
    // The floor only matters when both components are zero, where the ratio is defined to be zero.
    let ratio = smaller.broadcast_div(&larger.maximum(f64::MIN_POSITIVE)?)?;
    let angle = atan_unit_interval(&ratio)?;
    let half_pi = std::f64::consts::FRAC_PI_2;
    let angle = abs_y
        .gt(&abs_x)?
        .where_cond(&angle.neg()?.affine(1.0, half_pi)?, &angle)?;
    let angle = x
        .lt(0.0)?
        .where_cond(&angle.neg()?.affine(1.0, std::f64::consts::PI)?, &angle)?;
    let ones = Tensor::ones(angle.shape(), angle.dtype(), angle.device())?;
    let mirror = y.lt(0.0)?.where_cond(&ones.neg()?, &ones)?;
    Ok(angle.mul(&mirror)?)
}

/// Selects one row of a `[N, 1, D]` voice pack as a `[1, D]` style vector.
pub fn select_voice_row(pack: &Tensor, index: usize) -> Result<Tensor> {
    let rows = pack.dim(0)?;
    if index >= rows {
        return Err(KokoroError::InvalidInput {
            detail: format!("voice pack has {rows} rows, which does not reach index {index}"),
        });
    }
    Ok(pack.i(index)?.contiguous()?)
}
