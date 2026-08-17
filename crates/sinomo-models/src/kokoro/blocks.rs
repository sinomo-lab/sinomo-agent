//! The two style-conditioned residual blocks StyleTTS2 builds its decoder and predictor from.
//!
//! Both mix a style vector into every normalization step, which is how one set of weights produces
//! different voices. They differ in activation and in how they change length: `AdainResBlk1d` can
//! double its input with a transposed convolution, while `AdaInResBlock1` keeps length and stacks
//! dilated convolutions.

use super::Result;
use super::nn::{
    AdaIn1d, WeightNormConv1d, WeightNormConvTranspose1d, leaky_relu, snake,
    upsample_nearest_last_dim,
};
use candle_core::Tensor;
use candle_nn::{Conv1dConfig, ConvTranspose1dConfig, VarBuilder};

/// The slope StyleTTS2 uses wherever it applies a leaky rectifier.
const LEAKY_RELU_SLOPE: f64 = 0.2;

/// A style-conditioned residual block that optionally doubles its length.
///
/// The residual and shortcut branches are summed and scaled by `1/sqrt(2)` so that stacking blocks
/// does not grow the activation scale.
#[derive(Debug)]
pub struct AdainResBlk1d {
    norm1: AdaIn1d,
    norm2: AdaIn1d,
    conv1: WeightNormConv1d,
    conv2: WeightNormConv1d,
    /// Present only when the block changes channel count, matching `learned_sc` upstream.
    conv1x1: Option<WeightNormConv1d>,
    /// Present only when the block doubles its length.
    pool: Option<WeightNormConvTranspose1d>,
    upsample: bool,
}

impl AdainResBlk1d {
    pub fn load(
        dim_in: usize,
        dim_out: usize,
        style_dim: usize,
        upsample: bool,
        vb: VarBuilder,
    ) -> Result<Self> {
        let learned_shortcut = dim_in != dim_out;
        let conv1x1 = if learned_shortcut {
            Some(WeightNormConv1d::load(
                dim_in,
                dim_out,
                1,
                Conv1dConfig::default(),
                /*bias*/ false,
                vb.pp("conv1x1"),
            )?)
        } else {
            None
        };
        let pool = if upsample {
            // A depthwise transposed convolution: one group per channel, stride two.
            Some(WeightNormConvTranspose1d::load(
                dim_in,
                dim_in,
                3,
                ConvTranspose1dConfig {
                    padding: 1,
                    output_padding: 1,
                    stride: 2,
                    dilation: 1,
                    groups: dim_in,
                },
                /*bias*/ true,
                vb.pp("pool"),
            )?)
        } else {
            None
        };
        Ok(Self {
            norm1: AdaIn1d::load(style_dim, dim_in, vb.pp("norm1"))?,
            norm2: AdaIn1d::load(style_dim, dim_out, vb.pp("norm2"))?,
            conv1: WeightNormConv1d::load(
                dim_in,
                dim_out,
                3,
                Conv1dConfig {
                    padding: 1,
                    ..Default::default()
                },
                /*bias*/ true,
                vb.pp("conv1"),
            )?,
            conv2: WeightNormConv1d::load(
                dim_out,
                dim_out,
                3,
                Conv1dConfig {
                    padding: 1,
                    ..Default::default()
                },
                /*bias*/ true,
                vb.pp("conv2"),
            )?,
            conv1x1,
            pool,
            upsample,
        })
    }

    pub fn upsamples(&self) -> bool {
        self.upsample
    }

    pub fn forward(&self, xs: &Tensor, style: &Tensor) -> Result<Tensor> {
        let residual = self.residual(xs, style)?;
        let shortcut = self.shortcut(xs)?;
        Ok(((residual + shortcut)? * (0.5_f64).sqrt())?)
    }

    fn residual(&self, xs: &Tensor, style: &Tensor) -> Result<Tensor> {
        let hidden = self.norm1.forward(xs, style)?;
        let hidden = leaky_relu(&hidden, LEAKY_RELU_SLOPE)?;
        let hidden = match &self.pool {
            Some(pool) => pool.forward(&hidden)?,
            None => hidden,
        };
        let hidden = self.conv1.forward(&hidden)?;
        let hidden = self.norm2.forward(&hidden, style)?;
        let hidden = leaky_relu(&hidden, LEAKY_RELU_SLOPE)?;
        self.conv2.forward(&hidden)
    }

    fn shortcut(&self, xs: &Tensor) -> Result<Tensor> {
        let hidden = if self.upsample {
            upsample_nearest_last_dim(xs, 2)?
        } else {
            xs.clone()
        };
        match &self.conv1x1 {
            Some(conv1x1) => conv1x1.forward(&hidden),
            None => Ok(hidden),
        }
    }
}

/// A length-preserving residual stack of dilated convolutions with Snake activations.
///
/// Each of the three stages normalizes with the style vector, applies Snake, convolves with its own
/// dilation, then repeats undilated before adding the input back.
#[derive(Debug)]
pub struct AdaInResBlock1 {
    stages: Vec<AdaInResStage>,
}

#[derive(Debug)]
struct AdaInResStage {
    convs1: WeightNormConv1d,
    convs2: WeightNormConv1d,
    adain1: AdaIn1d,
    adain2: AdaIn1d,
    alpha1: Tensor,
    alpha2: Tensor,
}

impl AdaInResBlock1 {
    pub fn load(
        channels: usize,
        kernel_size: usize,
        dilations: &[usize],
        style_dim: usize,
        vb: VarBuilder,
    ) -> Result<Self> {
        let mut stages = Vec::with_capacity(dilations.len());
        for (index, dilation) in dilations.iter().copied().enumerate() {
            let key = index.to_string();
            stages.push(AdaInResStage {
                convs1: WeightNormConv1d::load(
                    channels,
                    channels,
                    kernel_size,
                    Conv1dConfig {
                        padding: same_padding(kernel_size, dilation),
                        dilation,
                        ..Default::default()
                    },
                    /*bias*/ true,
                    vb.pp("convs1").pp(&key),
                )?,
                convs2: WeightNormConv1d::load(
                    channels,
                    channels,
                    kernel_size,
                    Conv1dConfig {
                        padding: same_padding(kernel_size, 1),
                        ..Default::default()
                    },
                    /*bias*/ true,
                    vb.pp("convs2").pp(&key),
                )?,
                adain1: AdaIn1d::load(style_dim, channels, vb.pp("adain1").pp(&key))?,
                adain2: AdaIn1d::load(style_dim, channels, vb.pp("adain2").pp(&key))?,
                alpha1: vb.get((1, channels, 1), &format!("alpha1.{key}"))?,
                alpha2: vb.get((1, channels, 1), &format!("alpha2.{key}"))?,
            });
        }
        Ok(Self { stages })
    }

    pub fn forward(&self, xs: &Tensor, style: &Tensor) -> Result<Tensor> {
        let mut xs = xs.clone();
        for stage in &self.stages {
            let hidden = stage.adain1.forward(&xs, style)?;
            let hidden = snake(&hidden, &stage.alpha1)?;
            let hidden = stage.convs1.forward(&hidden)?;
            let hidden = stage.adain2.forward(&hidden, style)?;
            let hidden = snake(&hidden, &stage.alpha2)?;
            let hidden = stage.convs2.forward(&hidden)?;
            xs = (hidden + xs)?;
        }
        Ok(xs)
    }
}

/// The padding that keeps a dilated convolution length-preserving.
fn same_padding(kernel_size: usize, dilation: usize) -> usize {
    (kernel_size * dilation - dilation) / 2
}
