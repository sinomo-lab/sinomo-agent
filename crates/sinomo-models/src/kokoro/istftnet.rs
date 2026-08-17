//! The ISTFTNet vocoder that turns aligned features into a waveform.
//!
//! Two signals meet here. The upsampling stack carries the acoustic content, and a harmonic
//! excitation built from the predicted pitch is injected at every stage — that is what makes voiced
//! speech periodic rather than buzzy. The final convolution predicts a spectrogram, and an inverse
//! STFT produces samples, which is why this costs far less than predicting samples directly.

use super::blocks::{AdaInResBlock1, AdainResBlk1d};
use super::nn::{
    WeightNormConv1d, WeightNormConvTranspose1d, interpolate_linear_last_dim, leaky_relu,
    reflection_pad_left_one, upsample_nearest_last_dim,
};
use super::stft::Stft;
use super::{IstftNetConfig, KokoroConfig, KokoroError, Result};
use candle_core::{D, DType, Device, Module, Tensor};
use candle_nn::{Conv1dConfig, ConvTranspose1dConfig, Linear, VarBuilder, linear};

/// The rate the harmonic source is generated at, which is also the output sample rate.
pub const SAMPLE_RATE: usize = 24_000;
/// Harmonics above the fundamental in the excitation.
const HARMONIC_COUNT: usize = 8;
/// Amplitude of each sine component.
const SINE_AMPLITUDE: f64 = 0.1;
/// Pitch below this is treated as unvoiced and its excitation is muted.
const VOICED_THRESHOLD: f64 = 10.0;
/// The slope the generator's rectifiers use between upsampling stages.
const UPSAMPLE_LEAKY_SLOPE: f64 = 0.1;
/// The slope of the final rectifier before the spectrogram projection.
const POST_LEAKY_SLOPE: f64 = 0.01;

/// Standard deviation of the excitation noise inside voiced regions.
const VOICED_NOISE_STD: f64 = 0.003;

/// How the stochastic part of the harmonic excitation is produced.
///
/// The published model randomizes the initial phase of each overtone and adds noise to the
/// excitation — the noise is what gives unvoiced sounds their aperiodic character, so it is not
/// decoration. It also makes output differ run to run, which a comparison against a reference
/// implementation cannot tolerate, hence the second mode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ExcitationNoise {
    /// Random overtone phase and additive noise, as published.
    #[default]
    Stochastic,
    /// Both stochastic terms zeroed, so synthesis is a pure function of its inputs.
    Disabled,
}

/// Builds the harmonic excitation the vocoder is driven by.
///
/// The sine phases are accumulated at frame rate and then interpolated up to sample rate, which
/// keeps the pitch continuous across frame boundaries instead of restarting each frame.
#[derive(Debug)]
struct HarmonicSource {
    merge: Linear,
    upsample_scale: usize,
}

impl HarmonicSource {
    fn load(upsample_scale: usize, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            merge: linear(HARMONIC_COUNT + 1, 1, vb.pp("l_linear"))?,
            upsample_scale,
        })
    }

    /// `f0` is `[B, samples, 1]` at sample rate; the result is `[B, samples]`.
    fn forward(&self, f0: &Tensor, noise: ExcitationNoise) -> Result<Tensor> {
        let (batch, samples, _) = f0.dims3()?;
        let harmonics = HARMONIC_COUNT + 1;
        let multipliers = (1..=harmonics)
            .map(|harmonic| harmonic as f32)
            .collect::<Vec<_>>();
        let multipliers =
            Tensor::from_vec(multipliers, (1, 1, harmonics), f0.device())?.to_dtype(f0.dtype())?;
        let scaled = f0.broadcast_mul(&multipliers)?;
        // Phase per sample, as a fraction of a turn; the whole-turn part is irrelevant to a sine.
        let mut radians = fractional_part(&(scaled / SAMPLE_RATE as f64)?)?;
        if noise == ExcitationNoise::Stochastic {
            // Each overtone starts at its own random phase; the fundamental does not, so that the
            // pitch onset stays where the predictor put it.
            let offsets = Tensor::rand(0_f32, 1_f32, (batch, 1, harmonics), f0.device())?
                .to_dtype(f0.dtype())?;
            let keep_fundamental = std::iter::once(0_f32)
                .chain(std::iter::repeat_n(1_f32, harmonics - 1))
                .collect::<Vec<_>>();
            let keep_fundamental =
                Tensor::from_vec(keep_fundamental, (1, 1, harmonics), f0.device())?
                    .to_dtype(f0.dtype())?;
            let offsets = offsets.broadcast_mul(&keep_fundamental)?;
            let first = radians.narrow(1, 0, 1)?.broadcast_add(&offsets)?;
            let rest = radians.narrow(1, 1, samples - 1)?;
            radians = Tensor::cat(&[first, rest], 1)?.contiguous()?;
        }
        let frames = samples / self.upsample_scale;
        if frames == 0 {
            return Err(KokoroError::InvalidInput {
                detail: format!(
                    "a {samples}-sample pitch contour is shorter than one {}-sample frame",
                    self.upsample_scale
                ),
            });
        }
        // Accumulate at frame rate, then stretch back out: summing per-sample would drift.
        let per_frame =
            interpolate_linear_last_dim(&radians.transpose(1, 2)?.contiguous()?, frames)?;
        let phase = (per_frame.cumsum(D::Minus1)? * (2.0 * std::f64::consts::PI))?;
        let phase = interpolate_linear_last_dim(
            &(phase * self.upsample_scale as f64)?,
            frames * self.upsample_scale,
        )?;
        let sines = (phase.transpose(1, 2)?.contiguous()?.sin()? * SINE_AMPLITUDE)?;
        let voiced = f0.gt(VOICED_THRESHOLD)?.to_dtype(f0.dtype())?;
        let mut excited = sines.broadcast_mul(&voiced)?;
        if noise == ExcitationNoise::Stochastic {
            // Unvoiced regions get much louder noise than voiced ones: with no periodic component
            // left, the noise *is* the excitation there.
            let unvoiced_amplitude = SINE_AMPLITUDE / 3.0;
            let amplitude =
                voiced.affine(VOICED_NOISE_STD - unvoiced_amplitude, unvoiced_amplitude)?;
            let samples_noise = Tensor::randn(0_f32, 1_f32, excited.shape(), excited.device())?
                .to_dtype(excited.dtype())?;
            excited = (excited + samples_noise.broadcast_mul(&amplitude)?)?;
        }
        let merged = self.merge.forward(&excited)?.tanh()?;
        Ok(merged.reshape((batch, ()))?)
    }
}

/// Keeps the fractional part of each element, the behaviour of a positive modulo one.
fn fractional_part(xs: &Tensor) -> Result<Tensor> {
    Ok((xs - xs.floor()?)?)
}

#[derive(Debug)]
struct UpsampleStage {
    up: WeightNormConvTranspose1d,
    noise_conv: candle_nn::Conv1d,
    noise_res: AdaInResBlock1,
    resblocks: Vec<AdaInResBlock1>,
}

#[derive(Debug)]
pub struct Generator {
    source: HarmonicSource,
    stages: Vec<UpsampleStage>,
    conv_post: WeightNormConv1d,
    stft: Stft,
    /// Total stretch from a pitch frame to a sample.
    upsample_scale: usize,
    bins: usize,
}

impl Generator {
    pub fn load(
        config: &IstftNetConfig,
        style_dim: usize,
        device: &Device,
        dtype: DType,
        vb: VarBuilder,
    ) -> Result<Self> {
        let upsample_scale =
            config.upsample_rates.iter().product::<usize>() * config.gen_istft_hop_size;
        let kernels = config.resblock_kernel_sizes.len();
        if config.resblock_dilation_sizes.len() != kernels {
            return Err(KokoroError::Unsupported {
                detail: "each ISTFTNet residual kernel needs its own dilation schedule".to_string(),
            });
        }
        let mut stages = Vec::with_capacity(config.upsample_rates.len());
        let mut channels = config.upsample_initial_channel;
        for (index, (&rate, &kernel)) in config
            .upsample_rates
            .iter()
            .zip(&config.upsample_kernel_sizes)
            .enumerate()
        {
            let out_channels = config.upsample_initial_channel / 2_usize.pow(index as u32 + 1);
            let up = WeightNormConvTranspose1d::load(
                channels,
                out_channels,
                kernel,
                ConvTranspose1dConfig {
                    padding: (kernel - rate) / 2,
                    stride: rate,
                    ..Default::default()
                },
                /*bias*/ true,
                vb.pp("ups").pp(index.to_string()),
            )?;
            let mut resblocks = Vec::with_capacity(kernels);
            for (kernel_index, (&kernel, dilations)) in config
                .resblock_kernel_sizes
                .iter()
                .zip(&config.resblock_dilation_sizes)
                .enumerate()
            {
                resblocks.push(AdaInResBlock1::load(
                    out_channels,
                    kernel,
                    dilations,
                    style_dim,
                    vb.pp("resblocks")
                        .pp((index * kernels + kernel_index).to_string()),
                )?);
            }
            // Every stage but the last resamples the excitation down to its own rate; the last one
            // already matches and takes it through a pointwise convolution.
            let remaining = config.upsample_rates[index + 1..].iter().product::<usize>();
            let last = index + 1 == config.upsample_rates.len();
            let source_channels = config.gen_istft_n_fft + 2;
            let (noise_conv, noise_kernel) = if last {
                (
                    candle_nn::conv1d(
                        source_channels,
                        out_channels,
                        1,
                        Conv1dConfig::default(),
                        vb.pp("noise_convs").pp(index.to_string()),
                    )?,
                    11,
                )
            } else {
                (
                    candle_nn::conv1d(
                        source_channels,
                        out_channels,
                        remaining * 2,
                        Conv1dConfig {
                            stride: remaining,
                            padding: remaining.div_ceil(2),
                            ..Default::default()
                        },
                        vb.pp("noise_convs").pp(index.to_string()),
                    )?,
                    7,
                )
            };
            stages.push(UpsampleStage {
                up,
                noise_conv,
                noise_res: AdaInResBlock1::load(
                    out_channels,
                    noise_kernel,
                    &[1, 3, 5],
                    style_dim,
                    vb.pp("noise_res").pp(index.to_string()),
                )?,
                resblocks,
            });
            channels = out_channels;
        }
        let bins = config.gen_istft_n_fft / 2 + 1;
        Ok(Self {
            source: HarmonicSource::load(upsample_scale, vb.pp("m_source"))?,
            stages,
            conv_post: WeightNormConv1d::load(
                channels,
                config.gen_istft_n_fft + 2,
                7,
                Conv1dConfig {
                    padding: 3,
                    ..Default::default()
                },
                /*bias*/ true,
                vb.pp("conv_post"),
            )?,
            stft: Stft::new(
                config.gen_istft_n_fft,
                config.gen_istft_hop_size,
                device,
                dtype,
            )?,
            upsample_scale,
            bins,
        })
    }

    /// `xs` is `[B, C, frames]`, `style` is `[B, style_dim]`, and `f0` is `[B, frames]`.
    pub fn forward(
        &self,
        xs: &Tensor,
        style: &Tensor,
        f0: &Tensor,
        noise: ExcitationNoise,
    ) -> Result<Tensor> {
        let excitation = self.excitation(f0, noise)?;
        let mut hidden = xs.clone();
        let last_stage = self.stages.len() - 1;
        for (index, stage) in self.stages.iter().enumerate() {
            hidden = leaky_relu(&hidden, UPSAMPLE_LEAKY_SLOPE)?;
            let source = stage.noise_conv.forward(&excitation)?;
            let source = stage.noise_res.forward(&source, style)?;
            hidden = stage.up.forward(&hidden)?;
            if index == last_stage {
                // One extra frame so the stack lines up with the excitation's centered frames.
                hidden = reflection_pad_left_one(&hidden)?;
            }
            hidden = (hidden + source)?;
            let mut summed: Option<Tensor> = None;
            for resblock in &stage.resblocks {
                let block = resblock.forward(&hidden, style)?;
                summed = Some(match summed {
                    None => block,
                    Some(total) => (total + block)?,
                });
            }
            let count = stage.resblocks.len() as f64;
            hidden = (summed.expect("a stage has at least one residual block") / count)?;
        }
        let hidden = leaky_relu(&hidden, POST_LEAKY_SLOPE)?;
        let projected = self.conv_post.forward(&hidden)?;
        // The projection predicts a log-magnitude and a phase, which the inverse transform turns
        // into samples.
        let magnitude = projected.narrow(1, 0, self.bins)?.exp()?;
        let phase = projected
            .narrow(1, self.bins, projected.dim(1)? - self.bins)?
            .sin()?;
        self.stft.inverse(&magnitude, &phase)
    }

    /// Turns the pitch contour into the stacked magnitude and phase of the harmonic excitation.
    fn excitation(&self, f0: &Tensor, noise: ExcitationNoise) -> Result<Tensor> {
        let upsampled = upsample_nearest_last_dim(&f0.unsqueeze(1)?, self.upsample_scale)?;
        let source = self
            .source
            .forward(&upsampled.transpose(1, 2)?.contiguous()?, noise)?;
        let (magnitude, phase) = self.stft.transform(&source)?;
        Ok(Tensor::cat(&[magnitude, phase], 1)?.contiguous()?)
    }
}

/// Combines aligned acoustic features with predicted prosody and runs the vocoder.
#[derive(Debug)]
pub struct Decoder {
    f0_conv: WeightNormConv1d,
    n_conv: WeightNormConv1d,
    asr_res: WeightNormConv1d,
    encode: AdainResBlk1d,
    decode: Vec<AdainResBlk1d>,
    generator: Generator,
}

impl Decoder {
    pub fn load(
        config: &KokoroConfig,
        device: &Device,
        dtype: DType,
        vb: VarBuilder,
    ) -> Result<Self> {
        let style = config.style_dim;
        let hidden = config.hidden_dim;
        // A halved-stride convolution over the contours, so they meet the features at frame rate.
        let contour_conv = |vb: VarBuilder| -> Result<WeightNormConv1d> {
            WeightNormConv1d::load(
                1,
                1,
                3,
                Conv1dConfig {
                    padding: 1,
                    stride: 2,
                    ..Default::default()
                },
                /*bias*/ true,
                vb,
            )
        };
        // The residual path re-appends the acoustic features and both contours to every block, so
        // each block sees the same conditioning: content, pitch and energy.
        let residual_width = DECODE_CHANNELS + ASR_RESIDUAL_CHANNELS + 2;
        let mut decode = Vec::with_capacity(4);
        for index in 0..3 {
            decode.push(AdainResBlk1d::load(
                residual_width,
                DECODE_CHANNELS,
                style,
                /*upsample*/ false,
                vb.pp("decode").pp(index.to_string()),
            )?);
        }
        decode.push(AdainResBlk1d::load(
            residual_width,
            config.istftnet.upsample_initial_channel,
            style,
            /*upsample*/ true,
            vb.pp("decode").pp("3"),
        )?);
        Ok(Self {
            f0_conv: contour_conv(vb.pp("F0_conv"))?,
            n_conv: contour_conv(vb.pp("N_conv"))?,
            asr_res: WeightNormConv1d::load(
                hidden,
                ASR_RESIDUAL_CHANNELS,
                1,
                Conv1dConfig::default(),
                /*bias*/ true,
                vb.pp("asr_res").pp("0"),
            )?,
            encode: AdainResBlk1d::load(
                hidden + 2,
                DECODE_CHANNELS,
                style,
                /*upsample*/ false,
                vb.pp("encode"),
            )?,
            decode,
            generator: Generator::load(&config.istftnet, style, device, dtype, vb.pp("generator"))?,
        })
    }

    /// `asr` is `[B, hidden_dim, frames]`, the contours are `[B, 2 * frames]`, `style` is
    /// `[B, style_dim]`.
    ///
    /// Returns the waveform `[B, samples]` and the features that entered the vocoder, which are the
    /// last boundary before the harmonic excitation makes the output phase-sensitive.
    pub fn forward(
        &self,
        asr: &Tensor,
        f0_curve: &Tensor,
        energy: &Tensor,
        style: &Tensor,
        noise: ExcitationNoise,
    ) -> Result<(Tensor, Tensor)> {
        let f0 = self.f0_conv.forward(&f0_curve.unsqueeze(1)?)?;
        let energy_framed = self.n_conv.forward(&energy.unsqueeze(1)?)?;
        let mut hidden = Tensor::cat(&[asr, &f0, &energy_framed], 1)?.contiguous()?;
        hidden = self.encode.forward(&hidden, style)?;
        let asr_residual = self.asr_res.forward(asr)?;
        // Conditioning stops once a block has changed length, because the features it would be
        // concatenated with are still at the old rate.
        let mut conditioning = true;
        for block in &self.decode {
            if conditioning {
                hidden =
                    Tensor::cat(&[&hidden, &asr_residual, &f0, &energy_framed], 1)?.contiguous()?;
            }
            hidden = block.forward(&hidden, style)?;
            if block.upsamples() {
                conditioning = false;
            }
        }
        let audio = self.generator.forward(&hidden, style, f0_curve, noise)?;
        Ok((audio, hidden))
    }
}

/// Width of the decoder's residual trunk.
const DECODE_CHANNELS: usize = 1024;
/// Width of the acoustic features re-injected into each decoder block.
const ASR_RESIDUAL_CHANNELS: usize = 64;
