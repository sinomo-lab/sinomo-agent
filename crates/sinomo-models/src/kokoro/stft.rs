//! The short-time Fourier transform pair the ISTFTNet vocoder is built around.
//!
//! Both directions are expressed as a matrix product against a precomputed real-DFT basis. At the
//! 20-point transform Kokoro uses that is both exact and cheap, and it keeps every step a Candle op
//! so the transform runs wherever the rest of the model does.

use super::nn::{atan2, reflect_pad_last_dim};
use super::{KokoroError, Result};
use candle_core::{DType, Device, Tensor};

/// A centered STFT with a periodic Hann window.
#[derive(Debug)]
pub struct Stft {
    n_fft: usize,
    hop_length: usize,
    /// `[n_fft]` analysis window.
    window: Tensor,
    /// `[n_fft, bins]` cosine basis.
    cos_basis: Tensor,
    /// `[n_fft, bins]` negative sine basis, so a matmul yields the imaginary part directly.
    sin_basis: Tensor,
    /// `[n_fft]` window, squared, for the inverse overlap-add normalization.
    window_squared: Tensor,
}

impl Stft {
    pub fn new(n_fft: usize, hop_length: usize, device: &Device, dtype: DType) -> Result<Self> {
        if n_fft == 0 || hop_length == 0 {
            return Err(KokoroError::InvalidInput {
                detail: "STFT requires a non-zero transform size and hop".to_string(),
            });
        }
        if !n_fft.is_multiple_of(2) {
            return Err(KokoroError::Unsupported {
                detail: format!("STFT size {n_fft} must be even for a one-sided transform"),
            });
        }
        if !n_fft.is_multiple_of(hop_length) {
            return Err(KokoroError::Unsupported {
                detail: format!(
                    "STFT size {n_fft} must be a whole number of hops of {hop_length} so that \
                     overlap-add folds exactly"
                ),
            });
        }
        let bins = n_fft / 2 + 1;
        let window = hann_window(n_fft);
        let mut cos = vec![0_f32; n_fft * bins];
        let mut sin = vec![0_f32; n_fft * bins];
        for sample in 0..n_fft {
            for bin in 0..bins {
                let angle = -2.0 * std::f64::consts::PI * (bin * sample) as f64 / n_fft as f64;
                cos[sample * bins + bin] = angle.cos() as f32;
                sin[sample * bins + bin] = angle.sin() as f32;
            }
        }
        let window_squared = window.iter().map(|value| value * value).collect::<Vec<_>>();
        Ok(Self {
            n_fft,
            hop_length,
            window: Tensor::from_vec(window, n_fft, device)?.to_dtype(dtype)?,
            cos_basis: Tensor::from_vec(cos, (n_fft, bins), device)?.to_dtype(dtype)?,
            sin_basis: Tensor::from_vec(sin, (n_fft, bins), device)?.to_dtype(dtype)?,
            window_squared: Tensor::from_vec(window_squared, n_fft, device)?.to_dtype(dtype)?,
        })
    }

    pub fn bins(&self) -> usize {
        self.n_fft / 2 + 1
    }

    /// Transforms `[B, samples]` into magnitude and phase, each `[B, bins, frames]`.
    ///
    /// Frames are centered, so the signal is reflection-padded by half the transform size first and
    /// the frame count is `samples / hop + 1`.
    pub fn transform(&self, samples: &Tensor) -> Result<(Tensor, Tensor)> {
        let padded = reflect_pad_last_dim(samples, self.n_fft / 2)?;
        let frames = self.frame(&padded)?;
        let windowed = frames.broadcast_mul(&self.window)?;
        let real = windowed.matmul(&self.cos_basis.broadcast_left(windowed.dim(0)?)?)?;
        let imaginary = windowed.matmul(&self.sin_basis.broadcast_left(windowed.dim(0)?)?)?;
        let magnitude = (real.sqr()? + imaginary.sqr()?)?.sqrt()?;
        let phase = atan2(&imaginary, &real)?;
        Ok((
            magnitude.transpose(1, 2)?.contiguous()?,
            phase.transpose(1, 2)?.contiguous()?,
        ))
    }

    /// Reconstructs `[B, (frames - 1) * hop]` samples from magnitude and phase.
    ///
    /// Overlap-add is normalized by the summed squared window, and the centering padding introduced
    /// by the forward transform is removed, so this inverts `transform` for a signal whose length is
    /// a whole number of hops.
    pub fn inverse(&self, magnitude: &Tensor, phase: &Tensor) -> Result<Tensor> {
        let real = (magnitude * phase.cos()?)?;
        let imaginary = (magnitude * phase.sin()?)?;
        let frames = real.dim(2)?;
        // Undo the one-sided real DFT: the interior bins stand in for their conjugates, so they
        // count twice, while the DC and Nyquist bins count once.
        let scale = self.inverse_bin_scale(real.dtype(), real.device())?;
        let real = real.broadcast_mul(&scale)?.transpose(1, 2)?.contiguous()?;
        let imaginary = imaginary
            .broadcast_mul(&scale)?
            .transpose(1, 2)?
            .contiguous()?;
        let batch = real.dim(0)?;
        let cos = self.cos_basis.t()?.contiguous()?.broadcast_left(batch)?;
        let sin = self.sin_basis.t()?.contiguous()?.broadcast_left(batch)?;
        // The inverse needs `Re * cos(angle) - Im * sin(angle)`. `sin_basis` holds `sin(-angle)`, so
        // the imaginary term is added rather than subtracted.
        let samples = (real.matmul(&cos)? + imaginary.matmul(&sin)?)?;
        let samples = (samples / self.n_fft as f64)?;
        let windowed = samples.broadcast_mul(&self.window)?;
        let folded = self.overlap_add(&windowed)?;
        let envelope = self.overlap_add(
            &self
                .window_squared
                .broadcast_left((batch, frames))?
                .contiguous()?,
        )?;
        let trimmed = self.trim_center(&folded, frames)?;
        let envelope = self.trim_center(&envelope, frames)?;
        // The envelope of a Hann window at this overlap never reaches zero, so no floor is needed
        // beyond guarding against exact zeros at the very edges.
        Ok(trimmed.broadcast_div(&envelope.clamp(1e-11, f64::INFINITY)?)?)
    }

    /// Slices `[B, frames, n_fft]` frames out of a `[B, samples]` signal.
    fn frame(&self, samples: &Tensor) -> Result<Tensor> {
        let length = samples.dim(1)?;
        if length < self.n_fft {
            return Err(KokoroError::InvalidInput {
                detail: format!(
                    "a {length}-sample signal is shorter than the {} transform size",
                    self.n_fft
                ),
            });
        }
        let frames = (length - self.n_fft) / self.hop_length + 1;
        let mut indices = Vec::with_capacity(frames * self.n_fft);
        for frame in 0..frames {
            let start = frame * self.hop_length;
            indices.extend((start..start + self.n_fft).map(|index| index as u32));
        }
        let count = indices.len();
        let indices = Tensor::from_vec(indices, count, samples.device())?;
        Ok(samples
            .index_select(&indices, 1)?
            .reshape((samples.dim(0)?, frames, self.n_fft))?
            .contiguous()?)
    }

    /// Sums `[B, frames, n_fft]` frames back onto a `[B, samples]` signal at the hop spacing.
    ///
    /// Because the transform size is a whole number of hops, every output sample takes a
    /// contribution from the same number of frames and the fold is a sum of that many shifted
    /// slices rather than a scatter.
    fn overlap_add(&self, frames: &Tensor) -> Result<Tensor> {
        let (batch, frame_count, _) = frames.dims3()?;
        let hop = self.hop_length;
        let overlap = self.n_fft / hop;
        let length = (frame_count - 1) * hop + self.n_fft;
        let mut total: Option<Tensor> = None;
        for shift in 0..overlap {
            // Within one shift the frames' slices tile the signal without overlapping — successive
            // frames sit exactly one hop apart — so the whole shift is one contiguous run placed at
            // `shift * hop`, and the fold is a sum of `overlap` such runs.
            let lane = frames
                .narrow(2, shift * hop, hop)?
                .contiguous()?
                .reshape((batch, frame_count * hop))?;
            let leading = shift * hop;
            let trailing = length - leading - frame_count * hop;
            let mut parts = Vec::with_capacity(3);
            if leading > 0 {
                parts.push(Tensor::zeros(
                    (batch, leading),
                    frames.dtype(),
                    frames.device(),
                )?);
            }
            parts.push(lane);
            if trailing > 0 {
                parts.push(Tensor::zeros(
                    (batch, trailing),
                    frames.dtype(),
                    frames.device(),
                )?);
            }
            let placed = Tensor::cat(&parts, 1)?;
            total = Some(match total {
                None => placed,
                Some(total) => (total + placed)?,
            });
        }
        total.ok_or_else(|| KokoroError::InvalidInput {
            detail: "overlap-add needs at least one hop per transform".to_string(),
        })
    }

    /// Removes the centering padding, leaving `(frames - 1) * hop` samples.
    fn trim_center(&self, samples: &Tensor, frames: usize) -> Result<Tensor> {
        let keep = (frames - 1) * self.hop_length;
        Ok(samples.narrow(1, self.n_fft / 2, keep)?.contiguous()?)
    }

    fn inverse_bin_scale(&self, dtype: DType, device: &Device) -> Result<Tensor> {
        let bins = self.bins();
        let mut scale = vec![2_f32; bins];
        scale[0] = 1.0;
        scale[bins - 1] = 1.0;
        Ok(Tensor::from_vec(scale, (1, bins, 1), device)?.to_dtype(dtype)?)
    }
}

/// A periodic Hann window, the convention `torch.hann_window` uses by default.
fn hann_window(size: usize) -> Vec<f32> {
    (0..size)
        .map(|index| {
            let phase = 2.0 * std::f64::consts::PI * index as f64 / size as f64;
            (0.5 - 0.5 * phase.cos()) as f32
        })
        .collect()
}
