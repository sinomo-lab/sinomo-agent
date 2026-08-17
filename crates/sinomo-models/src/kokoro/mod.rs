//! Native Candle runtime for [Kokoro-82M](https://huggingface.co/hexgrad/Kokoro-82M).
//!
//! Kokoro is a StyleTTS2 model with an ISTFTNet vocoder: a phoneme-level ALBERT conditions a prosody
//! predictor, a separate encoder produces acoustic features, and the two are combined by a decoder
//! that predicts a spectrogram and inverts it. Predicting a spectrogram rather than samples is what
//! keeps an 82M-parameter model within reach of a CPU — synthesis runs at roughly real time, with the
//! vocoder's dilated convolutions accounting for most of it.
//!
//! The model takes **phonemes, not text** — it is language-blind by design, and grapheme-to-phoneme
//! conversion is a separate concern that lives outside this crate. [`KokoroModel::synthesize`] takes
//! a phoneme string, and [`PhonemeVocab`] exposes the checkpoint's own phoneme table.
//!
//! ```no_run
//! use sinomo_models::kokoro::{KokoroModel, KokoroPaths, SynthesisRequest};
//!
//! let paths = KokoroPaths::from_cache_dir(KokoroPaths::default_cache_dir()?)?;
//! let model = KokoroModel::load(&paths)?;
//! let voice = model.load_voice("af_heart")?;
//! let speech = model.synthesize(&SynthesisRequest::new("hɛlˈO", &voice))?;
//! std::fs::write("speech.wav", speech.to_wav()?)?;
//! # Ok::<(), sinomo_models::kokoro::KokoroError>(())
//! ```

mod albert;
mod blocks;
mod istftnet;
mod nn;
mod predictor;
mod stft;
mod voice;

pub use istftnet::{ExcitationNoise, SAMPLE_RATE};
pub use voice::{BOUNDARY_TOKEN, PhonemeVocab, TokenizedPhonemes, VoicePack};

use albert::PlBert;
use candle_core::{DType, Device, Module, Tensor};
use candle_nn::{Linear, VarBuilder, linear};
use istftnet::Decoder;
use predictor::{ProsodyPredictor, TextEncoder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use thiserror::Error;

/// The repository this runtime targets.
pub const DEFAULT_MODEL_ID: &str = "hexgrad/Kokoro-82M";
/// The checkpoint file inside that repository.
pub const DEFAULT_WEIGHTS_FILE: &str = "kokoro-v1_0.pth";
/// The voice this runtime falls back to.
pub const DEFAULT_VOICE: &str = "af_heart";
/// Environment variable naming the directory model files are cached in.
pub const MODELS_CACHE_ENV: &str = "SINOMO_MODELS_CACHE";

#[derive(Debug, Error)]
pub enum KokoroError {
    #[error("Kokoro io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Kokoro json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Kokoro candle error: {0}")]
    Candle(#[from] candle_core::Error),
    #[error("Kokoro unsupported architecture: {detail}")]
    Unsupported { detail: String },
    #[error("Kokoro invalid input: {detail}")]
    InvalidInput { detail: String },
    #[error("Kokoro model file is missing: {path}")]
    MissingModelFile { path: String },
}

pub type Result<T> = std::result::Result<T, KokoroError>;

/// The vocoder half of the model configuration.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct IstftNetConfig {
    pub upsample_kernel_sizes: Vec<usize>,
    pub upsample_rates: Vec<usize>,
    pub gen_istft_hop_size: usize,
    pub gen_istft_n_fft: usize,
    pub resblock_dilation_sizes: Vec<Vec<usize>>,
    pub resblock_kernel_sizes: Vec<usize>,
    pub upsample_initial_channel: usize,
}

/// The phoneme-level ALBERT configuration.
///
/// The checkpoint's `config.json` carries only the fields that differ from the ALBERT defaults, so
/// the rest are defaulted here to the same values the reference implementation resolves them to.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PlBertConfig {
    pub hidden_size: usize,
    pub num_attention_heads: usize,
    pub intermediate_size: usize,
    pub max_position_embeddings: usize,
    pub num_hidden_layers: usize,
    #[serde(default = "default_embedding_size")]
    pub embedding_size: usize,
    #[serde(default = "default_type_vocab_size")]
    pub type_vocab_size: usize,
}

fn default_embedding_size() -> usize {
    128
}

fn default_type_vocab_size() -> usize {
    2
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct KokoroConfig {
    pub istftnet: IstftNetConfig,
    pub plbert: PlBertConfig,
    pub dim_in: usize,
    pub hidden_dim: usize,
    pub max_conv_dim: usize,
    pub max_dur: usize,
    pub n_layer: usize,
    pub n_mels: usize,
    pub n_token: usize,
    pub style_dim: usize,
    pub text_encoder_kernel_size: usize,
    #[serde(default)]
    pub multispeaker: bool,
    /// The phoneme table, kept as raw JSON so [`PhonemeVocab`] owns its interpretation.
    #[serde(default)]
    pub vocab: serde_json::Value,
}

impl KokoroConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(KokoroError::MissingModelFile {
                path: path.display().to_string(),
            });
        }
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    }

    /// Rejects a configuration this runtime cannot serve, naming what it cannot serve.
    ///
    /// The point is to fail at load with a specific reason rather than to produce plausible-sounding
    /// noise from a checkpoint whose shape does not match.
    pub fn validate_native_support(&self) -> Result<()> {
        let mut unsupported = Vec::new();
        if self.istftnet.upsample_rates.len() != self.istftnet.upsample_kernel_sizes.len() {
            unsupported
                .push("ISTFTNet upsample rates and kernel sizes differ in length".to_string());
        }
        if !self.istftnet.gen_istft_n_fft.is_multiple_of(2) {
            unsupported.push(format!(
                "odd inverse-STFT size {}",
                self.istftnet.gen_istft_n_fft
            ));
        }
        if !self
            .istftnet
            .gen_istft_n_fft
            .is_multiple_of(self.istftnet.gen_istft_hop_size)
        {
            unsupported.push(format!(
                "inverse-STFT size {} is not a whole number of {}-sample hops",
                self.istftnet.gen_istft_n_fft, self.istftnet.gen_istft_hop_size
            ));
        }
        if !self.hidden_dim.is_multiple_of(2) {
            unsupported.push(format!("odd hidden dimension {}", self.hidden_dim));
        }
        if self.style_dim == 0 {
            unsupported.push("zero style dimension".to_string());
        }
        if unsupported.is_empty() {
            Ok(())
        } else {
            Err(KokoroError::Unsupported {
                detail: unsupported.join("; "),
            })
        }
    }
}

/// Where the model's files live on disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KokoroPaths {
    pub config: PathBuf,
    pub weights: PathBuf,
    pub voices: PathBuf,
}

impl KokoroPaths {
    /// The directory model files are cached in, honouring [`MODELS_CACHE_ENV`].
    pub fn default_cache_dir() -> Result<PathBuf> {
        if let Some(dir) = std::env::var_os(MODELS_CACHE_ENV) {
            return Ok(PathBuf::from(dir).join(cache_slug(DEFAULT_MODEL_ID)));
        }
        let home = std::env::var_os("HOME").ok_or_else(|| KokoroError::InvalidInput {
            detail: format!("neither {MODELS_CACHE_ENV} nor HOME is set"),
        })?;
        Ok(PathBuf::from(home)
            .join(".cache")
            .join("sinomo-models")
            .join(cache_slug(DEFAULT_MODEL_ID)))
    }

    /// Resolves the standard layout inside a model directory.
    pub fn from_cache_dir(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        Ok(Self {
            config: dir.join("config.json"),
            weights: dir.join(DEFAULT_WEIGHTS_FILE),
            voices: dir.join("voices"),
        })
    }

    /// Returns the first file this layout needs that is not present.
    pub fn first_missing(&self) -> Option<&Path> {
        [self.config.as_path(), self.weights.as_path()]
            .into_iter()
            .find(|&path| !path.exists())
            .map(|v| v as _)
    }

    pub fn voice_file(&self, voice: &str) -> PathBuf {
        self.voices.join(format!("{voice}.pt"))
    }
}

/// The cache directory name for a repository id, which cannot contain a path separator.
fn cache_slug(model_id: &str) -> String {
    model_id.replace('/', "--")
}

/// What to synthesize, and how.
#[derive(Clone, Debug)]
pub struct SynthesisRequest<'a> {
    /// The phonemes to speak. This is not text; see the module documentation.
    pub phonemes: &'a str,
    pub voice: &'a VoicePack,
    /// Divides the predicted durations, so above one speaks faster.
    pub speed: f32,
    /// Whether the vocoder's excitation keeps its stochastic terms.
    pub noise: ExcitationNoise,
}

impl<'a> SynthesisRequest<'a> {
    pub fn new(phonemes: &'a str, voice: &'a VoicePack) -> Self {
        Self {
            phonemes,
            voice,
            speed: 1.0,
            noise: ExcitationNoise::default(),
        }
    }

    pub fn with_speed(mut self, speed: f32) -> Self {
        self.speed = speed;
        self
    }

    /// Makes synthesis a pure function of its inputs, at the cost of the excitation's noise.
    ///
    /// Useful for comparing this runtime against a reference; see [`ExcitationNoise`].
    pub fn deterministic(mut self) -> Self {
        self.noise = ExcitationNoise::Disabled;
        self
    }
}

/// Intermediate tensors from one synthesis.
///
/// These exist so a port can be checked stage by stage against the reference implementation: when
/// the audio is wrong, the first stage that diverges says where to look. They are the tensors at the
/// module boundaries, named after the reference's own variables.
#[derive(Clone, Debug)]
pub struct SynthesisTrace {
    /// ALBERT's last hidden state, `[1, tokens, hidden_size]`.
    pub bert: Tensor,
    /// Projected prosody conditioning, `[1, hidden_dim, tokens]`.
    pub bert_encoded: Tensor,
    /// Pitch contour, `[1, 2 * frames]`.
    pub f0: Tensor,
    /// Energy contour, `[1, 2 * frames]`.
    pub energy: Tensor,
    /// Acoustic features before alignment, `[1, hidden_dim, tokens]`.
    pub text_features: Tensor,
    /// Acoustic features stretched onto the frame grid, `[1, hidden_dim, frames]`.
    pub aligned_features: Tensor,
    /// The features the vocoder was handed, `[1, upsample_initial_channel, 2 * frames]`.
    ///
    /// This is the last boundary at which two implementations can be expected to agree closely. Past
    /// it the harmonic excitation integrates the pitch contour into a phase, which amplifies
    /// single-precision differences in that contour into audible-scale phase offsets — the waveform
    /// stays perceptually the same but stops being comparable sample by sample.
    pub decoder_trunk: Tensor,
}

/// Synthesized speech plus what it cost to produce.
#[derive(Clone, Debug)]
pub struct SynthesizedSpeech {
    /// Mono samples in `[-1, 1]`.
    pub samples: Vec<f32>,
    pub sample_rate: usize,
    /// Token ids that were spoken, including boundary tokens.
    pub input_ids: Vec<u32>,
    /// Phonemes dropped because the vocabulary does not represent them.
    pub unknown_phonemes: Vec<char>,
    /// Predicted frames per phoneme token.
    pub durations: Vec<usize>,
    pub elapsed: Duration,
}

impl SynthesizedSpeech {
    pub fn duration(&self) -> Duration {
        Duration::from_secs_f64(self.samples.len() as f64 / self.sample_rate as f64)
    }

    /// Synthesized seconds per second of compute; above one is faster than real time.
    pub fn realtime_factor(&self) -> f64 {
        let elapsed = self.elapsed.as_secs_f64();
        if elapsed <= 0.0 {
            return f64::INFINITY;
        }
        self.duration().as_secs_f64() / elapsed
    }

    /// Root mean square amplitude, which distinguishes speech from silence.
    pub fn rms(&self) -> f32 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let sum: f64 = self
            .samples
            .iter()
            .map(|s| f64::from(*s) * f64::from(*s))
            .sum();
        (sum / self.samples.len() as f64).sqrt() as f32
    }

    pub fn peak(&self) -> f32 {
        self.samples
            .iter()
            .fold(0.0_f32, |peak, s| peak.max(s.abs()))
    }

    /// Encodes the samples as a 16-bit PCM WAV file.
    pub fn to_wav(&self) -> Result<Vec<u8>> {
        Ok(encode_wav_pcm16(&self.samples, self.sample_rate))
    }
}

/// The native Kokoro runtime.
#[derive(Debug)]
pub struct KokoroModel {
    config: KokoroConfig,
    vocab: PhonemeVocab,
    bert: PlBert,
    bert_encoder: Linear,
    predictor: ProsodyPredictor,
    text_encoder: TextEncoder,
    decoder: Decoder,
    device: Device,
    dtype: DType,
    voices_dir: PathBuf,
}

impl KokoroModel {
    /// Loads the model onto the CPU.
    pub fn load(paths: &KokoroPaths) -> Result<Self> {
        Self::load_on_device(paths, &Device::Cpu, DType::F32)
    }

    pub fn load_on_device(paths: &KokoroPaths, device: &Device, dtype: DType) -> Result<Self> {
        if let Some(missing) = paths.first_missing() {
            return Err(KokoroError::MissingModelFile {
                path: missing.display().to_string(),
            });
        }
        let config = KokoroConfig::from_file(&paths.config)?;
        config.validate_native_support()?;
        let vocab = PhonemeVocab::from_config_value(&config.vocab)?;
        // The checkpoint is a dictionary of per-module state dicts, and each of those prefixes its
        // keys with `module.` from having been saved through a data-parallel wrapper.
        let submodule = |name: &str| -> Result<VarBuilder<'static>> {
            Ok(load_submodule(&paths.weights, name, device, dtype)?.pp("module"))
        };
        let bert = PlBert::load(&config.plbert, config.n_token, submodule("bert")?)?;
        let bert_encoder = linear(
            config.plbert.hidden_size,
            config.hidden_dim,
            submodule("bert_encoder")?,
        )?;
        let predictor = ProsodyPredictor::load(&config, submodule("predictor")?)?;
        let text_encoder = TextEncoder::load(&config, submodule("text_encoder")?)?;
        let decoder = Decoder::load(&config, device, dtype, submodule("decoder")?)?;
        Ok(Self {
            config,
            vocab,
            bert,
            bert_encoder,
            predictor,
            text_encoder,
            decoder,
            device: device.clone(),
            dtype,
            voices_dir: paths.voices.clone(),
        })
    }

    pub fn config(&self) -> &KokoroConfig {
        &self.config
    }

    pub fn vocab(&self) -> &PhonemeVocab {
        &self.vocab
    }

    pub fn device(&self) -> &Device {
        &self.device
    }

    /// Loads a voice by name from the model's `voices` directory.
    pub fn load_voice(&self, voice: &str) -> Result<VoicePack> {
        let path = self.voices_dir.join(format!("{voice}.pt"));
        if !path.exists() {
            return Err(KokoroError::MissingModelFile {
                path: path.display().to_string(),
            });
        }
        let pack = VoicePack::from_pt_file(voice, &path, &self.device, self.dtype)?;
        // A voice row holds two style vectors end to end, one per conditioned stage.
        let expected = self.config.style_dim * 2;
        if pack.style_width() != expected {
            return Err(KokoroError::Unsupported {
                detail: format!(
                    "voice `{voice}` carries {}-wide styles but this model expects {expected}",
                    pack.style_width()
                ),
            });
        }
        Ok(pack)
    }

    /// Lists the voices available in the model's `voices` directory.
    pub fn available_voices(&self) -> Result<Vec<String>> {
        if !self.voices_dir.exists() {
            return Ok(Vec::new());
        }
        let mut voices = Vec::new();
        for entry in fs::read_dir(&self.voices_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("pt")
                && let Some(name) = path.file_stem().and_then(|stem| stem.to_str())
            {
                voices.push(name.to_string());
            }
        }
        voices.sort();
        Ok(voices)
    }

    /// Synthesizes speech for a phoneme string.
    pub fn synthesize(&self, request: &SynthesisRequest<'_>) -> Result<SynthesizedSpeech> {
        self.synthesize_with_trace(request)
            .map(|(speech, _trace)| speech)
    }

    /// Synthesizes speech and returns the stage boundaries alongside it.
    ///
    /// See [`SynthesisTrace`]; the audio is identical to what [`Self::synthesize`] produces.
    pub fn synthesize_with_trace(
        &self,
        request: &SynthesisRequest<'_>,
    ) -> Result<(SynthesizedSpeech, SynthesisTrace)> {
        if request.speed <= 0.0 {
            return Err(KokoroError::InvalidInput {
                detail: format!("speech speed must be positive, not {}", request.speed),
            });
        }
        let tokenized = self.vocab.tokenize(request.phonemes);
        if tokenized.phoneme_count() == 0 {
            return Err(KokoroError::InvalidInput {
                detail: "no phoneme in the request is in the model's vocabulary".to_string(),
            });
        }
        let limit = self.config.plbert.max_position_embeddings;
        if tokenized.input_ids.len() > limit {
            return Err(KokoroError::InvalidInput {
                detail: format!(
                    "{} tokens exceeds the {limit}-token context; split the text into shorter \
                     utterances",
                    tokenized.input_ids.len()
                ),
            });
        }
        let started = Instant::now();
        let input_ids = Tensor::from_vec(
            tokenized.input_ids.clone(),
            (1, tokenized.input_ids.len()),
            &self.device,
        )?;
        let style = request.voice.style_for(tokenized.phoneme_count())?;
        // The two halves of a voice row condition different stages: the decoder takes the first,
        // the prosody predictor the second.
        let half = self.config.style_dim;
        let decoder_style = style.narrow(1, 0, half)?.contiguous()?;
        let prosody_style = style.narrow(1, half, style.dim(1)? - half)?.contiguous()?;

        let hidden = self.bert.forward(&input_ids)?;
        let encoded = self
            .bert_encoder
            .forward(&hidden)?
            .transpose(1, 2)?
            .contiguous()?;
        let prosody = self
            .predictor
            .forward(&encoded, &prosody_style, request.speed)?;
        let features = self.text_encoder.forward(&input_ids)?;
        // The same alignment the durations produced, so content and prosody stay in step.
        let aligned_features = features.matmul(&prosody.alignment)?;
        let (audio, decoder_trunk) = self.decoder.forward(
            &aligned_features,
            &prosody.f0,
            &prosody.energy,
            &decoder_style,
            request.noise,
        )?;
        let samples = audio
            .flatten_all()?
            .to_dtype(DType::F32)?
            .to_vec1::<f32>()?;
        let speech = SynthesizedSpeech {
            samples,
            sample_rate: SAMPLE_RATE,
            input_ids: tokenized.input_ids,
            unknown_phonemes: tokenized.unknown,
            durations: prosody.durations,
            elapsed: started.elapsed(),
        };
        let trace = SynthesisTrace {
            bert: hidden,
            bert_encoded: encoded,
            f0: prosody.f0,
            energy: prosody.energy,
            text_features: features,
            aligned_features,
            decoder_trunk,
        };
        Ok((speech, trace))
    }
}

/// Loads one of the checkpoint's per-module state dictionaries.
fn load_submodule(
    weights: &Path,
    name: &str,
    device: &Device,
    dtype: DType,
) -> Result<VarBuilder<'static>> {
    let tensors = candle_core::pickle::read_all_with_key(weights, Some(name))?;
    let tensors = tensors
        .into_iter()
        .map(|(key, tensor)| -> Result<(String, Tensor)> {
            Ok((key, tensor.to_dtype(dtype)?.to_device(device)?))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if tensors.is_empty() {
        return Err(KokoroError::Unsupported {
            detail: format!("the checkpoint has no `{name}` module"),
        });
    }
    Ok(VarBuilder::from_tensors(tensors, dtype, device))
}

/// Encodes mono samples as a 16-bit PCM WAV file.
fn encode_wav_pcm16(samples: &[f32], sample_rate: usize) -> Vec<u8> {
    const HEADER_BYTES: u32 = 36;
    let data_bytes = (samples.len() * 2) as u32;
    let mut wav = Vec::with_capacity(HEADER_BYTES as usize + 8 + data_bytes as usize);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(HEADER_BYTES + data_bytes).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16_u32.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes()); // PCM
    wav.extend_from_slice(&1_u16.to_le_bytes()); // mono
    wav.extend_from_slice(&(sample_rate as u32).to_le_bytes());
    wav.extend_from_slice(&((sample_rate * 2) as u32).to_le_bytes()); // byte rate
    wav.extend_from_slice(&2_u16.to_le_bytes()); // block align
    wav.extend_from_slice(&16_u16.to_le_bytes()); // bits per sample
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_bytes.to_le_bytes());
    for sample in samples {
        let clamped = sample.clamp(-1.0, 1.0);
        wav.extend_from_slice(&((clamped * i16::MAX as f32) as i16).to_le_bytes());
    }
    wav
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
