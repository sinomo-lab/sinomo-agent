//! Compares the native Kokoro runtime against the reference PyTorch implementation.
//!
//! Ignored by default because it needs the 327 MB checkpoint and a fixture set dumped from the
//! reference. Run it deliberately:
//!
//! ```bash
//! SINOMO_KOKORO_FIXTURES=/path/to/fixtures \
//!   cargo test -p sinomo-models --test kokoro_reference -- --ignored --nocapture
//! ```
//!
//! The fixtures are produced by running the reference model with its stochastic excitation disabled,
//! which is what makes an exact comparison possible; this test disables the same terms. Every tensor
//! is a flat little-endian `f32` blob beside a `manifest.json` naming its shape.
//!
//! # Regenerating the fixtures
//!
//! The reference is the `kokoro` PyPI package, which contains the upstream StyleTTS2 modules:
//!
//! ```bash
//! uv venv --python 3.12 .venv
//! uv pip install --python .venv/bin/python torch --index-url https://download.pytorch.org/whl/cpu
//! uv pip install --python .venv/bin/python kokoro
//! ```
//!
//! Then run `KModel.forward_with_tokens` on the phonemes below and write each module boundary out as
//! an `f32` blob, having first monkey-patched `kokoro.istftnet.SineGen` so that `_f02sine` skips its
//! `rand_ini` term and `forward` returns zero noise, and `SourceModuleHnNSF.forward` returns zero
//! noise. Those three edits are exactly what [`SynthesisRequest::deterministic`] switches off on this
//! side. The tensor names this test reads are the reference's own variable names: `bert_dur`, `d_en`,
//! `f0_pred`, `n_pred`, `t_en`, `asr`, `dec_decode_3` (the fourth decoder block's output) and
//! `audio`; `manifest.json` carries `phonemes`, `voice`, `input_ids` and `pred_dur`.

use sinomo_models::kokoro::{KokoroModel, KokoroPaths, SynthesisRequest, VoicePack};
use std::path::{Path, PathBuf};

/// Names the directory holding `manifest.json` and the `*.f32` blobs.
const FIXTURES_ENV: &str = "SINOMO_KOKORO_FIXTURES";

/// Per-stage tolerance, relative to the magnitude of the tensor being compared.
///
/// Everything here is `f32` arithmetic over a 12-layer encoder and two recurrent stacks, so exact
/// equality is not the bar; the bar is that no stage drifts beyond accumulated rounding. The bound
/// has to be relative because these tensors differ in scale by four orders of magnitude — the pitch
/// contour is in hertz while the features are in `[-1, 1]`.
const STAGE_RELATIVE_TOLERANCE: f32 = 1e-4;
/// A floor for tensors whose values are all near zero, where a relative bound says nothing.
const ABSOLUTE_FLOOR: f32 = 1e-5;

/// How closely the waveform must track the reference.
///
/// The waveform is deliberately *not* compared sample by sample. Past the decoder trunk the vocoder
/// integrates the pitch contour into a harmonic phase, and that integral amplifies single-precision
/// differences without bound: perturbing the pitch contour by one part in 10^5 — which is the
/// difference between this runtime and the reference, and is also the difference between two PyTorch
/// runs on differently-vectorized CPUs — moves the excitation by several percent. Two correct
/// implementations therefore produce waveforms that differ in fine phase while carrying the same
/// sound. What must agree is length, level, and the spectrum.
const MIN_WAVEFORM_CORRELATION: f32 = 0.95;
/// How far the overall level may differ, as a fraction.
const MAX_RMS_DIFFERENCE: f32 = 0.05;
/// How far any frequency band's energy may differ, as a fraction.
const MAX_BAND_ENERGY_DIFFERENCE: f32 = 0.10;

struct Fixtures {
    dir: PathBuf,
    manifest: serde_json::Value,
}

impl Fixtures {
    fn load() -> Option<Self> {
        let dir = PathBuf::from(std::env::var_os(FIXTURES_ENV)?);
        let manifest = dir.join("manifest.json");
        if !manifest.exists() {
            panic!(
                "{FIXTURES_ENV} does not contain a manifest.json: {}",
                dir.display()
            );
        }
        let manifest = serde_json::from_slice(&std::fs::read(&manifest).expect("read manifest"))
            .expect("parse manifest");
        Some(Self { dir, manifest })
    }

    fn phonemes(&self) -> &str {
        self.manifest["phonemes"].as_str().expect("phonemes")
    }

    fn voice(&self) -> &str {
        self.manifest["voice"].as_str().expect("voice")
    }

    fn input_ids(&self) -> Vec<u32> {
        self.manifest["input_ids"]
            .as_array()
            .expect("input_ids")
            .iter()
            .map(|value| value.as_u64().expect("token id") as u32)
            .collect()
    }

    fn durations(&self) -> Vec<usize> {
        self.manifest["pred_dur"]
            .as_array()
            .expect("pred_dur")
            .iter()
            .map(|value| value.as_u64().expect("duration") as usize)
            .collect()
    }

    fn tensor(&self, name: &str) -> Vec<f32> {
        let path = self.dir.join(format!("{name}.f32"));
        let bytes =
            std::fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        assert_eq!(bytes.len() % 4, 0, "{name} is not a whole number of f32");
        bytes
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect()
    }
}

/// Compares against the reference, allowing a difference proportional to the tensor's own scale.
fn compare(name: &str, computed: &[f32], expected: &[f32], relative_tolerance: f32) {
    assert_eq!(
        computed.len(),
        expected.len(),
        "{name}: computed {} values, reference has {}",
        computed.len(),
        expected.len()
    );
    let (index, difference) = computed
        .iter()
        .zip(expected)
        .map(|(computed, expected)| (computed - expected).abs())
        .enumerate()
        .fold((0, 0.0_f32), |(worst_index, worst), (index, difference)| {
            if difference > worst {
                (index, difference)
            } else {
                (worst_index, worst)
            }
        });
    let scale = expected
        .iter()
        .fold(0.0_f32, |peak, value| peak.max(value.abs()));
    let allowed = (scale * relative_tolerance).max(ABSOLUTE_FLOOR);
    eprintln!(
        "{name}: {} values, peak |reference| {scale:.5}, max difference {difference:.6} at {index} \
         ({:.1e} relative, allowed {allowed:.6})",
        computed.len(),
        if scale > 0.0 { difference / scale } else { 0.0 }
    );
    assert!(
        difference <= allowed,
        "{name} diverges from the reference by {difference} at index {index} \
         (computed {}, reference {}), which exceeds the allowed {allowed}",
        computed[index],
        expected[index]
    );
}

/// Pearson correlation, which measures whether two signals are the same shape.
fn correlation(left: &[f32], right: &[f32]) -> f32 {
    let count = left.len() as f64;
    let mean = |values: &[f32]| values.iter().map(|v| f64::from(*v)).sum::<f64>() / count;
    let (left_mean, right_mean) = (mean(left), mean(right));
    let mut covariance = 0.0_f64;
    let mut left_variance = 0.0_f64;
    let mut right_variance = 0.0_f64;
    for (left, right) in left.iter().zip(right) {
        let left = f64::from(*left) - left_mean;
        let right = f64::from(*right) - right_mean;
        covariance += left * right;
        left_variance += left * left;
        right_variance += right * right;
    }
    let denominator = (left_variance * right_variance).sqrt();
    if denominator == 0.0 {
        return 0.0;
    }
    (covariance / denominator) as f32
}

fn rms(samples: &[f32]) -> f32 {
    if samples.is_empty() {
        return 0.0;
    }
    let sum: f64 = samples.iter().map(|s| f64::from(*s) * f64::from(*s)).sum();
    (sum / samples.len() as f64).sqrt() as f32
}

/// Total energy in each of `bands` equal-width frequency bands, over Hann-windowed frames.
///
/// A direct transform of only the bins that are needed: the point is a spectral fingerprint, not a
/// fast spectrogram.
fn band_energies(samples: &[f32], bands: usize) -> Vec<f32> {
    const FRAME: usize = 1024;
    const HOP: usize = 256;
    const BINS: usize = FRAME / 2 + 1;
    let window = (0..FRAME)
        .map(|index| {
            let phase = 2.0 * std::f64::consts::PI * index as f64 / FRAME as f64;
            0.5 - 0.5 * phase.cos()
        })
        .collect::<Vec<f64>>();
    // One representative bin per band keeps the cost down while still covering the spectrum.
    let bin_of_band = |band: usize| -> Vec<usize> {
        let width = BINS / bands;
        let start = band * width;
        (start..(start + width).min(BINS))
            .step_by(4.max(width / 8))
            .collect()
    };
    let mut energies = vec![0.0_f64; bands];
    let mut start = 0;
    while start + FRAME <= samples.len() {
        for (band, energy) in energies.iter_mut().enumerate() {
            for bin in bin_of_band(band) {
                let (mut real, mut imaginary) = (0.0_f64, 0.0_f64);
                for offset in 0..FRAME {
                    let value = f64::from(samples[start + offset]) * window[offset];
                    let angle = -2.0 * std::f64::consts::PI * (bin * offset) as f64 / FRAME as f64;
                    real += value * angle.cos();
                    imaginary += value * angle.sin();
                }
                *energy += real * real + imaginary * imaginary;
            }
        }
        start += HOP;
    }
    energies
        .into_iter()
        .map(|value| value.sqrt() as f32)
        .collect()
}

fn model_paths() -> KokoroPaths {
    let dir = KokoroPaths::default_cache_dir().expect("a model cache directory");
    KokoroPaths::from_cache_dir(dir).expect("model paths")
}

#[test]
#[ignore = "needs the Kokoro checkpoint and a reference fixture set"]
fn native_synthesis_matches_the_reference_stage_by_stage() {
    let Some(fixtures) = Fixtures::load() else {
        panic!("set {FIXTURES_ENV} to a directory of reference fixtures");
    };
    let paths = model_paths();
    if let Some(missing) = paths.first_missing() {
        panic!(
            "the Kokoro checkpoint is not downloaded: {}",
            missing.display()
        );
    }

    let model = KokoroModel::load(&paths).expect("load the native runtime");
    let voice = model.load_voice(fixtures.voice()).expect("load the voice");
    // The reference dump zeroed the excitation's stochastic terms, so this must too.
    let request = SynthesisRequest::new(fixtures.phonemes(), &voice).deterministic();

    let (speech, trace) = model
        .synthesize_with_trace(&request)
        .expect("synthesize speech");

    // Tokenization and durations decide the length of everything downstream, so a mismatch here
    // would make the tensor comparisons meaningless rather than merely wrong.
    assert_eq!(
        speech.input_ids,
        fixtures.input_ids(),
        "tokenization differs from the reference"
    );
    assert_eq!(
        speech.durations,
        fixtures.durations(),
        "predicted durations differ from the reference"
    );

    let flatten = |tensor: &candle_core::Tensor| -> Vec<f32> {
        tensor
            .flatten_all()
            .expect("flatten")
            .to_vec1::<f32>()
            .expect("f32 values")
    };
    compare(
        "bert",
        &flatten(&trace.bert),
        &fixtures.tensor("bert_dur"),
        STAGE_RELATIVE_TOLERANCE,
    );
    compare(
        "bert_encoded",
        &flatten(&trace.bert_encoded),
        &fixtures.tensor("d_en"),
        STAGE_RELATIVE_TOLERANCE,
    );
    compare(
        "f0",
        &flatten(&trace.f0),
        &fixtures.tensor("f0_pred"),
        STAGE_RELATIVE_TOLERANCE,
    );
    compare(
        "energy",
        &flatten(&trace.energy),
        &fixtures.tensor("n_pred"),
        STAGE_RELATIVE_TOLERANCE,
    );
    compare(
        "text_features",
        &flatten(&trace.text_features),
        &fixtures.tensor("t_en"),
        STAGE_RELATIVE_TOLERANCE,
    );
    compare(
        "aligned_features",
        &flatten(&trace.aligned_features),
        &fixtures.tensor("asr"),
        STAGE_RELATIVE_TOLERANCE,
    );
    // The last boundary before the excitation makes the output phase-sensitive. Everything above
    // this line is the whole model bar the vocoder, and it is compared exactly.
    compare(
        "decoder_trunk",
        &flatten(&trace.decoder_trunk),
        &fixtures.tensor("dec_decode_3"),
        STAGE_RELATIVE_TOLERANCE,
    );

    let reference_audio = fixtures.tensor("audio");
    assert_eq!(
        speech.samples.len(),
        reference_audio.len(),
        "the waveform is a different length from the reference"
    );
    let correlation = correlation(&speech.samples, &reference_audio);
    let (computed_rms, reference_rms) = (rms(&speech.samples), rms(&reference_audio));
    let level_difference = (computed_rms - reference_rms).abs() / reference_rms;
    let computed_bands = band_energies(&speech.samples, 8);
    let reference_bands = band_energies(&reference_audio, 8);
    eprintln!(
        "audio: {} samples, correlation {correlation:.5}, rms {computed_rms:.5} vs \
         {reference_rms:.5} ({:.2}% apart)",
        speech.samples.len(),
        level_difference * 100.0
    );
    for (band, (computed, reference)) in computed_bands.iter().zip(&reference_bands).enumerate() {
        let difference = (computed - reference).abs() / reference.max(f32::MIN_POSITIVE);
        eprintln!(
            "  band {band}: {computed:10.3} vs {reference:10.3} ({:.3}% apart)",
            difference * 100.0
        );
        assert!(
            difference <= MAX_BAND_ENERGY_DIFFERENCE,
            "band {band} energy differs by {:.1}%, beyond the allowed {:.1}%",
            difference * 100.0,
            MAX_BAND_ENERGY_DIFFERENCE * 100.0
        );
    }
    assert!(
        correlation >= MIN_WAVEFORM_CORRELATION,
        "the waveform correlates with the reference at only {correlation:.4}, below \
         {MIN_WAVEFORM_CORRELATION}"
    );
    assert!(
        level_difference <= MAX_RMS_DIFFERENCE,
        "the waveform level differs from the reference by {:.1}%, beyond the allowed {:.1}%",
        level_difference * 100.0,
        MAX_RMS_DIFFERENCE * 100.0
    );

    eprintln!(
        "synthesized {:.3}s in {:?} ({:.1}x real time), rms {:.5}, peak {:.5}",
        speech.duration().as_secs_f64(),
        speech.elapsed,
        speech.realtime_factor(),
        speech.rms(),
        speech.peak()
    );
}

#[test]
#[ignore = "needs the Kokoro checkpoint"]
fn the_published_voices_all_load_with_the_expected_style_width() {
    let paths = model_paths();
    if let Some(missing) = paths.first_missing() {
        panic!(
            "the Kokoro checkpoint is not downloaded: {}",
            missing.display()
        );
    }
    let model = KokoroModel::load(&paths).expect("load the native runtime");

    let voices = model.available_voices().expect("list voices");
    assert!(!voices.is_empty(), "no voice packs are downloaded");
    for voice in &voices {
        let pack = model
            .load_voice(voice)
            .unwrap_or_else(|error| panic!("load voice {voice}: {error}"));
        assert_eq!(
            pack.style_width(),
            model.config().style_dim * 2,
            "voice {voice} carries an unexpected style width"
        );
        // Every pack must reach the context the encoder allows, or long utterances would silently
        // fall back to a style fitted to a much shorter one.
        assert!(
            pack.rows() > 1,
            "voice {voice} has only {} style rows",
            pack.rows()
        );
    }
    eprintln!("{} voices load: {}", voices.len(), voices.join(", "));
}

/// Guards the claim in the module documentation that a voice file is a plain tensor.
#[test]
#[ignore = "needs the Kokoro checkpoint"]
fn a_voice_file_is_a_single_style_tensor() {
    let paths = model_paths();
    let voice = paths.voice_file("af_heart");
    if !voice.exists() {
        panic!("the af_heart voice is not downloaded: {}", voice.display());
    }

    let pack = VoicePack::from_pt_file(
        "af_heart",
        &voice,
        &candle_core::Device::Cpu,
        candle_core::DType::F32,
    )
    .expect("load the voice pack");

    assert_eq!(pack.name(), "af_heart");
    assert_eq!(pack.style_width(), 256);
    assert!(pack.rows() >= 510, "rows {}", pack.rows());
}

/// Keeps the test honest about where it looks for the model.
#[test]
fn the_cache_layout_is_the_one_the_readme_documents() {
    let paths =
        KokoroPaths::from_cache_dir(Path::new("/tmp/models/hexgrad--Kokoro-82M")).expect("paths");
    assert!(paths.weights.ends_with("kokoro-v1_0.pth"));
    assert!(paths.voices.ends_with("voices"));
}
