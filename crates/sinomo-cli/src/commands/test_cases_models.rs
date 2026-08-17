//! Live test cases for the native model runtimes.
//!
//! These run the real checkpoint rather than a stub, so they are gated on `--live` and on the model
//! being present in the cache. What they verify is end to end: a phoneme string goes in, speech comes
//! out, and the audio is checked for the properties that distinguish speech from silence or noise.
//!
//! Cost is reported two ways. Each case records what the synthesis cost into its own diagnostics,
//! which is what `sinomo test run <id> --json` prints and is the only channel a short-lived CLI
//! process can serve. It also emits the `sinomo.models.tts.*` metrics declared in
//! [`register_models_observe_metrics`]; those reach `sinomo observe metrics` only when synthesis runs
//! inside the daemon, because that surface is served by daemon-registered exporters reading persisted
//! state rather than by another process's counters.

use std::path::PathBuf;
use std::time::Instant;

use sinomo_models::kokoro::{
    DEFAULT_VOICE, KokoroModel, KokoroPaths, SynthesisRequest, SynthesizedSpeech, VoicePack,
};
use sinomo_runtime::observe::{
    ObserveMetricAggregation, ObserveMetricDescriptor, ObserveMetricKind, ObserveMetricSemantics,
    ObserveMetricUnit, register_observe_metric_descriptor,
};
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement,
};

/// Phonemes for "hello from sinomo", in the notation the checkpoint's vocabulary uses.
///
/// The model is language-blind and takes phonemes rather than text, so a fixed phoneme string keeps
/// these cases independent of any grapheme-to-phoneme implementation.
const SMOKE_PHONEMES: &str = "hɛlˈO fɹʌm sɪnˈOmO";
/// A longer utterance, used to check that cost scales with length rather than blowing up.
///
/// Note the voiced velar stop is `ɡ` (U+0261), the IPA letter, not an ASCII `g`: the checkpoint's
/// vocabulary contains only the former, and the latter would be silently dropped.
const SENTENCE_PHONEMES: &str =
    "ðə kwˈɪk bɹˈaʊn fˈɑks ʤˈʌmps ˈOvəɹ ðə lˈeɪzi dˈɔɡ ænd kəntˈɪnjuz ˈɑn ɪts wˈeɪ";

/// Speech quieter than this is silence, not sound.
const MIN_SPEECH_RMS: f32 = 0.005;
/// Speech at or above full scale has clipped.
const MAX_SPEECH_PEAK: f32 = 0.999;
/// A synthesized utterance shorter than this cannot be a sentence.
const MIN_SPEECH_SECONDS: f64 = 0.2;

/// The metric domain these cases report under.
const OBSERVE_DOMAIN: &str = "models";

pub fn register_models_test_cases(builder: SinomoTestRegistryBuilder) -> SinomoTestRegistryBuilder {
    register_models_observe_metrics();
    builder
        .register(kokoro_tts_synthesis())
        .register(kokoro_tts_sentence_throughput())
        .register(kokoro_tts_voices())
}

/// Declares the metrics the synthesis cases emit.
///
/// Declaring a metric is what gives it a unit, a help string and a safe cross-node aggregation, so
/// whichever process ends up serving these series describes them the same way rather than exposing
/// bare numbers.
pub fn register_models_observe_metrics() {
    for (name, kind, unit, help) in [
        (
            "sinomo.models.tts.syntheses",
            ObserveMetricKind::Counter,
            ObserveMetricUnit::Count,
            "Native text-to-speech syntheses run.",
        ),
        (
            "sinomo.models.tts.synthesis.us",
            ObserveMetricKind::Histogram,
            ObserveMetricUnit::Microseconds,
            "Wall time spent synthesizing one utterance.",
        ),
        (
            "sinomo.models.tts.load.us",
            ObserveMetricKind::Histogram,
            ObserveMetricUnit::Microseconds,
            "Wall time spent loading a native speech model.",
        ),
        (
            "sinomo.models.tts.audio.us",
            ObserveMetricKind::Histogram,
            ObserveMetricUnit::Microseconds,
            "Length of the audio a synthesis produced.",
        ),
        (
            "sinomo.models.tts.realtime_factor",
            ObserveMetricKind::Histogram,
            ObserveMetricUnit::Ratio,
            "Synthesized seconds per second of compute; above one is faster than real time.",
        ),
    ] {
        register_observe_metric_descriptor(
            ObserveMetricDescriptor::new(name, kind, Some(unit), help).with_semantics(
                ObserveMetricSemantics::new(
                    OBSERVE_DOMAIN,
                    ObserveMetricAggregation::None,
                    ["model", "voice"],
                ),
            ),
        );
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn kokoro_tts_synthesis(ctx) {
        id: "models.kokoro.tts",
        summary: "Kokoro-82M native speech synthesis produces audible speech end to end",
        tags: ["models", "kokoro", "tts", "live", "native-models"],
        requirements: [SinomoTestRequirement::LiveNetwork],
        run: {
            let speech = match synthesize_case(ctx, SMOKE_PHONEMES)? {
                Available::Ready(speech) => speech,
                Available::Unavailable(outcome) => return Ok(outcome),
            };
            Ok(SinomoTestOutcome::passed(format!(
                "synthesized {:.2}s of speech at {:.1}x real time",
                speech.duration().as_secs_f64(),
                speech.realtime_factor()
            )))
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn kokoro_tts_sentence_throughput(ctx) {
        id: "models.kokoro.tts-sentence",
        summary: "Kokoro-82M synthesizes a full sentence and reports its realtime factor",
        tags: ["models", "kokoro", "tts", "live", "native-models", "throughput"],
        requirements: [SinomoTestRequirement::LiveNetwork],
        run: {
            let speech = match synthesize_case(ctx, SENTENCE_PHONEMES)? {
                Available::Ready(speech) => speech,
                Available::Unavailable(outcome) => return Ok(outcome),
            };
            // A sentence must actually be longer than the smoke phrase; a duration predictor that
            // collapsed would still pass the audio checks above.
            if speech.duration().as_secs_f64() < MIN_SPEECH_SECONDS * 4.0 {
                return Err(SinomoTestError::failed(format!(
                    "a sentence synthesized to only {:.2}s of audio",
                    speech.duration().as_secs_f64()
                )));
            }
            Ok(SinomoTestOutcome::passed(format!(
                "synthesized {:.2}s of speech at {:.1}x real time",
                speech.duration().as_secs_f64(),
                speech.realtime_factor()
            )))
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn kokoro_tts_voices(ctx) {
        id: "models.kokoro.voices",
        summary: "Every cached Kokoro voice loads and changes the synthesized audio",
        tags: ["models", "kokoro", "tts", "live", "native-models", "voices"],
        requirements: [SinomoTestRequirement::LiveNetwork],
        run: {
            let model = match load_model(ctx)? {
                Available::Ready(model) => model,
                Available::Unavailable(outcome) => return Ok(outcome),
            };
            let voices = model
                .available_voices()
                .map_err(|error| SinomoTestError::failed(error.to_string()))?;
            if voices.is_empty() {
                return Ok(SinomoTestOutcome::skipped(
                    "no Kokoro voice packs are cached; fetch one with `sinomo-models-kokoro --fetch`",
                ));
            }
            ctx.record("voices", &voices);
            let mut fingerprints = Vec::new();
            for voice in &voices {
                let pack = model
                    .load_voice(voice)
                    .map_err(|error| SinomoTestError::failed(format!("voice {voice}: {error}")))?;
                let speech = model
                    .synthesize(&SynthesisRequest::new(SMOKE_PHONEMES, &pack).deterministic())
                    .map_err(|error| SinomoTestError::failed(format!("voice {voice}: {error}")))?;
                check_audio(voice, &speech)?;
                fingerprints.push((voice.clone(), speech.rms(), speech.durations.clone()));
            }
            // Two voices that produced byte-identical prosody would mean the style vector is not
            // reaching the predictor, which no per-voice audio check would catch on its own.
            if fingerprints.len() > 1 {
                let first = &fingerprints[0];
                if fingerprints
                    .iter()
                    .all(|(_, rms, durations)| *rms == first.1 && *durations == first.2)
                {
                    return Err(SinomoTestError::failed(
                        "every voice produced identical prosody, so the style vector is being ignored",
                    ));
                }
            }
            Ok(SinomoTestOutcome::passed(format!(
                "{} voice(s) load and synthesize: {}",
                voices.len(),
                voices.join(", ")
            )))
        }
    }
}

/// Either the thing a case needs, or the outcome to report because it is unavailable.
///
/// The registry turns any returned error into a failure, so "the checkpoint is not downloaded" has to
/// travel as an outcome rather than an error: it is a reason to skip, not a defect.
enum Available<T> {
    Ready(T),
    Unavailable(SinomoTestOutcome),
}

/// Loads the model named by the test parameters, or reports why it is unavailable.
fn load_model(ctx: &mut SinomoTestContext) -> Result<Available<KokoroModel>, SinomoTestError> {
    let cache_dir = match ctx
        .parameter("cache-dir")
        .map(str::trim)
        .filter(|dir| !dir.is_empty())
    {
        Some(dir) => PathBuf::from(dir),
        None => KokoroPaths::default_cache_dir()
            .map_err(|error| SinomoTestError::failed(error.to_string()))?,
    };
    let paths = KokoroPaths::from_cache_dir(&cache_dir)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    if let Some(missing) = paths.first_missing() {
        // The checkpoint is a 327 MB download that a machine may legitimately not have, so this
        // skips and names the command that fetches it.
        return Ok(Available::Unavailable(SinomoTestOutcome::skipped(format!(
            "{} is not cached; fetch it with `cargo run -p sinomo-models --release --bin \
             sinomo-models-kokoro -- --fetch`",
            missing.display()
        ))));
    }
    ctx.record("cache_dir", cache_dir.display().to_string());
    let started = Instant::now();
    let model =
        KokoroModel::load(&paths).map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let load_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as f64;
    ctx.record("load_ms", started.elapsed().as_millis() as u64);
    metrics::histogram!("sinomo.models.tts.load.us", "model" => "kokoro-82m", "voice" => "none")
        .record(load_us);
    Ok(Available::Ready(model))
}

/// Loads the model, synthesizes one utterance, checks it, and records what it cost.
fn synthesize_case(
    ctx: &mut SinomoTestContext,
    phonemes: &str,
) -> Result<Available<SynthesizedSpeech>, SinomoTestError> {
    let model = match load_model(ctx)? {
        Available::Ready(model) => model,
        Available::Unavailable(outcome) => return Ok(Available::Unavailable(outcome)),
    };
    let voice_name = ctx
        .parameter("voice")
        .map(str::trim)
        .filter(|voice| !voice.is_empty())
        .unwrap_or(DEFAULT_VOICE)
        .to_string();
    let voice = match model.load_voice(&voice_name) {
        Ok(voice) => voice,
        Err(error) => {
            return Ok(Available::Unavailable(SinomoTestOutcome::skipped(format!(
                "voice `{voice_name}` is not cached: {error}"
            ))));
        }
    };
    let phonemes = ctx
        .parameter("phonemes")
        .map(str::trim)
        .filter(|phonemes| !phonemes.is_empty())
        .unwrap_or(phonemes)
        .to_string();

    let speech = model
        .synthesize(&SynthesisRequest::new(&phonemes, &voice))
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    check_audio(&voice_name, &speech)?;
    record_speech(ctx, &voice, &speech);
    Ok(Available::Ready(speech))
}

/// Rejects audio that is not speech.
///
/// A vocoder can fail in ways that still produce a correctly-shaped buffer, so the checks are on the
/// signal: it must be present, it must not clip, and it must not be a constant.
fn check_audio(voice: &str, speech: &SynthesizedSpeech) -> Result<(), SinomoTestError> {
    if speech.samples.is_empty() {
        return Err(SinomoTestError::failed(format!(
            "voice {voice} produced no samples"
        )));
    }
    if speech.duration().as_secs_f64() < MIN_SPEECH_SECONDS {
        return Err(SinomoTestError::failed(format!(
            "voice {voice} produced only {:.3}s of audio",
            speech.duration().as_secs_f64()
        )));
    }
    let rms = speech.rms();
    if rms < MIN_SPEECH_RMS {
        return Err(SinomoTestError::failed(format!(
            "voice {voice} produced near-silence, rms {rms:.6}"
        )));
    }
    let peak = speech.peak();
    if peak >= MAX_SPEECH_PEAK {
        return Err(SinomoTestError::failed(format!(
            "voice {voice} clipped, peak {peak:.6}"
        )));
    }
    if !speech.samples.iter().all(|sample| sample.is_finite()) {
        return Err(SinomoTestError::failed(format!(
            "voice {voice} produced a non-finite sample"
        )));
    }
    // A constant buffer has energy but no sound; comparing against the first sample catches a
    // vocoder that collapsed to its bias.
    let first = speech.samples[0];
    if speech.samples.iter().all(|sample| *sample == first) {
        return Err(SinomoTestError::failed(format!(
            "voice {voice} produced a constant signal"
        )));
    }
    if !speech.unknown_phonemes.is_empty() {
        return Err(SinomoTestError::failed(format!(
            "voice {voice} could not pronounce {:?}",
            speech.unknown_phonemes
        )));
    }
    Ok(())
}

/// Records one synthesis in both the test diagnostics and the observe metrics.
fn record_speech(ctx: &mut SinomoTestContext, voice: &VoicePack, speech: &SynthesizedSpeech) {
    ctx.record("voice", voice.name());
    ctx.record("tokens", speech.input_ids.len() as u64);
    ctx.record("frames", speech.durations.iter().sum::<usize>() as u64);
    ctx.record("samples", speech.samples.len() as u64);
    ctx.record("audio_seconds", speech.duration().as_secs_f64());
    ctx.record("synthesis_ms", speech.elapsed.as_millis() as u64);
    ctx.record("realtime_factor", speech.realtime_factor());
    ctx.record("rms", speech.rms());
    ctx.record("peak", speech.peak());

    let labels = ("model", "kokoro-82m");
    metrics::counter!("sinomo.models.tts.syntheses", labels.0 => labels.1, "voice" => voice.name().to_string())
        .increment(1);
    metrics::histogram!("sinomo.models.tts.synthesis.us", labels.0 => labels.1, "voice" => voice.name().to_string())
        .record(speech.elapsed.as_micros().min(u128::from(u64::MAX)) as f64);
    metrics::histogram!("sinomo.models.tts.audio.us", labels.0 => labels.1, "voice" => voice.name().to_string())
        .record(speech.duration().as_micros() as f64);
    metrics::histogram!("sinomo.models.tts.realtime_factor", labels.0 => labels.1, "voice" => voice.name().to_string())
        .record(speech.realtime_factor());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Guards the wiring rather than the model: a feature-gating mistake would otherwise leave these
    /// cases unregistered and unrunnable, which no live run would reveal because nobody would invoke
    /// them.
    #[test]
    fn the_model_cases_are_registered_and_require_live_access() {
        let registry = crate::commands::test_registry().expect("registry");
        let cases = registry.metadata();
        for id in [
            "models.kokoro.tts",
            "models.kokoro.tts-sentence",
            "models.kokoro.voices",
        ] {
            let case = cases
                .iter()
                .find(|case| case.id == id)
                .unwrap_or_else(|| panic!("{id} is not registered"));
            assert!(
                case.tags.iter().any(|tag| tag == "tts"),
                "{id} is not tagged tts"
            );
            // Without this the case would run on a machine with no checkpoint and no network, and
            // report a failure that says nothing about the code.
            assert!(
                case.requirements
                    .iter()
                    .any(|requirement| requirement.label() == "live-network"),
                "{id} does not require live access"
            );
        }
    }

    /// A missing checkpoint has to skip, not fail: the registry turns any returned error into a
    /// failure, so the distinction lives in the return type rather than in the error.
    #[test]
    fn a_missing_checkpoint_skips_instead_of_failing() {
        let directory = sinomo_testdir::TestDir::new("kokoro-missing-checkpoint");
        let mut context = sinomo_runtime::test::SinomoTestContext::new(directory.path())
            .with_live(true)
            .with_parameter(
                "cache-dir",
                directory.path().join("empty").to_string_lossy(),
            );

        let outcome = load_model(&mut context).expect("resolving paths does not fail");

        match outcome {
            Available::Unavailable(outcome) => {
                assert_eq!(
                    outcome.status,
                    sinomo_runtime::test::SinomoTestStatus::Skipped
                );
                assert!(outcome.summary.contains("--fetch"), "{}", outcome.summary);
            }
            Available::Ready(_) => panic!("an empty cache directory must not produce a model"),
        }
    }

    /// A recorder that keeps every emission, so a test can assert what was actually reported.
    #[derive(Clone, Default)]
    struct CapturingRecorder {
        emissions: std::sync::Arc<std::sync::Mutex<Vec<(String, Vec<String>, f64)>>>,
    }

    impl CapturingRecorder {
        fn emissions(&self) -> Vec<(String, Vec<String>, f64)> {
            self.emissions.lock().expect("emissions lock").clone()
        }

        fn handle(&self, key: &metrics::Key) -> std::sync::Arc<CapturedMetric> {
            std::sync::Arc::new(CapturedMetric {
                name: key.name().to_string(),
                labels: key
                    .labels()
                    .map(|label| format!("{}={}", label.key(), label.value()))
                    .collect(),
                emissions: std::sync::Arc::clone(&self.emissions),
            })
        }
    }

    struct CapturedMetric {
        name: String,
        labels: Vec<String>,
        emissions: std::sync::Arc<std::sync::Mutex<Vec<(String, Vec<String>, f64)>>>,
    }

    impl CapturedMetric {
        fn push(&self, value: f64) {
            self.emissions.lock().expect("emissions lock").push((
                self.name.clone(),
                self.labels.clone(),
                value,
            ));
        }
    }

    impl metrics::CounterFn for CapturedMetric {
        fn increment(&self, value: u64) {
            self.push(value as f64);
        }
        fn absolute(&self, value: u64) {
            self.push(value as f64);
        }
    }

    impl metrics::HistogramFn for CapturedMetric {
        fn record(&self, value: f64) {
            self.push(value);
        }
    }

    impl metrics::GaugeFn for CapturedMetric {
        fn increment(&self, value: f64) {
            self.push(value);
        }
        fn decrement(&self, value: f64) {
            self.push(-value);
        }
        fn set(&self, value: f64) {
            self.push(value);
        }
    }

    impl metrics::Recorder for CapturingRecorder {
        fn describe_counter(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }
        fn register_counter(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            metrics::Counter::from_arc(self.handle(key))
        }
        fn register_gauge(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Gauge {
            metrics::Gauge::from_arc(self.handle(key))
        }
        fn register_histogram(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::from_arc(self.handle(key))
        }
    }

    /// The metric emission is otherwise unverified: nothing reads it back in a short-lived process,
    /// so a wrong name or a missing call would go unnoticed until someone went looking in the daemon.
    #[test]
    fn a_synthesis_emits_the_metrics_it_declares() {
        let device = candle_core::Device::Cpu;
        let voice = VoicePack::from_tensor(
            "af_test",
            candle_core::Tensor::zeros((4, 1, 256), candle_core::DType::F32, &device)
                .expect("styles"),
        )
        .expect("voice pack");
        let speech = SynthesizedSpeech {
            samples: vec![0.1; 48_000],
            sample_rate: sinomo_models::kokoro::SAMPLE_RATE,
            input_ids: vec![0, 50, 0],
            unknown_phonemes: Vec::new(),
            durations: vec![1, 2, 3],
            elapsed: std::time::Duration::from_millis(1_000),
        };
        let directory = sinomo_testdir::TestDir::new("kokoro-metric-emission");
        let mut context = sinomo_runtime::test::SinomoTestContext::new(directory.path());

        let recorder = CapturingRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            record_speech(&mut context, &voice, &speech);
        });

        let emissions = recorder.emissions();
        let names = emissions
            .iter()
            .map(|(name, _, _)| name.as_str())
            .collect::<Vec<_>>();
        for declared in [
            "sinomo.models.tts.syntheses",
            "sinomo.models.tts.synthesis.us",
            "sinomo.models.tts.audio.us",
            "sinomo.models.tts.realtime_factor",
        ] {
            assert!(
                names.contains(&declared),
                "{declared} was never emitted; emitted {names:?}"
            );
        }
        // Every series has to carry both labels its descriptor declares, or it cannot be broken down
        // by model or by voice.
        for (name, labels, _) in &emissions {
            assert!(
                labels.contains(&"model=kokoro-82m".to_string()),
                "{name} is missing the model label: {labels:?}"
            );
            assert!(
                labels.contains(&"voice=af_test".to_string()),
                "{name} is missing the voice label: {labels:?}"
            );
        }
        let value_of = |wanted: &str| {
            emissions
                .iter()
                .find(|(name, _, _)| name == wanted)
                .map(|(_, _, value)| *value)
                .unwrap_or_else(|| panic!("{wanted} was not emitted"))
        };
        assert_eq!(value_of("sinomo.models.tts.syntheses"), 1.0);
        assert_eq!(value_of("sinomo.models.tts.audio.us"), 2_000_000.0);
        assert_eq!(value_of("sinomo.models.tts.synthesis.us"), 1_000_000.0);
        assert_eq!(value_of("sinomo.models.tts.realtime_factor"), 2.0);
    }
}
