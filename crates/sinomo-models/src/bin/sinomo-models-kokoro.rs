//! Runs the native Kokoro-82M speech runtime from the command line.
//!
//! ```sh
//! # Fetch the checkpoint and one voice into the model cache (about 328 MB).
//! cargo run -p sinomo-models --release --bin sinomo-models-kokoro -- --fetch
//!
//! # Synthesize phonemes to a WAV file.
//! cargo run -p sinomo-models --release --bin sinomo-models-kokoro -- \
//!   --phonemes "hɛlˈO fɹʌm sɪnˈOmO" --voice af_heart --output /tmp/speech.wav
//!
//! # Report what the runtime sees, without synthesizing.
//! cargo run -p sinomo-models --release --bin sinomo-models-kokoro -- --list-voices --json
//! ```
//!
//! The model takes phonemes, not text; see the `kokoro` module documentation.

use sinomo_models::kokoro::{
    DEFAULT_MODEL_ID, DEFAULT_VOICE, DEFAULT_WEIGHTS_FILE, KokoroModel, KokoroPaths,
    SynthesisRequest,
};
use std::path::{Path, PathBuf};

/// Weight files the runtime needs, beyond the configuration and a voice.
const REQUIRED_FILES: [&str; 1] = [DEFAULT_WEIGHTS_FILE];

#[derive(Debug)]
struct Args {
    phonemes: Option<String>,
    voice: String,
    speed: f32,
    output: Option<PathBuf>,
    cache_dir: Option<PathBuf>,
    fetch: bool,
    list_voices: bool,
    deterministic: bool,
    json: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            phonemes: None,
            voice: DEFAULT_VOICE.to_string(),
            speed: 1.0,
            output: None,
            cache_dir: None,
            fetch: false,
            list_voices: false,
            deterministic: false,
            json: false,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    let cache_dir = match args.cache_dir.clone() {
        Some(dir) => dir,
        None => KokoroPaths::default_cache_dir()?,
    };
    let paths = KokoroPaths::from_cache_dir(&cache_dir)?;

    if args.fetch {
        fetch_model(&cache_dir, &args.voice)?;
    }
    if let Some(missing) = paths.first_missing() {
        return Err(format!(
            "{} is missing; run with --fetch to download it into {}",
            missing.display(),
            cache_dir.display()
        )
        .into());
    }

    let loaded_at = std::time::Instant::now();
    let model = KokoroModel::load(&paths)?;
    let load_ms = loaded_at.elapsed().as_millis();
    let voices = model.available_voices()?;

    if args.list_voices || args.phonemes.is_none() {
        let report = serde_json::json!({
            "model": DEFAULT_MODEL_ID,
            "cache_dir": cache_dir.display().to_string(),
            "load_ms": load_ms,
            "sample_rate": sinomo_models::kokoro::SAMPLE_RATE,
            "phoneme_vocab": model.vocab().len(),
            "context_tokens": model.config().plbert.max_position_embeddings,
            "voices": voices,
        });
        if args.json {
            println!("{}", serde_json::to_string_pretty(&report)?);
        } else {
            println!("Kokoro-82M native Candle runtime");
            println!("model: {DEFAULT_MODEL_ID}");
            println!("cache: {}", cache_dir.display());
            println!("loaded in {load_ms} ms");
            println!("phoneme vocabulary: {} symbols", model.vocab().len());
            println!("voices ({}): {}", voices.len(), voices.join(", "));
            if args.phonemes.is_none() && !args.list_voices {
                println!();
                println!("pass --phonemes to synthesize, for example:");
                println!("  --phonemes \"hɛlˈO fɹʌm sɪnˈOmO\" --output /tmp/speech.wav");
            }
        }
        if args.phonemes.is_none() {
            return Ok(());
        }
    }

    let phonemes = args.phonemes.as_deref().expect("checked above");
    let voice = model.load_voice(&args.voice)?;
    let mut request = SynthesisRequest::new(phonemes, &voice).with_speed(args.speed);
    if args.deterministic {
        request = request.deterministic();
    }
    let speech = model.synthesize(&request)?;

    let output = args
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from("kokoro-speech.wav"));
    std::fs::write(&output, speech.to_wav()?)?;

    let report = serde_json::json!({
        "model": DEFAULT_MODEL_ID,
        "voice": args.voice,
        "phonemes": phonemes,
        "speed": args.speed,
        "tokens": speech.input_ids.len(),
        "unknown_phonemes": speech.unknown_phonemes.iter().collect::<String>(),
        "frames": speech.durations.iter().sum::<usize>(),
        "samples": speech.samples.len(),
        "audio_seconds": speech.duration().as_secs_f64(),
        "synthesis_ms": speech.elapsed.as_millis(),
        "realtime_factor": speech.realtime_factor(),
        "rms": speech.rms(),
        "peak": speech.peak(),
        "output": output.display().to_string(),
    });
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "synthesized {:.3} s in {} ms ({:.1}x real time)",
            speech.duration().as_secs_f64(),
            speech.elapsed.as_millis(),
            speech.realtime_factor()
        );
        println!(
            "tokens: {}, frames: {}, rms: {:.4}, peak: {:.4}",
            speech.input_ids.len(),
            speech.durations.iter().sum::<usize>(),
            speech.rms(),
            speech.peak()
        );
        if !speech.unknown_phonemes.is_empty() {
            println!(
                "dropped {} phoneme(s) outside the vocabulary: {}",
                speech.unknown_phonemes.len(),
                speech.unknown_phonemes.iter().collect::<String>()
            );
        }
        println!("wrote {}", output.display());
    }
    Ok(())
}

/// Downloads the model files that are not already cached.
fn fetch_model(cache_dir: &Path, voice: &str) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(cache_dir.join("voices"))?;
    let mut wanted = vec!["config.json".to_string()];
    wanted.extend(REQUIRED_FILES.iter().map(|file| file.to_string()));
    wanted.push(format!("voices/{voice}.pt"));
    for file in wanted {
        let target = cache_dir.join(&file);
        if target.exists() {
            println!("have {}", target.display());
            continue;
        }
        let url = format!("https://huggingface.co/{DEFAULT_MODEL_ID}/resolve/main/{file}");
        println!("fetching {url}");
        // A subprocess rather than an HTTP dependency: this binary is support tooling, and the
        // library itself never reaches the network.
        let status = std::process::Command::new("curl")
            .arg("-fsSL")
            .arg("--create-dirs")
            .arg("-o")
            .arg(&target)
            .arg(&url)
            .status()?;
        if !status.success() {
            // A partial file would otherwise look like a cached one on the next run.
            let _ = std::fs::remove_file(&target);
            return Err(format!("failed to fetch {url}").into());
        }
    }
    Ok(())
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let raw = std::env::args().skip(1).collect::<Vec<_>>();
    let mut index = 0;
    while index < raw.len() {
        let value = |index: usize, name: &str| -> Result<String, String> {
            raw.get(index + 1)
                .cloned()
                .ok_or_else(|| format!("{name} needs a value"))
        };
        match raw[index].as_str() {
            "--phonemes" => {
                args.phonemes = Some(value(index, "--phonemes")?);
                index += 1;
            }
            "--voice" => {
                args.voice = value(index, "--voice")?;
                index += 1;
            }
            "--speed" => {
                args.speed = value(index, "--speed")?
                    .parse()
                    .map_err(|error| format!("--speed must be a number: {error}"))?;
                index += 1;
            }
            "--output" => {
                args.output = Some(PathBuf::from(value(index, "--output")?));
                index += 1;
            }
            "--cache-dir" => {
                args.cache_dir = Some(PathBuf::from(value(index, "--cache-dir")?));
                index += 1;
            }
            "--fetch" => args.fetch = true,
            "--list-voices" => args.list_voices = true,
            "--deterministic" => args.deterministic = true,
            "--json" => args.json = true,
            "--help" | "-h" => {
                return Err(
                    "usage: sinomo-models-kokoro [--phonemes IPA] [--voice NAME] [--speed N] \
                     [--output PATH] [--cache-dir PATH] [--fetch] [--list-voices] \
                     [--deterministic] [--json]"
                        .to_string(),
                );
            }
            other => return Err(format!("unexpected argument `{other}`")),
        }
        index += 1;
    }
    if args.speed <= 0.0 {
        return Err(format!("--speed must be positive, not {}", args.speed));
    }
    Ok(args)
}
