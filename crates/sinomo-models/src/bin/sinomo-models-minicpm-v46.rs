use candle_core::{DType, Device};
use sinomo_models::minicpm_v46::{
    BenchmarkReport, DEFAULT_MODEL_ID, MiniCpmV46Config, MiniCpmV46ForConditionalGeneration,
    MiniCpmV46PreprocessorConfig, benchmark_preprocess, benchmark_synthetic_vision_encoder_layer,
    inspect_safetensors, inspect_safetensors_header,
};
use std::fs;
use std::path::PathBuf;

#[derive(Debug)]
struct Args {
    config: Option<PathBuf>,
    preprocessor: Option<PathBuf>,
    safetensors: Option<PathBuf>,
    image: Option<PathBuf>,
    iterations: usize,
    json: bool,
    load_check: bool,
    text_forward_check: bool,
    image_forward_check: bool,
    cached_text_forward_check: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    let config = match args.config.as_ref() {
        Some(path) => MiniCpmV46Config::from_file(path)?,
        None => {
            let bytes = fetch_hf_file("config.json")?;
            serde_json::from_slice(&bytes)?
        }
    };
    let support = config.validate_native_support()?;
    let preprocessor = match args.preprocessor.as_ref() {
        Some(path) => MiniCpmV46PreprocessorConfig::from_file(path)?,
        None => match fetch_hf_file("preprocessor_config.json") {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(_) => MiniCpmV46PreprocessorConfig::default(),
        },
    };

    let mut reports = Vec::new();
    if let Some(image_path) = args.image.as_ref() {
        let image = image::open(image_path)?;
        reports.push(benchmark_preprocess(
            &image,
            &preprocessor,
            &Device::Cpu,
            args.iterations,
        )?);
    } else {
        let image = image::DynamicImage::new_rgb8(
            preprocessor.shortest_edge() as u32,
            preprocessor.shortest_edge() as u32,
        );
        reports.push(benchmark_preprocess(
            &image,
            &preprocessor,
            &Device::Cpu,
            args.iterations,
        )?);
    }
    reports.push(benchmark_synthetic_vision_encoder_layer(
        &Device::Cpu,
        args.iterations,
    )?);

    let safetensor_summary = match args.safetensors.as_ref() {
        Some(path) => {
            Some(inspect_safetensors(path).or_else(|_| inspect_safetensors_header(path))?)
        }
        None => None,
    };
    let model_check = match (
        args.load_check
            || args.text_forward_check
            || args.image_forward_check
            || args.cached_text_forward_check,
        args.safetensors.as_ref(),
    ) {
        (true, Some(path)) => {
            let paths = [path.as_path()];
            let started = std::time::Instant::now();
            let model = unsafe {
                MiniCpmV46ForConditionalGeneration::from_mmaped_safetensors(
                    &config,
                    &paths,
                    DType::F32,
                    &Device::Cpu,
                )?
            };
            let load_check = serde_json::json!({
                "ok": true,
                "elapsed_ms": started.elapsed().as_millis(),
            });
            let text_forward_check = if args.text_forward_check {
                let input_ids = candle_core::Tensor::from_vec(vec![0u32], (1, 1), &Device::Cpu)?;
                let started = std::time::Instant::now();
                let logits = model.forward_text_only_input_ids(&input_ids)?;
                let logits_values = logits.to_vec3::<f32>()?;
                let sample: Vec<f32> = logits_values[0][0].iter().copied().take(8).collect();
                Some(serde_json::json!({
                    "ok": true,
                    "elapsed_ms": started.elapsed().as_millis(),
                    "logits_shape": logits.dims(),
                    "logits_sample": sample,
                }))
            } else {
                None
            };
            let image_forward_check = if args.image_forward_check {
                let input_ids = candle_core::Tensor::from_vec(
                    vec![config.image_token_id],
                    (1, 1),
                    &Device::Cpu,
                )?;
                let pixel_values = candle_core::Tensor::zeros(
                    (1, config.vision_config.num_channels, 28usize, 28usize),
                    DType::F32,
                    &Device::Cpu,
                )?;
                let started = std::time::Instant::now();
                let logits = model.forward_with_image_input_ids(
                    &input_ids,
                    &pixel_values,
                    &[(2usize, 2usize)],
                    Some("4x"),
                )?;
                let logits_values = logits.to_vec3::<f32>()?;
                let sample: Vec<f32> = logits_values[0][0].iter().copied().take(8).collect();
                Some(serde_json::json!({
                    "ok": true,
                    "elapsed_ms": started.elapsed().as_millis(),
                    "logits_shape": logits.dims(),
                    "logits_sample": sample,
                    "input": {
                        "image_token_id": config.image_token_id,
                        "pixel_values_shape": [1, config.vision_config.num_channels, 28, 28],
                        "target_sizes": [[2, 2]],
                        "downsample_mode": "4x",
                    }
                }))
            } else {
                None
            };
            let cached_text_forward_check = if args.cached_text_forward_check {
                let input_ids =
                    candle_core::Tensor::from_vec(vec![0u32, 1, 2], (1, 3), &Device::Cpu)?;
                let full_logits = model.forward_text_only_input_ids(&input_ids)?;
                let mut cache = None;
                let mut chunks = Vec::new();
                let started = std::time::Instant::now();
                for token_id in [0u32, 1, 2] {
                    let token =
                        candle_core::Tensor::from_vec(vec![token_id], (1, 1), &Device::Cpu)?;
                    let (logits, next_cache) =
                        model.forward_text_only_input_ids_with_cache(&token, cache)?;
                    cache = Some(next_cache);
                    chunks.push(logits);
                }
                let refs: Vec<&candle_core::Tensor> = chunks.iter().collect();
                let cached_logits = candle_core::Tensor::cat(&refs, 1)?;
                let max_abs_diff = max_abs_diff(&cached_logits, &full_logits)?;
                let cached_values = cached_logits.to_vec3::<f32>()?;
                let sample: Vec<f32> = cached_values[0][2].iter().copied().take(8).collect();
                Some(serde_json::json!({
                    "ok": max_abs_diff < 1e-3,
                    "elapsed_ms": started.elapsed().as_millis(),
                    "logits_shape": cached_logits.dims(),
                    "max_abs_diff_vs_full_prefix": max_abs_diff,
                    "last_token_logits_sample": sample,
                    "input_ids": [0, 1, 2],
                }))
            } else {
                None
            };
            Some((
                load_check,
                text_forward_check,
                image_forward_check,
                cached_text_forward_check,
            ))
        }
        (true, None) => {
            return Err(
                "--load-check/--text-forward-check/--image-forward-check/--cached-text-forward-check requires --safetensors PATH"
                    .into(),
            );
        }
        _ => None,
    };
    let load_check = model_check.as_ref().map(|(load, _, _, _)| load.clone());
    let text_forward_check = model_check
        .as_ref()
        .and_then(|(_, forward, _, _)| forward.as_ref().cloned());
    let image_forward_check = model_check
        .as_ref()
        .and_then(|(_, _, forward, _)| forward.as_ref().cloned());
    let cached_text_forward_check = model_check
        .as_ref()
        .and_then(|(_, _, _, forward)| forward.as_ref().cloned());

    let output = serde_json::json!({
        "model": DEFAULT_MODEL_ID,
        "device": {
            "selected": "cpu",
            "dtype": format!("{:?}", DType::F32),
            "cuda_feature_enabled": cfg!(feature = "cuda"),
        },
        "config": {
            "model_type": config.model_type,
            "vision_layers": config.vision_config.num_hidden_layers,
            "vision_hidden_size": config.vision_config.hidden_size,
            "vision_image_size": config.vision_config.image_size,
            "text_model_type": config.text_config.model_type,
            "text_layers": config.text_config.num_hidden_layers,
            "text_layer_types": config.text_config.layer_types,
        },
        "native_support": support,
        "preprocessor": {
            "shortest_edge": preprocessor.shortest_edge(),
            "do_resize": preprocessor.do_resize,
            "do_rescale": preprocessor.do_rescale,
            "do_normalize": preprocessor.do_normalize,
        },
        "benchmarks": reports,
        "safetensors": safetensor_summary,
        "load_check": load_check,
        "text_forward_check": text_forward_check,
        "image_forward_check": image_forward_check,
        "cached_text_forward_check": cached_text_forward_check,
    });

    if args.json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        print_human(&output, &reports)?;
    }
    Ok(())
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        config: None,
        preprocessor: None,
        safetensors: None,
        image: None,
        iterations: 10,
        json: false,
        load_check: false,
        text_forward_check: false,
        image_forward_check: false,
        cached_text_forward_check: false,
    };
    let values: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < values.len() {
        match values[index].as_str() {
            "--config" => {
                index += 1;
                args.config = Some(PathBuf::from(
                    values.get(index).ok_or("--config requires a value")?,
                ));
            }
            "--preprocessor" => {
                index += 1;
                args.preprocessor = Some(PathBuf::from(
                    values.get(index).ok_or("--preprocessor requires a value")?,
                ));
            }
            "--safetensors" => {
                index += 1;
                args.safetensors = Some(PathBuf::from(
                    values.get(index).ok_or("--safetensors requires a value")?,
                ));
            }
            "--image" => {
                index += 1;
                args.image = Some(PathBuf::from(
                    values.get(index).ok_or("--image requires a value")?,
                ));
            }
            "--iterations" => {
                index += 1;
                args.iterations = values
                    .get(index)
                    .ok_or("--iterations requires a value")?
                    .parse()
                    .map_err(|source| format!("invalid --iterations value: {source}"))?;
            }
            "--json" => args.json = true,
            "--load-check" => args.load_check = true,
            "--text-forward-check" => args.text_forward_check = true,
            "--image-forward-check" => args.image_forward_check = true,
            "--cached-text-forward-check" => args.cached_text_forward_check = true,
            "--help" | "-h" => {
                return Err("usage: sinomo-models-minicpm-v46 [--config PATH] [--preprocessor PATH] [--safetensors PATH] [--image PATH] [--iterations N] [--load-check] [--text-forward-check] [--image-forward-check] [--cached-text-forward-check] [--json]".to_string());
            }
            value => return Err(format!("unexpected argument `{value}`")),
        }
        index += 1;
    }
    Ok(args)
}

fn fetch_hf_file(name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let cache_dir = std::env::var_os("SINOMO_MODELS_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/sinomo-models-cache"));
    let path = cache_dir
        .join(DEFAULT_MODEL_ID.replace('/', "--"))
        .join(name);
    if path.exists() {
        return Ok(fs::read(path)?);
    }
    fs::create_dir_all(path.parent().expect("cache path has parent"))?;
    let url = format!("https://huggingface.co/{DEFAULT_MODEL_ID}/raw/main/{name}");
    let output = std::process::Command::new("curl")
        .arg("-fsSL")
        .arg(&url)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to fetch {url}: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    fs::write(&path, &output.stdout)?;
    Ok(output.stdout)
}

fn print_human(
    value: &serde_json::Value,
    reports: &[BenchmarkReport],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("MiniCPM-V 4.6 native Candle status");
    println!(
        "model: {}",
        value["model"].as_str().unwrap_or(DEFAULT_MODEL_ID)
    );
    println!(
        "vision: {} layers, hidden {}",
        value["config"]["vision_layers"], value["config"]["vision_hidden_size"]
    );
    println!(
        "text: {} layers, backbone {}",
        value["config"]["text_layers"], value["config"]["text_model_type"]
    );
    println!(
        "generation supported: {}",
        value["native_support"]["generation_supported"]
    );
    if let Some(items) = value["native_support"]["unsupported"].as_array() {
        for item in items {
            println!("unsupported: {}", item.as_str().unwrap_or(""));
        }
    }
    for report in reports {
        println!(
            "benchmark {}: {} iterations, {:.3} ms/iter",
            report.name, report.iterations, report.average_ms
        );
    }
    if !value["safetensors"].is_null() {
        println!(
            "safetensors: {} tensors, {} bytes, complete={}",
            value["safetensors"]["tensors"],
            value["safetensors"]["bytes"],
            value["safetensors"]["complete_file"]
        );
    }
    if !value["load_check"].is_null() {
        println!(
            "load check: ok={}, {} ms",
            value["load_check"]["ok"], value["load_check"]["elapsed_ms"]
        );
    }
    if !value["text_forward_check"].is_null() {
        println!(
            "text forward: ok={}, {} ms, logits {}",
            value["text_forward_check"]["ok"],
            value["text_forward_check"]["elapsed_ms"],
            value["text_forward_check"]["logits_shape"]
        );
    }
    if !value["image_forward_check"].is_null() {
        println!(
            "image forward: ok={}, {} ms, logits {}",
            value["image_forward_check"]["ok"],
            value["image_forward_check"]["elapsed_ms"],
            value["image_forward_check"]["logits_shape"]
        );
    }
    if !value["cached_text_forward_check"].is_null() {
        println!(
            "cached text forward: ok={}, {} ms, max diff {}",
            value["cached_text_forward_check"]["ok"],
            value["cached_text_forward_check"]["elapsed_ms"],
            value["cached_text_forward_check"]["max_abs_diff_vs_full_prefix"]
        );
    }
    Ok(())
}

fn max_abs_diff(
    actual: &candle_core::Tensor,
    expected: &candle_core::Tensor,
) -> Result<f32, Box<dyn std::error::Error>> {
    let actual = actual.to_vec3::<f32>()?;
    let expected = expected.to_vec3::<f32>()?;
    let mut max_diff = 0.0f32;
    for (actual_batch, expected_batch) in actual.iter().zip(expected.iter()) {
        for (actual_row, expected_row) in actual_batch.iter().zip(expected_batch.iter()) {
            for (&actual, &expected) in actual_row.iter().zip(expected_row.iter()) {
                max_diff = max_diff.max((actual - expected).abs());
            }
        }
    }
    Ok(max_diff)
}
