use super::nn::{
    atan2, interpolate_linear_last_dim, reflect_pad_last_dim, reflection_pad_left_one, snake,
    upsample_nearest_last_dim,
};
use super::stft::Stft;
use super::*;
use candle_core::{DType, Device, Tensor};

fn max_abs_diff(left: &[f32], right: &[f32]) -> f32 {
    assert_eq!(left.len(), right.len(), "compared buffers differ in length");
    left.iter()
        .zip(right)
        .map(|(left, right)| (left - right).abs())
        .fold(0.0_f32, f32::max)
}

#[test]
fn atan2_matches_the_reference_including_its_axis_conventions() {
    let device = Device::Cpu;
    // Axis and quadrant cases, then a spread of ordinary angles. The axis cases are the ones that
    // matter in practice: a real DFT has an exactly zero imaginary part at DC and at Nyquist.
    let cases: [(f32, f32); 13] = [
        (0.0, 1.0),
        (0.0, -1.0),
        (1.0, 0.0),
        (-1.0, 0.0),
        (0.0, 0.0),
        (1.0, 1.0),
        (1.0, -1.0),
        (-1.0, 1.0),
        (-1.0, -1.0),
        (0.5, 2.0),
        (2.0, 0.5),
        (-3.25, 0.125),
        (7.5, -0.03125),
    ];
    let y =
        Tensor::from_vec(cases.iter().map(|c| c.0).collect::<Vec<_>>(), 13, &device).expect("y");
    let x =
        Tensor::from_vec(cases.iter().map(|c| c.1).collect::<Vec<_>>(), 13, &device).expect("x");

    let computed = atan2(&y, &x).expect("atan2").to_vec1::<f32>().expect("vec");

    let expected = cases.iter().map(|(y, x)| y.atan2(*x)).collect::<Vec<_>>();
    assert!(
        max_abs_diff(&computed, &expected) < 1e-6,
        "computed {computed:?} expected {expected:?}"
    );
}

#[test]
fn linear_interpolation_follows_the_half_sample_centre_convention() {
    let device = Device::Cpu;
    let xs = Tensor::from_vec(vec![0.0_f32, 1.0, 2.0, 3.0], (1, 1, 4), &device).expect("xs");

    // Doubling with align_corners=False maps output i to (i + 0.5) / 2 - 0.5, so the first and last
    // samples are clamped and the interior steps by half.
    let doubled = interpolate_linear_last_dim(&xs, 8)
        .expect("upsample")
        .flatten_all()
        .expect("flat")
        .to_vec1::<f32>()
        .expect("vec");
    let expected = [0.0_f32, 0.25, 0.75, 1.25, 1.75, 2.25, 2.75, 3.0];
    assert!(max_abs_diff(&doubled, &expected) < 1e-6, "{doubled:?}");

    // Halving samples between neighbours rather than dropping every other element.
    let halved = interpolate_linear_last_dim(&xs, 2)
        .expect("downsample")
        .flatten_all()
        .expect("flat")
        .to_vec1::<f32>()
        .expect("vec");
    assert!(max_abs_diff(&halved, &[0.5_f32, 2.5]) < 1e-6, "{halved:?}");
}

#[test]
fn nearest_upsampling_repeats_each_element() {
    let device = Device::Cpu;
    let xs = Tensor::from_vec(vec![1.0_f32, 2.0], (1, 1, 2), &device).expect("xs");

    let upsampled = upsample_nearest_last_dim(&xs, 3).expect("upsample");

    assert_eq!(upsampled.dims(), &[1, 1, 6]);
    assert_eq!(
        upsampled
            .flatten_all()
            .expect("flat")
            .to_vec1::<f32>()
            .expect("vec"),
        vec![1.0, 1.0, 1.0, 2.0, 2.0, 2.0]
    );
}

#[test]
fn reflection_padding_mirrors_across_the_edge_samples() {
    let device = Device::Cpu;
    let xs = Tensor::from_vec(vec![1.0_f32, 2.0, 3.0, 4.0], (1, 4), &device).expect("xs");

    let padded = reflect_pad_last_dim(&xs, 2).expect("pad");

    // The edge sample itself is not repeated, which is what distinguishes reflect from replicate.
    assert_eq!(
        padded
            .flatten_all()
            .expect("flat")
            .to_vec1::<f32>()
            .expect("vec"),
        vec![3.0, 2.0, 1.0, 2.0, 3.0, 4.0, 3.0, 2.0]
    );

    let one = reflection_pad_left_one(&xs).expect("pad one");
    assert_eq!(
        one.flatten_all()
            .expect("flat")
            .to_vec1::<f32>()
            .expect("vec"),
        vec![2.0, 1.0, 2.0, 3.0, 4.0]
    );
}

#[test]
fn snake_activation_is_periodic_around_the_identity() {
    let device = Device::Cpu;
    let xs = Tensor::from_vec(vec![0.0_f32, 0.5, -0.5], (1, 3, 1), &device).expect("xs");
    let alpha = Tensor::from_vec(vec![1.0_f32, 2.0, 0.5], (1, 3, 1), &device).expect("alpha");

    let activated = snake(&xs, &alpha)
        .expect("snake")
        .flatten_all()
        .expect("flat")
        .to_vec1::<f32>()
        .expect("vec");

    let expected = [0.0_f32, 0.5, -0.5]
        .iter()
        .zip([1.0_f32, 2.0, 0.5])
        .map(|(x, a)| x + (a * x).sin().powi(2) / a)
        .collect::<Vec<_>>();
    assert!(max_abs_diff(&activated, &expected) < 1e-6, "{activated:?}");
}

#[test]
fn the_inverse_transform_reconstructs_a_signal_the_forward_transform_analysed() {
    let device = Device::Cpu;
    let stft = Stft::new(20, 5, &device, DType::F32).expect("stft");
    // Two tones plus a constant offset, long enough that edge frames are not the whole signal.
    let samples = (0..600)
        .map(|index| {
            let t = index as f32;
            0.4 * (t * 0.07).sin() + 0.25 * (t * 0.31).cos() + 0.05
        })
        .collect::<Vec<_>>();
    let signal = Tensor::from_vec(samples.clone(), (1, samples.len()), &device).expect("signal");

    let (magnitude, phase) = stft.transform(&signal).expect("transform");
    let reconstructed = stft.inverse(&magnitude, &phase).expect("inverse");

    assert_eq!(magnitude.dims(), &[1, 11, 121]);
    let reconstructed = reconstructed
        .flatten_all()
        .expect("flat")
        .to_vec1::<f32>()
        .expect("vec");
    assert_eq!(reconstructed.len(), samples.len());
    // Perfect reconstruction is expected here: the window and hop satisfy the overlap-add condition
    // and the fold is normalized by the summed squared window.
    assert!(
        max_abs_diff(&reconstructed, &samples) < 1e-4,
        "max difference {}",
        max_abs_diff(&reconstructed, &samples)
    );
}

#[test]
fn a_transform_whose_hop_does_not_divide_its_size_is_refused() {
    let error = Stft::new(20, 6, &Device::Cpu, DType::F32).expect_err("uneven hop");
    assert!(
        error.to_string().contains("whole number of hops"),
        "{error}"
    );
}

#[test]
fn phoneme_tokenization_wraps_boundaries_and_reports_what_it_dropped() {
    let vocab = PhonemeVocab::from_config_value(&serde_json::json!({
        "h": 50, "ɛ": 51, "l": 52
    }))
    .expect("vocab");

    let tokenized = vocab.tokenize("hɛl?");

    assert_eq!(tokenized.input_ids, vec![0, 50, 51, 52, 0]);
    assert_eq!(tokenized.unknown, vec!['?']);
    // The voice index counts phonemes, not tokens, so the boundaries do not shift the style.
    assert_eq!(tokenized.phoneme_count(), 3);
}

#[test]
fn a_multi_character_phoneme_key_is_refused_rather_than_silently_ignored() {
    let error = PhonemeVocab::from_config_value(&serde_json::json!({ "tʃ": 7 }))
        .expect_err("multi-character key");
    assert!(error.to_string().contains("single character"), "{error}");
}

#[test]
fn a_voice_pack_selects_by_phoneme_count_and_saturates_at_its_last_row() {
    let device = Device::Cpu;
    let rows = 4;
    let width = 6;
    let values = (0..rows * width)
        .map(|value| value as f32)
        .collect::<Vec<_>>();
    let pack = VoicePack::from_tensor(
        "test",
        Tensor::from_vec(values, (rows, 1, width), &device).expect("styles"),
    )
    .expect("pack");

    let second = pack.style_for(2).expect("row");
    assert_eq!(second.dims(), &[1, width]);
    assert_eq!(
        second
            .flatten_all()
            .expect("flat")
            .to_vec1::<f32>()
            .expect("vec")[0],
        (2 * width) as f32
    );
    // An utterance longer than the pack covers still speaks, using the closest style it has.
    let saturated = pack.style_for(999).expect("row");
    assert_eq!(
        saturated
            .flatten_all()
            .expect("flat")
            .to_vec1::<f32>()
            .expect("vec")[0],
        ((rows - 1) * width) as f32
    );
}

#[test]
fn a_voice_pack_with_the_wrong_rank_is_refused() {
    let device = Device::Cpu;
    let error = VoicePack::from_tensor(
        "test",
        Tensor::zeros((4, 6), DType::F32, &device).expect("styles"),
    )
    .expect_err("rank");
    assert!(
        error.to_string().contains("[rows, 1, style_width]"),
        "{error}"
    );
}

#[test]
fn wav_encoding_declares_mono_pcm16_at_the_model_sample_rate() {
    let speech = SynthesizedSpeech {
        samples: vec![0.0, 0.5, -0.5, 1.0],
        sample_rate: SAMPLE_RATE,
        input_ids: vec![0, 1, 0],
        unknown_phonemes: Vec::new(),
        durations: vec![1, 1, 1],
        elapsed: Duration::from_millis(500),
    };

    let wav = speech.to_wav().expect("wav");

    assert_eq!(&wav[0..4], b"RIFF");
    assert_eq!(&wav[8..12], b"WAVE");
    assert_eq!(u16::from_le_bytes([wav[22], wav[23]]), 1, "mono");
    assert_eq!(
        u32::from_le_bytes([wav[24], wav[25], wav[26], wav[27]]),
        SAMPLE_RATE as u32
    );
    assert_eq!(u16::from_le_bytes([wav[34], wav[35]]), 16, "bits");
    assert_eq!(wav.len(), 44 + speech.samples.len() * 2);
    // A full-scale sample must not wrap around to negative when it is quantized.
    let last = i16::from_le_bytes([wav[wav.len() - 2], wav[wav.len() - 1]]);
    assert_eq!(last, i16::MAX);
}

#[test]
fn synthesis_reports_speed_against_real_time() {
    let speech = SynthesizedSpeech {
        samples: vec![0.0; SAMPLE_RATE * 2],
        sample_rate: SAMPLE_RATE,
        input_ids: vec![0, 1, 0],
        unknown_phonemes: Vec::new(),
        durations: vec![1, 1, 1],
        elapsed: Duration::from_secs(1),
    };

    assert_eq!(speech.duration(), Duration::from_secs(2));
    assert!((speech.realtime_factor() - 2.0).abs() < 1e-9);
    assert_eq!(speech.rms(), 0.0);
}

#[test]
fn the_default_cache_directory_follows_the_repository_id() {
    let dir = std::path::PathBuf::from("/models");
    let paths = KokoroPaths::from_cache_dir(dir.join("hexgrad--Kokoro-82M")).expect("paths");

    assert!(paths.config.ends_with("hexgrad--Kokoro-82M/config.json"));
    assert!(paths.weights.ends_with(DEFAULT_WEIGHTS_FILE));
    assert!(paths.voice_file("af_heart").ends_with("voices/af_heart.pt"));
    // Nothing exists under this path, so the first missing file is the configuration.
    assert_eq!(paths.first_missing(), Some(paths.config.as_path()));
}

#[test]
fn a_configuration_whose_transform_cannot_fold_is_refused() {
    let mut config = test_config();
    config.istftnet.gen_istft_hop_size = 6;

    let error = config.validate_native_support().expect_err("uneven hop");

    assert!(error.to_string().contains("whole number of"), "{error}");
}

#[test]
fn the_published_configuration_is_supported() {
    test_config()
        .validate_native_support()
        .expect("the shipped Kokoro configuration is served natively");
}

/// The configuration published with `hexgrad/Kokoro-82M`.
fn test_config() -> KokoroConfig {
    serde_json::from_value(serde_json::json!({
        "istftnet": {
            "upsample_kernel_sizes": [20, 12],
            "upsample_rates": [10, 6],
            "gen_istft_hop_size": 5,
            "gen_istft_n_fft": 20,
            "resblock_dilation_sizes": [[1, 3, 5], [1, 3, 5], [1, 3, 5]],
            "resblock_kernel_sizes": [3, 7, 11],
            "upsample_initial_channel": 512
        },
        "plbert": {
            "hidden_size": 768,
            "num_attention_heads": 12,
            "intermediate_size": 2048,
            "max_position_embeddings": 512,
            "num_hidden_layers": 12,
            "dropout": 0.1
        },
        "dim_in": 64,
        "dropout": 0.2,
        "hidden_dim": 512,
        "max_conv_dim": 512,
        "max_dur": 50,
        "multispeaker": true,
        "n_layer": 3,
        "n_mels": 80,
        "n_token": 178,
        "style_dim": 128,
        "text_encoder_kernel_size": 5,
        "vocab": { "h": 50 }
    }))
    .expect("configuration")
}
