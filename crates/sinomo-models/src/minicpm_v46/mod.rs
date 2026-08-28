use candle_core::{D, DType, Device, Module, Tensor};
use candle_nn::{
    Conv1d, Conv1dConfig, Conv2d, Conv2dConfig, Embedding, LayerNorm, Linear, VarBuilder,
    conv1d_no_bias, conv2d, embedding, layer_norm, linear,
};
use candle_transformers::models::siglip;
use image::{DynamicImage, imageops::FilterType};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use thiserror::Error;

pub const DEFAULT_MODEL_ID: &str = "openbmb/MiniCPM-V-4_6";
pub const IMAGE_MEAN: [f32; 3] = [0.5, 0.5, 0.5];
pub const IMAGE_STD: [f32; 3] = [0.5, 0.5, 0.5];

#[derive(Debug, Error)]
pub enum MiniCpmV46Error {
    #[error("MiniCPM-V 4.6 io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("MiniCPM-V 4.6 json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("MiniCPM-V 4.6 image error: {0}")]
    Image(#[from] image::ImageError),
    #[error("MiniCPM-V 4.6 safetensors error: {0}")]
    Safetensors(#[from] safetensors::SafeTensorError),
    #[error("MiniCPM-V 4.6 candle error: {0}")]
    Candle(#[from] candle_core::Error),
    #[error("MiniCPM-V 4.6 unsupported architecture: {detail}")]
    Unsupported { detail: String },
    #[error("MiniCPM-V 4.6 invalid input: {detail}")]
    InvalidInput { detail: String },
}

pub type Result<T> = std::result::Result<T, MiniCpmV46Error>;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MiniCpmV46Config {
    pub architectures: Vec<String>,
    pub model_type: String,
    pub image_size: usize,
    pub vision_config: MiniCpmV46VisionConfig,
    pub text_config: MiniCpmV46TextConfig,
    pub insert_layer_id: usize,
    pub image_token_id: u32,
    pub video_token_id: u32,
    #[serde(default)]
    pub drop_vision_last_layer: bool,
    #[serde(default = "default_downsample_mode")]
    pub downsample_mode: String,
    #[serde(default = "default_merge_kernel_size")]
    pub merge_kernel_size: Vec<usize>,
    #[serde(default = "default_merger_times")]
    pub merger_times: usize,
}

fn default_downsample_mode() -> String {
    "16x".to_string()
}

fn default_merge_kernel_size() -> Vec<usize> {
    vec![2, 2]
}

fn default_merger_times() -> usize {
    1
}

impl MiniCpmV46Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    }

    pub fn validate_native_support(&self) -> Result<NativeSupportReport> {
        let mut unsupported = Vec::new();
        if self.model_type != "minicpmv4_6" {
            unsupported.push(format!("model_type `{}`", self.model_type));
        }
        if self.text_config.model_type != "qwen3_5_text" {
            unsupported.push(format!(
                "text_config.model_type `{}`",
                self.text_config.model_type
            ));
        }
        if self.text_config.attention_bias {
            unsupported.push("Qwen3.5 attention_bias=true".to_string());
        }
        for layer_type in &self.text_config.layer_types {
            if layer_type != "linear_attention" && layer_type != "full_attention" {
                unsupported.push(format!("Qwen3.5 layer type `{layer_type}`"));
            }
        }
        if self.drop_vision_last_layer {
            unsupported.push("drop_vision_last_layer=true".to_string());
        }
        if parse_pair(&self.merge_kernel_size, "merge_kernel_size")? != (2, 2) {
            unsupported.push(format!("merge_kernel_size {:?}", self.merge_kernel_size));
        }
        Ok(NativeSupportReport {
            model_type: self.model_type.clone(),
            vision_supported: !unsupported
                .iter()
                .any(|item| item.contains("vision") || item.contains("merge")),
            text_supported: !unsupported
                .iter()
                .any(|item| item.contains("Qwen") || item.contains("text")),
            generation_supported: unsupported.is_empty(),
            unsupported,
        })
    }

    pub fn candle_siglip_vision_config(&self) -> siglip::VisionConfig {
        siglip::VisionConfig {
            hidden_size: self.vision_config.hidden_size,
            intermediate_size: self.vision_config.intermediate_size,
            num_hidden_layers: self.vision_config.num_hidden_layers,
            num_attention_heads: self.vision_config.num_attention_heads,
            num_channels: self.vision_config.num_channels,
            image_size: self.vision_config.image_size,
            patch_size: self.vision_config.patch_size,
            hidden_act: candle_nn::Activation::GeluPytorchTanh,
            layer_norm_eps: self.vision_config.layer_norm_eps,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MiniCpmV46VisionConfig {
    pub attention_dropout: f64,
    pub hidden_act: String,
    pub hidden_size: usize,
    pub image_size: usize,
    pub intermediate_size: usize,
    pub layer_norm_eps: f64,
    pub model_type: String,
    pub num_attention_heads: usize,
    pub num_channels: usize,
    pub num_hidden_layers: usize,
    pub patch_size: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MiniCpmV46TextConfig {
    pub attention_bias: bool,
    pub attention_dropout: f64,
    #[serde(default)]
    pub attn_output_gate: bool,
    pub full_attention_interval: usize,
    pub head_dim: usize,
    pub hidden_act: String,
    pub hidden_size: usize,
    pub initializer_range: f64,
    pub intermediate_size: usize,
    pub layer_types: Vec<String>,
    pub linear_conv_kernel_dim: usize,
    pub linear_key_head_dim: usize,
    pub linear_num_key_heads: usize,
    pub linear_num_value_heads: usize,
    pub linear_value_head_dim: usize,
    pub mamba_ssm_dtype: String,
    pub max_position_embeddings: usize,
    pub mlp_only_layers: Vec<usize>,
    pub mtp_num_hidden_layers: usize,
    pub mtp_use_dedicated_embeddings: bool,
    pub num_attention_heads: usize,
    pub num_hidden_layers: usize,
    pub num_key_value_heads: usize,
    pub partial_rotary_factor: f64,
    pub rms_norm_eps: f64,
    pub rope_parameters: RopeParameters,
    pub vocab_size: usize,
    pub model_type: String,
    pub tie_word_embeddings: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RopeParameters {
    pub partial_rotary_factor: f64,
    pub rope_theta: f64,
    pub rope_type: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct NativeSupportReport {
    pub model_type: String,
    pub vision_supported: bool,
    pub text_supported: bool,
    pub generation_supported: bool,
    pub unsupported: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MiniCpmV46PreprocessorConfig {
    #[serde(default = "default_do_normalize")]
    pub do_normalize: bool,
    #[serde(default = "default_do_rescale")]
    pub do_rescale: bool,
    #[serde(default = "default_do_resize")]
    pub do_resize: bool,
    #[serde(default = "default_image_mean")]
    pub image_mean: Vec<f32>,
    #[serde(default = "default_image_std")]
    pub image_std: Vec<f32>,
    #[serde(default = "default_rescale_factor")]
    pub rescale_factor: f32,
    #[serde(default = "default_size")]
    pub size: BTreeMap<String, usize>,
}

impl Default for MiniCpmV46PreprocessorConfig {
    fn default() -> Self {
        Self {
            do_normalize: default_do_normalize(),
            do_rescale: default_do_rescale(),
            do_resize: default_do_resize(),
            image_mean: default_image_mean(),
            image_std: default_image_std(),
            rescale_factor: default_rescale_factor(),
            size: default_size(),
        }
    }
}

impl MiniCpmV46PreprocessorConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    }

    pub fn shortest_edge(&self) -> usize {
        self.size.get("shortest_edge").copied().unwrap_or(980)
    }
}

fn default_do_normalize() -> bool {
    true
}

fn default_do_rescale() -> bool {
    true
}

fn default_do_resize() -> bool {
    true
}

fn default_image_mean() -> Vec<f32> {
    IMAGE_MEAN.to_vec()
}

fn default_image_std() -> Vec<f32> {
    IMAGE_STD.to_vec()
}

fn default_rescale_factor() -> f32 {
    1.0 / 255.0
}

fn default_size() -> BTreeMap<String, usize> {
    BTreeMap::from([("shortest_edge".to_string(), 980)])
}

pub fn preprocess_image(
    image: &DynamicImage,
    preprocessor: &MiniCpmV46PreprocessorConfig,
    device: &Device,
) -> Result<Tensor> {
    let size = preprocessor.shortest_edge() as u32;
    let image = if preprocessor.do_resize {
        image.resize_exact(size, size, FilterType::CatmullRom)
    } else {
        image.clone()
    };
    let rgb = image.to_rgb8();
    let (width, height) = rgb.dimensions();
    let mean = normalize_triplet(&preprocessor.image_mean, "image_mean")?;
    let std = normalize_triplet(&preprocessor.image_std, "image_std")?;
    let mut chw = vec![0f32; 3 * width as usize * height as usize];
    let plane = width as usize * height as usize;
    for (index, pixel) in rgb.pixels().enumerate() {
        for channel in 0..3 {
            let mut value = f32::from(pixel[channel]);
            if preprocessor.do_rescale {
                value *= preprocessor.rescale_factor;
            }
            if preprocessor.do_normalize {
                value = (value - mean[channel]) / std[channel];
            }
            chw[channel * plane + index] = value;
        }
    }
    Ok(Tensor::from_vec(
        chw,
        (1, 3, height as usize, width as usize),
        device,
    )?)
}

fn normalize_triplet(values: &[f32], name: &str) -> Result<[f32; 3]> {
    if values.len() != 3 {
        return Err(MiniCpmV46Error::InvalidInput {
            detail: format!("{name} must contain 3 values, got {}", values.len()),
        });
    }
    Ok([values[0], values[1], values[2]])
}

#[derive(Debug)]
pub struct MiniCpmV46VisionEmbeddings {
    patch_embedding: Conv2d,
    position_embedding: Embedding,
    patch_size: usize,
    base_num_patches_per_side: usize,
}

impl MiniCpmV46VisionEmbeddings {
    pub fn new(config: &MiniCpmV46VisionConfig, vb: VarBuilder) -> Result<Self> {
        let patch_embedding = conv2d(
            config.num_channels,
            config.hidden_size,
            config.patch_size,
            Conv2dConfig {
                stride: config.patch_size,
                ..Default::default()
            },
            vb.pp("patch_embedding"),
        )?;
        let base_num_patches_per_side = config.image_size / config.patch_size;
        let position_embedding = embedding(
            base_num_patches_per_side * base_num_patches_per_side,
            config.hidden_size,
            vb.pp("position_embedding"),
        )?;
        Ok(Self {
            patch_embedding,
            position_embedding,
            patch_size: config.patch_size,
            base_num_patches_per_side,
        })
    }

    pub fn forward(
        &self,
        pixel_values: &Tensor,
        target_sizes: &[(usize, usize)],
    ) -> Result<Tensor> {
        let (batch, _channels, height, width) = pixel_values.dims4()?;
        let embeddings = pixel_values
            .apply(&self.patch_embedding)?
            .flatten_from(2)?
            .transpose(1, 2)?;
        let patch_grid = (height / self.patch_size, width / self.patch_size);
        let pos_ids = self.position_ids(target_sizes, patch_grid, batch, pixel_values.device())?;
        let position_embeddings = self.position_embedding.forward(&pos_ids)?.unsqueeze(0)?;
        Ok(embeddings.broadcast_add(&position_embeddings)?)
    }

    fn position_ids(
        &self,
        target_sizes: &[(usize, usize)],
        patch_grid: (usize, usize),
        batch: usize,
        device: &Device,
    ) -> Result<Tensor> {
        if batch != 1 {
            if target_sizes.len() != batch {
                return Err(MiniCpmV46Error::InvalidInput {
                    detail: format!(
                        "target_sizes length {} must match pixel batch {batch}",
                        target_sizes.len()
                    ),
                });
            }
            let mut all = Vec::new();
            for &target_size in target_sizes {
                all.extend(self.position_ids_for_grid(target_size)?);
            }
            return Ok(Tensor::from_vec(
                all,
                (batch, patch_grid.0 * patch_grid.1),
                device,
            )?);
        }
        let total_tokens: usize = target_sizes.iter().map(|(h, w)| h * w).sum();
        if total_tokens != patch_grid.0 * patch_grid.1 {
            return Err(MiniCpmV46Error::InvalidInput {
                detail: format!(
                    "target_sizes describe {total_tokens} patches but pixel_values produce {} patches",
                    patch_grid.0 * patch_grid.1
                ),
            });
        }
        let mut ids = Vec::with_capacity(total_tokens);
        for &target_size in target_sizes {
            ids.extend(self.position_ids_for_grid(target_size)?);
        }
        Ok(Tensor::from_vec(ids, total_tokens, device)?)
    }

    fn position_ids_for_grid(&self, target_size: (usize, usize)) -> Result<Vec<u32>> {
        let (height, width) = target_size;
        if height == 0 || width == 0 {
            return Err(MiniCpmV46Error::InvalidInput {
                detail: format!("target size must be non-zero, got ({height}, {width})"),
            });
        }
        let base = self.base_num_patches_per_side;
        let mut ids = Vec::with_capacity(height * width);
        for row in 0..height {
            let bucket_row = row * base / height;
            for col in 0..width {
                let bucket_col = col * base / width;
                ids.push((bucket_row * base + bucket_col) as u32);
            }
        }
        Ok(ids)
    }
}

#[derive(Debug)]
pub struct MiniCpmV46VisionMlp {
    fc1: Linear,
    fc2: Linear,
}

impl MiniCpmV46VisionMlp {
    pub fn new(config: &MiniCpmV46VisionConfig, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            fc1: linear(config.hidden_size, config.intermediate_size, vb.pp("fc1"))?,
            fc2: linear(config.intermediate_size, config.hidden_size, vb.pp("fc2"))?,
        })
    }

    pub fn forward(&self, hidden_states: &Tensor) -> Result<Tensor> {
        Ok(hidden_states.apply(&self.fc1)?.gelu()?.apply(&self.fc2)?)
    }
}

#[derive(Debug)]
pub struct MiniCpmV46VisionEncoderLayer {
    layer_norm1: LayerNorm,
    self_attn: MiniCpmV46VisionAttention,
    layer_norm2: LayerNorm,
    mlp: MiniCpmV46VisionMlp,
}

impl MiniCpmV46VisionEncoderLayer {
    pub fn new(config: &MiniCpmV46VisionConfig, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            layer_norm1: layer_norm(
                config.hidden_size,
                config.layer_norm_eps,
                vb.pp("layer_norm1"),
            )?,
            self_attn: MiniCpmV46VisionAttention::new(config, vb.pp("self_attn"))?,
            layer_norm2: layer_norm(
                config.hidden_size,
                config.layer_norm_eps,
                vb.pp("layer_norm2"),
            )?,
            mlp: MiniCpmV46VisionMlp::new(config, vb.pp("mlp"))?,
        })
    }

    pub fn forward(&self, hidden_states: &Tensor, cu_seqlens: &[usize]) -> Result<Tensor> {
        let residual = hidden_states.clone();
        let hidden_states = hidden_states.apply(&self.layer_norm1)?;
        let hidden_states = self.self_attn.forward(&hidden_states, cu_seqlens)?;
        let hidden_states = (residual + hidden_states)?;

        let residual = hidden_states.clone();
        let hidden_states = hidden_states.apply(&self.layer_norm2)?;
        let hidden_states = self.mlp.forward(&hidden_states)?;
        Ok((residual + hidden_states)?)
    }
}

#[derive(Debug)]
pub struct MiniCpmV46VisionAttention {
    q_proj: Linear,
    k_proj: Linear,
    v_proj: Linear,
    out_proj: Linear,
    num_heads: usize,
    head_dim: usize,
    scaling: f64,
}

impl MiniCpmV46VisionAttention {
    pub fn new(config: &MiniCpmV46VisionConfig, vb: VarBuilder) -> Result<Self> {
        if !config
            .hidden_size
            .is_multiple_of(config.num_attention_heads)
        {
            return Err(MiniCpmV46Error::InvalidInput {
                detail: format!(
                    "vision hidden_size {} must be divisible by num_attention_heads {}",
                    config.hidden_size, config.num_attention_heads
                ),
            });
        }
        let head_dim = config.hidden_size / config.num_attention_heads;
        Ok(Self {
            q_proj: linear(config.hidden_size, config.hidden_size, vb.pp("q_proj"))?,
            k_proj: linear(config.hidden_size, config.hidden_size, vb.pp("k_proj"))?,
            v_proj: linear(config.hidden_size, config.hidden_size, vb.pp("v_proj"))?,
            out_proj: linear(config.hidden_size, config.hidden_size, vb.pp("out_proj"))?,
            num_heads: config.num_attention_heads,
            head_dim,
            scaling: (head_dim as f64).powf(-0.5),
        })
    }

    pub fn forward(&self, hidden_states: &Tensor, cu_seqlens: &[usize]) -> Result<Tensor> {
        let (batch, seq_len, dim) = hidden_states.dims3()?;
        if batch != 1 {
            return Err(MiniCpmV46Error::Unsupported {
                detail: format!(
                    "MiniCPM-V 4.6 packed vision attention expects batch=1, got {batch}"
                ),
            });
        }
        let q = hidden_states
            .apply(&self.q_proj)?
            .squeeze(0)?
            .reshape((seq_len, self.num_heads, self.head_dim))?
            .transpose(0, 1)?;
        let k = hidden_states
            .apply(&self.k_proj)?
            .squeeze(0)?
            .reshape((seq_len, self.num_heads, self.head_dim))?
            .transpose(0, 1)?;
        let v = hidden_states
            .apply(&self.v_proj)?
            .squeeze(0)?
            .reshape((seq_len, self.num_heads, self.head_dim))?
            .transpose(0, 1)?;
        let mut chunks = Vec::new();
        for window in cu_seqlens.windows(2) {
            let start = window[0];
            let len = window[1] - window[0];
            let q_chunk = q.narrow(1, start, len)?;
            let k_chunk = k.narrow(1, start, len)?;
            let v_chunk = v.narrow(1, start, len)?;
            let scores = (q_chunk.matmul(&k_chunk.transpose(1, 2)?)? * self.scaling)?;
            let weights = candle_nn::ops::softmax(&scores, D::Minus1)?;
            let out = weights
                .matmul(&v_chunk)?
                .transpose(0, 1)?
                .reshape((len, dim))?;
            chunks.push(out);
        }
        let chunk_refs: Vec<&Tensor> = chunks.iter().collect();
        let output = Tensor::cat(&chunk_refs, 0)?.unsqueeze(0)?;
        Ok(output.apply(&self.out_proj)?)
    }
}

#[derive(Debug)]
pub struct MiniCpmV46VitWindowAttentionMerger {
    window_kernel_size: (usize, usize),
    embed_dim: usize,
    self_attn: MiniCpmV46VisionAttention,
    layer_norm1: LayerNorm,
    pre_norm: LayerNorm,
    linear_1: Linear,
    linear_2: Linear,
}

impl MiniCpmV46VitWindowAttentionMerger {
    pub fn new(config: &MiniCpmV46VisionConfig, vb: VarBuilder) -> Result<Self> {
        let window_kernel_size = (2usize, 2usize);
        let window_hidden_size = config.hidden_size * window_kernel_size.0 * window_kernel_size.1;
        let window_intermediate_size =
            config.intermediate_size * window_kernel_size.0 * window_kernel_size.1;
        Ok(Self {
            window_kernel_size,
            embed_dim: config.hidden_size,
            self_attn: MiniCpmV46VisionAttention::new(config, vb.pp("self_attn"))?,
            layer_norm1: layer_norm(
                config.hidden_size,
                config.layer_norm_eps,
                vb.pp("layer_norm1"),
            )?,
            pre_norm: layer_norm(window_hidden_size, config.layer_norm_eps, vb.pp("pre_norm"))?,
            linear_1: linear(
                window_hidden_size,
                window_intermediate_size,
                vb.pp("linear_1"),
            )?,
            linear_2: linear(
                window_intermediate_size,
                config.hidden_size,
                vb.pp("linear_2"),
            )?,
        })
    }

    pub fn forward(
        &self,
        hidden_states: &Tensor,
        target_sizes: &[(usize, usize)],
        cu_seqlens: &[usize],
    ) -> Result<Tensor> {
        let (window_index, window_cu_seqlens) = self.get_window_index(target_sizes)?;
        let device = hidden_states.device();
        let window_index_tensor = Tensor::from_vec(
            window_index.iter().map(|value| *value as u32).collect(),
            window_index.len(),
            device,
        )?;
        let restore_index = inverse_permutation(&window_index);
        let restore_index_tensor = Tensor::from_vec(
            restore_index.iter().map(|value| *value as u32).collect(),
            restore_index.len(),
            device,
        )?;

        let residual = hidden_states.clone();
        let hidden_states = hidden_states.apply(&self.layer_norm1)?;
        let hidden_states = hidden_states.index_select(&window_index_tensor, 1)?;
        let hidden_states = self.self_attn.forward(&hidden_states, &window_cu_seqlens)?;
        let hidden_states = hidden_states.index_select(&restore_index_tensor, 1)?;
        let hidden_states = (residual + hidden_states)?;

        let mut outputs = Vec::with_capacity(target_sizes.len());
        for (batch_idx, &(height, width)) in target_sizes.iter().enumerate() {
            let start = cu_seqlens[batch_idx];
            let len = cu_seqlens[batch_idx + 1] - start;
            let patch = hidden_states.narrow(1, start, len)?.squeeze(0)?;
            outputs.push(self.merge_window_mlp(&patch, height, width)?);
        }
        let refs: Vec<&Tensor> = outputs.iter().collect();
        Ok(Tensor::cat(&refs, 0)?.unsqueeze(0)?)
    }

    pub fn get_window_index(
        &self,
        target_sizes: &[(usize, usize)],
    ) -> Result<(Vec<usize>, Vec<usize>)> {
        let (window_h, window_w) = self.window_kernel_size;
        let mut window_index = Vec::new();
        let mut cu_seqlens = vec![0usize];
        let mut token_offset = 0usize;
        for &(height, width) in target_sizes {
            if height % window_h != 0 || width % window_w != 0 {
                return Err(MiniCpmV46Error::InvalidInput {
                    detail: format!(
                        "height={height}, width={width} must be divisible by window size ({window_h}, {window_w})"
                    ),
                });
            }
            for block_h in 0..(height / window_h) {
                for block_w in 0..(width / window_w) {
                    for inner_h in 0..window_h {
                        for inner_w in 0..window_w {
                            window_index.push(
                                token_offset
                                    + (block_h * window_h + inner_h) * width
                                    + (block_w * window_w + inner_w),
                            );
                        }
                    }
                    cu_seqlens.push(cu_seqlens.last().copied().unwrap() + window_h * window_w);
                }
            }
            token_offset += height * width;
        }
        Ok((window_index, cu_seqlens))
    }

    fn merge_window_mlp(&self, patch: &Tensor, height: usize, width: usize) -> Result<Tensor> {
        let (window_h, window_w) = self.window_kernel_size;
        let merged_h = height / window_h;
        let merged_w = width / window_w;
        let patch_5d = patch
            .reshape((merged_h, window_h, merged_w, window_w, self.embed_dim))?
            .permute((0, 2, 1, 3, 4))?;
        let hidden_state =
            patch_5d.reshape((merged_h * merged_w, window_h * window_w * self.embed_dim))?;
        let residual = patch_5d
            .reshape((merged_h * merged_w, window_h * window_w, self.embed_dim))?
            .mean(1)?;
        Ok((hidden_state
            .apply(&self.pre_norm)?
            .apply(&self.linear_1)?
            .gelu()?
            .apply(&self.linear_2)?
            + residual)?)
    }
}

fn inverse_permutation(values: &[usize]) -> Vec<usize> {
    let mut inverse = vec![0usize; values.len()];
    for (position, &value) in values.iter().enumerate() {
        inverse[value] = position;
    }
    inverse
}

#[derive(Debug)]
pub struct MiniCpmV46NativeVisionTower {
    embeddings: MiniCpmV46VisionEmbeddings,
    encoder_layers: Vec<MiniCpmV46VisionEncoderLayer>,
    post_layernorm: LayerNorm,
    vit_merger: MiniCpmV46VitWindowAttentionMerger,
    insert_layer_id: usize,
}

impl MiniCpmV46NativeVisionTower {
    pub fn new(config: &MiniCpmV46Config, vb: VarBuilder) -> Result<Self> {
        if config.drop_vision_last_layer {
            return Err(MiniCpmV46Error::Unsupported {
                detail: "drop_vision_last_layer requires a custom encoder truncation path"
                    .to_string(),
            });
        }
        let mut encoder_layers = Vec::with_capacity(config.vision_config.num_hidden_layers);
        for index in 0..config.vision_config.num_hidden_layers {
            encoder_layers.push(MiniCpmV46VisionEncoderLayer::new(
                &config.vision_config,
                vb.pp("encoder").pp("layers").pp(index),
            )?);
        }
        Ok(Self {
            embeddings: MiniCpmV46VisionEmbeddings::new(
                &config.vision_config,
                vb.pp("embeddings"),
            )?,
            encoder_layers,
            post_layernorm: layer_norm(
                config.vision_config.hidden_size,
                config.vision_config.layer_norm_eps,
                vb.pp("post_layernorm"),
            )?,
            vit_merger: MiniCpmV46VitWindowAttentionMerger::new(
                &config.vision_config,
                vb.pp("vit_merger"),
            )?,
            insert_layer_id: config.insert_layer_id,
        })
    }

    /// Constructs the vision encoder from memory-mapped safetensor files.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the mapped files are not modified or
    /// replaced for as long as tensors backed by those mappings are alive.
    pub unsafe fn from_mmaped_safetensors(
        config: &MiniCpmV46Config,
        safetensors: &[impl AsRef<Path>],
        dtype: DType,
        device: &Device,
    ) -> Result<Self> {
        let vb = unsafe { VarBuilder::from_mmaped_safetensors(safetensors, dtype, device)? };
        Self::new(config, vb.pp("model").pp("vision_tower"))
    }

    pub fn forward(
        &self,
        pixel_values: &Tensor,
        target_sizes: &[(usize, usize)],
        use_vit_merger: bool,
    ) -> Result<Tensor> {
        let mut hidden_states = self.embeddings.forward(pixel_values, target_sizes)?;
        let mut target_sizes = target_sizes.to_vec();
        let mut cu_seqlens = cu_seqlens_for_target_sizes(&target_sizes);
        let insert_layer_id = if use_vit_merger {
            Some(self.insert_layer_id)
        } else {
            None
        };

        for (layer_index, encoder_layer) in self.encoder_layers.iter().enumerate() {
            hidden_states = encoder_layer.forward(&hidden_states, &cu_seqlens)?;
            if Some(layer_index) == insert_layer_id {
                hidden_states =
                    self.vit_merger
                        .forward(&hidden_states, &target_sizes, &cu_seqlens)?;
                target_sizes = halve_target_sizes(&target_sizes)?;
                cu_seqlens = cu_seqlens_for_target_sizes(&target_sizes);
            }
        }
        Ok(hidden_states.apply(&self.post_layernorm)?)
    }
}

fn cu_seqlens_for_target_sizes(target_sizes: &[(usize, usize)]) -> Vec<usize> {
    let mut out = Vec::with_capacity(target_sizes.len() + 1);
    out.push(0);
    for &(height, width) in target_sizes {
        out.push(out.last().copied().unwrap() + height * width);
    }
    out
}

fn halve_target_sizes(target_sizes: &[(usize, usize)]) -> Result<Vec<(usize, usize)>> {
    target_sizes
        .iter()
        .map(|&(height, width)| {
            if height % 2 != 0 || width % 2 != 0 {
                return Err(MiniCpmV46Error::InvalidInput {
                    detail: format!(
                        "patch grid ({height}, {width}) must be divisible by the ViT merger"
                    ),
                });
            }
            Ok((height / 2, width / 2))
        })
        .collect()
}

#[derive(Debug)]
pub struct MiniCpmV46VisionTower {
    model: siglip::VisionModel,
}

impl MiniCpmV46VisionTower {
    pub fn new(config: &MiniCpmV46Config, vb: VarBuilder) -> Result<Self> {
        if config.drop_vision_last_layer {
            return Err(MiniCpmV46Error::Unsupported {
                detail: "drop_vision_last_layer requires a custom SigLIP encoder truncation path"
                    .to_string(),
            });
        }
        let model = siglip::VisionModel::new(&config.candle_siglip_vision_config(), false, vb)?;
        Ok(Self { model })
    }

    /// Constructs the vision tower from memory-mapped safetensor files.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the mapped files are not modified or
    /// replaced for as long as tensors backed by those mappings are alive.
    pub unsafe fn from_mmaped_safetensors(
        config: &MiniCpmV46Config,
        safetensors: &[impl AsRef<Path>],
        dtype: DType,
        device: &Device,
    ) -> Result<Self> {
        let vb = unsafe { VarBuilder::from_mmaped_safetensors(safetensors, dtype, device)? };
        Self::new(config, vb.pp("model").pp("vision_tower"))
    }

    pub fn forward(&self, pixel_values: &Tensor) -> Result<Tensor> {
        Ok(self.model.forward(pixel_values)?)
    }
}

#[derive(Debug)]
pub struct MiniCpmV46DownsampleMlp {
    pre_norm: LayerNorm,
    linear_1: Linear,
    linear_2: Linear,
}

impl MiniCpmV46DownsampleMlp {
    pub fn new(hidden_size: usize, output_dim: usize, vb: VarBuilder) -> Result<Self> {
        let merged_hidden_size = hidden_size * 4;
        Ok(Self {
            pre_norm: layer_norm(merged_hidden_size, 1e-6, vb.pp("pre_norm"))?,
            linear_1: linear(merged_hidden_size, merged_hidden_size, vb.pp("linear_1"))?,
            linear_2: linear(merged_hidden_size, output_dim, vb.pp("linear_2"))?,
        })
    }

    pub fn forward(&self, hidden_states: &Tensor) -> Result<Tensor> {
        Ok(hidden_states
            .apply(&self.pre_norm)?
            .apply(&self.linear_1)?
            .gelu_erf()?
            .apply(&self.linear_2)?)
    }
}

#[derive(Debug)]
pub struct MiniCpmV46Merger {
    merge_kernel_size: (usize, usize),
    mlp: Vec<MiniCpmV46DownsampleMlp>,
}

impl MiniCpmV46Merger {
    pub fn new(config: &MiniCpmV46Config, vb: VarBuilder) -> Result<Self> {
        let merge_kernel_size = parse_pair(&config.merge_kernel_size, "merge_kernel_size")?;
        if merge_kernel_size != (2, 2) {
            return Err(MiniCpmV46Error::Unsupported {
                detail: format!(
                    "only 2x2 MiniCPM-V 4.6 merger kernels are implemented, got {:?}",
                    config.merge_kernel_size
                ),
            });
        }
        if config.merger_times == 0 {
            return Err(MiniCpmV46Error::InvalidInput {
                detail: "merger_times must be at least 1".to_string(),
            });
        }
        let mut mlp = Vec::new();
        for index in 0..config.merger_times {
            let output_dim = if index + 1 == config.merger_times {
                config.text_config.hidden_size
            } else {
                config.vision_config.hidden_size
            };
            mlp.push(MiniCpmV46DownsampleMlp::new(
                config.vision_config.hidden_size,
                output_dim,
                vb.pp("mlp").pp(index),
            )?);
        }
        Ok(Self {
            merge_kernel_size,
            mlp,
        })
    }

    pub fn forward(
        &self,
        hidden_states: &Tensor,
        target_sizes: &[(usize, usize)],
    ) -> Result<Vec<Tensor>> {
        let (batch, _seq, embed_dim) = hidden_states.dims3()?;
        if batch != 1 {
            return Err(MiniCpmV46Error::Unsupported {
                detail: format!("MiniCPM-V 4.6 merger expects NaViT-packed batch=1, got {batch}"),
            });
        }
        let (merge_h, merge_w) = self.merge_kernel_size;
        let mut start = 0usize;
        let mut processed = Vec::with_capacity(target_sizes.len());
        for &(mut height, mut width) in target_sizes {
            if height % merge_h != 0 || width % merge_w != 0 {
                return Err(MiniCpmV46Error::InvalidInput {
                    detail: format!(
                        "patch grid ({height}, {width}) must be divisible by merger kernel ({merge_h}, {merge_w})"
                    ),
                });
            }
            let num_patches = height * width;
            let patch = hidden_states.narrow(1, start, num_patches)?.squeeze(0)?;
            let mut hidden_state = merge_2x2_windows(&patch, height, width, embed_dim)?;
            hidden_state = self.mlp[0].forward(&hidden_state)?;
            for index in 1..self.mlp.len() {
                height /= merge_h;
                width /= merge_w;
                if height % merge_h != 0 || width % merge_w != 0 {
                    return Err(MiniCpmV46Error::InvalidInput {
                        detail: format!(
                            "patch grid ({height}, {width}) must be divisible by merger kernel ({merge_h}, {merge_w}) at merge round {index}"
                        ),
                    });
                }
                let inner_dim = hidden_state.dim(1)?;
                hidden_state = merge_2x2_windows(&hidden_state, height, width, inner_dim)?;
                hidden_state = self.mlp[index].forward(&hidden_state)?;
            }
            start += num_patches;
            processed.push(hidden_state);
        }
        Ok(processed)
    }
}

pub fn merge_2x2_windows(
    hidden_states: &Tensor,
    height: usize,
    width: usize,
    embed_dim: usize,
) -> Result<Tensor> {
    if !height.is_multiple_of(2) || !width.is_multiple_of(2) {
        return Err(MiniCpmV46Error::InvalidInput {
            detail: format!("patch grid ({height}, {width}) must be divisible by 2x2 windows"),
        });
    }
    Ok(hidden_states
        .reshape((height / 2, 2, width / 2, 2, embed_dim))?
        .permute((0, 2, 1, 3, 4))?
        .reshape((height / 2 * width / 2, 4 * embed_dim))?)
}

fn parse_pair(values: &[usize], name: &str) -> Result<(usize, usize)> {
    if values.len() != 2 {
        return Err(MiniCpmV46Error::InvalidInput {
            detail: format!("{name} must contain exactly 2 values, got {}", values.len()),
        });
    }
    Ok((values[0], values[1]))
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct SafetensorSummary {
    pub path: PathBuf,
    pub tensors: usize,
    pub bytes: u64,
    pub likely_prefixes: BTreeMap<String, usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_data_bytes: Option<u64>,
    pub complete_file: bool,
}

pub fn inspect_safetensors(path: impl AsRef<Path>) -> Result<SafetensorSummary> {
    let path = path.as_ref();
    let bytes = fs::read(path)?;
    let tensors = safetensors::SafeTensors::deserialize(&bytes)?;
    let mut likely_prefixes = BTreeMap::new();
    for name in tensors.names() {
        let prefix = name
            .split('.')
            .next()
            .filter(|value| !value.is_empty())
            .unwrap_or("<root>");
        *likely_prefixes.entry(prefix.to_string()).or_insert(0) += 1;
    }
    Ok(SafetensorSummary {
        path: path.to_path_buf(),
        tensors: tensors.len(),
        bytes: bytes.len() as u64,
        likely_prefixes,
        declared_data_bytes: Some(bytes.len() as u64),
        complete_file: true,
    })
}

pub fn inspect_safetensors_header(path: impl AsRef<Path>) -> Result<SafetensorSummary> {
    let path = path.as_ref();
    let bytes = fs::read(path)?;
    if bytes.len() < 8 {
        return Err(MiniCpmV46Error::InvalidInput {
            detail: format!(
                "{} is too small to contain a safetensors header",
                path.display()
            ),
        });
    }
    let header_len = u64::from_le_bytes(bytes[0..8].try_into().expect("slice length")) as usize;
    let header_end =
        8usize
            .checked_add(header_len)
            .ok_or_else(|| MiniCpmV46Error::InvalidInput {
                detail: "safetensors header length overflow".to_string(),
            })?;
    if bytes.len() < header_end {
        return Err(MiniCpmV46Error::InvalidInput {
            detail: format!(
                "{} does not contain the full safetensors header: need {header_end} bytes, got {}",
                path.display(),
                bytes.len()
            ),
        });
    }
    let header: serde_json::Value = serde_json::from_slice(&bytes[8..header_end])?;
    let object = header
        .as_object()
        .ok_or_else(|| MiniCpmV46Error::InvalidInput {
            detail: "safetensors header is not a JSON object".to_string(),
        })?;
    let mut likely_prefixes = BTreeMap::new();
    let mut tensors = 0usize;
    let mut declared_data_bytes = 0u64;
    for (name, value) in object {
        if name == "__metadata__" {
            continue;
        }
        tensors += 1;
        let prefix = name
            .split('.')
            .next()
            .filter(|value| !value.is_empty())
            .unwrap_or("<root>");
        *likely_prefixes.entry(prefix.to_string()).or_insert(0) += 1;
        if let Some(offsets) = value.get("data_offsets").and_then(|item| item.as_array())
            && let Some(end) = offsets.get(1).and_then(|item| item.as_u64())
        {
            declared_data_bytes = declared_data_bytes.max(end);
        }
    }
    let required_bytes = 8u64 + header_len as u64 + declared_data_bytes;
    Ok(SafetensorSummary {
        path: path.to_path_buf(),
        tensors,
        bytes: bytes.len() as u64,
        likely_prefixes,
        declared_data_bytes: Some(declared_data_bytes),
        complete_file: bytes.len() as u64 >= required_bytes,
    })
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct BenchmarkReport {
    pub name: String,
    pub iterations: usize,
    pub elapsed_ms: u128,
    pub average_ms: f64,
}

impl BenchmarkReport {
    fn new(name: impl Into<String>, iterations: usize, elapsed: Duration) -> Self {
        Self {
            name: name.into(),
            iterations,
            elapsed_ms: elapsed.as_millis(),
            average_ms: elapsed.as_secs_f64() * 1000.0 / iterations.max(1) as f64,
        }
    }
}

pub fn benchmark_preprocess(
    image: &DynamicImage,
    preprocessor: &MiniCpmV46PreprocessorConfig,
    device: &Device,
    iterations: usize,
) -> Result<BenchmarkReport> {
    let start = Instant::now();
    for _ in 0..iterations {
        let tensor = preprocess_image(image, preprocessor, device)?;
        std::hint::black_box(tensor);
    }
    Ok(BenchmarkReport::new(
        "minicpm-v-4.6-image-preprocess",
        iterations,
        start.elapsed(),
    ))
}

pub fn benchmark_synthetic_vision_encoder_layer(
    device: &Device,
    iterations: usize,
) -> Result<BenchmarkReport> {
    let config = MiniCpmV46VisionConfig {
        attention_dropout: 0.0,
        hidden_act: "gelu_pytorch_tanh".to_string(),
        hidden_size: 64,
        image_size: 112,
        intermediate_size: 128,
        layer_norm_eps: 1e-6,
        model_type: "minicpmv4_6_vision".to_string(),
        num_attention_heads: 4,
        num_channels: 3,
        num_hidden_layers: 1,
        patch_size: 14,
    };
    let tensors = synthetic_vision_encoder_layer_tensors(&config, device)?;
    let layer = MiniCpmV46VisionEncoderLayer::new(
        &config,
        VarBuilder::from_tensors(tensors, DType::F32, device),
    )?;
    let hidden_states = Tensor::zeros((1, 64, config.hidden_size), DType::F32, device)?;
    let cu_seqlens = [0usize, 64usize];

    let start = Instant::now();
    for _ in 0..iterations {
        let tensor = layer.forward(&hidden_states, &cu_seqlens)?;
        std::hint::black_box(tensor);
    }
    Ok(BenchmarkReport::new(
        "minicpm-v-4.6-synthetic-vision-encoder-layer",
        iterations,
        start.elapsed(),
    ))
}

fn synthetic_vision_encoder_layer_tensors(
    config: &MiniCpmV46VisionConfig,
    device: &Device,
) -> Result<HashMap<String, Tensor>> {
    let hidden = config.hidden_size;
    let intermediate = config.intermediate_size;
    let mut tensors = HashMap::new();
    for name in ["layer_norm1.weight", "layer_norm2.weight"] {
        tensors.insert(name.to_string(), Tensor::ones(hidden, DType::F32, device)?);
    }
    for name in ["layer_norm1.bias", "layer_norm2.bias"] {
        tensors.insert(name.to_string(), Tensor::zeros(hidden, DType::F32, device)?);
    }
    for name in [
        "self_attn.q_proj.weight",
        "self_attn.k_proj.weight",
        "self_attn.v_proj.weight",
        "self_attn.out_proj.weight",
    ] {
        tensors.insert(
            name.to_string(),
            Tensor::zeros((hidden, hidden), DType::F32, device)?,
        );
    }
    for name in [
        "self_attn.q_proj.bias",
        "self_attn.k_proj.bias",
        "self_attn.v_proj.bias",
        "self_attn.out_proj.bias",
    ] {
        tensors.insert(name.to_string(), Tensor::zeros(hidden, DType::F32, device)?);
    }
    tensors.insert(
        "mlp.fc1.weight".to_string(),
        Tensor::zeros((intermediate, hidden), DType::F32, device)?,
    );
    tensors.insert(
        "mlp.fc1.bias".to_string(),
        Tensor::zeros(intermediate, DType::F32, device)?,
    );
    tensors.insert(
        "mlp.fc2.weight".to_string(),
        Tensor::zeros((hidden, intermediate), DType::F32, device)?,
    );
    tensors.insert(
        "mlp.fc2.bias".to_string(),
        Tensor::zeros(hidden, DType::F32, device)?,
    );
    Ok(tensors)
}

pub fn unsupported_generation_error(config: &MiniCpmV46Config) -> MiniCpmV46Error {
    let unsupported = config
        .validate_native_support()
        .map(|report| report.unsupported)
        .unwrap_or_default();
    MiniCpmV46Error::Unsupported {
        detail: if unsupported.is_empty() {
            "native generation path is not wired yet".to_string()
        } else {
            unsupported.join(", ")
        },
    }
}

#[derive(Debug, Clone)]
pub struct Qwen3_5RmsNorm {
    weight: Tensor,
    eps: f64,
}

impl Qwen3_5RmsNorm {
    pub fn new(dim: usize, eps: f64, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            weight: vb.get(dim, "weight")?,
            eps,
        })
    }

    pub fn forward(&self, x: &Tensor) -> Result<Tensor> {
        let dtype = x.dtype();
        let x = x.to_dtype(DType::F32)?;
        let scale = (x.sqr()?.mean_keepdim(D::Minus1)? + self.eps)?.sqrt()?;
        let normalized = x.broadcast_div(&scale)?;
        let weight = (self.weight.to_dtype(DType::F32)? + 1.0)?;
        Ok(normalized.broadcast_mul(&weight)?.to_dtype(dtype)?)
    }
}

#[derive(Debug, Clone)]
pub struct Qwen3_5Mlp {
    gate_proj: Linear,
    up_proj: Linear,
    down_proj: Linear,
}

impl Qwen3_5Mlp {
    pub fn new(hidden_size: usize, intermediate_size: usize, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            gate_proj: candle_nn::linear_no_bias(
                hidden_size,
                intermediate_size,
                vb.pp("gate_proj"),
            )?,
            up_proj: candle_nn::linear_no_bias(hidden_size, intermediate_size, vb.pp("up_proj"))?,
            down_proj: candle_nn::linear_no_bias(
                intermediate_size,
                hidden_size,
                vb.pp("down_proj"),
            )?,
        })
    }

    pub fn forward(&self, x: &Tensor) -> Result<Tensor> {
        let gate = candle_nn::Activation::Silu.forward(&x.apply(&self.gate_proj)?)?;
        let up = x.apply(&self.up_proj)?;
        Ok((gate * up)?.apply(&self.down_proj)?)
    }
}

#[derive(Debug, Clone)]
pub struct Qwen3_5RmsNormGated {
    weight: Tensor,
    eps: f64,
}

impl Qwen3_5RmsNormGated {
    pub fn new(dim: usize, eps: f64, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            weight: vb.get(dim, "weight")?,
            eps,
        })
    }

    pub fn forward(&self, hidden_states: &Tensor, gate: &Tensor) -> Result<Tensor> {
        let dtype = hidden_states.dtype();
        let hidden_states = hidden_states.to_dtype(DType::F32)?;
        let scale = (hidden_states.sqr()?.mean_keepdim(D::Minus1)? + self.eps)?.sqrt()?;
        let hidden_states = hidden_states.broadcast_div(&scale)?;
        let hidden_states = hidden_states.broadcast_mul(&self.weight.to_dtype(DType::F32)?)?;
        let gate = candle_nn::ops::silu(&gate.to_dtype(DType::F32)?)?;
        Ok((hidden_states * gate)?.to_dtype(dtype)?)
    }
}

#[derive(Debug, Clone)]
pub struct Qwen3_5GatedDeltaNet {
    num_v_heads: usize,
    num_k_heads: usize,
    head_k_dim: usize,
    head_v_dim: usize,
    key_dim: usize,
    value_dim: usize,
    conv1d: Conv1d,
    dt_bias: Tensor,
    a_log: Tensor,
    norm: Qwen3_5RmsNormGated,
    out_proj: Linear,
    in_proj_qkv: Linear,
    in_proj_z: Linear,
    in_proj_b: Linear,
    in_proj_a: Linear,
    conv_kernel_size: usize,
}

#[derive(Debug, Clone)]
pub struct Qwen3_5LinearAttentionCache {
    conv_state: Tensor,
    recurrent_state: Tensor,
}

#[derive(Debug, Clone)]
pub struct Qwen3_5FullAttentionCache {
    key_states: Tensor,
    value_states: Tensor,
}

#[derive(Debug, Clone)]
pub enum Qwen3_5LayerCache {
    LinearAttention(Qwen3_5LinearAttentionCache),
    FullAttention(Qwen3_5FullAttentionCache),
}

#[derive(Debug, Clone, Default)]
pub struct Qwen3_5TextCache {
    layers: Vec<Option<Qwen3_5LayerCache>>,
    seq_len: usize,
}

impl Qwen3_5TextCache {
    pub fn new(num_layers: usize) -> Self {
        Self {
            layers: vec![None; num_layers],
            seq_len: 0,
        }
    }

    pub fn seq_len(&self) -> usize {
        self.seq_len
    }
}

impl Qwen3_5GatedDeltaNet {
    pub fn new(config: &MiniCpmV46TextConfig, vb: VarBuilder) -> Result<Self> {
        let key_dim = config.linear_key_head_dim * config.linear_num_key_heads;
        let value_dim = config.linear_value_head_dim * config.linear_num_value_heads;
        let conv_dim = key_dim * 2 + value_dim;
        Ok(Self {
            num_v_heads: config.linear_num_value_heads,
            num_k_heads: config.linear_num_key_heads,
            head_k_dim: config.linear_key_head_dim,
            head_v_dim: config.linear_value_head_dim,
            key_dim,
            value_dim,
            conv1d: conv1d_no_bias(
                conv_dim,
                conv_dim,
                config.linear_conv_kernel_dim,
                Conv1dConfig {
                    padding: config.linear_conv_kernel_dim.saturating_sub(1),
                    groups: conv_dim,
                    ..Default::default()
                },
                vb.pp("conv1d"),
            )?,
            dt_bias: vb.get(config.linear_num_value_heads, "dt_bias")?,
            a_log: vb.get(config.linear_num_value_heads, "A_log")?,
            norm: Qwen3_5RmsNormGated::new(
                config.linear_value_head_dim,
                config.rms_norm_eps,
                vb.pp("norm"),
            )?,
            out_proj: candle_nn::linear_no_bias(value_dim, config.hidden_size, vb.pp("out_proj"))?,
            in_proj_qkv: candle_nn::linear_no_bias(
                config.hidden_size,
                conv_dim,
                vb.pp("in_proj_qkv"),
            )?,
            in_proj_z: candle_nn::linear_no_bias(
                config.hidden_size,
                value_dim,
                vb.pp("in_proj_z"),
            )?,
            in_proj_b: candle_nn::linear_no_bias(
                config.hidden_size,
                config.linear_num_value_heads,
                vb.pp("in_proj_b"),
            )?,
            in_proj_a: candle_nn::linear_no_bias(
                config.hidden_size,
                config.linear_num_value_heads,
                vb.pp("in_proj_a"),
            )?,
            conv_kernel_size: config.linear_conv_kernel_dim,
        })
    }

    pub fn forward(&self, hidden_states: &Tensor) -> Result<Tensor> {
        let (output, _cache) = self.forward_with_cache(hidden_states, None)?;
        Ok(output)
    }

    pub fn forward_with_cache(
        &self,
        hidden_states: &Tensor,
        cache: Option<Qwen3_5LinearAttentionCache>,
    ) -> Result<(Tensor, Qwen3_5LinearAttentionCache)> {
        let (batch, seq_len, _hidden) = hidden_states.dims3()?;
        let mut mixed_qkv = hidden_states.apply(&self.in_proj_qkv)?.transpose(1, 2)?;
        let previous_recurrent_state = cache.as_ref().map(|cache| cache.recurrent_state.clone());
        if let Some(cache) = cache.as_ref() {
            mixed_qkv = Tensor::cat(&[&cache.conv_state, &mixed_qkv], 2)?;
        }
        let new_conv_state = trailing_conv_state(&mixed_qkv, self.conv_kernel_size)?;
        let mixed_len = mixed_qkv.dim(2)?;
        let conv_output_len = if cache.is_some() { mixed_len } else { seq_len };
        let mut mixed_qkv = mixed_qkv
            .apply(&self.conv1d)?
            .narrow(2, 0, conv_output_len)?;
        if cache.is_some() {
            mixed_qkv = mixed_qkv.narrow(2, conv_output_len - seq_len, seq_len)?;
        }
        let mixed_qkv = candle_nn::ops::silu(&mixed_qkv)?.transpose(1, 2)?;

        let z = hidden_states.apply(&self.in_proj_z)?.reshape((
            batch,
            seq_len,
            self.num_v_heads,
            self.head_v_dim,
        ))?;
        let beta = candle_nn::ops::sigmoid(&hidden_states.apply(&self.in_proj_b)?)?;
        let a = hidden_states.apply(&self.in_proj_a)?;
        let g = self.gate_decay(&a)?;

        let query = mixed_qkv.narrow(D::Minus1, 0, self.key_dim)?.reshape((
            batch,
            seq_len,
            self.num_k_heads,
            self.head_k_dim,
        ))?;
        let key = mixed_qkv
            .narrow(D::Minus1, self.key_dim, self.key_dim)?
            .reshape((batch, seq_len, self.num_k_heads, self.head_k_dim))?;
        let value = mixed_qkv
            .narrow(D::Minus1, self.key_dim * 2, self.value_dim)?
            .reshape((batch, seq_len, self.num_v_heads, self.head_v_dim))?;

        let query = self.repeat_key_heads(query)?;
        let key = self.repeat_key_heads(key)?;
        let query = (l2norm(&query, D::Minus1, 1e-6)? * (self.head_k_dim as f64).powf(-0.5))?;
        let key = l2norm(&key, D::Minus1, 1e-6)?;
        let (core_attn_out, recurrent_state) = self.recurrent_gated_delta_rule(
            &query,
            &key,
            &value,
            &g,
            &beta,
            previous_recurrent_state,
        )?;
        let core_attn_out =
            core_attn_out.reshape((batch * seq_len * self.num_v_heads, self.head_v_dim))?;
        let z = z.reshape((batch * seq_len * self.num_v_heads, self.head_v_dim))?;
        let core_attn_out =
            self.norm
                .forward(&core_attn_out, &z)?
                .reshape((batch, seq_len, self.value_dim))?;
        Ok((
            core_attn_out.apply(&self.out_proj)?,
            Qwen3_5LinearAttentionCache {
                conv_state: new_conv_state,
                recurrent_state,
            },
        ))
    }

    fn gate_decay(&self, a: &Tensor) -> Result<Tensor> {
        let heads = self.dt_bias.dim(0)?;
        let dt_bias = self.dt_bias.to_dtype(DType::F32)?.reshape((1, 1, heads))?;
        let a = a.to_dtype(DType::F32)?.broadcast_add(&dt_bias)?;
        let softplus = softplus(&a)?;
        let a_log = self
            .a_log
            .to_dtype(DType::F32)?
            .exp()?
            .neg()?
            .reshape((1, 1, heads))?;
        Ok(softplus.broadcast_mul(&a_log)?)
    }

    fn repeat_key_heads(&self, tensor: Tensor) -> Result<Tensor> {
        if self.num_v_heads == self.num_k_heads {
            return Ok(tensor);
        }
        if !self.num_v_heads.is_multiple_of(self.num_k_heads) {
            return Err(MiniCpmV46Error::InvalidInput {
                detail: format!(
                    "linear_num_value_heads {} must be divisible by linear_num_key_heads {}",
                    self.num_v_heads, self.num_k_heads
                ),
            });
        }
        let (batch, seq_len, key_heads, head_dim) = tensor.dims4()?;
        let repeat = self.num_v_heads / self.num_k_heads;
        Ok(tensor
            .unsqueeze(3)?
            .expand((batch, seq_len, key_heads, repeat, head_dim))?
            .reshape((batch, seq_len, self.num_v_heads, head_dim))?)
    }

    fn recurrent_gated_delta_rule(
        &self,
        query: &Tensor,
        key: &Tensor,
        value: &Tensor,
        g: &Tensor,
        beta: &Tensor,
        initial_state: Option<Tensor>,
    ) -> Result<(Tensor, Tensor)> {
        let (batch, seq_len, heads, k_dim) = query.dims4()?;
        let v_dim = value.dim(D::Minus1)?;
        let mut state = match initial_state {
            Some(state) => state.to_dtype(DType::F32)?,
            None => Tensor::zeros((batch, heads, k_dim, v_dim), DType::F32, query.device())?,
        };
        let mut outputs = Vec::with_capacity(seq_len);
        for index in 0..seq_len {
            let q_t = query
                .narrow(1, index, 1)?
                .squeeze(1)?
                .to_dtype(DType::F32)?;
            let k_t = key.narrow(1, index, 1)?.squeeze(1)?.to_dtype(DType::F32)?;
            let v_t = value
                .narrow(1, index, 1)?
                .squeeze(1)?
                .to_dtype(DType::F32)?;
            let g_t = g
                .narrow(1, index, 1)?
                .squeeze(1)?
                .exp()?
                .unsqueeze(D::Minus1)?
                .unsqueeze(D::Minus1)?;
            let beta_t = beta.narrow(1, index, 1)?.squeeze(1)?.unsqueeze(D::Minus1)?;

            state = state.broadcast_mul(&g_t)?;
            let kv_mem = state.broadcast_mul(&k_t.unsqueeze(D::Minus1)?)?.sum(2)?;
            let delta = (v_t - kv_mem)?.broadcast_mul(&beta_t)?;
            state = (state
                + k_t
                    .unsqueeze(D::Minus1)?
                    .broadcast_mul(&delta.unsqueeze(2)?)?)?;
            let output = state
                .broadcast_mul(&q_t.unsqueeze(D::Minus1)?)?
                .sum(2)?
                .unsqueeze(1)?;
            outputs.push(output);
        }
        let refs: Vec<&Tensor> = outputs.iter().collect();
        Ok((Tensor::cat(&refs, 1)?.to_dtype(query.dtype())?, state))
    }
}

fn trailing_conv_state(input: &Tensor, state_len: usize) -> Result<Tensor> {
    let current_len = input.dim(2)?;
    if current_len >= state_len {
        Ok(input.narrow(2, current_len - state_len, state_len)?)
    } else {
        input
            .pad_with_zeros(2, state_len - current_len, 0)
            .map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
pub struct Qwen3_5Attention {
    q_proj: Linear,
    k_proj: Linear,
    v_proj: Linear,
    o_proj: Linear,
    q_norm: Qwen3_5RmsNorm,
    k_norm: Qwen3_5RmsNorm,
    num_attention_heads: usize,
    num_key_value_heads: usize,
    head_dim: usize,
    scaling: f64,
}

impl Qwen3_5Attention {
    pub fn new(config: &MiniCpmV46TextConfig, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            q_proj: candle_nn::linear_no_bias(
                config.hidden_size,
                config.num_attention_heads * config.head_dim * 2,
                vb.pp("q_proj"),
            )?,
            k_proj: candle_nn::linear_no_bias(
                config.hidden_size,
                config.num_key_value_heads * config.head_dim,
                vb.pp("k_proj"),
            )?,
            v_proj: candle_nn::linear_no_bias(
                config.hidden_size,
                config.num_key_value_heads * config.head_dim,
                vb.pp("v_proj"),
            )?,
            o_proj: candle_nn::linear_no_bias(
                config.num_attention_heads * config.head_dim,
                config.hidden_size,
                vb.pp("o_proj"),
            )?,
            q_norm: Qwen3_5RmsNorm::new(config.head_dim, config.rms_norm_eps, vb.pp("q_norm"))?,
            k_norm: Qwen3_5RmsNorm::new(config.head_dim, config.rms_norm_eps, vb.pp("k_norm"))?,
            num_attention_heads: config.num_attention_heads,
            num_key_value_heads: config.num_key_value_heads,
            head_dim: config.head_dim,
            scaling: (config.head_dim as f64).powf(-0.5),
        })
    }

    pub fn forward(&self, hidden_states: &Tensor, cos: &Tensor, sin: &Tensor) -> Result<Tensor> {
        let (output, _cache) = self.forward_with_cache(hidden_states, cos, sin, None, 0)?;
        Ok(output)
    }

    pub fn forward_with_cache(
        &self,
        hidden_states: &Tensor,
        cos: &Tensor,
        sin: &Tensor,
        cache: Option<Qwen3_5FullAttentionCache>,
        past_len: usize,
    ) -> Result<(Tensor, Qwen3_5FullAttentionCache)> {
        let (batch, seq_len, _hidden) = hidden_states.dims3()?;
        let q_proj = hidden_states.apply(&self.q_proj)?.reshape((
            batch,
            seq_len,
            self.num_attention_heads,
            self.head_dim * 2,
        ))?;
        let query_states = q_proj.narrow(D::Minus1, 0, self.head_dim)?;
        let gate = q_proj
            .narrow(D::Minus1, self.head_dim, self.head_dim)?
            .reshape((batch, seq_len, self.num_attention_heads * self.head_dim))?;
        let query_states = self.q_norm.forward(&query_states)?.transpose(1, 2)?;
        let key_states = self
            .k_norm
            .forward(&hidden_states.apply(&self.k_proj)?.reshape((
                batch,
                seq_len,
                self.num_key_value_heads,
                self.head_dim,
            ))?)?
            .transpose(1, 2)?;
        let value_states = hidden_states
            .apply(&self.v_proj)?
            .reshape((batch, seq_len, self.num_key_value_heads, self.head_dim))?
            .transpose(1, 2)?;
        let (query_states, key_states) =
            apply_rotary_pos_emb(&query_states, &key_states, cos, sin)?;
        let (key_states, value_states) = match cache {
            Some(cache) => (
                Tensor::cat(&[&cache.key_states, &key_states], 2)?,
                Tensor::cat(&[&cache.value_states, &value_states], 2)?,
            ),
            None => (key_states, value_states),
        };
        let next_cache = Qwen3_5FullAttentionCache {
            key_states: key_states.clone(),
            value_states: value_states.clone(),
        };
        let key_states = repeat_kv_heads(key_states, self.num_attention_heads)?;
        let value_states = repeat_kv_heads(value_states, self.num_attention_heads)?;

        let scores = (query_states.matmul(&key_states.transpose(2, 3)?)? * self.scaling)?;
        let scores = add_causal_mask(&scores, past_len)?;
        let weights = candle_nn::ops::softmax(&scores, D::Minus1)?;
        let attn_output = weights.matmul(&value_states)?.transpose(1, 2)?.reshape((
            batch,
            seq_len,
            self.num_attention_heads * self.head_dim,
        ))?;
        let gate = candle_nn::ops::sigmoid(&gate)?;
        Ok(((attn_output * gate)?.apply(&self.o_proj)?, next_cache))
    }
}

pub fn qwen3_5_text_rope(
    config: &MiniCpmV46TextConfig,
    batch: usize,
    seq_len: usize,
    device: &Device,
) -> Result<(Tensor, Tensor)> {
    qwen3_5_text_rope_from(config, batch, seq_len, 0, device)
}

pub fn qwen3_5_text_rope_from(
    config: &MiniCpmV46TextConfig,
    batch: usize,
    seq_len: usize,
    position_offset: usize,
    device: &Device,
) -> Result<(Tensor, Tensor)> {
    let rotary_dim = ((config.head_dim as f64) * config.partial_rotary_factor) as usize;
    let half = rotary_dim / 2;
    let mut inv_freq = Vec::with_capacity(half);
    for index in 0..half {
        let exponent = (2 * index) as f64 / rotary_dim as f64;
        inv_freq.push((1.0 / config.rope_parameters.rope_theta.powf(exponent)) as f32);
    }
    let mut values = Vec::with_capacity(batch * seq_len * rotary_dim);
    for _ in 0..batch {
        for index in 0..seq_len {
            let position = position_offset + index;
            for &freq in &inv_freq {
                values.push(position as f32 * freq);
            }
            for &freq in &inv_freq {
                values.push(position as f32 * freq);
            }
        }
    }
    let freqs = Tensor::from_vec(values, (batch, seq_len, rotary_dim), device)?;
    Ok((freqs.cos()?, freqs.sin()?))
}

fn apply_rotary_pos_emb(
    query: &Tensor,
    key: &Tensor,
    cos: &Tensor,
    sin: &Tensor,
) -> Result<(Tensor, Tensor)> {
    let rotary_dim = cos.dim(D::Minus1)?;
    let q_rot = query.narrow(D::Minus1, 0, rotary_dim)?;
    let q_pass = query.narrow(D::Minus1, rotary_dim, query.dim(D::Minus1)? - rotary_dim)?;
    let k_rot = key.narrow(D::Minus1, 0, rotary_dim)?;
    let k_pass = key.narrow(D::Minus1, rotary_dim, key.dim(D::Minus1)? - rotary_dim)?;
    let cos = cos.unsqueeze(1)?;
    let sin = sin.unsqueeze(1)?;
    let q_embed = (q_rot.broadcast_mul(&cos)? + rotate_half(&q_rot)?.broadcast_mul(&sin)?)?;
    let k_embed = (k_rot.broadcast_mul(&cos)? + rotate_half(&k_rot)?.broadcast_mul(&sin)?)?;
    Ok((
        Tensor::cat(&[&q_embed, &q_pass], D::Minus1)?,
        Tensor::cat(&[&k_embed, &k_pass], D::Minus1)?,
    ))
}

fn rotate_half(x: &Tensor) -> Result<Tensor> {
    let dim = x.dim(D::Minus1)?;
    let x1 = x.narrow(D::Minus1, 0, dim / 2)?;
    let x2 = x.narrow(D::Minus1, dim / 2, dim - dim / 2)?;
    Tensor::cat(&[&x2.neg()?, &x1], D::Minus1).map_err(Into::into)
}

fn repeat_kv_heads(tensor: Tensor, target_heads: usize) -> Result<Tensor> {
    let (batch, key_heads, seq_len, head_dim) = tensor.dims4()?;
    if key_heads == target_heads {
        return Ok(tensor);
    }
    if !target_heads.is_multiple_of(key_heads) {
        return Err(MiniCpmV46Error::InvalidInput {
            detail: format!(
                "target heads {target_heads} must be divisible by key heads {key_heads}"
            ),
        });
    }
    let repeat = target_heads / key_heads;
    Ok(tensor
        .unsqueeze(2)?
        .expand((batch, key_heads, repeat, seq_len, head_dim))?
        .reshape((batch, target_heads, seq_len, head_dim))?)
}

fn add_causal_mask(scores: &Tensor, past_len: usize) -> Result<Tensor> {
    let q_len = scores.dim(2)?;
    let k_len = scores.dim(3)?;
    let mut mask = vec![0f32; q_len * k_len];
    for row in 0..q_len {
        let max_visible = past_len + row;
        for col in (max_visible + 1)..k_len {
            mask[row * k_len + col] = f32::NEG_INFINITY;
        }
    }
    let mask = Tensor::from_vec(mask, (1, 1, q_len, k_len), scores.device())?;
    Ok(scores.broadcast_add(&mask)?)
}

#[derive(Debug, Clone)]
enum Qwen3_5TokenMixer {
    LinearAttention(Qwen3_5GatedDeltaNet),
    FullAttention(Qwen3_5Attention),
}

#[derive(Debug, Clone)]
pub struct Qwen3_5DecoderLayer {
    mixer: Qwen3_5TokenMixer,
    mlp: Qwen3_5Mlp,
    input_layernorm: Qwen3_5RmsNorm,
    post_attention_layernorm: Qwen3_5RmsNorm,
}

impl Qwen3_5DecoderLayer {
    pub fn new(config: &MiniCpmV46TextConfig, layer_idx: usize, vb: VarBuilder) -> Result<Self> {
        let layer_type =
            config
                .layer_types
                .get(layer_idx)
                .ok_or_else(|| MiniCpmV46Error::InvalidInput {
                    detail: format!(
                        "missing layer_types entry for layer {layer_idx}; only {} entries provided",
                        config.layer_types.len()
                    ),
                })?;
        let mixer = match layer_type.as_str() {
            "linear_attention" => Qwen3_5TokenMixer::LinearAttention(Qwen3_5GatedDeltaNet::new(
                config,
                vb.pp("linear_attn"),
            )?),
            "full_attention" => {
                Qwen3_5TokenMixer::FullAttention(Qwen3_5Attention::new(config, vb.pp("self_attn"))?)
            }
            other => {
                return Err(MiniCpmV46Error::Unsupported {
                    detail: format!("unsupported Qwen3.5 layer type `{other}`"),
                });
            }
        };
        Ok(Self {
            mixer,
            mlp: Qwen3_5Mlp::new(config.hidden_size, config.intermediate_size, vb.pp("mlp"))?,
            input_layernorm: Qwen3_5RmsNorm::new(
                config.hidden_size,
                config.rms_norm_eps,
                vb.pp("input_layernorm"),
            )?,
            post_attention_layernorm: Qwen3_5RmsNorm::new(
                config.hidden_size,
                config.rms_norm_eps,
                vb.pp("post_attention_layernorm"),
            )?,
        })
    }

    pub fn forward(&self, hidden_states: &Tensor, cos: &Tensor, sin: &Tensor) -> Result<Tensor> {
        let (output, _cache) = self.forward_with_cache(hidden_states, cos, sin, None, 0)?;
        Ok(output)
    }

    pub fn forward_with_cache(
        &self,
        hidden_states: &Tensor,
        cos: &Tensor,
        sin: &Tensor,
        cache: Option<Qwen3_5LayerCache>,
        past_len: usize,
    ) -> Result<(Tensor, Qwen3_5LayerCache)> {
        let residual = hidden_states.clone();
        let hidden_states = self.input_layernorm.forward(hidden_states)?;
        let (hidden_states, next_cache) = match &self.mixer {
            Qwen3_5TokenMixer::LinearAttention(linear_attn) => {
                let cache = match cache {
                    Some(Qwen3_5LayerCache::LinearAttention(cache)) => Some(cache),
                    Some(Qwen3_5LayerCache::FullAttention(_)) => {
                        return Err(MiniCpmV46Error::InvalidInput {
                            detail: "full-attention cache provided for linear-attention layer"
                                .to_string(),
                        });
                    }
                    None => None,
                };
                let (output, cache) = linear_attn.forward_with_cache(&hidden_states, cache)?;
                (output, Qwen3_5LayerCache::LinearAttention(cache))
            }
            Qwen3_5TokenMixer::FullAttention(self_attn) => {
                let cache = match cache {
                    Some(Qwen3_5LayerCache::FullAttention(cache)) => Some(cache),
                    Some(Qwen3_5LayerCache::LinearAttention(_)) => {
                        return Err(MiniCpmV46Error::InvalidInput {
                            detail: "linear-attention cache provided for full-attention layer"
                                .to_string(),
                        });
                    }
                    None => None,
                };
                let (output, cache) =
                    self_attn.forward_with_cache(&hidden_states, cos, sin, cache, past_len)?;
                (output, Qwen3_5LayerCache::FullAttention(cache))
            }
        };
        let hidden_states = (residual + hidden_states)?;

        let residual = hidden_states.clone();
        let hidden_states = self.post_attention_layernorm.forward(&hidden_states)?;
        let hidden_states = self.mlp.forward(&hidden_states)?;
        Ok(((residual + hidden_states)?, next_cache))
    }
}

#[derive(Debug, Clone)]
pub struct Qwen3_5TextModel {
    embed_tokens: Embedding,
    layers: Vec<Qwen3_5DecoderLayer>,
    norm: Qwen3_5RmsNorm,
    config: MiniCpmV46TextConfig,
}

impl Qwen3_5TextModel {
    pub fn new(config: &MiniCpmV46TextConfig, vb: VarBuilder) -> Result<Self> {
        let mut layers = Vec::with_capacity(config.num_hidden_layers);
        for index in 0..config.num_hidden_layers {
            layers.push(Qwen3_5DecoderLayer::new(
                config,
                index,
                vb.pp("layers").pp(index),
            )?);
        }
        Ok(Self {
            embed_tokens: embedding(config.vocab_size, config.hidden_size, vb.pp("embed_tokens"))?,
            layers,
            norm: Qwen3_5RmsNorm::new(config.hidden_size, config.rms_norm_eps, vb.pp("norm"))?,
            config: config.clone(),
        })
    }

    pub fn forward_input_ids(&self, input_ids: &Tensor) -> Result<Tensor> {
        let inputs_embeds = self.embed_input_ids(input_ids)?;
        self.forward_inputs_embeds(&inputs_embeds)
    }

    pub fn forward_input_ids_with_cache(
        &self,
        input_ids: &Tensor,
        cache: Option<Qwen3_5TextCache>,
    ) -> Result<(Tensor, Qwen3_5TextCache)> {
        let inputs_embeds = self.embed_input_ids(input_ids)?;
        self.forward_inputs_embeds_with_cache(&inputs_embeds, cache)
    }

    pub fn embed_input_ids(&self, input_ids: &Tensor) -> Result<Tensor> {
        Ok(self.embed_tokens.forward(input_ids)?)
    }

    pub fn forward_inputs_embeds(&self, inputs_embeds: &Tensor) -> Result<Tensor> {
        let (batch, seq_len, _hidden) = inputs_embeds.dims3()?;
        let (cos, sin) = qwen3_5_text_rope(&self.config, batch, seq_len, inputs_embeds.device())?;
        let mut hidden_states = inputs_embeds.clone();
        for layer in &self.layers {
            hidden_states = layer.forward(&hidden_states, &cos, &sin)?;
        }
        self.norm.forward(&hidden_states)
    }

    pub fn forward_inputs_embeds_with_cache(
        &self,
        inputs_embeds: &Tensor,
        cache: Option<Qwen3_5TextCache>,
    ) -> Result<(Tensor, Qwen3_5TextCache)> {
        let (batch, seq_len, _hidden) = inputs_embeds.dims3()?;
        let mut cache = cache.unwrap_or_else(|| Qwen3_5TextCache::new(self.layers.len()));
        if cache.layers.len() != self.layers.len() {
            return Err(MiniCpmV46Error::InvalidInput {
                detail: format!(
                    "cache has {} layers but model has {} layers",
                    cache.layers.len(),
                    self.layers.len()
                ),
            });
        }
        let past_len = cache.seq_len;
        let (cos, sin) = qwen3_5_text_rope_from(
            &self.config,
            batch,
            seq_len,
            past_len,
            inputs_embeds.device(),
        )?;
        let mut hidden_states = inputs_embeds.clone();
        for (index, layer) in self.layers.iter().enumerate() {
            let layer_cache = cache.layers[index].take();
            let (next_hidden_states, next_cache) =
                layer.forward_with_cache(&hidden_states, &cos, &sin, layer_cache, past_len)?;
            hidden_states = next_hidden_states;
            cache.layers[index] = Some(next_cache);
        }
        cache.seq_len += seq_len;
        Ok((self.norm.forward(&hidden_states)?, cache))
    }
}

#[derive(Debug, Clone)]
pub struct Qwen3_5ForCausalLm {
    model: Qwen3_5TextModel,
    lm_head: Linear,
}

impl Qwen3_5ForCausalLm {
    pub fn new(config: &MiniCpmV46TextConfig, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            model: Qwen3_5TextModel::new(config, vb.pp("model"))?,
            lm_head: candle_nn::linear_no_bias(
                config.hidden_size,
                config.vocab_size,
                vb.pp("lm_head"),
            )?,
        })
    }

    pub fn forward_input_ids(&self, input_ids: &Tensor) -> Result<Tensor> {
        Ok(self
            .model
            .forward_input_ids(input_ids)?
            .apply(&self.lm_head)?)
    }

    pub fn forward_input_ids_with_cache(
        &self,
        input_ids: &Tensor,
        cache: Option<Qwen3_5TextCache>,
    ) -> Result<(Tensor, Qwen3_5TextCache)> {
        let (hidden_states, cache) = self.model.forward_input_ids_with_cache(input_ids, cache)?;
        Ok((hidden_states.apply(&self.lm_head)?, cache))
    }

    pub fn forward_inputs_embeds(&self, inputs_embeds: &Tensor) -> Result<Tensor> {
        Ok(self
            .model
            .forward_inputs_embeds(inputs_embeds)?
            .apply(&self.lm_head)?)
    }

    pub fn forward_inputs_embeds_with_cache(
        &self,
        inputs_embeds: &Tensor,
        cache: Option<Qwen3_5TextCache>,
    ) -> Result<(Tensor, Qwen3_5TextCache)> {
        let (hidden_states, cache) = self
            .model
            .forward_inputs_embeds_with_cache(inputs_embeds, cache)?;
        Ok((hidden_states.apply(&self.lm_head)?, cache))
    }
}

#[derive(Debug, Clone)]
pub struct MiniCpmV46ImageFeatures {
    pub last_hidden_state: Tensor,
    pub pooled_features: Vec<Tensor>,
    pub target_sizes: Vec<(usize, usize)>,
}

#[derive(Debug)]
pub struct MiniCpmV46Model {
    vision_tower: MiniCpmV46NativeVisionTower,
    language_model: Qwen3_5TextModel,
    merger: MiniCpmV46Merger,
    config: MiniCpmV46Config,
}

impl MiniCpmV46Model {
    pub fn new(config: &MiniCpmV46Config, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            vision_tower: MiniCpmV46NativeVisionTower::new(config, vb.pp("vision_tower"))?,
            language_model: Qwen3_5TextModel::new(&config.text_config, vb.pp("language_model"))?,
            merger: MiniCpmV46Merger::new(config, vb.pp("merger"))?,
            config: config.clone(),
        })
    }

    pub fn get_image_features(
        &self,
        pixel_values: &Tensor,
        target_sizes: &[(usize, usize)],
        downsample_mode: Option<&str>,
    ) -> Result<MiniCpmV46ImageFeatures> {
        let downsample_mode = downsample_mode.unwrap_or(&self.config.downsample_mode);
        let use_vit_merger = downsample_mode != "4x";
        let last_hidden_state =
            self.vision_tower
                .forward(pixel_values, target_sizes, use_vit_merger)?;
        let merger_target_sizes = if use_vit_merger {
            halve_target_sizes(target_sizes)?
        } else {
            target_sizes.to_vec()
        };
        let pooled_features = self
            .merger
            .forward(&last_hidden_state, &merger_target_sizes)?;
        Ok(MiniCpmV46ImageFeatures {
            last_hidden_state,
            pooled_features,
            target_sizes: merger_target_sizes,
        })
    }

    pub fn get_video_features(
        &self,
        pixel_values_videos: &Tensor,
        target_sizes_videos: &[(usize, usize)],
        downsample_mode: Option<&str>,
    ) -> Result<MiniCpmV46ImageFeatures> {
        let (num_frames, channels, patch_size, seq_len) = pixel_values_videos.dims4()?;
        let pixel_values = pixel_values_videos.permute((1, 2, 0, 3))?.reshape((
            1,
            channels,
            patch_size,
            num_frames * seq_len,
        ))?;
        let mut target_sizes = Vec::with_capacity(num_frames * target_sizes_videos.len());
        for _ in 0..num_frames {
            target_sizes.extend_from_slice(target_sizes_videos);
        }
        self.get_image_features(&pixel_values, &target_sizes, downsample_mode)
    }

    pub fn embed_input_ids(&self, input_ids: &Tensor) -> Result<Tensor> {
        self.language_model.embed_input_ids(input_ids)
    }

    pub fn forward_input_ids(&self, input_ids: &Tensor) -> Result<Tensor> {
        self.language_model.forward_input_ids(input_ids)
    }

    pub fn forward_input_ids_with_cache(
        &self,
        input_ids: &Tensor,
        cache: Option<Qwen3_5TextCache>,
    ) -> Result<(Tensor, Qwen3_5TextCache)> {
        self.language_model
            .forward_input_ids_with_cache(input_ids, cache)
    }

    pub fn forward_inputs_embeds(&self, inputs_embeds: &Tensor) -> Result<Tensor> {
        self.language_model.forward_inputs_embeds(inputs_embeds)
    }

    pub fn forward_inputs_embeds_with_cache(
        &self,
        inputs_embeds: &Tensor,
        cache: Option<Qwen3_5TextCache>,
    ) -> Result<(Tensor, Qwen3_5TextCache)> {
        self.language_model
            .forward_inputs_embeds_with_cache(inputs_embeds, cache)
    }
}

#[derive(Debug)]
pub struct MiniCpmV46ForConditionalGeneration {
    model: MiniCpmV46Model,
    lm_head: Linear,
}

impl MiniCpmV46ForConditionalGeneration {
    pub fn new(config: &MiniCpmV46Config, vb: VarBuilder) -> Result<Self> {
        let lm_head = match candle_nn::linear_no_bias(
            config.text_config.hidden_size,
            config.text_config.vocab_size,
            vb.pp("lm_head"),
        ) {
            Ok(lm_head) => lm_head,
            Err(source) => {
                if !config.text_config.tie_word_embeddings {
                    return Err(source.into());
                }
                let weight = vb.pp("model").pp("language_model").pp("embed_tokens").get(
                    (
                        config.text_config.vocab_size,
                        config.text_config.hidden_size,
                    ),
                    "weight",
                )?;
                Linear::new(weight, None)
            }
        };
        Ok(Self {
            model: MiniCpmV46Model::new(config, vb.pp("model"))?,
            lm_head,
        })
    }

    /// Constructs the model from memory-mapped safetensor files.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the mapped files are not modified or
    /// replaced for as long as tensors backed by those mappings are alive.
    pub unsafe fn from_mmaped_safetensors(
        config: &MiniCpmV46Config,
        safetensors: &[impl AsRef<Path>],
        dtype: DType,
        device: &Device,
    ) -> Result<Self> {
        let vb = unsafe { VarBuilder::from_mmaped_safetensors(safetensors, dtype, device)? };
        Self::new(config, vb)
    }

    pub fn get_image_features(
        &self,
        pixel_values: &Tensor,
        target_sizes: &[(usize, usize)],
        downsample_mode: Option<&str>,
    ) -> Result<MiniCpmV46ImageFeatures> {
        self.model
            .get_image_features(pixel_values, target_sizes, downsample_mode)
    }

    pub fn get_video_features(
        &self,
        pixel_values_videos: &Tensor,
        target_sizes_videos: &[(usize, usize)],
        downsample_mode: Option<&str>,
    ) -> Result<MiniCpmV46ImageFeatures> {
        self.model
            .get_video_features(pixel_values_videos, target_sizes_videos, downsample_mode)
    }

    pub fn forward_text_only_input_ids(&self, input_ids: &Tensor) -> Result<Tensor> {
        Ok(self
            .model
            .forward_input_ids(input_ids)?
            .apply(&self.lm_head)?)
    }

    pub fn forward_text_only_input_ids_with_cache(
        &self,
        input_ids: &Tensor,
        cache: Option<Qwen3_5TextCache>,
    ) -> Result<(Tensor, Qwen3_5TextCache)> {
        let (hidden_states, cache) = self.model.forward_input_ids_with_cache(input_ids, cache)?;
        Ok((hidden_states.apply(&self.lm_head)?, cache))
    }

    pub fn forward_inputs_embeds(&self, inputs_embeds: &Tensor) -> Result<Tensor> {
        Ok(self
            .model
            .forward_inputs_embeds(inputs_embeds)?
            .apply(&self.lm_head)?)
    }

    pub fn forward_with_image_input_ids(
        &self,
        input_ids: &Tensor,
        pixel_values: &Tensor,
        target_sizes: &[(usize, usize)],
        downsample_mode: Option<&str>,
    ) -> Result<Tensor> {
        let inputs_embeds = self.model.embed_input_ids(input_ids)?;
        let image_features =
            self.get_image_features(pixel_values, target_sizes, downsample_mode)?;
        let feature_refs: Vec<&Tensor> = image_features.pooled_features.iter().collect();
        let features = Tensor::cat(&feature_refs, 0)?;
        let inputs_embeds = replace_token_embeddings(
            input_ids,
            &inputs_embeds,
            &features,
            self.model.config.image_token_id,
        )?;
        self.forward_inputs_embeds(&inputs_embeds)
    }
}

fn replace_token_embeddings(
    input_ids: &Tensor,
    inputs_embeds: &Tensor,
    features: &Tensor,
    token_id: u32,
) -> Result<Tensor> {
    let ids = input_ids.to_vec2::<u32>()?;
    let mut embeds = inputs_embeds.to_vec3::<f32>()?;
    let feature_values = features.to_vec2::<f32>()?;
    let hidden = inputs_embeds.dim(D::Minus1)?;
    let mut feature_index = 0usize;
    for (batch, row) in ids.iter().enumerate() {
        for (position, &id) in row.iter().enumerate() {
            if id == token_id {
                let feature = feature_values.get(feature_index).ok_or_else(|| {
                    MiniCpmV46Error::InvalidInput {
                        detail: format!(
                            "more multimodal placeholder tokens than features: token index {feature_index}, features {}",
                            feature_values.len()
                        ),
                    }
                })?;
                if feature.len() != hidden {
                    return Err(MiniCpmV46Error::InvalidInput {
                        detail: format!(
                            "feature hidden size {} does not match input embedding hidden size {hidden}",
                            feature.len()
                        ),
                    });
                }
                embeds[batch][position].clone_from(feature);
                feature_index += 1;
            }
        }
    }
    if feature_index != feature_values.len() {
        return Err(MiniCpmV46Error::InvalidInput {
            detail: format!(
                "multimodal placeholder tokens and features do not match: tokens {feature_index}, features {}",
                feature_values.len()
            ),
        });
    }
    let batch = embeds.len();
    let seq_len = embeds.first().map(Vec::len).unwrap_or(0);
    let flat: Vec<f32> = embeds.into_iter().flatten().flatten().collect();
    Ok(Tensor::from_vec(
        flat,
        (batch, seq_len, hidden),
        inputs_embeds.device(),
    )?)
}

fn softplus(x: &Tensor) -> Result<Tensor> {
    Ok((x.exp()? + 1.0)?.log()?)
}

fn l2norm<Dm: candle_core::shape::Dim>(x: &Tensor, dim: Dm, eps: f64) -> Result<Tensor> {
    let norm = (x.sqr()?.sum_keepdim(dim)? + eps)?.sqrt()?;
    Ok(x.broadcast_div(&norm)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use candle_core::DType;
    use std::collections::HashMap;

    const CONFIG_JSON: &str = r#"{
      "architectures": ["MiniCPMV4_6ForConditionalGeneration"],
      "drop_vision_last_layer": false,
      "image_size": 1120,
      "model_type": "minicpmv4_6",
      "vision_config": {
        "attention_dropout": 0.0,
        "hidden_act": "gelu_pytorch_tanh",
        "hidden_size": 1152,
        "image_size": 980,
        "intermediate_size": 4304,
        "layer_norm_eps": 1e-6,
        "model_type": "minicpmv4_6_vision",
        "num_attention_heads": 16,
        "num_channels": 3,
        "num_hidden_layers": 27,
        "patch_size": 14
      },
      "text_config": {
        "attention_bias": false,
        "attention_dropout": 0.0,
        "attn_output_gate": true,
        "full_attention_interval": 4,
        "head_dim": 256,
        "hidden_act": "silu",
        "hidden_size": 1024,
        "initializer_range": 0.02,
        "intermediate_size": 3584,
        "layer_types": ["linear_attention", "full_attention"],
        "linear_conv_kernel_dim": 4,
        "linear_key_head_dim": 128,
        "linear_num_key_heads": 16,
        "linear_num_value_heads": 16,
        "linear_value_head_dim": 128,
        "mamba_ssm_dtype": "float32",
        "max_position_embeddings": 262144,
        "mlp_only_layers": [],
        "mtp_num_hidden_layers": 1,
        "mtp_use_dedicated_embeddings": false,
        "num_attention_heads": 8,
        "num_hidden_layers": 24,
        "num_key_value_heads": 2,
        "partial_rotary_factor": 0.25,
        "rms_norm_eps": 1e-6,
        "rope_parameters": {
          "partial_rotary_factor": 0.25,
          "rope_theta": 10000000.0,
          "rope_type": "default"
        },
        "vocab_size": 248094,
        "model_type": "qwen3_5_text",
        "tie_word_embeddings": true
      },
      "insert_layer_id": 6,
      "image_token_id": 248056,
      "video_token_id": 248057
    }"#;

    #[test]
    fn parses_upstream_minicpm_v46_config_shape() {
        let config: MiniCpmV46Config = serde_json::from_str(CONFIG_JSON).unwrap();
        assert_eq!(config.vision_config.num_hidden_layers, 27);
        assert_eq!(config.text_config.layer_types.len(), 2);
        let report = config.validate_native_support().unwrap();
        assert!(report.vision_supported);
        assert!(report.text_supported);
        assert!(report.generation_supported);
        assert!(report.unsupported.is_empty());
        assert!(!report.unsupported.iter().any(|item| item.contains("NaViT")));
    }

    #[test]
    fn preprocesses_rgb_image_to_bchw_float_tensor() {
        let image = DynamicImage::new_rgb8(32, 48);
        let device = Device::Cpu;
        let mut preprocessor = MiniCpmV46PreprocessorConfig::default();
        preprocessor.size.insert("shortest_edge".to_string(), 16);
        let tensor = preprocess_image(&image, &preprocessor, &device).unwrap();
        assert_eq!(tensor.dims(), &[1, 3, 16, 16]);
        assert_eq!(tensor.dtype(), DType::F32);
    }

    #[test]
    fn packs_2x2_windows_like_pytorch_merger_layout() {
        let device = Device::Cpu;
        let tensor =
            Tensor::from_vec((0..16).map(|value| value as f32).collect(), (4, 4), &device).unwrap();
        let merged = merge_2x2_windows(&tensor, 2, 2, 4).unwrap();
        assert_eq!(merged.dims(), &[1, 16]);
        assert_eq!(
            merged.to_vec2::<f32>().unwrap(),
            vec![vec![
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0,
                15.0,
            ]]
        );

        let tensor =
            Tensor::from_vec((0..16).map(|value| value as f32).collect(), (4, 1), &device).unwrap();
        let merged = merge_2x2_windows(&tensor, 2, 2, 1).unwrap();
        assert_eq!(
            merged.to_vec2::<f32>().unwrap(),
            vec![vec![0.0, 1.0, 2.0, 3.0]]
        );
    }

    #[test]
    fn merger_matches_pytorch_fixture() {
        let device = Device::Cpu;
        let mut config: MiniCpmV46Config = serde_json::from_str(CONFIG_JSON).unwrap();
        config.vision_config.hidden_size = 1;
        config.text_config.hidden_size = 2;
        config.merge_kernel_size = vec![2, 2];
        config.merger_times = 1;

        let tensors = HashMap::from([
            (
                "mlp.0.pre_norm.weight".to_string(),
                Tensor::from_vec(vec![1.0f32, 1.1, 1.2, 1.3], 4, &device).unwrap(),
            ),
            (
                "mlp.0.pre_norm.bias".to_string(),
                Tensor::from_vec(vec![0.1f32, -0.2, 0.3, -0.4], 4, &device).unwrap(),
            ),
            (
                "mlp.0.linear_1.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.1f32, 0.2, 0.3, 0.4, -0.2, 0.1, 0.0, 0.3, 0.5, -0.4, 0.2, 0.1, 0.0, 0.25,
                        -0.35, 0.45,
                    ],
                    (4, 4),
                    &device,
                )
                .unwrap(),
            ),
            (
                "mlp.0.linear_1.bias".to_string(),
                Tensor::from_vec(vec![0.01f32, -0.02, 0.03, -0.04], 4, &device).unwrap(),
            ),
            (
                "mlp.0.linear_2.weight".to_string(),
                Tensor::from_vec(
                    vec![0.2f32, -0.1, 0.3, 0.4, -0.5, 0.6, -0.2, 0.1],
                    (2, 4),
                    &device,
                )
                .unwrap(),
            ),
            (
                "mlp.0.linear_2.bias".to_string(),
                Tensor::from_vec(vec![0.05f32, -0.07], 2, &device).unwrap(),
            ),
        ]);
        let vb = VarBuilder::from_tensors(tensors, DType::F32, &device);
        let merger = MiniCpmV46Merger::new(&config, vb).unwrap();
        let hidden = Tensor::from_vec(vec![0.0f32, 1.0, 2.0, 3.0], (1, 4, 1), &device).unwrap();
        let outputs = merger.forward(&hidden, &[(2, 2)]).unwrap();
        let values = outputs[0].to_vec2::<f32>().unwrap();
        assert!((values[0][0] - 0.104_932_4).abs() < 1e-5, "{values:?}");
        assert!((values[0][1] - -0.011_553_116).abs() < 1e-5, "{values:?}");
    }

    #[test]
    fn qwen3_5_rms_norm_matches_pytorch_fixture() {
        let device = Device::Cpu;
        let tensors = HashMap::from([(
            "weight".to_string(),
            Tensor::from_vec(vec![0.1f32, -0.2, 0.3], 3, &device).unwrap(),
        )]);
        let norm = Qwen3_5RmsNorm::new(
            3,
            1e-6,
            VarBuilder::from_tensors(tensors, DType::F32, &device),
        )
        .unwrap();
        let input = Tensor::from_vec(
            vec![0.5f32, -1.0, 2.0, -0.25, 0.75, -1.5],
            (1, 2, 3),
            &device,
        )
        .unwrap();
        let values = norm.forward(&input).unwrap().to_vec3::<f32>().unwrap();
        let expected = [
            [0.415_760_82, -0.604_743, 1.965_414_6],
            [-0.280_914_55, 0.612_904_5, -1.991_939_4],
        ];
        for row in 0..2 {
            for col in 0..3 {
                assert!(
                    (values[0][row][col] - expected[row][col]).abs() < 1e-5,
                    "{values:?}"
                );
            }
        }
    }

    #[test]
    fn qwen3_5_mlp_matches_pytorch_fixture() {
        let device = Device::Cpu;
        let tensors = HashMap::from([
            (
                "gate_proj.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.2f32, -0.1, 0.3, 0.0, 0.4, -0.5, -0.3, 0.2, 0.1, 0.5, -0.4, 0.2,
                    ],
                    (4, 3),
                    &device,
                )
                .unwrap(),
            ),
            (
                "up_proj.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.1f32, 0.3, -0.2, -0.2, 0.5, 0.4, 0.6, -0.1, 0.0, -0.3, 0.2, 0.7,
                    ],
                    (4, 3),
                    &device,
                )
                .unwrap(),
            ),
            (
                "down_proj.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.2f32, -0.5, 0.3, 0.1, -0.4, 0.6, -0.2, 0.5, 0.7, 0.1, 0.2, -0.3,
                    ],
                    (3, 4),
                    &device,
                )
                .unwrap(),
            ),
        ]);
        let mlp =
            Qwen3_5Mlp::new(3, 4, VarBuilder::from_tensors(tensors, DType::F32, &device)).unwrap();
        let input = Tensor::from_vec(
            vec![0.5f32, -1.0, 2.0, -0.25, 0.75, -1.5],
            (1, 2, 3),
            &device,
        )
        .unwrap();
        let values = mlp.forward(&input).unwrap().to_vec3::<f32>().unwrap();
        let expected = [
            [0.029_281_069, 0.524_184_5, -0.507_251_6],
            [0.064_244_13, 0.059_071_932, -0.146_378_56],
        ];
        for row in 0..2 {
            for col in 0..3 {
                assert!(
                    (values[0][row][col] - expected[row][col]).abs() < 1e-5,
                    "{values:?}"
                );
            }
        }
    }

    #[test]
    fn qwen3_5_gated_delta_net_matches_pytorch_fixture() {
        let device = Device::Cpu;
        let config = tiny_qwen3_5_text_config();
        let tensors = HashMap::from([
            (
                "conv1d.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.2f32, -0.1, 0.0, 0.3, -0.4, 0.5, 0.6, -0.2, 0.1, 0.25, -0.3, 0.4,
                    ],
                    (6, 1, 2),
                    &device,
                )
                .unwrap(),
            ),
            (
                "dt_bias".to_string(),
                Tensor::from_vec(vec![0.7f32], 1, &device).unwrap(),
            ),
            (
                "A_log".to_string(),
                Tensor::from_vec(vec![0.2f32.ln()], 1, &device).unwrap(),
            ),
            (
                "norm.weight".to_string(),
                Tensor::from_vec(vec![1.1f32, 0.9], 2, &device).unwrap(),
            ),
            (
                "in_proj_qkv.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.2f32, -0.1, 0.3, -0.4, 0.5, 0.1, 0.0, 0.25, -0.35, 0.45, -0.2, 0.15, 0.1,
                        0.2, -0.3, -0.25, 0.4, 0.05,
                    ],
                    (6, 3),
                    &device,
                )
                .unwrap(),
            ),
            (
                "in_proj_z.weight".to_string(),
                Tensor::from_vec(vec![0.3f32, -0.2, 0.1, -0.1, 0.5, 0.2], (2, 3), &device).unwrap(),
            ),
            (
                "in_proj_b.weight".to_string(),
                Tensor::from_vec(vec![0.2f32, -0.3, 0.4], (1, 3), &device).unwrap(),
            ),
            (
                "in_proj_a.weight".to_string(),
                Tensor::from_vec(vec![-0.1f32, 0.25, 0.35], (1, 3), &device).unwrap(),
            ),
            (
                "out_proj.weight".to_string(),
                Tensor::from_vec(vec![0.2f32, -0.1, 0.4, 0.3, -0.5, 0.6], (3, 2), &device).unwrap(),
            ),
        ]);
        let layer = Qwen3_5GatedDeltaNet::new(
            &config,
            VarBuilder::from_tensors(tensors, DType::F32, &device),
        )
        .unwrap();
        let input = Tensor::from_vec(
            vec![0.5f32, -1.0, 2.0, -0.25, 0.75, -1.5, 1.25, 0.0, -0.5],
            (1, 3, 3),
            &device,
        )
        .unwrap();
        let values = layer.forward(&input).unwrap().to_vec3::<f32>().unwrap();
        let expected = [
            [-0.085_968_14, -0.142_135_11, 0.235_781_13],
            [-0.031_701_777, -0.035_503_246, 0.098_784_655],
            [-0.001_479_941_4, 0.059_467_185, 0.047_398_8],
        ];
        for row in 0..3 {
            for col in 0..3 {
                assert!(
                    (values[0][row][col] - expected[row][col]).abs() < 1e-5,
                    "{values:?}"
                );
            }
        }
    }

    #[test]
    fn qwen3_5_full_attention_matches_pytorch_fixture() {
        let device = Device::Cpu;
        let mut config = tiny_qwen3_5_text_config();
        config.hidden_size = 4;
        config.intermediate_size = 6;
        config.num_attention_heads = 2;
        config.num_key_value_heads = 1;
        config.head_dim = 2;
        config.layer_types = vec!["full_attention".to_string()];
        let tensors = HashMap::from([
            (
                "q_proj.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.2f32, -0.1, 0.3, 0.0, -0.4, 0.5, 0.1, -0.2, 0.0, 0.25, -0.35, 0.45, -0.2,
                        0.15, 0.1, 0.2, -0.3, -0.25, 0.4, 0.05, 0.6, -0.1, 0.2, -0.4, 0.35, 0.05,
                        -0.15, 0.25, -0.05, 0.45, 0.3, -0.2,
                    ],
                    (8, 4),
                    &device,
                )
                .unwrap(),
            ),
            (
                "k_proj.weight".to_string(),
                Tensor::from_vec(
                    vec![0.1f32, -0.2, 0.3, 0.4, -0.5, 0.2, 0.1, -0.3],
                    (2, 4),
                    &device,
                )
                .unwrap(),
            ),
            (
                "v_proj.weight".to_string(),
                Tensor::from_vec(
                    vec![0.4f32, 0.1, -0.2, 0.3, -0.1, 0.5, 0.2, -0.4],
                    (2, 4),
                    &device,
                )
                .unwrap(),
            ),
            (
                "o_proj.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.2f32, -0.1, 0.4, 0.3, -0.5, 0.6, 0.1, -0.2, 0.05, 0.25, -0.35, 0.45, 0.3,
                        -0.4, 0.2, 0.1,
                    ],
                    (4, 4),
                    &device,
                )
                .unwrap(),
            ),
            (
                "q_norm.weight".to_string(),
                Tensor::from_vec(vec![0.1f32, -0.2], 2, &device).unwrap(),
            ),
            (
                "k_norm.weight".to_string(),
                Tensor::from_vec(vec![-0.05f32, 0.15], 2, &device).unwrap(),
            ),
        ]);
        let attention = Qwen3_5Attention::new(
            &config,
            VarBuilder::from_tensors(tensors, DType::F32, &device),
        )
        .unwrap();
        let input = Tensor::from_vec(
            vec![
                0.5f32, -1.0, 2.0, 0.25, -0.25, 0.75, -1.5, 0.5, 1.25, 0.0, -0.5, -1.0,
            ],
            (1, 3, 4),
            &device,
        )
        .unwrap();
        let (cos, sin) = qwen3_5_text_rope(&config, 1, 3, &device).unwrap();
        let values = attention
            .forward(&input, &cos, &sin)
            .unwrap()
            .to_vec3::<f32>()
            .unwrap();
        let expected = [
            [
                -0.082_469_78,
                -0.025_698_869,
                -0.055_844_09,
                -0.004_593_394_7,
            ],
            [0.074_963_16, -0.137_782_96, -0.070_227_37, 0.112_699_37],
            [
                0.061_168_53,
                -0.002_188_069_3,
                -0.075_992_554,
                0.044_574_276,
            ],
        ];
        for row in 0..3 {
            for col in 0..4 {
                assert!(
                    (values[0][row][col] - expected[row][col]).abs() < 1e-5,
                    "{values:?}"
                );
            }
        }
    }

    #[test]
    fn qwen3_5_gated_delta_cache_matches_full_prefix() {
        let device = Device::Cpu;
        let config = tiny_qwen3_5_text_config();
        let layer = Qwen3_5GatedDeltaNet::new(
            &config,
            VarBuilder::from_tensors(gated_delta_fixture_tensors(&device), DType::F32, &device),
        )
        .unwrap();
        let input = Tensor::from_vec(
            vec![0.5f32, -1.0, 2.0, -0.25, 0.75, -1.5, 1.25, 0.0, -0.5],
            (1, 3, 3),
            &device,
        )
        .unwrap();
        let full = layer.forward(&input).unwrap();

        let mut cache = None;
        let mut chunks = Vec::new();
        for index in 0..3 {
            let token = input.narrow(1, index, 1).unwrap();
            let (output, next_cache) = layer.forward_with_cache(&token, cache).unwrap();
            cache = Some(next_cache);
            chunks.push(output);
        }
        let refs: Vec<&Tensor> = chunks.iter().collect();
        let cached = Tensor::cat(&refs, 1).unwrap();
        assert_close3(&cached, &full, 1e-5);
    }

    #[test]
    fn qwen3_5_full_attention_cache_matches_full_prefix() {
        let device = Device::Cpu;
        let mut config = tiny_qwen3_5_text_config();
        config.hidden_size = 4;
        config.intermediate_size = 6;
        config.num_attention_heads = 2;
        config.num_key_value_heads = 1;
        config.head_dim = 2;
        config.layer_types = vec!["full_attention".to_string()];
        let attention = Qwen3_5Attention::new(
            &config,
            VarBuilder::from_tensors(full_attention_fixture_tensors(&device), DType::F32, &device),
        )
        .unwrap();
        let input = Tensor::from_vec(
            vec![
                0.5f32, -1.0, 2.0, 0.25, -0.25, 0.75, -1.5, 0.5, 1.25, 0.0, -0.5, -1.0,
            ],
            (1, 3, 4),
            &device,
        )
        .unwrap();
        let (cos, sin) = qwen3_5_text_rope(&config, 1, 3, &device).unwrap();
        let full = attention.forward(&input, &cos, &sin).unwrap();

        let mut cache = None;
        let mut chunks = Vec::new();
        for index in 0..3 {
            let token = input.narrow(1, index, 1).unwrap();
            let (cos, sin) = qwen3_5_text_rope_from(&config, 1, 1, index, &device).unwrap();
            let (output, next_cache) = attention
                .forward_with_cache(&token, &cos, &sin, cache, index)
                .unwrap();
            cache = Some(next_cache);
            chunks.push(output);
        }
        let refs: Vec<&Tensor> = chunks.iter().collect();
        let cached = Tensor::cat(&refs, 1).unwrap();
        assert_close3(&cached, &full, 1e-5);
    }

    fn assert_close3(actual: &Tensor, expected: &Tensor, tolerance: f32) {
        let actual = actual.to_vec3::<f32>().unwrap();
        let expected = expected.to_vec3::<f32>().unwrap();
        assert_eq!(actual.len(), expected.len());
        for batch in 0..actual.len() {
            assert_eq!(actual[batch].len(), expected[batch].len());
            for row in 0..actual[batch].len() {
                assert_eq!(actual[batch][row].len(), expected[batch][row].len());
                for col in 0..actual[batch][row].len() {
                    assert!(
                        (actual[batch][row][col] - expected[batch][row][col]).abs() < tolerance,
                        "actual={actual:?}, expected={expected:?}"
                    );
                }
            }
        }
    }

    fn gated_delta_fixture_tensors(device: &Device) -> HashMap<String, Tensor> {
        HashMap::from([
            (
                "conv1d.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.2f32, -0.1, 0.0, 0.3, -0.4, 0.5, 0.6, -0.2, 0.1, 0.25, -0.3, 0.4,
                    ],
                    (6, 1, 2),
                    device,
                )
                .unwrap(),
            ),
            (
                "dt_bias".to_string(),
                Tensor::from_vec(vec![0.7f32], 1, device).unwrap(),
            ),
            (
                "A_log".to_string(),
                Tensor::from_vec(vec![0.2f32.ln()], 1, device).unwrap(),
            ),
            (
                "norm.weight".to_string(),
                Tensor::from_vec(vec![1.1f32, 0.9], 2, device).unwrap(),
            ),
            (
                "in_proj_qkv.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.2f32, -0.1, 0.3, -0.4, 0.5, 0.1, 0.0, 0.25, -0.35, 0.45, -0.2, 0.15, 0.1,
                        0.2, -0.3, -0.25, 0.4, 0.05,
                    ],
                    (6, 3),
                    device,
                )
                .unwrap(),
            ),
            (
                "in_proj_z.weight".to_string(),
                Tensor::from_vec(vec![0.3f32, -0.2, 0.1, -0.1, 0.5, 0.2], (2, 3), device).unwrap(),
            ),
            (
                "in_proj_b.weight".to_string(),
                Tensor::from_vec(vec![0.2f32, -0.3, 0.4], (1, 3), device).unwrap(),
            ),
            (
                "in_proj_a.weight".to_string(),
                Tensor::from_vec(vec![-0.1f32, 0.25, 0.35], (1, 3), device).unwrap(),
            ),
            (
                "out_proj.weight".to_string(),
                Tensor::from_vec(vec![0.2f32, -0.1, 0.4, 0.3, -0.5, 0.6], (3, 2), device).unwrap(),
            ),
        ])
    }

    fn full_attention_fixture_tensors(device: &Device) -> HashMap<String, Tensor> {
        HashMap::from([
            (
                "q_proj.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.2f32, -0.1, 0.3, 0.0, -0.4, 0.5, 0.1, -0.2, 0.0, 0.25, -0.35, 0.45, -0.2,
                        0.15, 0.1, 0.2, -0.3, -0.25, 0.4, 0.05, 0.6, -0.1, 0.2, -0.4, 0.35, 0.05,
                        -0.15, 0.25, -0.05, 0.45, 0.3, -0.2,
                    ],
                    (8, 4),
                    device,
                )
                .unwrap(),
            ),
            (
                "k_proj.weight".to_string(),
                Tensor::from_vec(
                    vec![0.1f32, -0.2, 0.3, 0.4, -0.5, 0.2, 0.1, -0.3],
                    (2, 4),
                    device,
                )
                .unwrap(),
            ),
            (
                "v_proj.weight".to_string(),
                Tensor::from_vec(
                    vec![0.4f32, 0.1, -0.2, 0.3, -0.1, 0.5, 0.2, -0.4],
                    (2, 4),
                    device,
                )
                .unwrap(),
            ),
            (
                "o_proj.weight".to_string(),
                Tensor::from_vec(
                    vec![
                        0.2f32, -0.1, 0.4, 0.3, -0.5, 0.6, 0.1, -0.2, 0.05, 0.25, -0.35, 0.45, 0.3,
                        -0.4, 0.2, 0.1,
                    ],
                    (4, 4),
                    device,
                )
                .unwrap(),
            ),
            (
                "q_norm.weight".to_string(),
                Tensor::from_vec(vec![0.1f32, -0.2], 2, device).unwrap(),
            ),
            (
                "k_norm.weight".to_string(),
                Tensor::from_vec(vec![-0.05f32, 0.15], 2, device).unwrap(),
            ),
        ])
    }

    fn tiny_qwen3_5_text_config() -> MiniCpmV46TextConfig {
        MiniCpmV46TextConfig {
            attention_bias: false,
            attention_dropout: 0.0,
            attn_output_gate: true,
            full_attention_interval: 4,
            head_dim: 2,
            hidden_act: "silu".to_string(),
            hidden_size: 3,
            initializer_range: 0.02,
            intermediate_size: 4,
            layer_types: vec!["linear_attention".to_string()],
            linear_conv_kernel_dim: 2,
            linear_key_head_dim: 2,
            linear_num_key_heads: 1,
            linear_num_value_heads: 1,
            linear_value_head_dim: 2,
            mamba_ssm_dtype: "float32".to_string(),
            max_position_embeddings: 32,
            mlp_only_layers: vec![],
            mtp_num_hidden_layers: 1,
            mtp_use_dedicated_embeddings: false,
            num_attention_heads: 1,
            num_hidden_layers: 1,
            num_key_value_heads: 1,
            partial_rotary_factor: 1.0,
            rms_norm_eps: 1e-6,
            rope_parameters: RopeParameters {
                partial_rotary_factor: 1.0,
                rope_theta: 10_000.0,
                rope_type: "default".to_string(),
            },
            vocab_size: 16,
            model_type: "qwen3_5_text".to_string(),
            tie_word_embeddings: false,
        }
    }

    #[test]
    fn vision_position_ids_match_pytorch_bucketizing() {
        let device = Device::Cpu;
        let config = MiniCpmV46VisionConfig {
            attention_dropout: 0.0,
            hidden_act: "gelu_pytorch_tanh".to_string(),
            hidden_size: 2,
            image_size: 4,
            intermediate_size: 4,
            layer_norm_eps: 1e-6,
            model_type: "minicpmv4_6_vision".to_string(),
            num_attention_heads: 1,
            num_channels: 3,
            num_hidden_layers: 1,
            patch_size: 1,
        };
        let tensors = HashMap::from([
            (
                "patch_embedding.weight".to_string(),
                Tensor::zeros((2, 3, 1, 1), DType::F32, &device).unwrap(),
            ),
            (
                "patch_embedding.bias".to_string(),
                Tensor::zeros(2, DType::F32, &device).unwrap(),
            ),
            (
                "position_embedding.weight".to_string(),
                Tensor::zeros((16, 2), DType::F32, &device).unwrap(),
            ),
        ]);
        let embeddings = MiniCpmV46VisionEmbeddings::new(
            &config,
            VarBuilder::from_tensors(tensors, DType::F32, &device),
        )
        .unwrap();
        let ids = embeddings
            .position_ids(&[(2, 4)], (2, 4), 1, &device)
            .unwrap()
            .to_vec1::<u32>()
            .unwrap();
        assert_eq!(ids, vec![0, 1, 2, 3, 8, 9, 10, 11]);
    }

    #[test]
    fn vision_encoder_layer_zero_projection_preserves_hidden_states() {
        let device = Device::Cpu;
        let config = MiniCpmV46VisionConfig {
            attention_dropout: 0.0,
            hidden_act: "gelu_pytorch_tanh".to_string(),
            hidden_size: 2,
            image_size: 2,
            intermediate_size: 3,
            layer_norm_eps: 1e-6,
            model_type: "minicpmv4_6_vision".to_string(),
            num_attention_heads: 1,
            num_channels: 3,
            num_hidden_layers: 1,
            patch_size: 1,
        };
        let mut tensors = HashMap::new();
        for name in ["layer_norm1.weight", "layer_norm2.weight"] {
            tensors.insert(
                name.to_string(),
                Tensor::ones(2, DType::F32, &device).unwrap(),
            );
        }
        for name in ["layer_norm1.bias", "layer_norm2.bias"] {
            tensors.insert(
                name.to_string(),
                Tensor::zeros(2, DType::F32, &device).unwrap(),
            );
        }
        for name in [
            "self_attn.q_proj.weight",
            "self_attn.k_proj.weight",
            "self_attn.v_proj.weight",
            "self_attn.out_proj.weight",
        ] {
            tensors.insert(
                name.to_string(),
                Tensor::zeros((2, 2), DType::F32, &device).unwrap(),
            );
        }
        for name in [
            "self_attn.q_proj.bias",
            "self_attn.k_proj.bias",
            "self_attn.v_proj.bias",
            "self_attn.out_proj.bias",
        ] {
            tensors.insert(
                name.to_string(),
                Tensor::zeros(2, DType::F32, &device).unwrap(),
            );
        }
        tensors.insert(
            "mlp.fc1.weight".to_string(),
            Tensor::zeros((3, 2), DType::F32, &device).unwrap(),
        );
        tensors.insert(
            "mlp.fc1.bias".to_string(),
            Tensor::zeros(3, DType::F32, &device).unwrap(),
        );
        tensors.insert(
            "mlp.fc2.weight".to_string(),
            Tensor::zeros((2, 3), DType::F32, &device).unwrap(),
        );
        tensors.insert(
            "mlp.fc2.bias".to_string(),
            Tensor::zeros(2, DType::F32, &device).unwrap(),
        );
        let layer = MiniCpmV46VisionEncoderLayer::new(
            &config,
            VarBuilder::from_tensors(tensors, DType::F32, &device),
        )
        .unwrap();
        let input = Tensor::from_vec(
            (0..8).map(|value| value as f32).collect(),
            (1, 4, 2),
            &device,
        )
        .unwrap();
        let output = layer.forward(&input, &[0, 4]).unwrap();
        assert_eq!(
            output.to_vec3::<f32>().unwrap(),
            input.to_vec3::<f32>().unwrap()
        );
    }

    #[test]
    fn vision_encoder_layer_matches_pytorch_fixture() {
        let device = Device::Cpu;
        let config = MiniCpmV46VisionConfig {
            attention_dropout: 0.0,
            hidden_act: "gelu_pytorch_tanh".to_string(),
            hidden_size: 2,
            image_size: 2,
            intermediate_size: 3,
            layer_norm_eps: 1e-6,
            model_type: "minicpmv4_6_vision".to_string(),
            num_attention_heads: 1,
            num_channels: 3,
            num_hidden_layers: 1,
            patch_size: 1,
        };
        let tensors = HashMap::from([
            (
                "layer_norm1.weight".to_string(),
                Tensor::from_vec(vec![1.0f32, 1.1], 2, &device).unwrap(),
            ),
            (
                "layer_norm1.bias".to_string(),
                Tensor::from_vec(vec![0.1f32, -0.2], 2, &device).unwrap(),
            ),
            (
                "layer_norm2.weight".to_string(),
                Tensor::from_vec(vec![0.9f32, 1.2], 2, &device).unwrap(),
            ),
            (
                "layer_norm2.bias".to_string(),
                Tensor::from_vec(vec![-0.1f32, 0.05], 2, &device).unwrap(),
            ),
            (
                "self_attn.q_proj.weight".to_string(),
                Tensor::from_vec(vec![0.2f32, -0.1, 0.3, 0.4], (2, 2), &device).unwrap(),
            ),
            (
                "self_attn.q_proj.bias".to_string(),
                Tensor::from_vec(vec![0.01f32, -0.02], 2, &device).unwrap(),
            ),
            (
                "self_attn.k_proj.weight".to_string(),
                Tensor::from_vec(vec![0.5f32, 0.2, -0.3, 0.1], (2, 2), &device).unwrap(),
            ),
            (
                "self_attn.k_proj.bias".to_string(),
                Tensor::from_vec(vec![0.03f32, 0.04], 2, &device).unwrap(),
            ),
            (
                "self_attn.v_proj.weight".to_string(),
                Tensor::from_vec(vec![0.1f32, -0.4, 0.6, 0.2], (2, 2), &device).unwrap(),
            ),
            (
                "self_attn.v_proj.bias".to_string(),
                Tensor::from_vec(vec![-0.05f32, 0.02], 2, &device).unwrap(),
            ),
            (
                "self_attn.out_proj.weight".to_string(),
                Tensor::from_vec(vec![0.25f32, -0.35, 0.45, 0.15], (2, 2), &device).unwrap(),
            ),
            (
                "self_attn.out_proj.bias".to_string(),
                Tensor::from_vec(vec![0.07f32, -0.08], 2, &device).unwrap(),
            ),
            (
                "mlp.fc1.weight".to_string(),
                Tensor::from_vec(vec![0.2f32, -0.1, 0.0, 0.3, -0.4, 0.5], (3, 2), &device).unwrap(),
            ),
            (
                "mlp.fc1.bias".to_string(),
                Tensor::from_vec(vec![0.01f32, -0.02, 0.03], 3, &device).unwrap(),
            ),
            (
                "mlp.fc2.weight".to_string(),
                Tensor::from_vec(vec![0.1f32, -0.2, 0.4, -0.3, 0.5, 0.2], (2, 3), &device).unwrap(),
            ),
            (
                "mlp.fc2.bias".to_string(),
                Tensor::from_vec(vec![0.04f32, -0.06], 2, &device).unwrap(),
            ),
        ]);
        let layer = MiniCpmV46VisionEncoderLayer::new(
            &config,
            VarBuilder::from_tensors(tensors, DType::F32, &device),
        )
        .unwrap();
        let input = Tensor::from_vec(
            vec![0.5f32, -1.0, 1.5, 0.25, -0.75, 0.8],
            (1, 3, 2),
            &device,
        )
        .unwrap();
        let output = layer.forward(&input, &[0, 2, 3]).unwrap();
        let values = output.to_vec3::<f32>().unwrap();
        let expected = [
            [0.584_549_6, -0.967_097_3],
            [1.584_549_5, 0.282_902_72],
            [-0.342_763_42, 0.713_156_6],
        ];
        for row in 0..3 {
            for col in 0..2 {
                assert!(
                    (values[0][row][col] - expected[row][col]).abs() < 1e-5,
                    "{values:?}"
                );
            }
        }
    }

    #[test]
    fn vit_window_merger_matches_pytorch_zero_projection_fixture() {
        let device = Device::Cpu;
        let config = MiniCpmV46VisionConfig {
            attention_dropout: 0.0,
            hidden_act: "gelu_pytorch_tanh".to_string(),
            hidden_size: 2,
            image_size: 2,
            intermediate_size: 3,
            layer_norm_eps: 1e-6,
            model_type: "minicpmv4_6_vision".to_string(),
            num_attention_heads: 1,
            num_channels: 3,
            num_hidden_layers: 1,
            patch_size: 1,
        };
        let mut tensors = HashMap::new();
        for name in ["layer_norm1.weight", "pre_norm.weight"] {
            let len = if name.starts_with("pre_norm") { 8 } else { 2 };
            tensors.insert(
                name.to_string(),
                Tensor::ones(len, DType::F32, &device).unwrap(),
            );
        }
        for name in ["layer_norm1.bias", "pre_norm.bias"] {
            let len = if name.starts_with("pre_norm") { 8 } else { 2 };
            tensors.insert(
                name.to_string(),
                Tensor::zeros(len, DType::F32, &device).unwrap(),
            );
        }
        for name in [
            "self_attn.q_proj.weight",
            "self_attn.k_proj.weight",
            "self_attn.v_proj.weight",
            "self_attn.out_proj.weight",
        ] {
            tensors.insert(
                name.to_string(),
                Tensor::zeros((2, 2), DType::F32, &device).unwrap(),
            );
        }
        for name in [
            "self_attn.q_proj.bias",
            "self_attn.k_proj.bias",
            "self_attn.v_proj.bias",
            "self_attn.out_proj.bias",
        ] {
            tensors.insert(
                name.to_string(),
                Tensor::zeros(2, DType::F32, &device).unwrap(),
            );
        }
        tensors.insert(
            "linear_1.weight".to_string(),
            Tensor::zeros((12, 8), DType::F32, &device).unwrap(),
        );
        tensors.insert(
            "linear_1.bias".to_string(),
            Tensor::zeros(12, DType::F32, &device).unwrap(),
        );
        tensors.insert(
            "linear_2.weight".to_string(),
            Tensor::zeros((2, 12), DType::F32, &device).unwrap(),
        );
        tensors.insert(
            "linear_2.bias".to_string(),
            Tensor::zeros(2, DType::F32, &device).unwrap(),
        );
        let merger = MiniCpmV46VitWindowAttentionMerger::new(
            &config,
            VarBuilder::from_tensors(tensors, DType::F32, &device),
        )
        .unwrap();
        let hidden = Tensor::from_vec(
            (0..8).map(|value| value as f32).collect(),
            (1, 4, 2),
            &device,
        )
        .unwrap();
        let output = merger.forward(&hidden, &[(2, 2)], &[0, 4]).unwrap();
        assert_eq!(output.dims(), &[1, 1, 2]);
        assert_eq!(output.to_vec3::<f32>().unwrap(), vec![vec![vec![3.0, 4.0]]]);
        assert_eq!(
            merger.get_window_index(&[(2, 4)]).unwrap(),
            (vec![0, 1, 4, 5, 2, 3, 6, 7], vec![0, 4, 8])
        );
    }
}
