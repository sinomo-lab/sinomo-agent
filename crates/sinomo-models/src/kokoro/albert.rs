//! The phoneme-level ALBERT encoder Kokoro conditions its prosody on.
//!
//! This is a plain ALBERT with one layer group and one layer inside it, so a single set of weights
//! is applied `num_hidden_layers` times. That parameter sharing is why a 12-layer encoder costs so
//! little of the model's 82M parameters, and it is the reason the loop below reuses one layer rather
//! than indexing a list.

use super::{KokoroError, PlBertConfig, Result};
use candle_core::{D, Module, Tensor};
use candle_nn::{Embedding, LayerNorm, Linear, VarBuilder, embedding, layer_norm, linear};

/// ALBERT keeps its own layer-norm epsilon, which is not the one the StyleTTS2 layers use.
const LAYER_NORM_EPS: f64 = 1e-12;
/// Sequences carry no segment structure, so every token takes token type zero.
const TOKEN_TYPE_ID: u32 = 0;

#[derive(Debug)]
struct AlbertEmbeddings {
    word: Embedding,
    position: Embedding,
    token_type: Embedding,
    norm: LayerNorm,
}

impl AlbertEmbeddings {
    fn load(config: &PlBertConfig, vocab_size: usize, vb: VarBuilder) -> Result<Self> {
        Ok(Self {
            word: embedding(vocab_size, config.embedding_size, vb.pp("word_embeddings"))?,
            position: embedding(
                config.max_position_embeddings,
                config.embedding_size,
                vb.pp("position_embeddings"),
            )?,
            token_type: embedding(
                config.type_vocab_size,
                config.embedding_size,
                vb.pp("token_type_embeddings"),
            )?,
            norm: layer_norm(
                config.embedding_size,
                candle_nn::LayerNormConfig {
                    eps: LAYER_NORM_EPS,
                    ..Default::default()
                },
                vb.pp("LayerNorm"),
            )?,
        })
    }

    fn forward(&self, input_ids: &Tensor) -> Result<Tensor> {
        let (_batch, tokens) = input_ids.dims2()?;
        let device = input_ids.device();
        let positions = Tensor::arange(0_u32, tokens as u32, device)?;
        let token_types = Tensor::full(TOKEN_TYPE_ID, tokens, device)?;
        let embedded = self.word.forward(input_ids)?;
        let embedded = embedded.broadcast_add(&self.position.forward(&positions)?)?;
        let embedded = embedded.broadcast_add(&self.token_type.forward(&token_types)?)?;
        Ok(self.norm.forward(&embedded)?)
    }
}

/// One ALBERT layer: post-norm self attention followed by a post-norm feed-forward block.
#[derive(Debug)]
struct AlbertLayer {
    query: Linear,
    key: Linear,
    value: Linear,
    dense: Linear,
    attention_norm: LayerNorm,
    ffn: Linear,
    ffn_output: Linear,
    full_layer_norm: LayerNorm,
    heads: usize,
    head_dim: usize,
}

impl AlbertLayer {
    fn load(config: &PlBertConfig, vb: VarBuilder) -> Result<Self> {
        if !config
            .hidden_size
            .is_multiple_of(config.num_attention_heads)
        {
            return Err(KokoroError::Unsupported {
                detail: format!(
                    "ALBERT hidden size {} does not divide into {} attention heads",
                    config.hidden_size, config.num_attention_heads
                ),
            });
        }
        let hidden = config.hidden_size;
        let attention = vb.pp("attention");
        Ok(Self {
            query: linear(hidden, hidden, attention.pp("query"))?,
            key: linear(hidden, hidden, attention.pp("key"))?,
            value: linear(hidden, hidden, attention.pp("value"))?,
            dense: linear(hidden, hidden, attention.pp("dense"))?,
            attention_norm: layer_norm(
                hidden,
                candle_nn::LayerNormConfig {
                    eps: LAYER_NORM_EPS,
                    ..Default::default()
                },
                attention.pp("LayerNorm"),
            )?,
            ffn: linear(hidden, config.intermediate_size, vb.pp("ffn"))?,
            ffn_output: linear(config.intermediate_size, hidden, vb.pp("ffn_output"))?,
            full_layer_norm: layer_norm(
                hidden,
                candle_nn::LayerNormConfig {
                    eps: LAYER_NORM_EPS,
                    ..Default::default()
                },
                vb.pp("full_layer_layer_norm"),
            )?,
            heads: config.num_attention_heads,
            head_dim: hidden / config.num_attention_heads,
        })
    }

    fn forward(&self, xs: &Tensor) -> Result<Tensor> {
        let attended = self.attention(xs)?;
        let hidden = self.ffn.forward(&attended)?;
        let hidden = gelu_new(&hidden)?;
        let hidden = self.ffn_output.forward(&hidden)?;
        Ok(self.full_layer_norm.forward(&(hidden + &attended)?)?)
    }

    fn attention(&self, xs: &Tensor) -> Result<Tensor> {
        let (batch, tokens, _hidden) = xs.dims3()?;
        let split = |projected: Tensor| -> Result<Tensor> {
            Ok(projected
                .reshape((batch, tokens, self.heads, self.head_dim))?
                .transpose(1, 2)?
                .contiguous()?)
        };
        let query = split(self.query.forward(xs)?)?;
        let key = split(self.key.forward(xs)?)?;
        let value = split(self.value.forward(xs)?)?;
        // Every sequence is dense — a Kokoro batch is one utterance with no padding — so there is no
        // attention mask to add here.
        let scores =
            (query.matmul(&key.transpose(D::Minus2, D::Minus1)?)? / (self.head_dim as f64).sqrt())?;
        let weights = candle_nn::ops::softmax_last_dim(&scores)?;
        let context = weights
            .matmul(&value)?
            .transpose(1, 2)?
            .contiguous()?
            .reshape((batch, tokens, self.heads * self.head_dim))?;
        let projected = self.dense.forward(&context)?;
        Ok(self.attention_norm.forward(&(projected + xs)?)?)
    }
}

/// The `gelu_new` activation ALBERT is configured with.
///
/// `gelu_new` is the tanh approximation of GELU, which is what Candle's `gelu` computes; its
/// `gelu_erf` is the exact form and would not match the checkpoint's training-time activation.
fn gelu_new(xs: &Tensor) -> Result<Tensor> {
    Ok(xs.gelu()?)
}

#[derive(Debug)]
pub struct PlBert {
    embeddings: AlbertEmbeddings,
    hidden_mapping: Linear,
    layer: AlbertLayer,
    layers: usize,
}

impl PlBert {
    pub fn load(config: &PlBertConfig, vocab_size: usize, vb: VarBuilder) -> Result<Self> {
        let encoder = vb.pp("encoder");
        Ok(Self {
            embeddings: AlbertEmbeddings::load(config, vocab_size, vb.pp("embeddings"))?,
            hidden_mapping: linear(
                config.embedding_size,
                config.hidden_size,
                encoder.pp("embedding_hidden_mapping_in"),
            )?,
            // One group holding one layer, reused for every hidden layer.
            layer: AlbertLayer::load(
                config,
                encoder
                    .pp("albert_layer_groups")
                    .pp("0")
                    .pp("albert_layers")
                    .pp("0"),
            )?,
            layers: config.num_hidden_layers,
        })
    }

    /// `input_ids` is `[B, T]`; the result is the last hidden state, `[B, T, hidden_size]`.
    pub fn forward(&self, input_ids: &Tensor) -> Result<Tensor> {
        let mut hidden = self
            .hidden_mapping
            .forward(&self.embeddings.forward(input_ids)?)?;
        for _ in 0..self.layers {
            hidden = self.layer.forward(&hidden)?;
        }
        Ok(hidden)
    }
}
