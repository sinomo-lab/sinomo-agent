//! Phoneme tokenization and voice packs.
//!
//! Kokoro is language-blind: it takes phonemes, not text, and the mapping from phoneme to token id
//! is a table in the checkpoint's `config.json`. A voice is not a weight set but a style vector, and
//! a voice pack holds one vector per phoneme count so that longer utterances get a style fitted to
//! their length.

use super::nn::select_voice_row;
use super::{KokoroError, Result};
use candle_core::{DType, Device, Tensor};
use std::collections::BTreeMap;
use std::path::Path;

/// Token that opens and closes every sequence.
pub const BOUNDARY_TOKEN: u32 = 0;

/// The phoneme-to-token table from the model configuration.
#[derive(Clone, Debug, Default)]
pub struct PhonemeVocab {
    tokens: BTreeMap<char, u32>,
}

/// Result of tokenizing a phoneme string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenizedPhonemes {
    /// Token ids, including the leading and trailing boundary tokens.
    pub input_ids: Vec<u32>,
    /// Phonemes that are not in the vocabulary and were dropped.
    pub unknown: Vec<char>,
}

impl TokenizedPhonemes {
    /// The number of phoneme tokens, excluding the two boundary tokens.
    ///
    /// This is also the index into a voice pack: the style is chosen by how many phonemes are
    /// spoken.
    pub fn phoneme_count(&self) -> usize {
        self.input_ids.len().saturating_sub(2)
    }
}

impl PhonemeVocab {
    pub fn new(tokens: BTreeMap<char, u32>) -> Self {
        Self { tokens }
    }

    /// Builds the table from the `vocab` object of a model configuration.
    pub fn from_config_value(value: &serde_json::Value) -> Result<Self> {
        let object = value.as_object().ok_or_else(|| KokoroError::Unsupported {
            detail: "the model configuration's `vocab` must be an object".to_string(),
        })?;
        let mut tokens = BTreeMap::new();
        for (phoneme, id) in object {
            let mut characters = phoneme.chars();
            let (Some(character), None) = (characters.next(), characters.next()) else {
                // Every Kokoro phoneme is one character; a multi-character key would silently never
                // match, so it is a configuration error rather than something to skip.
                return Err(KokoroError::Unsupported {
                    detail: format!("phoneme `{phoneme}` is not a single character"),
                });
            };
            let id = id.as_u64().ok_or_else(|| KokoroError::Unsupported {
                detail: format!("phoneme `{phoneme}` does not map to a token id"),
            })?;
            tokens.insert(character, id as u32);
        }
        Ok(Self { tokens })
    }

    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    pub fn token(&self, phoneme: char) -> Option<u32> {
        self.tokens.get(&phoneme).copied()
    }

    /// Maps a phoneme string to token ids, wrapped in boundary tokens.
    ///
    /// Unrepresentable phonemes are dropped rather than refused, matching the reference
    /// implementation, and are reported so a caller can see what was lost.
    pub fn tokenize(&self, phonemes: &str) -> TokenizedPhonemes {
        let mut input_ids = vec![BOUNDARY_TOKEN];
        let mut unknown = Vec::new();
        for phoneme in phonemes.chars() {
            match self.token(phoneme) {
                Some(id) => input_ids.push(id),
                None => unknown.push(phoneme),
            }
        }
        input_ids.push(BOUNDARY_TOKEN);
        TokenizedPhonemes { input_ids, unknown }
    }
}

/// Reads a `.pt` file that holds one tensor rather than a state dictionary.
///
/// Candle's loader only walks a top-level dictionary, which is what a saved model is. A voice pack is
/// a bare `torch.save(tensor)`, so the pickle is parsed here with Candle's own reader and the single
/// storage entry is read out of the archive directly.
fn read_bare_tensor(path: &Path) -> Result<Tensor> {
    use candle_core::pickle::{Object, Stack};

    let archive = std::fs::File::open(path)?;
    let mut archive = zip::ZipArchive::new(std::io::BufReader::new(archive)).map_err(|source| {
        KokoroError::InvalidInput {
            detail: format!("`{}` is not a torch archive: {source}", path.display()),
        }
    })?;
    let pickle_name = archive
        .file_names()
        .find(|name| name.ends_with("data.pkl"))
        .map(str::to_string)
        .ok_or_else(|| KokoroError::InvalidInput {
            detail: format!("`{}` has no pickled payload", path.display()),
        })?;
    let directory = std::path::PathBuf::from(
        pickle_name
            .strip_suffix(".pkl")
            .expect("the entry name ends with .pkl"),
    );
    let info = {
        let entry = archive
            .by_name(&pickle_name)
            .map_err(|source| KokoroError::InvalidInput {
                detail: format!("cannot read {pickle_name}: {source}"),
            })?;
        let mut stack = Stack::empty();
        stack.read_loop(&mut std::io::BufReader::new(entry))?;
        let object = stack.finalize()?;
        object
            .into_tensor_info(Object::Unicode("style".to_string()), &directory)?
            .ok_or_else(|| KokoroError::InvalidInput {
                detail: format!("`{}` does not hold a single tensor", path.display()),
            })?
    };
    if !info.layout.is_contiguous() {
        return Err(KokoroError::Unsupported {
            detail: format!(
                "`{}` holds a non-contiguous tensor, which cannot be read in place",
                path.display()
            ),
        });
    }
    let mut storage = archive
        .by_name(&info.path)
        .map_err(|source| KokoroError::InvalidInput {
            detail: format!("cannot read storage {}: {source}", info.path),
        })?;
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut storage, &mut bytes)?;
    let shape = info.layout.shape().clone();
    let elements = shape.elem_count();
    // A tensor saved as a view into a larger storage starts partway into it.
    let offset = info.layout.start_offset() * info.dtype.size_in_bytes();
    if offset > bytes.len() {
        return Err(KokoroError::InvalidInput {
            detail: format!(
                "`{}` starts {offset} bytes into a {}-byte storage",
                path.display(),
                bytes.len()
            ),
        });
    }
    let bytes = bytes.split_off(offset);
    // Torch storage is little-endian and, having checked the layout above, contiguous.
    let values = match info.dtype {
        DType::F32 => {
            expect_storage_size(path, &bytes, elements, 4)?;
            bytes
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect::<Vec<f32>>()
        }
        DType::F64 => {
            expect_storage_size(path, &bytes, elements, 8)?;
            bytes
                .chunks_exact(8)
                .map(|chunk| {
                    f64::from_le_bytes([
                        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6],
                        chunk[7],
                    ]) as f32
                })
                .collect::<Vec<f32>>()
        }
        dtype => {
            return Err(KokoroError::Unsupported {
                detail: format!(
                    "`{}` holds {dtype:?} styles; only float voice packs are read",
                    path.display()
                ),
            });
        }
    };
    Ok(Tensor::from_vec(values, shape, &Device::Cpu)?)
}

/// Checks that a storage entry holds exactly the declared number of elements.
fn expect_storage_size(path: &Path, bytes: &[u8], elements: usize, width: usize) -> Result<()> {
    if bytes.len() != elements * width {
        return Err(KokoroError::InvalidInput {
            detail: format!(
                "`{}` declares {elements} elements but its storage holds {} bytes",
                path.display(),
                bytes.len()
            ),
        });
    }
    Ok(())
}

/// A voice: one style vector per phoneme count.
#[derive(Debug)]
pub struct VoicePack {
    name: String,
    /// `[rows, 1, style_width]`, where `style_width` covers both halves of the style vector.
    styles: Tensor,
}

impl VoicePack {
    /// Loads a voice pack from the `.pt` tensor published alongside the model.
    pub fn from_pt_file(
        name: impl Into<String>,
        path: impl AsRef<Path>,
        device: &Device,
        dtype: DType,
    ) -> Result<Self> {
        let path = path.as_ref();
        let styles = read_bare_tensor(path)?;
        Self::from_tensor(name, styles.to_dtype(dtype)?.to_device(device)?)
    }

    pub fn from_tensor(name: impl Into<String>, styles: Tensor) -> Result<Self> {
        let dims = styles.dims();
        if dims.len() != 3 || dims[1] != 1 {
            return Err(KokoroError::Unsupported {
                detail: format!(
                    "a voice pack must be shaped [rows, 1, style_width], but this one is {dims:?}"
                ),
            });
        }
        Ok(Self {
            name: name.into(),
            styles,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn rows(&self) -> usize {
        self.styles.dims()[0]
    }

    pub fn style_width(&self) -> usize {
        self.styles.dims()[2]
    }

    /// Returns the `[1, style_width]` style vector for an utterance of `phoneme_count` phonemes.
    ///
    /// Longer utterances than the pack covers reuse its last row, which is preferable to refusing to
    /// speak: the style only conditions prosody.
    pub fn style_for(&self, phoneme_count: usize) -> Result<Tensor> {
        let index = phoneme_count.min(self.rows().saturating_sub(1));
        select_voice_row(&self.styles, index)
    }
}
