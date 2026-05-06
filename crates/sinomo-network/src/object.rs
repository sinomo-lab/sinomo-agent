use crate::error::NetworkError;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sinomo_object::{LengthMode, serde_ext};

pub const SINOMO_NETWORK_OBJECT_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SinomoObjectCodec {
    pub version_id: u16,
    pub length_mode: LengthMode,
}

impl Default for SinomoObjectCodec {
    fn default() -> Self {
        Self {
            version_id: SINOMO_NETWORK_OBJECT_VERSION,
            length_mode: LengthMode::U32,
        }
    }
}

impl SinomoObjectCodec {
    pub fn encode<T: serde::Serialize + ?Sized>(self, value: &T) -> Result<Vec<u8>, NetworkError> {
        serde_ext::to_bytes(value, self.version_id, self.length_mode).map_err(NetworkError::object)
    }

    pub fn decode<T: serde::de::DeserializeOwned>(self, bytes: &[u8]) -> Result<T, NetworkError> {
        serde_ext::from_bytes(bytes).map_err(NetworkError::object)
    }
}

pub fn encode_object_text_payload<T: serde::Serialize + ?Sized>(
    value: &T,
) -> Result<String, NetworkError> {
    let bytes = SinomoObjectCodec::default().encode(value)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub fn decode_object_text_payload<T: serde::de::DeserializeOwned>(
    payload: &str,
) -> Result<T, NetworkError> {
    let bytes =
        URL_SAFE_NO_PAD
            .decode(payload.trim())
            .map_err(|source| NetworkError::Serialization {
                detail: source.to_string(),
            })?;
    SinomoObjectCodec::default().decode(&bytes)
}
