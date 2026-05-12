use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, thiserror::Error)]
pub enum ConverterError {
    #[error("encode failed: {0}")]
    Encode(String),
    #[error("decode failed: {0}")]
    Decode(String),
}

pub trait PayloadConverter: Send + Sync {
    fn encoding(&self) -> &'static str;

    fn to_bytes<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, ConverterError>;
    fn from_bytes<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, ConverterError>;
}

pub struct JsonConverter;

impl PayloadConverter for JsonConverter {
    fn encoding(&self) -> &'static str {
        "json"
    }

    fn to_bytes<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, ConverterError> {
        serde_json::to_vec(value).map_err(|e| ConverterError::Encode(e.to_string()))
    }

    fn from_bytes<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, ConverterError> {
        serde_json::from_slice(bytes).map_err(|e| ConverterError::Decode(e.to_string()))
    }
}
