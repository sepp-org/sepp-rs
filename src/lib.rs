use std::collections::HashMap;

use serde::{Serialize, de::DeserializeOwned};
use tonic::transport::Channel;

use crate::pb::sepp::v1::queue_service_client::QueueServiceClient;

pub mod error;
pub mod payload_converter;
mod pb;

pub trait Job: 'static {
    const KIND: &'static str;

    type Args: Serialize + DeserializeOwned + Send + 'static;
}

#[derive(Debug, Clone, PartialEq)]
pub enum Primitive {
    String(String),
    Int(i64),
    Double(f64),
    Bool(bool),
}

impl From<&str> for Primitive {
    fn from(v: &str) -> Self {
        Self::String(v.into())
    }
}
impl From<String> for Primitive {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}
impl From<i64> for Primitive {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}
impl From<i32> for Primitive {
    fn from(v: i32) -> Self {
        Self::Int(v.into())
    }
}
impl From<f64> for Primitive {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}
impl From<bool> for Primitive {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

#[derive(Debug)]
pub struct JobCtx {
    pub id: String,
    pub kind: String,
    pub attempt: u32,
    pub custom: HashMap<String, Primitive>,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("retryable: {0}")]
    Retry(String),
    #[error("permanent: {0}")]
    Permanent(String),
}

#[allow(async_fn_in_trait)]
pub trait Worker<J: Job>: Send + Sync + 'static {
    async fn run(&self, args: J::Args, ctx: JobCtx) -> Result<(), WorkerError>;
}
