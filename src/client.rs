use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

use crate::pb::sepp::v1::queue_service_client::QueueServiceClient;

#[derive(Clone)]
pub struct SeppClient {
    inner: QueueServiceClient<Channel>,
}

// Assert that SeppClient is Send + Sync at compile time
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SeppClient>();
};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Failed to connect to Sepp server: {0}")]
    Connect(#[from] tonic::transport::Error),
    #[error("Internal RPC error: {0}")]
    Rpc(#[from] tonic::Status),
}

impl SeppClient {
    pub async fn connect(addr: impl Into<String>) -> Result<Self, ClientError> {
        let channel = Endpoint::from_shared(addr.into())?
            .connect_timeout(Duration::from_secs(5))
            .user_agent(concat!("sepp-rs/", env!("CARGO_PKG_VERSION")))? // So we can tell from the server POV which client this is
            .http2_keep_alive_interval(Duration::from_secs(30)) // For streaming reserve
            .keep_alive_timeout(Duration::from_secs(10)) // For streaming reserve
            .keep_alive_while_idle(true) // For streaming reserve
            .connect()
            .await?;

        Ok(Self {
            inner: QueueServiceClient::new(channel),
        })
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: QueueServiceClient::new(channel),
        }
    }
}
