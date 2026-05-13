use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::future::BoxFuture;

use crate::{
    JobCtx, Payload, ReserveOptions,
    client::{ClientError, SeppClient},
};

type Handler = Arc<
    dyn Fn(Option<Payload>, JobCtx) -> BoxFuture<'static, Result<(), WorkerError>> + Send + Sync,
>;

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("retry: {0}")]
    Retry(String),
    #[error("retry after {1:?}: {0}")]
    RetryAfter(String, Duration),
    #[error("permanent: {0}")]
    Permanent(String),
}

impl WorkerError {
    pub fn retry(reason: impl Into<String>) -> Self {
        Self::Retry(reason.into())
    }
    pub fn retry_after(reason: impl Into<String>, delay: Duration) -> Self {
        Self::RetryAfter(reason.into(), delay)
    }
    pub fn permanent(reason: impl Into<String>) -> Self {
        Self::Permanent(reason.into())
    }
}

pub struct Worker {
    client: SeppClient,
    opts: ReserveOptions,
    handlers: HashMap<String, Handler>,
    max_in_flight: usize,
}

impl Worker {
    pub fn handle<F, Fut>(mut self, job_type: &str, h: F) -> Self
    where
        F: Fn(Option<Payload>, JobCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), WorkerError>> + Send + 'static,
    {
        let h = Arc::new(h);
        self.handlers.insert(
            job_type.to_string(),
            Arc::new(move |payload, ctx| Box::pin(h(payload, ctx))),
        );
        self
    }

    pub async fn run(self) -> Result<(), ClientError> {
        todo!();
    }
}
