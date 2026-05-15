use std::{collections::HashMap, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures::{FutureExt, future::BoxFuture};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use crate::{
    Job, JobCtx, Payload, ReserveOptions,
    client::{ClientError, RetryDirective, SeppClient},
};

type Handler = Arc<
    dyn Fn(Option<Payload>, Arc<JobCtx>) -> BoxFuture<'static, Result<(), WorkerError>>
        + Send
        + Sync,
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
    reserve_error_backoff: Duration,
}

impl Worker {
    /// Create a worker that reserves jobs using `opts`.
    ///
    /// Register handlers with [`Worker::handle`], then call [`Worker::run`].
    pub fn new(client: SeppClient, opts: ReserveOptions) -> Self {
        Self {
            client,
            opts,
            handlers: HashMap::new(),
            max_in_flight: 16,
            reserve_error_backoff: Duration::from_secs(1),
        }
    }

    /// Set the maximum number of jobs processed concurrently. Default: 16.
    pub fn with_max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = max_in_flight;
        self
    }

    /// Set how long to wait before re-polling after a failed `reserve`.
    /// Default: 1s.
    pub fn with_reserve_error_backoff(mut self, backoff: Duration) -> Self {
        self.reserve_error_backoff = backoff;
        self
    }

    pub fn handle<F, Fut>(mut self, job_type: &str, h: F) -> Self
    where
        F: Fn(Option<Payload>, Arc<JobCtx>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), WorkerError>> + Send + 'static,
    {
        let h = Arc::new(h);
        self.handlers.insert(
            job_type.to_string(),
            Arc::new(move |payload, ctx| Box::pin(h(payload, ctx))),
        );
        self
    }

    #[tracing::instrument(skip_all)]
    pub async fn run(self) -> Result<(), ClientError> {
        let semaphore = Arc::new(Semaphore::new(self.max_in_flight.max(1)));
        let handlers = Arc::new(self.handlers);
        info!(
            max_in_flight = self.max_in_flight,
            handlers = handlers.len(),
            "worker started"
        );

        loop {
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore is never closed");

            let job = match self.client.reserve(&self.opts).await {
                Ok(Some(job)) => job,
                Ok(None) => continue, // wait window elapsed — re-poll immediately
                Err(_err) => {
                    warn!(
                        "reserve error: {_err}; backing off for {:?}",
                        self.reserve_error_backoff
                    );
                    tokio::time::sleep(self.reserve_error_backoff).await;
                    continue;
                }
            };

            let client = self.client.clone();
            let handlers = Arc::clone(&handlers);
            tokio::spawn(async move {
                let _permit = permit; // held for the job's lifetime, released on completion
                process_job(&client, &handlers, job).await;
            });
        }
    }
}

#[tracing::instrument(
    skip_all,
    fields(job_id = %job.ctx.id, job_type = %job.ctx.job_type, attempt = job.ctx.attempt)
)]
async fn process_job(client: &SeppClient, handlers: &HashMap<String, Handler>, job: Job) {
    let Job { payload, ctx } = job;
    let ctx = Arc::new(ctx);

    let Some(handler) = handlers.get(&ctx.job_type) else {
        warn!("no handler registered for job_type `{}`", ctx.job_type);

        let _ = client
            .nack(
                &ctx,
                RetryDirective::Default,
                "no handler registered for job_type",
            )
            .await;
        return;
    };

    // catch_unwind so a panicking handler becomes a nack rather than killing
    // the spawned task and silently leaking the job until its lease expires.
    let outcome = AssertUnwindSafe(handler(payload, Arc::clone(&ctx)))
        .catch_unwind()
        .await;

    let result = match outcome {
        Ok(Ok(())) => {
            debug!("job completed; acking");
            client.ack(&ctx).await.map(drop)
        }
        Ok(Err(err)) => {
            warn!("handler returned error; nacking: {err}");
            let (retry, reason) = match err {
                WorkerError::Retry(r) => (RetryDirective::Default, r),
                WorkerError::RetryAfter(r, d) => (RetryDirective::After(d), r),
                WorkerError::Permanent(r) => (RetryDirective::DeadLetter, r),
            };
            client.nack(&ctx, retry, reason).await.map(drop)
        }
        Err(_panic) => {
            error!("handler panicked; nacking");
            client
                .nack(&ctx, RetryDirective::Default, "handler panicked")
                .await
                .map(drop)
        }
    };

    // A failed ack/nack means the job is redelivered once its lease expires.
    if let Err(err) = result {
        error!("failed to ack/nack job; it will be redelivered after lease expiry: {err}");
    }
}
