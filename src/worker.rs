use std::{collections::HashMap, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures::{FutureExt, future::BoxFuture};
use tokio::sync::Semaphore;
use tracing::{Instrument, debug, error, info, warn};

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
    pub fn new(client: SeppClient, opts: ReserveOptions) -> Self {
        let client = if client.worker_id().is_some() {
            client
        } else {
            client.with_worker_id(default_worker_id())
        };
        Self {
            client,
            opts,
            handlers: HashMap::new(),
            max_in_flight: 16,
            reserve_error_backoff: Duration::from_secs(1),
        }
    }

    pub fn with_max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = max_in_flight;
        self
    }

    pub fn with_reserve_error_backoff(mut self, backoff: Duration) -> Self {
        self.reserve_error_backoff = backoff;
        self
    }

    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.client = self.client.with_worker_id(worker_id);
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

    pub async fn run(self) -> Result<(), ClientError> {
        let semaphore = Arc::new(Semaphore::new(self.max_in_flight.max(1)));
        let handlers = Arc::new(self.handlers);
        info!(
            worker_id = self.client.worker_id().unwrap_or("<none>"),
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

            let mut opts = self.opts.clone();
            if let Some(user_max) = opts.max_jobs {
                let capacity = (1 + semaphore.available_permits()).min(u32::MAX as usize) as u32;
                opts.max_jobs = Some(user_max.min(capacity));
            }

            let jobs = match self.client.reserve(&opts).await {
                Ok(Some(jobs)) => jobs,
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

            let mut jobs = jobs.into_iter();
            let Some(first) = jobs.next() else { continue };
            {
                let client = self.client.clone();
                let handlers = Arc::clone(&handlers);
                tokio::spawn(async move {
                    let _permit = permit; // held for the job's lifetime
                    process_job(&client, &handlers, first).await;
                });
            }
            for job in jobs {
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("semaphore is never closed");
                let client = self.client.clone();
                let handlers = Arc::clone(&handlers);
                tokio::spawn(async move {
                    let _permit = permit;
                    process_job(&client, &handlers, job).await;
                });
            }
        }
    }
}

async fn process_job(client: &SeppClient, handlers: &HashMap<String, Handler>, job: Job) {
    let span = tracing::info_span!(
        "sepp-rs.process",
        job_id = %job.ctx.id,
        job_type = %job.ctx.job_type,
        attempt = job.ctx.attempt,
    );

    #[cfg(feature = "opentelemetry")]
    if let Some(link) = job
        .ctx
        .trace_context
        .as_ref()
        .and_then(crate::TraceContext::otel_span_context)
    {
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        span.add_link(link);
    }

    run_job(client, handlers, job).instrument(span).await
}

async fn run_job(client: &SeppClient, handlers: &HashMap<String, Handler>, job: Job) {
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

    if let Err(err) = result {
        error!("failed to ack/nack job; it will be redelivered after lease expiry: {err}");
    }
}

fn default_worker_id() -> String {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    let rand = uuid::Uuid::new_v4().simple().to_string();
    format!("{host}-{}-{}", std::process::id(), &rand[..8])
}
