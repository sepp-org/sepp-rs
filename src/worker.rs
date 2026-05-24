use std::{collections::HashMap, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures::{FutureExt, future::BoxFuture};
use tokio::{sync::Semaphore, task::AbortHandle};
use tracing::{Instrument, debug, error, info, warn};

use crate::{
    Job, JobCtx, Payload, ReserveOptions,
    client::{ClientError, Lease, LeaseError, RetryDirective, SeppClient},
    now_millis,
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
    auto_extend: Option<AutoExtend>,
}

#[derive(Debug, Clone, Copy)]
struct AutoExtend {
    interval: Duration,
    extend_by: Duration,
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
            auto_extend: None,
        }
    }

    pub fn with_auto_extend(mut self) -> Self {
        let lease = self.opts.lease_duration;
        self.auto_extend = Some(AutoExtend {
            interval: heartbeat_interval(lease),
            extend_by: lease,
        });
        self
    }

    pub fn with_auto_extend_interval(mut self, interval: Duration) -> Self {
        let lease = self.opts.lease_duration;
        self.auto_extend = Some(AutoExtend {
            interval: interval.max(Duration::from_millis(1)),
            extend_by: lease,
        });
        self
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
        let auto_extend = self.auto_extend;
        info!(
            worker_id = self.client.worker_id().unwrap_or("<none>"),
            max_in_flight = self.max_in_flight,
            handlers = handlers.len(),
            auto_extend = auto_extend.is_some(),
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
                    process_job(&client, &handlers, auto_extend, first).await;
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
                    process_job(&client, &handlers, auto_extend, job).await;
                });
            }
        }
    }
}

async fn process_job(
    client: &SeppClient,
    handlers: &HashMap<String, Handler>,
    auto_extend: Option<AutoExtend>,
    job: Job,
) {
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

    run_job(client, handlers, auto_extend, job)
        .instrument(span)
        .await
}

async fn run_job(
    client: &SeppClient,
    handlers: &HashMap<String, Handler>,
    auto_extend: Option<AutoExtend>,
    job: Job,
) {
    let Job { payload, ctx } = job;
    let lease = ctx.lease.clone();
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

    let fut = handler(payload, Arc::clone(&ctx));

    let disposition = match auto_extend {
        None => match AssertUnwindSafe(fut).catch_unwind().await {
            Ok(result) => Disposition::Completed(result),
            Err(_panic) => Disposition::Panicked,
        },
        Some(cfg) => {
            let handler_task = tokio::spawn(fut);
            let abort = handler_task.abort_handle();
            let heartbeat_task = tokio::spawn(heartbeat(lease, cfg, abort));

            let joined = handler_task.await;
            heartbeat_task.abort(); // handler finished — stop extending

            match joined {
                Ok(result) => Disposition::Completed(result),
                Err(err) if err.is_cancelled() => {
                    error!("lease lost; handler aborted");
                    return;
                }
                Err(_panic) => Disposition::Panicked,
            }
        }
    };

    if let Err(err) = dispose(client, &ctx, disposition).await {
        error!("failed to ack/nack job; it will be redelivered after lease expiry: {err}");
    }
}

enum Disposition {
    Completed(Result<(), WorkerError>),
    Panicked,
}

async fn dispose(
    client: &SeppClient,
    ctx: &JobCtx,
    disposition: Disposition,
) -> Result<(), LeaseError> {
    match disposition {
        Disposition::Completed(Ok(())) => {
            debug!("job completed; acking");
            client.ack(ctx).await.map(drop)
        }
        Disposition::Completed(Err(err)) => {
            warn!("handler returned error; nacking: {err}");
            let (retry, reason) = match err {
                WorkerError::Retry(r) => (RetryDirective::Default, r),
                WorkerError::RetryAfter(r, d) => (RetryDirective::After(d), r),
                WorkerError::Permanent(r) => (RetryDirective::DeadLetter, r),
            };
            client.nack(ctx, retry, reason).await.map(drop)
        }
        Disposition::Panicked => {
            error!("handler panicked; nacking");
            client
                .nack(ctx, RetryDirective::Default, "handler panicked")
                .await
                .map(drop)
        }
    }
}

async fn heartbeat(lease: Lease, cfg: AutoExtend, handler: AbortHandle) {
    loop {
        tokio::time::sleep(cfg.interval).await;

        match lease.extend(cfg.extend_by).await {
            Ok(expiry) => debug!(?expiry, "lease extended"),
            Err(err @ (LeaseError::AttemptMismatch | LeaseError::JobNotFound)) => {
                error!(
                    "lease reassigned by server ({err}); aborting handler to avoid double processing"
                );
                handler.abort();
                return;
            }
            Err(err) => {
                if now_millis() >= lease.known_expiry_ms() {
                    error!("lease lost ({err}); aborting handler to avoid double processing");
                    handler.abort();
                    return;
                }
                warn!("lease extend failed ({err}); lease still valid, will retry");
            }
        }
    }
}

fn heartbeat_interval(lease: Duration) -> Duration {
    (lease / 3).max(Duration::from_millis(1))
}

fn default_worker_id() -> String {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    let rand = uuid::Uuid::new_v4().simple().to_string();
    format!("{host}-{}-{}", std::process::id(), &rand[..8])
}
