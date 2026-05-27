use std::{collections::HashMap, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures::{FutureExt, future::BoxFuture};
use tokio::{sync::Semaphore, task::AbortHandle};
use tokio_util::sync::CancellationToken;
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
    shutdown: ShutdownHandle,
    metrics: Arc<Metrics>,
}

#[derive(Debug, Clone, Copy)]
struct AutoExtend {
    interval: Duration,
    extend_by: Duration,
}

#[cfg(feature = "opentelemetry")]
struct Metrics {
    jobs_processed: opentelemetry::metrics::Counter<u64>,
    jobs_nacked: opentelemetry::metrics::Counter<u64>,
    jobs_in_flight: opentelemetry::metrics::UpDownCounter<i64>,
    reserves_completed: opentelemetry::metrics::Counter<u64>,
    reserves_failed: opentelemetry::metrics::Counter<u64>,
}

#[cfg(not(feature = "opentelemetry"))]
struct Metrics;

impl Metrics {
    #[cfg(feature = "opentelemetry")]
    fn new() -> Self {
        let meter = opentelemetry::global::meter("sepp-rs");
        Self {
            jobs_processed: meter
                .u64_counter("sepp_rs.jobs.processed")
                .with_description("Jobs successfully acked.")
                .build(),
            jobs_nacked: meter
                .u64_counter("sepp_rs.jobs.nacked")
                .with_description(
                    "Jobs nacked. Attribute `outcome` is `retry` or `dead_letter`.",
                )
                .build(),
            jobs_in_flight: meter
                .i64_up_down_counter("sepp_rs.jobs.in_flight")
                .with_description("Jobs currently being processed by handlers.")
                .build(),
            reserves_completed: meter
                .u64_counter("sepp_rs.reserves.completed")
                .with_description(
                    "Reserve RPCs that returned. Attribute `jobs` is `some` or `empty`.",
                )
                .build(),
            reserves_failed: meter
                .u64_counter("sepp_rs.reserves.failed")
                .with_description("Reserve RPCs that failed.")
                .build(),
        }
    }

    #[cfg(not(feature = "opentelemetry"))]
    fn new() -> Self {
        Self
    }

    fn record_processed(&self) {
        #[cfg(feature = "opentelemetry")]
        self.jobs_processed.add(1, &[]);
    }

    fn record_nacked(&self, dead_lettered: bool) {
        #[cfg(feature = "opentelemetry")]
        {
            let outcome = if dead_lettered { "dead_letter" } else { "retry" };
            self.jobs_nacked
                .add(1, &[opentelemetry::KeyValue::new("outcome", outcome)]);
        }
        #[cfg(not(feature = "opentelemetry"))]
        let _ = dead_lettered;
    }

    fn record_in_flight_delta(&self, delta: i64) {
        #[cfg(feature = "opentelemetry")]
        self.jobs_in_flight.add(delta, &[]);
        #[cfg(not(feature = "opentelemetry"))]
        let _ = delta;
    }

    fn record_reserve_ok(&self, empty: bool) {
        #[cfg(feature = "opentelemetry")]
        {
            let jobs = if empty { "empty" } else { "some" };
            self.reserves_completed
                .add(1, &[opentelemetry::KeyValue::new("jobs", jobs)]);
        }
        #[cfg(not(feature = "opentelemetry"))]
        let _ = empty;
    }

    fn record_reserve_failed(&self) {
        #[cfg(feature = "opentelemetry")]
        self.reserves_failed.add(1, &[]);
    }
}

struct InFlightGuard {
    metrics: Arc<Metrics>,
}

impl InFlightGuard {
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.record_in_flight_delta(1);
        Self { metrics }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.metrics.record_in_flight_delta(-1);
    }
}

#[derive(Debug, Clone)]
pub struct ShutdownHandle {
    token: CancellationToken,
}

impl ShutdownHandle {
    fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    pub fn shutdown(&self) {
        self.token.cancel();
    }

    pub fn is_shutdown(&self) -> bool {
        self.token.is_cancelled()
    }
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
            shutdown: ShutdownHandle::new(),
            metrics: Arc::new(Metrics::new()),
        }
    }

    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown.clone()
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
        let max_permits = self.max_in_flight.max(1);
        let semaphore = Arc::new(Semaphore::new(max_permits));
        let handlers = Arc::new(self.handlers);
        let auto_extend = self.auto_extend;
        let shutdown = self.shutdown.clone();
        let metrics = Arc::clone(&self.metrics);
        info!(
            worker_id = self.client.worker_id().unwrap_or("<none>"),
            max_in_flight = self.max_in_flight,
            handlers = handlers.len(),
            auto_extend = auto_extend.is_some(),
            "worker started"
        );

        'outer: loop {
            let permit = tokio::select! {
                biased;
                () = shutdown.token.cancelled() => break 'outer,
                p = semaphore.clone().acquire_owned() => p.expect("semaphore is never closed"),
            };

            let mut opts = self.opts.clone();
            if let Some(user_max) = opts.max_jobs {
                let capacity = (1 + semaphore.available_permits()).min(u32::MAX as usize) as u32;
                opts.max_jobs = Some(user_max.min(capacity));
            }

            let jobs = tokio::select! {
                biased;
                () = shutdown.token.cancelled() => {
                    drop(permit);
                    break 'outer;
                }
                res = self.client.reserve(&opts) => match res {
                    Ok(Some(jobs)) => {
                        metrics.record_reserve_ok(false);
                        jobs
                    }
                    Ok(None) => {
                        metrics.record_reserve_ok(true);
                        continue; // wait window elapsed — re-poll immediately
                    }
                    Err(_err) => {
                        metrics.record_reserve_failed();
                        warn!(
                            "reserve error: {_err}; backing off for {:?}",
                            self.reserve_error_backoff
                        );
                        drop(permit);
                        tokio::select! {
                            biased;
                            () = shutdown.token.cancelled() => break 'outer,
                            () = tokio::time::sleep(self.reserve_error_backoff) => continue,
                        }
                    }
                },
            };

            let mut jobs = jobs.into_iter();
            let Some(first) = jobs.next() else { continue };
            {
                let client = self.client.clone();
                let handlers = Arc::clone(&handlers);
                let metrics = Arc::clone(&metrics);
                let in_flight = InFlightGuard::new(Arc::clone(&metrics));
                tokio::spawn(async move {
                    let _permit = permit; // held for the job's lifetime
                    let _in_flight = in_flight;
                    process_job(&client, &handlers, auto_extend, first, &metrics).await;
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
                let metrics = Arc::clone(&metrics);
                let in_flight = InFlightGuard::new(Arc::clone(&metrics));
                tokio::spawn(async move {
                    let _permit = permit;
                    let _in_flight = in_flight;
                    process_job(&client, &handlers, auto_extend, job, &metrics).await;
                });
            }
        }

        info!("worker shutting down; waiting for in-flight jobs to finish");
        let _drain = semaphore
            .acquire_many(max_permits as u32)
            .await
            .expect("semaphore is never closed");
        info!("worker stopped");
        Ok(())
    }
}

async fn process_job(
    client: &SeppClient,
    handlers: &HashMap<String, Handler>,
    auto_extend: Option<AutoExtend>,
    job: Job,
    metrics: &Metrics,
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

    run_job(client, handlers, auto_extend, job, metrics)
        .instrument(span)
        .await
}

async fn run_job(
    client: &SeppClient,
    handlers: &HashMap<String, Handler>,
    auto_extend: Option<AutoExtend>,
    job: Job,
    metrics: &Metrics,
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

    if let Err(err) = dispose(client, &ctx, disposition, metrics).await {
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
    metrics: &Metrics,
) -> Result<(), LeaseError> {
    match disposition {
        Disposition::Completed(Ok(())) => {
            debug!("job completed; acking");
            client.ack(ctx).await?;
            metrics.record_processed();
            Ok(())
        }
        Disposition::Completed(Err(err)) => {
            warn!("handler returned error; nacking: {err}");
            let (retry, reason) = match err {
                WorkerError::Retry(r) => (RetryDirective::Default, r),
                WorkerError::RetryAfter(r, d) => (RetryDirective::After(d), r),
                WorkerError::Permanent(r) => (RetryDirective::DeadLetter, r),
            };
            let dead_lettered = client.nack(ctx, retry, reason).await?;
            metrics.record_nacked(dead_lettered);
            Ok(())
        }
        Disposition::Panicked => {
            error!("handler panicked; nacking");
            let dead_lettered = client
                .nack(ctx, RetryDirective::Default, "handler panicked")
                .await?;
            metrics.record_nacked(dead_lettered);
            Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_interval_third_of_lease() {
        assert_eq!(
            heartbeat_interval(Duration::from_secs(3)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn heartbeat_interval_nine_seconds() {
        assert_eq!(
            heartbeat_interval(Duration::from_secs(9)),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn heartbeat_interval_floor_at_one_ms_for_tiny_lease() {
        assert_eq!(
            heartbeat_interval(Duration::from_millis(1)),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn heartbeat_interval_floor_at_one_ms_for_zero_lease() {
        assert_eq!(heartbeat_interval(Duration::ZERO), Duration::from_millis(1));
    }

    #[test]
    fn worker_err_retry() {
        let e = WorkerError::retry("network");
        assert!(matches!(e, WorkerError::Retry(s) if s == "network"));
    }

    #[test]
    fn worker_err_retry_after() {
        let e = WorkerError::retry_after("rate limited", Duration::from_secs(5));
        assert!(matches!(
            e,
            WorkerError::RetryAfter(s, d) if s == "rate limited" && d == Duration::from_secs(5)
        ));
    }

    #[test]
    fn worker_err_permanent() {
        let e = WorkerError::permanent("bad input");
        assert!(matches!(e, WorkerError::Permanent(s) if s == "bad input"));
    }

    #[test]
    fn shutdown_handle_starts_unsignaled() {
        let h = ShutdownHandle::new();
        assert!(!h.is_shutdown());
    }

    #[test]
    fn shutdown_handle_is_signaled_after_shutdown() {
        let h = ShutdownHandle::new();
        h.shutdown();
        assert!(h.is_shutdown());
    }

    #[test]
    fn shutdown_handle_clones_share_state() {
        let h = ShutdownHandle::new();
        let h2 = h.clone();
        h.shutdown();
        assert!(h2.is_shutdown());
    }

    #[tokio::test]
    async fn shutdown_handle_cancelled_resolves_when_already_signaled() {
        let h = ShutdownHandle::new();
        h.shutdown();
        tokio::time::timeout(Duration::from_secs(1), h.token.cancelled())
            .await
            .expect("cancelled() should resolve immediately when already signaled");
    }

    #[tokio::test]
    async fn shutdown_handle_cancelled_resolves_on_late_signal() {
        let h = ShutdownHandle::new();
        let h2 = h.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            h2.shutdown();
        });
        tokio::time::timeout(Duration::from_secs(1), h.token.cancelled())
            .await
            .expect("cancelled() should be woken by shutdown signal");
    }
}
