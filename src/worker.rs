//! A high-level worker that runs the reserve → process → ack/nack loop.
//!
//! [`Worker`] wraps a [`SeppClient`] and drives job consumption for you:
//! reserve jobs, dispatch each to the handler registered for its `job_type`,
//! and ack on success or nack on failure. It adds bounded concurrency, optional
//! lease auto-extension, graceful shutdown via a [`ShutdownHandle`], and (with
//! the `opentelemetry` feature) metrics and trace linkage.
//!
//! Register handlers with [`Worker::handle`], then call [`Worker::run`]:
//!
//! ```no_run
//! use std::time::Duration;
//! use sepp_rs::client::SeppClient;
//! use sepp_rs::worker::{HandlerError, Worker};
//!
//! # async fn run(client: SeppClient) -> Result<(), Box<dyn std::error::Error>> {
//! Worker::new(client, ["emails"], Duration::from_secs(30))?
//!     .with_max_in_flight(32)
//!     .with_auto_extend()
//!     .handle("send_welcome", |payload, ctx| async move {
//!         // ... do the work ...
//!         Ok(())
//!     })?
//!     .handle("send_receipt", |payload, ctx| async move {
//!         Err(HandlerError::retry("payment service unavailable"))
//!     })?
//!     .run()
//!     .await;
//! # Ok(())
//! # }
//! ```
//!
//! A handler's return value decides the job's fate: `Ok(())` acks it, and an
//! [`Err`] of [`HandlerError`] nacks it with the corresponding
//! [`RetryDirective`]. A handler that panics is
//! caught and nacked rather than bringing the worker down.

use std::{collections::HashMap, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures::{FutureExt, future::BoxFuture};
use tokio::{sync::Semaphore, task::AbortHandle};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, warn};

use crate::{
    Job, JobCtx, Payload, ReserveOptions, ReserveOptionsError,
    client::{Lease, LeaseError, RetryDirective, SeppClient},
    now_millis,
};

type Handler = Arc<
    dyn Fn(Option<Payload>, Arc<JobCtx>) -> BoxFuture<'static, Result<(), HandlerError>>
        + Send
        + Sync,
>;

fn wrap_handler<F, Fut>(h: F) -> Handler
where
    F: Fn(Option<Payload>, Arc<JobCtx>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
{
    let h = Arc::new(h);
    Arc::new(move |payload, ctx| Box::pin(h(payload, ctx)))
}

/// The error a job handler returns to nack its job, choosing how it should be
/// retried.
///
/// Each variant maps to a [`RetryDirective`]:
/// [`Retry`](Self::Retry) → `Default`, [`RetryAfter`](Self::RetryAfter) →
/// `After`, [`Permanent`](Self::Permanent) → `DeadLetter`. Use the
/// [`retry`](Self::retry), [`retry_after`](Self::retry_after), and
/// [`permanent`](Self::permanent) constructors rather than the variants
/// directly.
#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    /// Retry using the queue's default retry policy.
    #[error("retry: {0}")]
    Retry(String),
    /// Retry, but not before the given delay.
    #[error("retry after {1:?}: {0}")]
    RetryAfter(String, Duration),
    /// Do not retry; dead-letter the job immediately.
    #[error("permanent: {0}")]
    Permanent(String),
}

impl HandlerError {
    /// Nack the job for retry under the queue's default policy.
    pub fn retry(reason: impl Into<String>) -> Self {
        Self::Retry(reason.into())
    }
    /// Nack the job for retry after at least `delay`.
    pub fn retry_after(reason: impl Into<String>, delay: Duration) -> Self {
        Self::RetryAfter(reason.into(), delay)
    }
    /// Nack the job as a permanent failure, sending it straight to the
    /// dead-letter queue.
    pub fn permanent(reason: impl Into<String>) -> Self {
        Self::Permanent(reason.into())
    }
}

/// Returned by the [`Worker`] builder methods on invalid configuration.
#[derive(Debug, thiserror::Error)]
pub enum WorkerBuilderError {
    /// [`handle`](Worker::handle) was called twice for the same job type. Use
    /// [`replace_handler`](Worker::replace_handler) to overwrite intentionally.
    #[error("handler for job_type {0:?} is already registered")]
    DuplicateHandler(String),
    /// The underlying [`ReserveOptions`] were invalid (e.g. an empty queue or
    /// worker id).
    #[error(transparent)]
    ReserveOptions(#[from] ReserveOptionsError),
}

/// A job-processing loop built on a [`SeppClient`].
///
/// Configure it fluently — queues and lease duration via [`new`](Self::new),
/// then `with_*` tuning and one [`handle`](Self::handle) call per job type —
/// and start it with [`run`](Self::run). `run` consumes the worker and only
/// returns after a [`ShutdownHandle`] is triggered and in-flight jobs have
/// drained.
///
/// Each reserved job runs on its own task, bounded by
/// [`with_max_in_flight`](Self::with_max_in_flight). A job whose `job_type` has
/// no registered handler is nacked for retry.
pub struct Worker {
    client: SeppClient,
    opts: ReserveOptions,
    handlers: HashMap<String, Handler>,
    catch_all_handler: Option<Handler>,
    max_in_flight: usize,
    reserve_error_backoff: Duration,
    auto_extend: Option<AutoExtend>,
    shutdown: ShutdownHandle,
    metrics: Arc<Metrics>,
}

#[derive(Debug, Clone, Copy)]
struct AutoExtend {
    // None = derive the interval from the granted lease each cycle (default);
    // Some = the caller's explicit interval. The default must track the GRANTED
    // lease, not the requested one: if the server clamps the lease below the
    // request, a requested-lease/3 interval fires only after the granted lease
    // has already expired, so the job is redelivered and runs twice.
    explicit_interval: Option<Duration>,
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
                .with_description("Jobs nacked. Attribute `outcome` is `retry` or `dead_letter`.")
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
            let outcome = if dead_lettered {
                "dead_letter"
            } else {
                "retry"
            };
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

/// A cloneable handle for triggering a [`Worker`]'s graceful shutdown.
///
/// Obtain one from [`Worker::shutdown_handle`] *before* calling
/// [`Worker::run`] (which consumes the worker). Calling
/// [`shutdown`](Self::shutdown) stops new reservations; `run` then waits for
/// in-flight jobs to finish before returning. Clones share the same signal, so
/// you can hand a handle to a signal-handler task.
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

    /// Signals the worker to stop reserving new jobs and begin draining.
    pub fn shutdown(&self) {
        self.token.cancel();
    }

    /// Returns whether shutdown has been signalled.
    pub fn is_shutdown(&self) -> bool {
        self.token.is_cancelled()
    }
}

impl Worker {
    /// Creates a worker that reserves from `queues` with the given lease
    /// duration.
    ///
    /// Sensible defaults are applied: up to 16 jobs in flight, a 1s backoff
    /// after a failed reserve, no lease auto-extension, and a generated
    /// [`worker_id`](Self::with_worker_id) derived from the hostname and PID.
    /// Register at least one handler with [`handle`](Self::handle) before
    /// [`run`](Self::run).
    pub fn new(
        client: SeppClient,
        queues: impl IntoIterator<Item = impl Into<String>>,
        lease_duration: Duration,
    ) -> Result<Self, WorkerBuilderError> {
        let mut opts = ReserveOptions::new(queues, lease_duration)?;
        opts.worker_id = Some(default_worker_id());
        Ok(Self {
            client,
            opts,
            handlers: HashMap::new(),
            catch_all_handler: None,
            max_in_flight: 16,
            reserve_error_backoff: Duration::from_secs(1),
            auto_extend: None,
            shutdown: ShutdownHandle::new(),
            metrics: Arc::new(Metrics::new()),
        })
    }

    /// Sets the long-poll wait timeout for each reserve. See
    /// [`ReserveOptions::with_wait_timeout`].
    pub fn with_wait_timeout(mut self, wait: Duration) -> Self {
        self.opts.wait_timeout = wait;
        self
    }

    /// Caps how many jobs a single reserve may return. The worker already
    /// limits this to its free in-flight capacity, so set this only to request
    /// fewer.
    ///
    /// # Panics
    ///
    /// Panics if `max` is 0. The server requires `max_jobs >= 1` and would
    /// reject every reserve, hanging the worker.
    pub fn with_max_jobs(mut self, max: u32) -> Self {
        assert!(max >= 1, "max_jobs must be at least 1");
        self.opts.max_jobs = Some(max);
        self
    }

    /// Returns a [`ShutdownHandle`] for stopping the worker. Obtain it before
    /// calling [`run`](Self::run), which consumes `self`.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown.clone()
    }

    /// Enables automatic lease extension while a handler runs, using a
    /// heartbeat interval of one third of the lease duration. Use with caution:
    /// if the handler hangs indefinitely, the lease will be extended forever.
    ///
    /// With this on, long-running handlers keep their lease alive without
    /// calling [`JobCtx::extend`](crate::JobCtx::extend) themselves. If the
    /// server reassigns the lease anyway, the handler task is aborted to avoid
    /// double processing.
    pub fn with_auto_extend(mut self) -> Self {
        self.auto_extend = Some(AutoExtend {
            explicit_interval: None,
            extend_by: self.opts.lease_duration,
        });
        self
    }

    /// Like [`with_auto_extend`](Self::with_auto_extend) but with an explicit
    /// heartbeat interval (floored at 1ms). The interval should be comfortably
    /// shorter than the lease duration.
    pub fn with_auto_extend_interval(mut self, interval: Duration) -> Self {
        self.auto_extend = Some(AutoExtend {
            explicit_interval: Some(interval.max(Duration::from_millis(1))),
            extend_by: self.opts.lease_duration,
        });
        self
    }

    /// Sets the maximum number of jobs processed concurrently (default 16).
    /// Values below 1 are treated as 1.
    pub fn with_max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = max_in_flight;
        self
    }

    /// Sets how long to wait after a failed reserve before retrying (default
    /// 1s), preventing a hot loop when the server is unreachable.
    pub fn with_reserve_error_backoff(mut self, backoff: Duration) -> Self {
        self.reserve_error_backoff = backoff;
        self
    }

    /// Overrides the auto-generated worker id. Must be non-empty.
    pub fn with_worker_id(
        mut self,
        worker_id: impl Into<String>,
    ) -> Result<Self, WorkerBuilderError> {
        let id = worker_id.into();
        if id.is_empty() {
            return Err(ReserveOptionsError::EmptyWorkerId.into());
        }
        self.opts.worker_id = Some(id);
        Ok(self)
    }

    /// Registers the handler for a `job_type`.
    ///
    /// The handler receives the job's optional [`Payload`] and an
    /// `Arc<JobCtx>`, and returns `Ok(())` to ack or a [`HandlerError`] to
    /// nack. Returns [`WorkerBuilderError::DuplicateHandler`] if a handler is
    /// already registered for this type.
    pub fn handle<F, Fut>(mut self, job_type: &str, h: F) -> Result<Self, WorkerBuilderError>
    where
        F: Fn(Option<Payload>, Arc<JobCtx>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        if self.handlers.contains_key(job_type) {
            return Err(WorkerBuilderError::DuplicateHandler(job_type.to_string()));
        }
        self.handlers.insert(job_type.to_string(), wrap_handler(h));
        Ok(self)
    }

    /// Registers a catch-all handler for job types without a specific handler.
    pub fn with_catch_all_handler<F, Fut>(mut self, h: F) -> Self
    where
        F: Fn(Option<Payload>, Arc<JobCtx>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        self.catch_all_handler = Some(wrap_handler(h));
        self
    }

    /// Registers a handler, overwriting any existing one for the same
    /// `job_type` instead of erroring.
    pub fn replace_handler<F, Fut>(mut self, job_type: &str, h: F) -> Self
    where
        F: Fn(Option<Payload>, Arc<JobCtx>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        self.handlers.insert(job_type.to_string(), wrap_handler(h));
        self
    }

    /// Unregisters the handler for a `job_type`, if any. Jobs of an unhandled
    /// type are nacked for retry.
    pub fn remove_handler(mut self, job_type: &str) -> Self {
        self.handlers.remove(job_type);
        self
    }

    /// Runs the reserve → process → ack/nack loop until shutdown.
    ///
    /// Consumes the worker and does not return until a [`ShutdownHandle`] is
    /// triggered *and* all in-flight jobs have finished draining. Reserve
    /// errors are logged and retried after
    /// [`with_reserve_error_backoff`](Self::with_reserve_error_backoff); they do
    /// not stop the loop. Take a [`shutdown_handle`](Self::shutdown_handle)
    /// beforehand to be able to stop it.
    pub async fn run(self) {
        let max_permits = self.max_in_flight.max(1);
        let semaphore = Arc::new(Semaphore::new(max_permits));
        let handlers = Arc::new(self.handlers);
        let auto_extend = self.auto_extend;
        let shutdown = self.shutdown.clone();
        let metrics = Arc::clone(&self.metrics);
        info!(
            worker_id = self.opts.worker_id.as_deref().unwrap_or("<none>"),
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
                let catch_all_handler = self.catch_all_handler.clone();
                let metrics = Arc::clone(&metrics);
                let in_flight = InFlightGuard::new(Arc::clone(&metrics));
                tokio::spawn(async move {
                    let _permit = permit; // held for the job's lifetime
                    let _in_flight = in_flight;
                    process_job(
                        &client,
                        &handlers,
                        &catch_all_handler,
                        auto_extend,
                        first,
                        &metrics,
                    )
                    .await;
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
                let catch_all_handler = self.catch_all_handler.clone();
                let metrics = Arc::clone(&metrics);
                let in_flight = InFlightGuard::new(Arc::clone(&metrics));
                tokio::spawn(async move {
                    let _permit = permit;
                    let _in_flight = in_flight;
                    process_job(
                        &client,
                        &handlers,
                        &catch_all_handler,
                        auto_extend,
                        job,
                        &metrics,
                    )
                    .await;
                });
            }
        }

        info!("worker shutting down; waiting for in-flight jobs to finish");
        let _drain = semaphore
            .acquire_many(max_permits as u32)
            .await
            .expect("semaphore is never closed");
        info!("worker stopped");
    }
}

async fn process_job(
    client: &SeppClient,
    handlers: &HashMap<String, Handler>,
    catch_all_handler: &Option<Handler>,
    auto_extend: Option<AutoExtend>,
    job: Job,
    metrics: &Metrics,
) {
    let span = tracing::info_span!(
        "sepp-rs.process",
        otel.kind = "consumer",
        otel.status_code = tracing::field::Empty,
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

    run_job(
        client,
        handlers,
        catch_all_handler,
        auto_extend,
        job,
        metrics,
    )
    .instrument(span)
    .await
}

async fn run_job(
    client: &SeppClient,
    handlers: &HashMap<String, Handler>,
    catch_all_handler: &Option<Handler>,
    auto_extend: Option<AutoExtend>,
    job: Job,
    metrics: &Metrics,
) {
    let Job { payload, ctx } = job;
    let lease = ctx.lease.clone();
    let ctx = Arc::new(ctx);

    let Some(handler) = handlers.get(&ctx.job_type).or(catch_all_handler.as_ref()) else {
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
        error!(
            "failed to ack/nack job: {err}; either the lease was lost and the job will be redelivered, or a retried attempt already succeeded and only its response was lost"
        );
    }
}

enum Disposition {
    Completed(Result<(), HandlerError>),
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
            tracing::Span::current().record("otel.status_code", "error");
            warn!("handler returned error; nacking: {err}");
            let (retry, reason) = match err {
                HandlerError::Retry(r) => (RetryDirective::Default, r),
                HandlerError::RetryAfter(r, d) => (RetryDirective::After(d), r),
                HandlerError::Permanent(r) => (RetryDirective::DeadLetter, r),
            };
            let dead_lettered = client.nack(ctx, retry, reason).await?;
            metrics.record_nacked(dead_lettered);
            Ok(())
        }
        Disposition::Panicked => {
            tracing::Span::current().record("otel.status_code", "error");
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
        // Derive from the granted lease; see AutoExtend::explicit_interval.
        let interval = cfg.explicit_interval.unwrap_or_else(|| {
            let remaining_ms = lease.known_expiry_ms().saturating_sub(now_millis()).max(0) as u64;
            heartbeat_interval(Duration::from_millis(remaining_ms))
        });
        tokio::time::sleep(interval).await;

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
        let e = HandlerError::retry("network");
        assert!(matches!(e, HandlerError::Retry(s) if s == "network"));
    }

    #[test]
    fn worker_err_retry_after() {
        let e = HandlerError::retry_after("rate limited", Duration::from_secs(5));
        assert!(matches!(
            e,
            HandlerError::RetryAfter(s, d) if s == "rate limited" && d == Duration::from_secs(5)
        ));
    }

    #[test]
    fn worker_err_permanent() {
        let e = HandlerError::permanent("bad input");
        assert!(matches!(e, HandlerError::Permanent(s) if s == "bad input"));
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
