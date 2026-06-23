//! The gRPC client: connecting, enqueuing, reserving, and lease management.
//!
//! [`SeppClient`] is a cheaply-cloneable handle to a Sepp server. Build one with
//! [`SeppClient::connect`] for the common case, or [`SeppClient::builder`] to
//! configure authentication, TLS, timeouts, and an RPC [`RetryPolicy`]. All RPC methods
//! take `&self`, so a single client can be shared across tasks by cloning.
//!
//! For consuming jobs you can call [`reserve`](SeppClient::reserve),
//! [`ack`](SeppClient::ack), [`nack`](SeppClient::nack), and
//! [`extend`](SeppClient::extend) directly, or hand the client to a
//! [`Worker`](crate::worker::Worker) and let it drive that loop.

use crate::{
    DeadLetterRecord, EnqueueAck, JobRejection, ServerInfo, ServerInfoError, pb::sepp::v1 as pb,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, SystemTime},
};

#[cfg(feature = "tls")]
use tonic::transport::{Certificate, ClientTlsConfig};
use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataValue},
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Channel, Endpoint},
};
use tracing::{debug, error, info, warn};

use crate::{
    EnqueueRequest, Job, JobConversionError, JobCtx, ReserveOptions,
    pb::sepp::v1::queue_service_client::QueueServiceClient,
};

const RESERVE_DEADLINE_SLACK: Duration = Duration::from_secs(10);
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(30);

type AuthChannel = InterceptedService<Channel, ApiKeyInterceptor>;

/// A handle to a Sepp server.
///
/// Cloning is cheap — clones share the same underlying connection and retry
/// policy — so clone freely to use the client across tasks. Every RPC method
/// takes `&self`.
#[derive(Clone)]
pub struct SeppClient {
    inner: QueueServiceClient<AuthChannel>,
    retry_policy: Arc<RetryPolicy>,
    rpc_timeout: Duration,
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SeppClient>();
};

/// The general client error type, shared by most RPCs.
///
/// gRPC status codes are mapped onto these variants: `Unavailable` /
/// `DeadlineExceeded` / `Aborted` / `Cancelled` become [`Transport`](Self::Transport),
/// `ResourceExhausted` becomes [`Overloaded`](Self::Overloaded), and so on. The
/// transport-ish variants are the ones the [`RetryPolicy`] retries.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// Establishing the connection failed.
    #[error("could not connect to Sepp server at {addr}: {reason}")]
    Connect { addr: String, reason: String },
    /// The configured API key could not be encoded as an HTTP header value.
    #[error("the API key is not a valid HTTP header value")]
    InvalidApiKey,
    /// A transient transport-level failure (connection dropped, deadline
    /// exceeded, request aborted/cancelled). Generally safe to retry.
    #[error("transport failure: {0}")]
    Transport(String),
    /// The server rejected the credentials (missing/invalid API key, or
    /// permission denied).
    #[error("authentication failed: {0}")]
    Unauthenticated(String),
    /// The server is shedding load (`ResourceExhausted`); back off and retry.
    #[error("server is overloaded: {0}")]
    Overloaded(String),
    /// The server rejected the request as malformed (`InvalidArgument`).
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// The server hit an internal error (`Internal` / `DataLoss` / `Unknown`).
    #[error("server internal error: {0}")]
    ServerInternal(String),
    /// The server returned a status code this client does not map to a more
    /// specific variant.
    #[error("server returned unexpected status {code:?}: {message}")]
    UnexpectedStatus { code: tonic::Code, message: String },
    /// An enqueue was attempted with no jobs.
    #[error("empty batch")]
    EmptyBatch,
    /// The server returned a different number of results than jobs sent — a
    /// protocol violation.
    #[error("server returned {got} results for a batch of {expected} jobs")]
    BatchResultCountMismatch { expected: usize, got: usize },
    /// A response was missing a field the protocol requires.
    #[error("malformed response: {0}")]
    MalformedResponse(&'static str),
    /// A job in a response could not be decoded; see [`JobConversionError`].
    #[error("server returned a malformed job: {0}")]
    MalformedJob(#[from] JobConversionError),
    /// A server-info response could not be decoded; see [`ServerInfoError`].
    #[error("server returned malformed server info: {0}")]
    MalformedServerInfo(#[from] ServerInfoError),
}

impl From<tonic::Status> for ClientError {
    fn from(s: tonic::Status) -> Self {
        use tonic::Code;
        let msg = s.message().to_string();
        match s.code() {
            Code::Unavailable | Code::DeadlineExceeded | Code::Aborted | Code::Cancelled => {
                Self::Transport(msg)
            }
            Code::Unauthenticated | Code::PermissionDenied => Self::Unauthenticated(msg),
            Code::ResourceExhausted => Self::Overloaded(msg),
            Code::InvalidArgument => Self::InvalidRequest(msg),
            Code::Internal | Code::DataLoss | Code::Unknown => Self::ServerInternal(msg),
            code => Self::UnexpectedStatus { code, message: msg },
        }
    }
}

/// The error type of [`SeppClient::enqueue`] (the single-job convenience
/// wrapper).
///
/// Separates a deterministic per-job [`JobRejection`] from a
/// connection/protocol-level [`ClientError`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EnqueueError {
    /// The server accepted the request but rejected this specific job.
    #[error("server rejected the job: {0}")]
    Rejected(JobRejection),
    /// The call failed before a per-job verdict was reached.
    #[error(transparent)]
    Client(#[from] ClientError),
}

impl From<tonic::Status> for EnqueueError {
    fn from(s: tonic::Status) -> Self {
        Self::Client(s.into())
    }
}

/// The error type of the lease operations [`ack`](SeppClient::ack),
/// [`nack`](SeppClient::nack), and [`extend`](SeppClient::extend).
///
/// [`JobNotFound`](Self::JobNotFound) and [`AttemptMismatch`](Self::AttemptMismatch)
/// both mean the worker no longer holds the lease — typically because it was
/// allowed to expire and the job was redelivered. In that case any work the
/// handler did may be processed again by another worker. With a retrying
/// [`RetryPolicy`], `JobNotFound` can also mean an earlier attempt of this
/// same call succeeded and only its response was lost.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LeaseError {
    /// No in-flight job has this id: it was already acked, the lease expired,
    /// or it never existed.
    #[error("no in-flight job with this id (already acked, expired, or never existed)")]
    JobNotFound,
    /// The attempt number no longer matches the server's: the lease was
    /// reassigned to another delivery.
    #[error("attempt mismatch: the lease was reassigned")]
    AttemptMismatch,
    /// A transport- or protocol-level failure.
    #[error(transparent)]
    Client(#[from] ClientError),
}

impl From<tonic::Status> for LeaseError {
    fn from(s: tonic::Status) -> Self {
        use tonic::Code;
        match s.code() {
            Code::NotFound => Self::JobNotFound,
            Code::FailedPrecondition => Self::AttemptMismatch,
            _ => Self::Client(s.into()),
        }
    }
}

/// The error type of [`SeppClient::reserve`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReserveError {
    /// The server is in strict mode and one or more requested queues are not
    /// declared; the message lists them.
    #[error("requested queues are not declared on the server: {0}")]
    UnknownQueues(String),
    /// A transport- or protocol-level failure.
    #[error(transparent)]
    Client(#[from] ClientError),
}

impl From<tonic::Status> for ReserveError {
    fn from(s: tonic::Status) -> Self {
        use tonic::Code;
        match s.code() {
            Code::FailedPrecondition => Self::UnknownQueues(s.message().to_string()),
            _ => Self::Client(s.into()),
        }
    }
}

/// How the server should handle a [`nack`](SeppClient::nack)ed job's next
/// delivery.
///
/// This is *job-level* retry (the handler failed), distinct from the
/// connection-level [`RetryPolicy`] (the RPC failed).
#[derive(Debug, Clone)]
pub enum RetryDirective {
    /// Apply the queue's configured retry policy (backoff, max attempts).
    Default,
    /// Retry, but not before the given delay has elapsed.
    After(Duration),
    /// Do not retry; send the job straight to the dead-letter queue.
    DeadLetter,
}

/// Backoff policy for retrying *transient* RPC failures (those mapped to
/// [`ClientError::Transport`] / [`Overloaded`](ClientError::Overloaded)).
///
/// Applies to enqueue, ack, nack, extend, and get-server-info — but not to
/// [`reserve`](SeppClient::reserve), which is a long poll. The default policy
/// performs **no** retries (`max_attempts == 1`); opt in by building one up and
/// passing it to [`SeppClientBuilder::retry_policy`].
///
/// ```
/// use std::time::Duration;
/// use sepp_rs::client::RetryPolicy;
///
/// let policy = RetryPolicy::default()
///     .with_max_attempts(5)
///     .with_initial_backoff(Duration::from_millis(50))
///     .with_max_backoff(Duration::from_secs(5));
/// ```
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    max_attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    multiplier: f64,
    jitter: bool,
}

impl RetryPolicy {
    /// Sets the total number of attempts (including the first). Values below 1
    /// are clamped to 1, so `1` means "no retries".
    pub fn with_max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }

    /// Sets the backoff before the first retry. Each subsequent retry multiplies
    /// this by [`with_multiplier`](Self::with_multiplier), capped at
    /// [`with_max_backoff`](Self::with_max_backoff).
    pub fn with_initial_backoff(mut self, d: Duration) -> Self {
        self.initial_backoff = d;
        self
    }

    /// Caps the backoff between retries.
    pub fn with_max_backoff(mut self, d: Duration) -> Self {
        self.max_backoff = d;
        self
    }

    /// Sets the exponential growth factor for the backoff. Values below 1.0 are
    /// clamped to 1.0 (constant backoff).
    pub fn with_multiplier(mut self, m: f64) -> Self {
        self.multiplier = m.max(1.0);
        self
    }

    /// Disables jitter. By default each delay is randomized within
    /// `[0.5, 1.0)` of its computed value to avoid thundering herds.
    pub fn without_jitter(mut self) -> Self {
        self.jitter = false;
        self
    }

    /// Returns the configured number of attempts.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

impl SeppClient {
    /// Connects to a Sepp server over plaintext with no authentication.
    ///
    /// `addr` is a URI such as `http://127.0.0.1:50051`. For API-key auth, TLS,
    /// or a custom [`RetryPolicy`], use [`builder`](Self::builder) instead.
    pub async fn connect(addr: impl Into<String>) -> Result<Self, ClientError> {
        Self::builder(addr).connect().await
    }

    /// Starts building a client for `addr`, allowing authentication, TLS, and
    /// retry configuration before [`connect`](SeppClientBuilder::connect).
    pub fn builder(addr: impl Into<String>) -> SeppClientBuilder {
        SeppClientBuilder::new(addr)
    }

    /// Wraps an already-established tonic [`Channel`], with no authentication
    /// and the default [`RetryPolicy`].
    ///
    /// Use this to share a channel or apply custom tonic transport
    /// configuration the builder does not expose.
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: QueueServiceClient::with_interceptor(channel, ApiKeyInterceptor::disabled()),
            retry_policy: Arc::new(RetryPolicy::default()),
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
        }
    }

    /// Builds a unary request with the client's RPC deadline and trace metadata
    /// applied. Reserve builds its own request: its deadline follows the wait
    /// timeout instead.
    fn unary_request<T>(&self, body: T) -> Request<T> {
        let mut request = Request::new(body);
        request.set_timeout(self.rpc_timeout);
        inject_metadata(&mut request);
        request
    }

    #[tracing::instrument(
        name = "sepp-rs.enqueue",
        skip_all,
        fields(otel.kind = "client", otel.status_code = tracing::field::Empty, jobs)
    )]
    /// Enqueues a batch of jobs on a best-effort basis.
    ///
    /// Each job is accepted or rejected independently: the returned vector has
    /// one entry per submitted job, in the same order, where the inner `Result`
    /// is `Ok` for an accepted job or `Err` for a per-job [`JobRejection`]. The
    /// outer `Err` is reserved for whole-call failures (empty batch, transport
    /// error, protocol violation). Transient failures are retried per the
    /// client's [`RetryPolicy`].
    ///
    /// For all-or-nothing semantics, use [`enqueue_atomic`](Self::enqueue_atomic).
    pub async fn enqueue_batch(
        &self,
        jobs: impl IntoIterator<Item = EnqueueRequest>,
    ) -> Result<Vec<Result<EnqueueAck, JobRejection>>, ClientError> {
        let jobs: Vec<pb::EnqueueRequest> = jobs.into_iter().map(Into::into).collect();
        if jobs.is_empty() {
            return Err(ClientError::EmptyBatch);
        }
        let sent = jobs.len();
        tracing::Span::current().record("jobs", sent);

        let response = with_retry(&self.retry_policy, "enqueue_batch", || {
            let mut request = self.unary_request(pb::EnqueueBatchRequest { jobs: jobs.clone() });
            inject_trace_context(&mut request);
            let mut inner = self.inner.clone();
            async move { inner.enqueue_batch(request).await.map(|r| r.into_inner()) }
        })
        .await?;

        if response.results.len() != sent {
            return Err(ClientError::BatchResultCountMismatch {
                expected: sent,
                got: response.results.len(),
            });
        }

        let mut results = Vec::with_capacity(sent);
        for job_req in response.results {
            results.push(match job_req.outcome {
                Some(pb::job_result::Outcome::Success(r)) => {
                    debug!(job_id = %r.job_id, deduplicated = r.deduplicated, "job enqueued successfully");
                    Ok(r.into())
                }
                Some(pb::job_result::Outcome::Rejection(r)) => {
                    let rejection: JobRejection = r.into();
                    debug!(error = %rejection, "server rejected the job");
                    Err(rejection)
                }
                None => {
                    return Err(ClientError::MalformedResponse(
                        "missing outcome in job result",
                    ));
                }
            });
        }

        Ok(results)
    }

    /// Enqueues a single job.
    ///
    /// A convenience wrapper over [`enqueue_batch`](Self::enqueue_batch) that
    /// flattens the result: a per-job rejection becomes
    /// [`EnqueueError::Rejected`].
    pub async fn enqueue(&self, job: EnqueueRequest) -> Result<EnqueueAck, EnqueueError> {
        let mut results = self.enqueue_batch(std::iter::once(job)).await?.into_iter();

        match results.next() {
            Some(Ok(ack)) => Ok(ack),
            Some(Err(rej)) => Err(EnqueueError::Rejected(rej)),
            None => Err(EnqueueError::Client(ClientError::MalformedResponse(
                "empty results for single-job batch",
            ))),
        }
    }

    #[tracing::instrument(
        name = "sepp-rs.enqueue_atomic",
        skip_all,
        fields(otel.kind = "client", otel.status_code = tracing::field::Empty, jobs)
    )]
    /// Enqueues a batch of jobs atomically: either all are accepted or none are.
    ///
    /// On success, returns one [`EnqueueAck`] per job, in order. If any job
    /// fails validation, nothing is enqueued and every failure is returned
    /// together as [`AtomicEnqueueError::Validation`](crate::AtomicEnqueueError::Validation).
    /// Use this when the jobs are coordinated steps and a partial enqueue would
    /// leave the system inconsistent.
    pub async fn enqueue_atomic(
        &self,
        jobs: impl IntoIterator<Item = EnqueueRequest>,
    ) -> Result<Vec<EnqueueAck>, crate::AtomicEnqueueError> {
        let jobs: Vec<pb::EnqueueRequest> = jobs.into_iter().map(Into::into).collect();
        if jobs.is_empty() {
            return Err(ClientError::EmptyBatch.into());
        }
        let sent = jobs.len();
        tracing::Span::current().record("jobs", sent);

        let response = with_retry(&self.retry_policy, "enqueue_atomic", || {
            let mut request = self.unary_request(pb::EnqueueBatchRequest { jobs: jobs.clone() });
            inject_trace_context(&mut request);
            let mut inner = self.inner.clone();
            async move { inner.enqueue_atomic(request).await.map(|r| r.into_inner()) }
        })
        .await?;

        use pb::enqueue_atomic_response::Outcome;
        match response.outcome {
            Some(Outcome::Success(s)) => {
                if s.responses.len() != sent {
                    return Err(ClientError::BatchResultCountMismatch {
                        expected: sent,
                        got: s.responses.len(),
                    }
                    .into());
                }
                Ok(s.responses.into_iter().map(Into::into).collect())
            }
            Some(Outcome::Rejection(r)) => {
                let errors: Vec<crate::JobValidationError> =
                    r.errors.into_iter().map(Into::into).collect();
                for e in &errors {
                    debug!(index = e.index, error = %e.rejection, "atomic batch rejected job");
                }
                Err(crate::AtomicEnqueueError::Validation(errors))
            }
            None => Err(
                ClientError::MalformedResponse("missing outcome in EnqueueAtomicResponse").into(),
            ),
        }
    }

    #[tracing::instrument(
        name = "sepp-rs.reserve",
        skip_all,
        fields(
            otel.kind = "client",
            otel.status_code = tracing::field::Empty,
            jobs,
            worker_id = opts.worker_id.as_deref().unwrap_or("<none>"),
        )
    )]
    /// Long-polls for jobs to process.
    ///
    /// Blocks up to the options' [`wait_timeout`](ReserveOptions::wait_timeout)
    /// for at least one job. Returns `Ok(Some(jobs))` with one or more leased
    /// [`Job`]s, or `Ok(None)` if the wait elapsed with nothing available (poll
    /// again). Each returned job must be [`ack`](Self::ack)ed,
    /// [`nack`](Self::nack)ed, or [`extend`](Self::extend)ed before its lease
    /// expires.
    ///
    /// Unlike the other RPCs, reserve is **not** retried by the
    /// [`RetryPolicy`]: as a long poll, an empty return is the normal idle
    /// outcome and the caller loops anyway. A malformed job in the response is
    /// logged and skipped rather than failing the whole batch.
    pub async fn reserve(&self, opts: &ReserveOptions) -> Result<Option<Vec<Job>>, ReserveError> {
        let msg = pb::ReserveRequest::from(opts);
        let mut request = Request::new(msg);
        request.set_timeout(opts.wait_timeout() + RESERVE_DEADLINE_SLACK);
        inject_metadata(&mut request);

        let response = match self.inner.clone().reserve(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                tracing::Span::current().record("otel.status_code", "error");
                return Err(status.into());
            }
        };

        // A single malformed job must not discard the rest of the batch
        let mut jobs = Vec::with_capacity(response.jobs.len());
        for job in response.jobs {
            match crate::job_from_pb(self, job, opts.worker_id.as_deref()) {
                Ok(job) => jobs.push(job),
                Err(e) => warn!(error = %e, "skipping malformed job in reserve response"),
            }
        }

        if jobs.is_empty() {
            tracing::Span::current().record("jobs", 0);
            return Ok(None);
        }

        tracing::Span::current().record("jobs", jobs.len());
        Ok(Some(jobs))
    }

    #[tracing::instrument(
        name = "sepp-rs.ack",
        skip_all,
        fields(
            otel.kind = "client",
            otel.status_code = tracing::field::Empty,
            job_id = %ctx.id,
            attempt = ctx.attempt,
            worker_id = ctx.lease.worker_id.as_deref().unwrap_or("<none>"),
        )
    )]
    /// Acknowledges that a job completed successfully, removing it from the
    /// queue.
    ///
    /// The `attempt` carried by `ctx` guards against acking a job whose lease
    /// was already reassigned — that surfaces as
    /// [`LeaseError::AttemptMismatch`] or [`LeaseError::JobNotFound`].
    pub async fn ack(&self, ctx: &JobCtx) -> Result<(), LeaseError> {
        let body = pb::AckRequest {
            job_id: ctx.id.clone(),
            attempt: ctx.attempt,
            worker_id: ctx.lease.worker_id.clone(),
        };
        with_retry(&self.retry_policy, "ack", || {
            let request = self.unary_request(body.clone());
            let mut inner = self.inner.clone();
            async move { inner.ack(request).await.map(|_| ()) }
        })
        .await?;
        Ok(())
    }

    #[tracing::instrument(
        name = "sepp-rs.nack",
        skip_all,
        fields(
            otel.kind = "client",
            otel.status_code = tracing::field::Empty,
            job_id = %ctx.id,
            attempt = ctx.attempt,
            worker_id = ctx.lease.worker_id.as_deref().unwrap_or("<none>"),
        )
    )]
    /// Negatively acknowledges a job, signalling that processing failed.
    ///
    /// `retry` selects what the server does next (see [`RetryDirective`]) and
    /// `reason` is recorded for debugging and metrics. Returns `true` if this
    /// nack moved the job to the dead-letter queue (because `DeadLetter` was
    /// requested or `max_attempts` was reached), `false` if it will be retried.
    pub async fn nack(
        &self,
        ctx: &JobCtx,
        retry: RetryDirective,
        reason: impl Into<String>,
    ) -> Result<bool, LeaseError> {
        let strategy = match retry {
            RetryDirective::Default => pb::nack_retry::Strategy::Default(()),
            RetryDirective::After(d) => {
                pb::nack_retry::Strategy::Delay(crate::duration_to_proto(d))
            }
            RetryDirective::DeadLetter => pb::nack_retry::Strategy::DeadLetter(()),
        };
        let body = pb::NackRequest {
            job_id: ctx.id.clone(),
            attempt: ctx.attempt,
            reason: Some(reason.into()),
            retry: Some(pb::NackRetry {
                strategy: Some(strategy),
            }),
            worker_id: ctx.lease.worker_id.clone(),
        };

        let response = with_retry(&self.retry_policy, "nack", || {
            let request = self.unary_request(body.clone());
            let mut inner = self.inner.clone();
            async move { inner.nack(request).await.map(|r| r.into_inner()) }
        })
        .await?;
        Ok(response.dead_lettered)
    }

    /// Extends a job's lease by `extension`, measured from now, returning the
    /// new expiry.
    ///
    /// Call this when a handler needs longer than the original lease. Equivalent
    /// to [`JobCtx::extend`]; a [`Worker`](crate::worker::Worker) with
    /// [`with_auto_extend`](crate::worker::Worker::with_auto_extend) does it
    /// automatically.
    pub async fn extend(
        &self,
        ctx: &JobCtx,
        extension: Duration,
    ) -> Result<SystemTime, LeaseError> {
        self.extend_inner(
            &ctx.id,
            ctx.attempt,
            extension,
            ctx.lease.worker_id.as_deref(),
        )
        .await
    }

    #[tracing::instrument(
        name = "sepp-rs.extend",
        skip_all,
        fields(
            otel.kind = "client",
            otel.status_code = tracing::field::Empty,
            job_id = %job_id,
            attempt,
            worker_id = worker_id.unwrap_or("<none>"),
        )
    )]
    pub(crate) async fn extend_inner(
        &self,
        job_id: &str,
        attempt: u32,
        extension: Duration,
        worker_id: Option<&str>,
    ) -> Result<SystemTime, LeaseError> {
        let body = pb::ExtendRequest {
            job_id: job_id.to_string(),
            attempt,
            lease_duration: Some(crate::duration_to_proto(extension)),
            worker_id: worker_id.map(String::from),
        };

        let response = with_retry(&self.retry_policy, "extend", || {
            let request = self.unary_request(body.clone());
            let mut inner = self.inner.clone();
            async move { inner.extend(request).await.map(|r| r.into_inner()) }
        })
        .await?;
        crate::timestamp_to_system_time(response.lease_expires_at).ok_or_else(|| {
            ClientError::MalformedResponse("extend returned an invalid lease_expires_at").into()
        })
    }

    #[tracing::instrument(
        name = "sepp-rs.get_server_info",
        skip_all,
        fields(otel.kind = "client", otel.status_code = tracing::field::Empty)
    )]
    /// Fetches the server's [`ServerInfo`]: version, capabilities, and limits.
    ///
    /// Useful once at startup so a producer can validate jobs locally against
    /// the advertised limits and avoid round-trips that would only be rejected.
    pub async fn get_server_info(&self) -> Result<ServerInfo, ClientError> {
        let response = with_retry(&self.retry_policy, "get_server_info", || {
            let request = self.unary_request(pb::GetServerInfoRequest {});
            let mut inner = self.inner.clone();
            async move { inner.get_server_info(request).await.map(|r| r.into_inner()) }
        })
        .await?;

        Ok(ServerInfo::try_from(response)?)
    }

    #[tracing::instrument(
        name = "sepp-rs.drain_dead_letters",
        skip_all,
        fields(
            otel.kind = "client",
            otel.status_code = tracing::field::Empty,
            queue = queue.unwrap_or("<all>"),
            drained = tracing::field::Empty,
        )
    )]
    /// Drains dead-lettered jobs for inspection and manual replay.
    ///
    /// Returns up to `max` [`DeadLetterRecord`]s (oldest-first, optionally
    /// filtered to one `queue`) and **removes them from the server**. This is
    /// destructive: the records are gone once returned, so a dropped response
    /// loses exactly that batch — for that reason it is **not** retried by the
    /// [`RetryPolicy`]. Inspect each record, then replay any you want with
    /// [`DeadLetterRecord::to_enqueue_request`].
    ///
    /// An empty result means nothing matched, which is indistinguishable from
    /// dead-letter retention being disabled — check
    /// [`ServerInfo::dead_letter_retention_enabled`](crate::ServerInfo::dead_letter_retention_enabled).
    pub async fn drain_dead_letters(
        &self,
        queue: Option<&str>,
        max: u32,
    ) -> Result<Vec<DeadLetterRecord>, ClientError> {
        let request = self.unary_request(pb::DrainDeadLettersRequest {
            queue: queue.map(String::from),
            max: Some(max.max(1)),
        });

        let response = match self.inner.clone().drain_dead_letters(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                tracing::Span::current().record("otel.status_code", "error");
                return Err(status.into());
            }
        };

        let mut records = Vec::with_capacity(response.records.len());
        for record in response.records {
            match crate::dead_letter_record_from_pb(record) {
                Ok(r) => records.push(r),
                Err(e) => {
                    warn!(error = %e, "skipping malformed dead-letter record in drain response")
                }
            }
        }

        tracing::Span::current().record("drained", records.len());
        Ok(records)
    }
}

#[derive(Clone)]
pub(crate) struct Lease {
    client: SeppClient,
    job_id: String,
    attempt: u32,
    expiry: Arc<AtomicI64>,
    worker_id: Option<String>,
}

impl Lease {
    pub(crate) fn new(
        client: SeppClient,
        job_id: String,
        attempt: u32,
        lease_expires_at: SystemTime,
        worker_id: Option<String>,
    ) -> Self {
        Self {
            client,
            job_id,
            attempt,
            expiry: Arc::new(AtomicI64::new(crate::system_time_to_millis(
                lease_expires_at,
            ))),
            worker_id,
        }
    }

    pub(crate) fn known_expiry_ms(&self) -> i64 {
        self.expiry.load(Ordering::Acquire)
    }

    pub(crate) async fn extend(&self, by: Duration) -> Result<SystemTime, LeaseError> {
        let new_expiry = self
            .client
            .extend_inner(&self.job_id, self.attempt, by, self.worker_id.as_deref())
            .await?;
        self.expiry
            .store(crate::system_time_to_millis(new_expiry), Ordering::Release);
        Ok(new_expiry)
    }
}

impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lease")
            .field("job_id", &self.job_id)
            .field("attempt", &self.attempt)
            .field("known_expiry_ms", &self.known_expiry_ms())
            .finish()
    }
}

/// Configures and connects a [`SeppClient`].
///
/// Created by [`SeppClient::builder`]. Set an [`api_key`](Self::api_key), a
/// [`retry_policy`](Self::retry_policy), and (with the `tls` feature) TLS
/// options, then call [`connect`](Self::connect).
///
/// ```no_run
/// use sepp_rs::client::{RetryPolicy, SeppClient};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let client = SeppClient::builder("http://127.0.0.1:50051")
///     .api_key("secret")
///     .retry_policy(RetryPolicy::default().with_max_attempts(3))
///     .connect()
///     .await?;
/// # let _ = client;
/// # Ok(())
/// # }
/// ```
pub struct SeppClientBuilder {
    addr: String,
    api_key: Option<String>,
    retry_policy: RetryPolicy,
    rpc_timeout: Duration,
    max_receive_message_bytes: Option<usize>,
    #[cfg(feature = "tls")]
    tls: Option<ClientTlsConfig>,
}

impl SeppClientBuilder {
    fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            api_key: None,
            retry_policy: RetryPolicy::default(),
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
            max_receive_message_bytes: None,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Sends an `Authorization: Bearer <key>` header on every request.
    ///
    /// Without TLS the key travels in plaintext, so [`connect`](Self::connect)
    /// logs a warning if you set a key but no TLS.
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    /// Sets the [`RetryPolicy`] for transient RPC failures. The default policy
    /// does not retry.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Sets the per-call deadline for every unary RPC except
    /// [`reserve`](SeppClient::reserve), whose deadline follows the requested
    /// wait timeout instead. Defaults to 30 seconds.
    ///
    /// Enqueuing very large batches may need a higher value.
    pub fn rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = timeout;
        self
    }

    /// Sets the largest gRPC message this client accepts, replacing tonic's
    /// 4 MiB default.
    ///
    /// Workers should consider raising this: a reserve response can carry up
    /// to the server's `max_reserve_batch` × `max_payload_bytes`, which can
    /// exceed the 4 MiB default, and an oversized response fails client-side
    /// while its jobs stay leased until they expire.
    pub fn max_receive_message_bytes(mut self, bytes: usize) -> Self {
        self.max_receive_message_bytes = Some(bytes);
        self
    }

    /// Enables TLS using the platform's native root certificates.
    ///
    /// *Requires the `tls` feature.*
    #[cfg(feature = "tls")]
    pub fn tls(mut self) -> Self {
        self.tls = Some(self.tls.unwrap_or_default().with_native_roots());
        self
    }

    /// Enables TLS and trusts the given PEM-encoded CA certificate, e.g. for a
    /// private/self-signed server.
    ///
    /// *Requires the `tls` feature.*
    #[cfg(feature = "tls")]
    pub fn tls_ca_certificate(mut self, pem: impl AsRef<[u8]>) -> Self {
        let config = self.tls.unwrap_or_default();
        self.tls = Some(config.ca_certificate(Certificate::from_pem(pem)));
        self
    }

    /// Overrides the domain name verified against the server certificate, for
    /// when the connection address differs from the certificate's name.
    ///
    /// *Requires the `tls` feature.*
    #[cfg(feature = "tls")]
    pub fn tls_domain(mut self, domain: impl Into<String>) -> Self {
        let config = self.tls.unwrap_or_default();
        self.tls = Some(config.domain_name(domain));
        self
    }

    /// Sets a fully custom tonic [`ClientTlsConfig`], replacing any TLS options
    /// set by the other `tls_*` methods.
    ///
    /// *Requires the `tls` feature.*
    #[cfg(feature = "tls")]
    pub fn tls_config(mut self, config: ClientTlsConfig) -> Self {
        self.tls = Some(config);
        self
    }

    /// Connects to the server with the configured options, yielding a ready
    /// [`SeppClient`].
    pub async fn connect(self) -> Result<SeppClient, ClientError> {
        let addr = self.addr;
        let interceptor =
            ApiKeyInterceptor::new(self.api_key.as_deref()).ok_or(ClientError::InvalidApiKey)?;

        #[cfg(feature = "tls")]
        let tls = self.tls;
        #[cfg(feature = "tls")]
        let tls_enabled = tls.is_some();
        #[cfg(not(feature = "tls"))]
        let tls_enabled = false;

        if interceptor.is_enabled() && !tls_enabled {
            warn!(
                "API key configured without TLS; it will be sent over the connection in plaintext"
            );
        }

        let channel = async {
            #[allow(unused_mut)]
            let mut endpoint = Endpoint::from_shared(addr.clone())?
                .connect_timeout(Duration::from_secs(5))
                .user_agent(concat!("sepp-rs/", env!("CARGO_PKG_VERSION")))? // So we can tell from the server POV which client this is
                .http2_keep_alive_interval(Duration::from_secs(30)) // Long polling
                .keep_alive_timeout(Duration::from_secs(10)) // Long polling
                .keep_alive_while_idle(true); // For streaming reserve
            #[cfg(feature = "tls")]
            if let Some(tls) = tls {
                endpoint = endpoint.tls_config(tls)?;
            }
            endpoint.connect().await
        }
        .await
        .map_err(|e| {
            error!(%addr, error = %e, "failed to connect to Sepp server");

            ClientError::Connect {
                addr: addr.clone(),
                reason: root_cause(&e),
            }
        })?;

        info!(
            %addr,
            tls = tls_enabled,
            auth = interceptor.is_enabled(),
            "connected to Sepp server",
        );

        let mut inner = QueueServiceClient::with_interceptor(channel, interceptor);
        if let Some(bytes) = self.max_receive_message_bytes {
            inner = inner.max_decoding_message_size(bytes);
        }

        Ok(SeppClient {
            inner,
            retry_policy: Arc::new(self.retry_policy),
            rpc_timeout: self.rpc_timeout,
        })
    }
}

/// A tonic interceptor that attaches the configured API key as an
/// `Authorization: Bearer <key>` header on each request.
///
/// Installed by [`SeppClientBuilder::api_key`]; not constructed directly.
#[derive(Clone)]
pub struct ApiKeyInterceptor {
    // Pre-rendered `Bearer <key>` header value; None disables the interceptor.
    bearer: Option<MetadataValue<Ascii>>,
}

impl ApiKeyInterceptor {
    /// Returns `None` if the key cannot form a valid HTTP header value.
    fn new(api_key: Option<&str>) -> Option<Self> {
        let bearer = match api_key {
            Some(key) => Some(format!("Bearer {key}").parse().ok()?),
            None => None,
        };
        Some(Self { bearer })
    }

    fn disabled() -> Self {
        Self { bearer: None }
    }

    fn is_enabled(&self) -> bool {
        self.bearer.is_some()
    }
}

impl Interceptor for ApiKeyInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(bearer) = &self.bearer {
            request
                .metadata_mut()
                .insert("authorization", bearer.clone());
        }
        Ok(request)
    }
}

fn root_cause(err: &(dyn std::error::Error + 'static)) -> String {
    let mut current = err;
    while let Some(source) = current.source() {
        current = source;
    }
    current.to_string()
}

/// Returns whether a gRPC status is worth retrying.
fn is_transient(status: &Status) -> bool {
    use tonic::Code;
    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::Aborted | Code::ResourceExhausted
    )
}

/// Equal-jitter factor: returns a value in `[0.5, 1.0)`.
fn jitter_factor() -> f64 {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let mut x = nanos as u64;
    x ^= x >> 17;
    x = x.wrapping_mul(0xed5ad4bb);
    x ^= x >> 11;
    0.5 + 0.5 * ((x & 0xffff) as f64 / 65536.0)
}

/// Run `f` repeatedly until it succeeds, runs out of attempts, or hits a
/// non-transient error. Sleeps between attempts according to `policy`.
async fn with_retry<F, Fut, T>(
    policy: &RetryPolicy,
    operation: &'static str,
    mut f: F,
) -> Result<T, Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    let mut attempt: u32 = 1;
    let mut backoff = policy.initial_backoff;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(status) if attempt >= policy.max_attempts || !is_transient(&status) => {
                tracing::Span::current().record("otel.status_code", "error");
                return Err(status);
            }
            Err(status) => {
                let delay = if policy.jitter {
                    Duration::from_secs_f64(backoff.as_secs_f64() * jitter_factor())
                } else {
                    backoff
                };
                warn!(
                    operation,
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    code = ?status.code(),
                    message = %status.message(),
                    "retrying after transient error"
                );
                tokio::time::sleep(delay).await;
                backoff = Duration::from_secs_f64(
                    (backoff.as_secs_f64() * policy.multiplier)
                        .min(policy.max_backoff.as_secs_f64()),
                );
                attempt += 1;
            }
        }
    }
}

fn inject_metadata<T>(request: &mut Request<T>) {
    #[cfg(feature = "opentelemetry")]
    {
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let cx = tracing::Span::current().context();
        if let Some(tc) = crate::inject_pb_trace_context(&cx) {
            if let Ok(value) = tc.traceparent.parse() {
                request.metadata_mut().insert("traceparent", value);
            }
            if let Some(value) = tc.tracestate.as_deref().and_then(|s| s.parse().ok()) {
                request.metadata_mut().insert("tracestate", value);
            }
        }
    }
    #[cfg(not(feature = "opentelemetry"))]
    let _ = request;
}

fn inject_trace_context(request: &mut Request<pb::EnqueueBatchRequest>) {
    #[cfg(feature = "opentelemetry")]
    {
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let cx = tracing::Span::current().context();
        if let Some(tc) = crate::inject_pb_trace_context(&cx) {
            for job in &mut request.get_mut().jobs {
                job.trace_context.get_or_insert_with(|| tc.clone());
            }
        }
    }
    #[cfg(not(feature = "opentelemetry"))]
    let _ = request;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    fn st(code: Code, msg: &str) -> tonic::Status {
        tonic::Status::new(code, msg)
    }

    #[test]
    fn client_err_unavailable_is_transport() {
        assert!(matches!(
            ClientError::from(st(Code::Unavailable, "x")),
            ClientError::Transport(_)
        ));
    }

    #[test]
    fn client_err_deadline_is_transport() {
        assert!(matches!(
            ClientError::from(st(Code::DeadlineExceeded, "x")),
            ClientError::Transport(_)
        ));
    }

    #[test]
    fn client_err_aborted_is_transport() {
        assert!(matches!(
            ClientError::from(st(Code::Aborted, "x")),
            ClientError::Transport(_)
        ));
    }

    #[test]
    fn client_err_cancelled_is_transport() {
        assert!(matches!(
            ClientError::from(st(Code::Cancelled, "x")),
            ClientError::Transport(_)
        ));
    }

    #[test]
    fn client_err_unauthenticated() {
        assert!(matches!(
            ClientError::from(st(Code::Unauthenticated, "x")),
            ClientError::Unauthenticated(_)
        ));
    }

    #[test]
    fn client_err_permission_denied_is_unauthenticated() {
        assert!(matches!(
            ClientError::from(st(Code::PermissionDenied, "x")),
            ClientError::Unauthenticated(_)
        ));
    }

    #[test]
    fn client_err_resource_exhausted_is_overloaded() {
        assert!(matches!(
            ClientError::from(st(Code::ResourceExhausted, "x")),
            ClientError::Overloaded(_)
        ));
    }

    #[test]
    fn client_err_invalid_argument_is_invalid_request() {
        assert!(matches!(
            ClientError::from(st(Code::InvalidArgument, "x")),
            ClientError::InvalidRequest(_)
        ));
    }

    #[test]
    fn client_err_internal_is_server_internal() {
        assert!(matches!(
            ClientError::from(st(Code::Internal, "x")),
            ClientError::ServerInternal(_)
        ));
    }

    #[test]
    fn client_err_data_loss_is_server_internal() {
        assert!(matches!(
            ClientError::from(st(Code::DataLoss, "x")),
            ClientError::ServerInternal(_)
        ));
    }

    #[test]
    fn client_err_unknown_is_server_internal() {
        assert!(matches!(
            ClientError::from(st(Code::Unknown, "x")),
            ClientError::ServerInternal(_)
        ));
    }

    #[test]
    fn client_err_other_is_unexpected_status() {
        match ClientError::from(st(Code::NotFound, "x")) {
            ClientError::UnexpectedStatus { code, .. } => assert_eq!(code, Code::NotFound),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_err_preserves_message() {
        match ClientError::from(st(Code::Internal, "boom")) {
            ClientError::ServerInternal(m) => assert_eq!(m, "boom"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn enqueue_err_wraps_client() {
        assert!(matches!(
            EnqueueError::from(st(Code::Unavailable, "x")),
            EnqueueError::Client(ClientError::Transport(_))
        ));
    }

    #[test]
    fn lease_err_not_found() {
        assert!(matches!(
            LeaseError::from(st(Code::NotFound, "x")),
            LeaseError::JobNotFound
        ));
    }

    #[test]
    fn lease_err_failed_precondition_is_attempt_mismatch() {
        assert!(matches!(
            LeaseError::from(st(Code::FailedPrecondition, "x")),
            LeaseError::AttemptMismatch
        ));
    }

    #[test]
    fn lease_err_other_wraps_client() {
        assert!(matches!(
            LeaseError::from(st(Code::Unavailable, "x")),
            LeaseError::Client(ClientError::Transport(_))
        ));
    }

    #[test]
    fn reserve_err_failed_precondition_is_unknown_queues() {
        match ReserveError::from(st(Code::FailedPrecondition, "queues: a, b")) {
            ReserveError::UnknownQueues(m) => assert_eq!(m, "queues: a, b"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn reserve_err_other_wraps_client() {
        assert!(matches!(
            ReserveError::from(st(Code::Unavailable, "x")),
            ReserveError::Client(ClientError::Transport(_))
        ));
    }

    #[test]
    fn atomic_enqueue_err_wraps_client() {
        assert!(matches!(
            crate::AtomicEnqueueError::from(st(Code::Unavailable, "x")),
            crate::AtomicEnqueueError::Client(ClientError::Transport(_))
        ));
    }

    #[test]
    fn api_key_interceptor_rejects_invalid_header_value() {
        // Newline in header value is not a valid HTTP header
        assert!(ApiKeyInterceptor::new(Some("bad\nkey")).is_none());
    }

    #[test]
    fn api_key_interceptor_none_disables_auth() {
        let interceptor = ApiKeyInterceptor::new(None).unwrap();
        assert!(!interceptor.is_enabled());
    }

    #[test]
    fn api_key_interceptor_some_enables_auth() {
        let interceptor = ApiKeyInterceptor::new(Some("token")).unwrap();
        assert!(interceptor.is_enabled());
    }

    #[test]
    fn api_key_interceptor_injects_bearer_header() {
        let mut interceptor = ApiKeyInterceptor::new(Some("token")).unwrap();
        let req = interceptor.call(Request::new(())).unwrap();
        let auth = req.metadata().get("authorization").unwrap();
        assert_eq!(auth.to_str().unwrap(), "Bearer token");
    }

    #[test]
    fn api_key_interceptor_disabled_leaves_metadata_empty() {
        let mut interceptor = ApiKeyInterceptor::disabled();
        let req = interceptor.call(Request::new(())).unwrap();
        assert!(req.metadata().get("authorization").is_none());
    }

    #[test]
    fn is_transient_classifies_codes() {
        assert!(is_transient(&st(Code::Unavailable, "")));
        assert!(is_transient(&st(Code::DeadlineExceeded, "")));
        assert!(is_transient(&st(Code::Aborted, "")));
        assert!(is_transient(&st(Code::ResourceExhausted, "")));

        assert!(!is_transient(&st(Code::Cancelled, "")));
        assert!(!is_transient(&st(Code::InvalidArgument, "")));
        assert!(!is_transient(&st(Code::NotFound, "")));
        assert!(!is_transient(&st(Code::FailedPrecondition, "")));
        assert!(!is_transient(&st(Code::Unauthenticated, "")));
        assert!(!is_transient(&st(Code::PermissionDenied, "")));
        assert!(!is_transient(&st(Code::Internal, "")));
        assert!(!is_transient(&st(Code::DataLoss, "")));
        assert!(!is_transient(&st(Code::Unknown, "")));
    }

    #[test]
    fn jitter_factor_in_range() {
        for _ in 0..256 {
            let f = jitter_factor();
            assert!((0.5..1.0).contains(&f), "jitter factor out of range: {f}");
        }
    }

    fn fast_policy(max_attempts: u32) -> RetryPolicy {
        RetryPolicy::default()
            .with_max_attempts(max_attempts)
            .with_initial_backoff(Duration::from_millis(1))
            .with_max_backoff(Duration::from_millis(1))
            .without_jitter()
    }

    #[tokio::test]
    async fn with_retry_returns_immediately_on_success() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = calls.clone();
        let result: Result<u32, Status> = with_retry(&fast_policy(5), "test", move || {
            let calls = calls2.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn with_retry_succeeds_after_transient_failures() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = calls.clone();
        let result: Result<u32, Status> = with_retry(&fast_policy(5), "test", move || {
            let calls = calls2.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    Err(st(Code::Unavailable, "blip"))
                } else {
                    Ok(7)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn with_retry_gives_up_after_max_attempts() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = calls.clone();
        let result: Result<(), Status> = with_retry(&fast_policy(3), "test", move || {
            let calls = calls2.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(st(Code::Unavailable, "still down"))
            }
        })
        .await;
        assert_eq!(result.unwrap_err().code(), Code::Unavailable);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn with_retry_does_not_retry_non_transient_errors() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = calls.clone();
        let result: Result<(), Status> = with_retry(&fast_policy(5), "test", move || {
            let calls = calls2.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(st(Code::InvalidArgument, "nope"))
            }
        })
        .await;
        assert_eq!(result.unwrap_err().code(), Code::InvalidArgument);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn with_retry_default_policy_runs_once() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = calls.clone();
        let result: Result<(), Status> = with_retry(&RetryPolicy::default(), "test", move || {
            let calls = calls2.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(st(Code::Unavailable, "transient"))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn retry_policy_max_attempts_clamps_to_one() {
        let p = RetryPolicy::default().with_max_attempts(0);
        assert_eq!(p.max_attempts(), 1);
    }

    #[test]
    fn retry_policy_default_is_no_retry() {
        assert_eq!(RetryPolicy::default().max_attempts(), 1);
    }

    #[test]
    fn builder_defaults_rpc_timeout_and_message_size() {
        let b = SeppClient::builder("http://localhost:1");
        assert_eq!(b.rpc_timeout, DEFAULT_RPC_TIMEOUT);
        assert!(b.max_receive_message_bytes.is_none());
    }

    #[test]
    fn builder_rpc_timeout_overrides_default() {
        let b = SeppClient::builder("http://localhost:1").rpc_timeout(Duration::from_secs(5));
        assert_eq!(b.rpc_timeout, Duration::from_secs(5));
    }

    #[test]
    fn builder_max_receive_message_bytes_set() {
        let b =
            SeppClient::builder("http://localhost:1").max_receive_message_bytes(16 * 1024 * 1024);
        assert_eq!(b.max_receive_message_bytes, Some(16 * 1024 * 1024));
    }

    #[tokio::test]
    async fn unary_request_sets_grpc_timeout() {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let client = SeppClient::from_channel(channel);
        let request = client.unary_request(());
        assert!(request.metadata().contains_key("grpc-timeout"));
    }

    #[test]
    fn root_cause_returns_error_message() {
        let err = std::io::Error::new(std::io::ErrorKind::Other, "bottom");
        assert_eq!(root_cause(&err), "bottom");
    }

    #[derive(Debug)]
    struct ChainedErr {
        msg: &'static str,
        source: Option<Box<dyn std::error::Error + 'static>>,
    }

    impl std::fmt::Display for ChainedErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.msg)
        }
    }

    impl std::error::Error for ChainedErr {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.source.as_deref()
        }
    }

    #[test]
    fn root_cause_walks_error_chain() {
        let inner = ChainedErr {
            msg: "deep cause",
            source: None,
        };
        let outer = ChainedErr {
            msg: "wrapper",
            source: Some(Box::new(inner)),
        };
        assert_eq!(root_cause(&outer), "deep cause");
    }

    #[test]
    fn retry_policy_with_max_attempts_clamps_to_one() {
        let p = RetryPolicy::default().with_max_attempts(0);
        assert_eq!(p.max_attempts(), 1);
        let p = RetryPolicy::default().with_max_attempts(1);
        assert_eq!(p.max_attempts(), 1);
        let p = RetryPolicy::default().with_max_attempts(5);
        assert_eq!(p.max_attempts(), 5);
    }

    #[test]
    fn retry_policy_without_jitter_does_not_panic() {
        let _p = RetryPolicy::default().without_jitter();
    }

    #[test]
    fn retry_policy_multiplier_methods_do_not_panic() {
        let _p = RetryPolicy::default().with_multiplier(0.5);
        let _p = RetryPolicy::default().with_multiplier(1.0);
        let _p = RetryPolicy::default().with_multiplier(2.5);
    }

    #[test]
    fn enqueue_error_rejected_displays_job_rejection() {
        let err = EnqueueError::Rejected(crate::JobRejection::Unknown);
        let msg = err.to_string();
        assert!(msg.contains("unrecognized rejection variant"));
    }

    #[test]
    fn client_error_empty_batch_display() {
        assert!(ClientError::EmptyBatch.to_string().contains("empty batch"));
    }

    #[test]
    fn client_error_batch_result_count_mismatch_display() {
        let err = ClientError::BatchResultCountMismatch {
            expected: 10,
            got: 7,
        };
        let msg = err.to_string();
        assert!(msg.contains("10"));
        assert!(msg.contains("7"));
    }

    #[test]
    fn client_error_malformed_response_display() {
        let err = ClientError::MalformedResponse("missing field id");
        assert!(err.to_string().contains("missing field id"));
    }

    #[tokio::test]
    async fn lease_new_and_known_expiry() {
        let channel = Endpoint::from_static("http://[::1]:1").connect_lazy();
        let client = SeppClient::from_channel(channel);
        let expiry = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let lease = Lease::new(client, "job-1".into(), 1, expiry, Some("worker-1".into()));
        assert_eq!(lease.known_expiry_ms(), 100_000);
    }

    #[tokio::test]
    async fn lease_known_expiry_ms_without_worker_id() {
        let channel = Endpoint::from_static("http://[::1]:1").connect_lazy();
        let client = SeppClient::from_channel(channel);
        let expiry = SystemTime::UNIX_EPOCH + Duration::from_millis(42);
        let lease = Lease::new(client, "j".into(), 3, expiry, None);
        assert_eq!(lease.known_expiry_ms(), 42);
    }

    #[tokio::test]
    async fn lease_debug_format() {
        let channel = Endpoint::from_static("http://[::1]:1").connect_lazy();
        let client = SeppClient::from_channel(channel);
        let expiry = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let lease = Lease::new(client, "job-1".into(), 1, expiry, None);
        let debug = format!("{:?}", lease);
        assert!(debug.contains("job-1"));
    }

    #[cfg(feature = "tls")]
    mod tls_tests {
        use super::*;

        #[test]
        fn builder_tls_does_not_panic() {
            let _b = SeppClient::builder("http://localhost:1").tls();
        }

        #[test]
        fn builder_tls_chain_with_api_key() {
            let _b = SeppClient::builder("http://localhost:1")
                .api_key("secret")
                .tls();
        }

        #[test]
        fn builder_tls_domain_sets_domain() {
            let _b = SeppClient::builder("http://localhost:1").tls_domain("example.com");
        }

        #[test]
        fn builder_tls_ca_certificate_accepts_pem() {
            let pem = b"-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----";
            let _b = SeppClient::builder("http://localhost:1").tls_ca_certificate(pem.as_ref());
        }

        #[test]
        fn builder_tls_config_accepts_default() {
            let config = tonic::transport::ClientTlsConfig::default();
            let _b = SeppClient::builder("http://localhost:1").tls_config(config);
        }
    }
}
