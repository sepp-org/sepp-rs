use crate::{EnqueueAck, JobRejection, ServerInfo, ServerInfoError, pb::sepp::v1 as pb};
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
    BatchOutcome, EnqueueRequest, Job, JobConversionError, JobCtx, ReserveOptions,
    pb::sepp::v1::queue_service_client::QueueServiceClient,
};

const RESERVE_DEADLINE_SLACK: Duration = Duration::from_secs(10);

type AuthChannel = InterceptedService<Channel, ApiKeyInterceptor>;

#[derive(Clone)]
pub struct SeppClient {
    inner: QueueServiceClient<AuthChannel>,
    retry_policy: Arc<RetryPolicy>,
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SeppClient>();
};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    #[error("could not connect to Sepp server at {addr}: {reason}")]
    Connect { addr: String, reason: String },
    #[error("the API key is not a valid HTTP header value")]
    InvalidApiKey,
    #[error("transport failure: {0}")]
    Transport(String),
    #[error("authentication failed: {0}")]
    Unauthenticated(String),
    #[error("server is overloaded: {0}")]
    Overloaded(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("server internal error: {0}")]
    ServerInternal(String),
    #[error("server returned unexpected status {code:?}: {message}")]
    UnexpectedStatus { code: tonic::Code, message: String },
    #[error("empty batch")]
    EmptyBatch,
    #[error("server returned {got} results for a batch of {expected} jobs")]
    BatchResultCountMismatch { expected: usize, got: usize },
    #[error("malformed response: {0}")]
    MalformedResponse(&'static str),
    #[error("server returned a malformed job: {0}")]
    MalformedJob(#[from] JobConversionError),
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

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EnqueueError {
    #[error("server rejected the job: {0}")]
    Rejected(JobRejection),
    #[error(transparent)]
    Client(#[from] ClientError),
}

impl From<tonic::Status> for EnqueueError {
    fn from(s: tonic::Status) -> Self {
        Self::Client(s.into())
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LeaseError {
    #[error("no in-flight job with this id (already acked, expired, or never existed)")]
    JobNotFound,
    #[error("attempt mismatch: the lease was reassigned")]
    AttemptMismatch,
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

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReserveError {
    #[error("requested queues are not declared on the server: {0}")]
    UnknownQueues(String),
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

#[derive(Debug, Clone)]
pub enum RetryDirective {
    Default,
    After(Duration),
    DeadLetter,
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    max_attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    multiplier: f64,
    jitter: bool,
}

impl RetryPolicy {
    pub fn with_max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }

    pub fn with_initial_backoff(mut self, d: Duration) -> Self {
        self.initial_backoff = d;
        self
    }

    pub fn with_max_backoff(mut self, d: Duration) -> Self {
        self.max_backoff = d;
        self
    }

    pub fn with_multiplier(mut self, m: f64) -> Self {
        self.multiplier = m.max(1.0);
        self
    }

    pub fn without_jitter(mut self) -> Self {
        self.jitter = false;
        self
    }

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
    /// Connect to a Sepp server over plaintext with no authentication.
    pub async fn connect(addr: impl Into<String>) -> Result<Self, ClientError> {
        Self::builder(addr).connect().await
    }

    pub fn builder(addr: impl Into<String>) -> SeppClientBuilder {
        SeppClientBuilder::new(addr)
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: QueueServiceClient::with_interceptor(channel, ApiKeyInterceptor::disabled()),
            retry_policy: Arc::new(RetryPolicy::default()),
        }
    }

    #[tracing::instrument(name = "sepp-rs.enqueue", skip_all, fields(jobs))]
    pub async fn enqueue_batch(
        &self,
        jobs: impl IntoIterator<Item = EnqueueRequest>,
    ) -> Result<BatchOutcome, ClientError> {
        let jobs: Vec<pb::EnqueueRequest> = jobs.into_iter().map(Into::into).collect();
        if jobs.is_empty() {
            return Err(ClientError::EmptyBatch);
        }
        let sent = jobs.len();
        tracing::Span::current().record("jobs", sent);

        let response = with_retry(&self.retry_policy, "enqueue_batch", || {
            let mut request = Request::new(pb::EnqueueBatchRequest { jobs: jobs.clone() });
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

        Ok(BatchOutcome { results })
    }

    pub async fn enqueue(&self, job: EnqueueRequest) -> Result<EnqueueAck, EnqueueError> {
        let mut results = self
            .enqueue_batch(std::iter::once(job))
            .await?
            .into_results()
            .into_iter();

        match results.next() {
            Some(Ok(ack)) => Ok(ack),
            Some(Err(rej)) => Err(EnqueueError::Rejected(rej)),
            None => Err(EnqueueError::Client(ClientError::MalformedResponse(
                "empty results for single-job batch",
            ))),
        }
    }

    #[tracing::instrument(name = "sepp-rs.enqueue_atomic", skip_all, fields(jobs))]
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
            let mut request = Request::new(pb::EnqueueBatchRequest { jobs: jobs.clone() });
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

    #[tracing::instrument(name = "sepp-rs.reserve", skip_all, fields(jobs, worker_id = opts.worker_id.as_deref().unwrap_or("<none>")))]
    pub async fn reserve(&self, opts: &ReserveOptions) -> Result<Option<Vec<Job>>, ReserveError> {
        let msg = pb::ReserveRequest::from(opts);
        let mut request = Request::new(msg);
        request.set_timeout(opts.wait_timeout() + RESERVE_DEADLINE_SLACK);
        inject_metadata(&mut request);

        let response = self.inner.clone().reserve(request).await?.into_inner();

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

    #[tracing::instrument(name = "sepp-rs.ack", skip_all, fields(job_id = %ctx.id, attempt = ctx.attempt, worker_id = ctx.lease.worker_id.as_deref().unwrap_or("<none>")))]
    pub async fn ack(&self, ctx: &JobCtx) -> Result<(), LeaseError> {
        let body = pb::AckRequest {
            job_id: ctx.id.clone(),
            attempt: ctx.attempt,
            worker_id: ctx.lease.worker_id.clone(),
        };
        with_retry(&self.retry_policy, "ack", || {
            let mut request = Request::new(body.clone());
            inject_metadata(&mut request);
            let mut inner = self.inner.clone();
            async move { inner.ack(request).await.map(|_| ()) }
        })
        .await?;
        Ok(())
    }

    #[tracing::instrument(name = "sepp-rs.nack", skip_all, fields(job_id = %ctx.id, attempt = ctx.attempt, worker_id = ctx.lease.worker_id.as_deref().unwrap_or("<none>")))]
    pub async fn nack(
        &self,
        ctx: &JobCtx,
        retry: RetryDirective,
        reason: impl Into<String>,
    ) -> Result<bool, LeaseError> {
        let strategy = match retry {
            RetryDirective::Default => pb::nack_retry::Strategy::Default(()),
            RetryDirective::After(d) => pb::nack_retry::Strategy::DelayMs(d.as_millis() as u64),
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
            let mut request = Request::new(body.clone());
            inject_metadata(&mut request);
            let mut inner = self.inner.clone();
            async move { inner.nack(request).await.map(|r| r.into_inner()) }
        })
        .await?;
        Ok(response.dead_lettered)
    }

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
        fields(job_id = %job_id, attempt, worker_id = worker_id.unwrap_or("<none>"))
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
            lease_duration_ms: extension.as_millis() as u64,
            worker_id: worker_id.map(String::from),
        };

        let response = with_retry(&self.retry_policy, "extend", || {
            let mut request = Request::new(body.clone());
            inject_metadata(&mut request);
            let mut inner = self.inner.clone();
            async move { inner.extend(request).await.map(|r| r.into_inner()) }
        })
        .await?;
        crate::millis_to_system_time(response.lease_expires_at).ok_or_else(|| {
            ClientError::MalformedResponse("extend returned an invalid lease_expires_at").into()
        })
    }

    pub async fn get_server_info(&self) -> Result<ServerInfo, ClientError> {
        let response = with_retry(&self.retry_policy, "get_server_info", || {
            let request = Request::new(pb::GetServerInfoRequest {});
            let mut inner = self.inner.clone();
            async move { inner.get_server_info(request).await.map(|r| r.into_inner()) }
        })
        .await?;

        Ok(ServerInfo::try_from(response)?)
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

pub struct SeppClientBuilder {
    addr: String,
    api_key: Option<String>,
    retry_policy: RetryPolicy,
    #[cfg(feature = "tls")]
    tls: Option<ClientTlsConfig>,
}

impl SeppClientBuilder {
    fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            api_key: None,
            retry_policy: RetryPolicy::default(),
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    #[cfg(feature = "tls")]
    pub fn tls(mut self) -> Self {
        self.tls = Some(self.tls.unwrap_or_default().with_native_roots());
        self
    }

    #[cfg(feature = "tls")]
    pub fn tls_ca_certificate(mut self, pem: impl AsRef<[u8]>) -> Self {
        let config = self.tls.unwrap_or_default();
        self.tls = Some(config.ca_certificate(Certificate::from_pem(pem)));
        self
    }

    #[cfg(feature = "tls")]
    pub fn tls_domain(mut self, domain: impl Into<String>) -> Self {
        let config = self.tls.unwrap_or_default();
        self.tls = Some(config.domain_name(domain));
        self
    }

    #[cfg(feature = "tls")]
    pub fn tls_config(mut self, config: ClientTlsConfig) -> Self {
        self.tls = Some(config);
        self
    }

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
                .http2_keep_alive_interval(Duration::from_secs(30)) // For streaming reserve
                .keep_alive_timeout(Duration::from_secs(10)) // For streaming reserve
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

        Ok(SeppClient {
            inner: QueueServiceClient::with_interceptor(channel, interceptor),
            retry_policy: Arc::new(self.retry_policy),
        })
    }
}

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
    inject_metadata(request);
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
}
