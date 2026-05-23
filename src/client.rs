use crate::{EnqueueAck, JobRejection, ServerInfo, ServerInfoError, pb::sepp::v1 as pb};
use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, SystemTime},
};

use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataValue},
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Channel, Endpoint},
};
#[cfg(feature = "tls")]
use tonic::transport::{Certificate, ClientTlsConfig};
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
    worker_id: Option<String>,
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SeppClient>();
};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("could not connect to Sepp server at {addr}: {reason}")]
    Connect { addr: String, reason: String },
    #[error("the API key is not a valid HTTP header value")]
    InvalidApiKey,
    #[error("Internal RPC error: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("Empty batch")]
    EmptyBatch,
    #[error("server returned {got} results for a batch of {expected} jobs")]
    BatchResultCountMismatch { expected: usize, got: usize },
    #[error("Malformed response: {0}")]
    MalformedResponse(&'static str),
    #[error("server returned a malformed job: {0}")]
    MalformedJob(#[from] JobConversionError),
    #[error("server returned malformed server info: {0}")]
    MalformedServerInfo(#[from] ServerInfoError),
}

#[derive(Debug, Clone)]
pub enum RetryDirective {
    Default,
    After(Duration),
    DeadLetter,
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
            worker_id: None,
        }
    }

    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    /// The worker identifier this client reports, if any.
    pub fn worker_id(&self) -> Option<&str> {
        self.worker_id.as_deref()
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

        let mut request = Request::new(pb::EnqueueBatchRequest { jobs });

        inject_trace_context(&mut request);
        let response = self
            .inner
            .clone()
            .enqueue_batch(request)
            .await?
            .into_inner();

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

    pub async fn enqueue(
        &self,
        job: EnqueueRequest,
    ) -> Result<Result<EnqueueAck, JobRejection>, ClientError> {
        let mut outcome = self
            .enqueue_batch(std::iter::once(job))
            .await?
            .results
            .into_iter();

        match outcome.next() {
            Some(Ok(ack)) => Ok(Ok(ack)),
            Some(Err(rej)) => Ok(Err(rej)),
            None => Err(ClientError::MalformedResponse(
                "empty results for single-job batch",
            )),
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

        let mut request = Request::new(pb::EnqueueBatchRequest { jobs });
        inject_trace_context(&mut request);

        let response = self
            .inner
            .clone()
            .enqueue_atomic(request)
            .await?
            .into_inner();

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
            None => Err(ClientError::MalformedResponse(
                "missing outcome in EnqueueAtomicResponse",
            )
            .into()),
        }
    }

    #[tracing::instrument(name = "sepp-rs.reserve", skip_all, fields(jobs, worker_id = self.worker_id.as_deref().unwrap_or("<none>")))]
    pub async fn reserve(&self, opts: &ReserveOptions) -> Result<Option<Vec<Job>>, ClientError> {
        let mut msg = pb::ReserveRequest::from(opts);
        msg.worker_id = self.worker_id.clone();
        let mut request = Request::new(msg);
        request.set_timeout(opts.wait_timeout() + RESERVE_DEADLINE_SLACK);
        inject_metadata(&mut request);

        let response = self.inner.clone().reserve(request).await?.into_inner();

        // A single malformed job must not discard the rest of the batch
        let mut jobs = Vec::with_capacity(response.jobs.len());
        for job in response.jobs {
            match crate::job_from_pb(self, job) {
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

    #[tracing::instrument(name = "sepp-rs.ack", skip_all, fields(job_id = %ctx.id, attempt = ctx.attempt, worker_id = self.worker_id.as_deref().unwrap_or("<none>")))]
    pub async fn ack(&self, ctx: &JobCtx) -> Result<(), ClientError> {
        let mut request = Request::new(pb::AckRequest {
            job_id: ctx.id.clone(),
            attempt: ctx.attempt,
            worker_id: self.worker_id.clone(),
        });
        inject_metadata(&mut request);

        self.inner.clone().ack(request).await?;
        Ok(())
    }

    #[tracing::instrument(name = "sepp-rs.nack", skip_all, fields(job_id = %ctx.id, attempt = ctx.attempt, worker_id = self.worker_id.as_deref().unwrap_or("<none>")))]
    pub async fn nack(
        &self,
        ctx: &JobCtx,
        retry: RetryDirective,
        reason: impl Into<String>,
    ) -> Result<bool, ClientError> {
        let strategy = match retry {
            RetryDirective::Default => pb::nack_retry::Strategy::Default(()),
            RetryDirective::After(d) => pb::nack_retry::Strategy::DelayMs(d.as_millis() as u64),
            RetryDirective::DeadLetter => pb::nack_retry::Strategy::DeadLetter(()),
        };
        let mut request = Request::new(pb::NackRequest {
            job_id: ctx.id.clone(),
            attempt: ctx.attempt,
            reason: Some(reason.into()),
            retry: Some(pb::NackRetry {
                strategy: Some(strategy),
            }),
            worker_id: self.worker_id.clone(),
        });
        inject_metadata(&mut request);

        let response = self.inner.clone().nack(request).await?.into_inner();
        Ok(response.dead_lettered)
    }

    pub async fn extend(
        &self,
        ctx: &JobCtx,
        extension: Duration,
    ) -> Result<SystemTime, ClientError> {
        self.extend_inner(&ctx.id, ctx.attempt, extension).await
    }

    #[tracing::instrument(
        name = "sepp-rs.extend",
        skip_all,
        fields(job_id = %job_id, attempt, worker_id = self.worker_id.as_deref().unwrap_or("<none>"))
    )]
    pub(crate) async fn extend_inner(
        &self,
        job_id: &str,
        attempt: u32,
        extension: Duration,
    ) -> Result<SystemTime, ClientError> {
        let mut request = Request::new(pb::ExtendRequest {
            job_id: job_id.to_string(),
            attempt,
            lease_duration_ms: extension.as_millis() as u64,
            worker_id: self.worker_id.clone(),
        });
        inject_metadata(&mut request);

        let response = self.inner.clone().extend(request).await?.into_inner();
        crate::millis_to_system_time(response.lease_expires_at).ok_or(
            ClientError::MalformedResponse("extend returned an invalid lease_expires_at"),
        )
    }

    pub async fn get_server_info(&self) -> Result<ServerInfo, ClientError> {
        let request = Request::new(pb::GetServerInfoRequest {});
        let response = self
            .inner
            .clone()
            .get_server_info(request)
            .await?
            .into_inner();

        Ok(ServerInfo::try_from(response)?)
    }
}

#[derive(Clone)]
pub(crate) struct Lease {
    client: SeppClient,
    job_id: String,
    attempt: u32,
    expiry: Arc<AtomicI64>,
}

impl Lease {
    pub(crate) fn new(
        client: SeppClient,
        job_id: String,
        attempt: u32,
        lease_expires_at: SystemTime,
    ) -> Self {
        Self {
            client,
            job_id,
            attempt,
            expiry: Arc::new(AtomicI64::new(crate::system_time_to_millis(lease_expires_at))),
        }
    }

    pub(crate) fn known_expiry_ms(&self) -> i64 {
        self.expiry.load(Ordering::Acquire)
    }

    pub(crate) async fn extend(&self, by: Duration) -> Result<SystemTime, ClientError> {
        let new_expiry = self
            .client
            .extend_inner(&self.job_id, self.attempt, by)
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
    worker_id: Option<String>,
    #[cfg(feature = "tls")]
    tls: Option<ClientTlsConfig>,
}

impl SeppClientBuilder {
    fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            api_key: None,
            worker_id: None,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    pub fn worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
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
            warn!("API key configured without TLS; it will be sent over the connection in plaintext");
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
            worker_id: self.worker_id,
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
