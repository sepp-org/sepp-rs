use crate::{EnqueueAck, JobRejection, ServerInfo, ServerInfoError, pb::sepp::v1 as pb};
use std::time::Duration;

use tonic::{
    Request,
    transport::{Channel, Endpoint},
};
use tracing::{debug, error, info, warn};

use crate::{
    BatchOutcome, EnqueueRequest, Job, JobConversionError, JobCtx, ReserveOptions,
    pb::sepp::v1::queue_service_client::QueueServiceClient,
};

const RESERVE_DEADLINE_SLACK: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct SeppClient {
    inner: QueueServiceClient<Channel>,
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
    pub async fn connect(addr: impl Into<String>) -> Result<Self, ClientError> {
        let addr = addr.into();
        let channel = async {
            Endpoint::from_shared(addr.clone())?
                .connect_timeout(Duration::from_secs(5))
                .user_agent(concat!("sepp-rs/", env!("CARGO_PKG_VERSION")))? // So we can tell from the server POV which client this is
                .http2_keep_alive_interval(Duration::from_secs(30)) // For streaming reserve
                .keep_alive_timeout(Duration::from_secs(10)) // For streaming reserve
                .keep_alive_while_idle(true) // For streaming reserve
                .connect()
                .await
        }
        .await
        .map_err(|e| {
            error!(%addr, error = %e, "failed to connect to Sepp server");

            ClientError::Connect {
                addr: addr.clone(),
                reason: root_cause(&e),
            }
        })?;

        info!(%addr, "connected to Sepp server");

        Ok(Self {
            inner: QueueServiceClient::new(channel),
            worker_id: None,
        })
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: QueueServiceClient::new(channel),
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
                },
                Some(pb::job_result::Outcome::Error(e)) => {
                    debug!(error = ?e, "server rejected the job");
                    Err(e.into())
                },
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

    #[tracing::instrument(name = "sepp-rs.reserve", skip_all, fields(jobs, worker_id = self.worker_id.as_deref().unwrap_or("<none>")))]
    pub async fn reserve(&self, opts: &ReserveOptions) -> Result<Option<Vec<Job>>, ClientError> {
        let mut msg = pb::ReserveRequest::from(opts);
        msg.worker_id = self.worker_id.clone();
        let mut request = Request::new(msg);
        request.set_timeout(opts.wait_timeout() + RESERVE_DEADLINE_SLACK);
        inject_metadata(&mut request);

        let response = self.inner.clone().reserve(request).await?.into_inner();

        // A single malformed job must not discard the rest of the batch: skip
        // it (it redelivers after its lease expires) and deliver the good ones.
        let mut jobs = Vec::with_capacity(response.jobs.len());
        for job in response.jobs {
            match Job::try_from(job) {
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

    #[tracing::instrument(name = "sepp-rs.extend", skip_all, fields(job_id = %ctx.id, attempt = ctx.attempt, worker_id = self.worker_id.as_deref().unwrap_or("<none>")))]
    pub async fn extend(&self, ctx: &JobCtx, extension: Duration) -> Result<(), ClientError> {
        let mut request = Request::new(pb::ExtendRequest {
            job_id: ctx.id.clone(),
            attempt: ctx.attempt,
            lease_duration_ms: extension.as_millis() as u64,
            worker_id: self.worker_id.clone(),
        });
        inject_metadata(&mut request);

        self.inner.clone().extend(request).await?;
        Ok(())
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
