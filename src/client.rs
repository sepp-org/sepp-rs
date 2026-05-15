use crate::{EnqueueAck, JobRejection, ServerInfo, ServerInfoError, pb::sepp::v1 as pb};
use std::time::Duration;

use tonic::{
    Request,
    transport::{Channel, Endpoint},
};

use crate::{
    BatchOutcome, EnqueueRequest, Job, JobConversionError, JobCtx, ReserveOptions,
    pb::sepp::v1::queue_service_client::QueueServiceClient,
};

// We need to give some extra time on top of the server's `wait_timeout` to account for network latency etc.
const RESERVE_DEADLINE_SLACK: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct SeppClient {
    inner: QueueServiceClient<Channel>,
}

// Assert that SeppClient is Send + Sync at compile time
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SeppClient>();
};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Failed to connect to Sepp server: {0}")]
    Connect(#[from] tonic::transport::Error),
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

// How the server should treat a nacked job.
#[derive(Debug, Clone)]
pub enum RetryDirective {
    // Apply the queue's configured retry policy.
    Default,
    // Retry, but not before the given delay has elapsed.
    After(Duration),
    // Do not retry, move the job straight to the dead-letter queue.
    DeadLetter,
}

impl SeppClient {
    pub async fn connect(addr: impl Into<String>) -> Result<Self, ClientError> {
        let addr = addr.into();
        let channel = Endpoint::from_shared(addr.clone())?
            .connect_timeout(Duration::from_secs(5))
            .user_agent(concat!("sepp-rs/", env!("CARGO_PKG_VERSION")))? // So we can tell from the server POV which client this is
            .http2_keep_alive_interval(Duration::from_secs(30)) // For streaming reserve
            .keep_alive_timeout(Duration::from_secs(10)) // For streaming reserve
            .keep_alive_while_idle(true) // For streaming reserve
            .connect()
            .await?;
        tracing::debug!(server = %addr, "connected to Sepp server");

        Ok(Self {
            inner: QueueServiceClient::new(channel),
        })
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: QueueServiceClient::new(channel),
        }
    }

    pub async fn enqueue_batch(
        &self,
        jobs: impl IntoIterator<Item = EnqueueRequest>,
    ) -> Result<BatchOutcome, ClientError> {
        let jobs: Vec<pb::EnqueueRequest> = jobs.into_iter().map(Into::into).collect();
        if jobs.is_empty() {
            return Err(ClientError::EmptyBatch);
        }
        let sent = jobs.len();

        let request = Request::new(pb::EnqueueBatchRequest { jobs });
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
                Some(pb::job_result::Outcome::Success(r)) => Ok(r.into()),
                Some(pb::job_result::Outcome::Error(e)) => Err(e.into()),
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

    #[tracing::instrument(skip_all, level = "debug")]
    pub async fn reserve(&self, opts: &ReserveOptions) -> Result<Option<Job>, ClientError> {
        let mut request = Request::new(pb::ReserveRequest::from(opts));
        request.set_timeout(opts.wait_timeout() + RESERVE_DEADLINE_SLACK);

        let response = self.inner.clone().reserve(request).await?.into_inner();

        match response.job {
            Some(job) => Ok(Some(Job::try_from(job)?)),
            None => Ok(None),
        }
    }

    #[tracing::instrument(skip_all, level = "debug", fields(job_id = %ctx.id, attempt = ctx.attempt))]
    pub async fn ack(&self, ctx: &JobCtx) -> Result<(), ClientError> {
        let request = Request::new(pb::AckRequest {
            job_id: ctx.id.clone(),
            attempt: ctx.attempt,
            worker_id: None,
        });

        self.inner.clone().ack(request).await?;
        Ok(())
    }

    #[tracing::instrument(skip_all, level = "debug", fields(job_id = %ctx.id, attempt = ctx.attempt))]
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
        let request = Request::new(pb::NackRequest {
            job_id: ctx.id.clone(),
            attempt: ctx.attempt,
            reason: Some(reason.into()),
            retry: Some(pb::NackRetry {
                strategy: Some(strategy),
            }),
        });

        let response = self.inner.clone().nack(request).await?.into_inner();
        Ok(response.dead_lettered)
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
