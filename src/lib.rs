use std::{
    collections::HashMap,
    fmt,
    time::{Duration, SystemTime},
};

mod pb;

pub mod client;
pub mod worker;

#[derive(Debug, Clone, PartialEq)]
pub enum Primitive {
    String(String),
    Int(i64),
    Double(f64),
    Bool(bool),
}

impl From<Primitive> for crate::pb::sepp::v1::PrimitiveValue {
    fn from(p: Primitive) -> Self {
        use crate::pb::sepp::v1::primitive_value::Value;

        let value = match p {
            Primitive::String(s) => Value::StringValue(s),
            Primitive::Int(i) => Value::IntValue(i),
            Primitive::Double(d) => Value::DoubleValue(d),
            Primitive::Bool(b) => Value::BoolValue(b),
        };
        Self { value: Some(value) }
    }
}

impl From<&str> for Primitive {
    fn from(v: &str) -> Self {
        Self::String(v.into())
    }
}
impl From<String> for Primitive {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}
impl From<i64> for Primitive {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}
impl From<i32> for Primitive {
    fn from(v: i32) -> Self {
        Self::Int(v.into())
    }
}
impl From<f64> for Primitive {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}
impl From<bool> for Primitive {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payload {
    pub data: Vec<u8>,
    pub encoding: String,
}

impl From<Payload> for crate::pb::sepp::v1::Payload {
    fn from(p: Payload) -> Self {
        Self {
            data: p.data,
            encoding: p.encoding,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Priority(u8);

#[derive(Debug, thiserror::Error)]
#[error("priority must be 0-9, got {0}")]
pub struct PriorityOutOfRange(pub u8);

impl Priority {
    pub const MIN: Self = Self(0);
    pub const MAX: Self = Self(9);

    pub fn new(value: u8) -> Result<Self, PriorityOutOfRange> {
        if value > Self::MAX.0 {
            Err(PriorityOutOfRange(value))
        } else {
            Ok(Self(value))
        }
    }

    pub fn get(&self) -> u8 {
        self.0
    }
}

impl TryFrom<u8> for Priority {
    type Error = PriorityOutOfRange;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TraceContext {
    traceparent: String,
    tracestate: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum TraceContextError {
    #[error("invalid traceparent: {0}")]
    InvalidTraceparent(&'static str),
}

impl TraceContext {
    pub fn new(traceparent: impl Into<String>) -> Result<Self, TraceContextError> {
        let traceparent = traceparent.into();
        validate_traceparent(&traceparent)?;
        Ok(Self {
            traceparent,
            tracestate: None,
        })
    }

    pub fn with_tracestate(mut self, ts: impl Into<String>) -> Self {
        self.tracestate = Some(ts.into());
        self
    }

    pub fn traceparent(&self) -> &str {
        &self.traceparent
    }
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }
}

#[cfg(feature = "opentelemetry")]
impl TraceContext {
    pub fn from_current_otel() -> Option<Self> {
        use opentelemetry::propagation::TextMapPropagator;
        use opentelemetry::trace::TraceContextExt;
        use opentelemetry_sdk::propagation::TraceContextPropagator;

        let cx = opentelemetry::Context::current();
        if !cx.span().span_context().is_valid() {
            return None;
        }

        let mut carrier = std::collections::HashMap::new();
        TraceContextPropagator::new().inject_context(&cx, &mut HashMapInjector(&mut carrier));

        let traceparent = carrier.remove("traceparent")?;
        let tracestate = carrier.remove("tracestate");
        Some(Self {
            traceparent,
            tracestate,
        })
    }

    pub fn attach_to_otel(&self) -> opentelemetry::ContextGuard {
        use opentelemetry::propagation::TextMapPropagator;
        use opentelemetry_sdk::propagation::TraceContextPropagator;

        let mut carrier = std::collections::HashMap::new();
        carrier.insert("traceparent".to_string(), self.traceparent.clone());
        if let Some(ts) = &self.tracestate {
            carrier.insert("tracestate".to_string(), ts.clone());
        }

        let extracted = TraceContextPropagator::new().extract(&HashMapExtractor(&carrier));
        extracted.attach()
    }

    pub fn otel_span_context(&self) -> Option<opentelemetry::trace::SpanContext> {
        use opentelemetry::propagation::TextMapPropagator;
        use opentelemetry::trace::TraceContextExt;
        use opentelemetry_sdk::propagation::TraceContextPropagator;

        let mut carrier = HashMap::new();
        carrier.insert("traceparent".to_string(), self.traceparent.clone());
        if let Some(ts) = &self.tracestate {
            carrier.insert("tracestate".to_string(), ts.clone());
        }
        let cx = TraceContextPropagator::new().extract(&HashMapExtractor(&carrier));
        let span_context = cx.span().span_context().clone();
        span_context.is_valid().then_some(span_context)
    }
}

#[cfg(feature = "opentelemetry")]
pub(crate) fn inject_pb_trace_context(
    cx: &opentelemetry::Context,
) -> Option<crate::pb::sepp::v1::TraceContext> {
    use opentelemetry::propagation::TextMapPropagator;
    use opentelemetry::trace::TraceContextExt;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    if !cx.span().span_context().is_valid() {
        return None;
    }
    let mut carrier = HashMap::new();
    TraceContextPropagator::new().inject_context(cx, &mut HashMapInjector(&mut carrier));
    Some(crate::pb::sepp::v1::TraceContext {
        traceparent: carrier.remove("traceparent")?,
        tracestate: carrier.remove("tracestate"),
    })
}

#[cfg(feature = "opentelemetry")]
struct HashMapInjector<'a>(&'a mut HashMap<String, String>);

#[cfg(feature = "opentelemetry")]
impl opentelemetry::propagation::Injector for HashMapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

#[cfg(feature = "opentelemetry")]
struct HashMapExtractor<'a>(&'a HashMap<String, String>);

#[cfg(feature = "opentelemetry")]
impl opentelemetry::propagation::Extractor for HashMapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

fn validate_traceparent(s: &str) -> Result<(), TraceContextError> {
    // W3C: version-trace_id-span_id-flags  ->  "00-<32hex>-<16hex>-<2hex>"
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 4 {
        return Err(TraceContextError::InvalidTraceparent(
            "expected 4 hyphen-separated fields",
        ));
    }
    let [ver, trace_id, span_id, flags] = [parts[0], parts[1], parts[2], parts[3]];
    if ver.len() != 2 || !ver.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(TraceContextError::InvalidTraceparent(
            "version must be 2 hex chars",
        ));
    }
    if trace_id.len() != 32 || !trace_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(TraceContextError::InvalidTraceparent(
            "trace_id must be 32 hex chars",
        ));
    }
    if trace_id.bytes().all(|b| b == b'0') {
        return Err(TraceContextError::InvalidTraceparent(
            "trace_id must not be all zeros",
        ));
    }
    if span_id.len() != 16 || !span_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(TraceContextError::InvalidTraceparent(
            "span_id must be 16 hex chars",
        ));
    }
    if span_id.bytes().all(|b| b == b'0') {
        return Err(TraceContextError::InvalidTraceparent(
            "span_id must not be all zeros",
        ));
    }
    if flags.len() != 2 || !flags.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(TraceContextError::InvalidTraceparent(
            "flags must be 2 hex chars",
        ));
    }
    Ok(())
}

impl From<TraceContext> for crate::pb::sepp::v1::TraceContext {
    fn from(tc: TraceContext) -> Self {
        Self {
            traceparent: tc.traceparent,
            tracestate: tc.tracestate,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnqueueRequest {
    queue: String,
    job_type: String,
    payload: Option<Payload>,
    idempotency_key: Option<String>,
    priority: Option<Priority>,
    max_attempts: Option<u32>,
    custom: HashMap<String, Primitive>,
    trace_context: Option<TraceContext>,
    scheduled_at: Option<SystemTime>,
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueueRequestBuilderError {
    #[error("queue name must not be empty")]
    EmptyQueue,
    #[error("job type must not be empty")]
    EmptyJobType,
}

impl EnqueueRequest {
    pub fn new(
        queue: impl Into<String>,
        job_type: impl Into<String>,
    ) -> Result<Self, EnqueueRequestBuilderError> {
        let queue = queue.into();
        if queue.is_empty() {
            return Err(EnqueueRequestBuilderError::EmptyQueue);
        }
        let job_type = job_type.into();
        if job_type.is_empty() {
            return Err(EnqueueRequestBuilderError::EmptyJobType);
        }

        Ok(Self {
            queue,
            job_type,
            payload: None,
            idempotency_key: None,
            priority: None,
            max_attempts: None,
            custom: HashMap::new(),
            trace_context: None,
            scheduled_at: None,
        })
    }

    pub fn with_payload(mut self, payload: Payload) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = Some(attempts);
        self
    }

    pub fn with_custom(mut self, custom: HashMap<String, Primitive>) -> Self {
        self.custom = custom; // This is cheap enough that Optional is not needed
        self
    }

    pub fn with_trace_context(mut self, trace_context: TraceContext) -> Self {
        self.trace_context = Some(trace_context);
        self
    }

    pub fn with_scheduled_at(mut self, scheduled_at: SystemTime) -> Self {
        self.scheduled_at = Some(scheduled_at);
        self
    }
}

impl From<EnqueueRequest> for crate::pb::sepp::v1::EnqueueRequest {
    fn from(req: EnqueueRequest) -> Self {
        Self {
            queue: req.queue,
            job_type: req.job_type,
            payload: req.payload.map(Into::into),
            idempotency_key: req.idempotency_key,
            priority: req.priority.map(|p| p.get() as u32),
            max_attempts: req.max_attempts,
            custom: req.custom.into_iter().map(|(k, v)| (k, v.into())).collect(),
            trace_context: req.trace_context.map(Into::into),
            scheduled_at: req
                .scheduled_at
                .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BatchOutcome {
    results: Vec<Result<EnqueueAck, JobRejection>>,
}

impl BatchOutcome {
    pub fn all_succeeded(&self) -> bool {
        self.results.iter().all(Result::is_ok)
    }

    pub fn results(&self) -> &[Result<EnqueueAck, JobRejection>] {
        &self.results
    }

    pub fn rejected(&self) -> impl Iterator<Item = (usize, &JobRejection)> {
        self.results
            .iter()
            .enumerate()
            .filter_map(|(i, r)| r.as_ref().err().map(|e| (i, e)))
    }

    pub fn succeeded(&self) -> impl Iterator<Item = (usize, &EnqueueAck)> {
        self.results
            .iter()
            .enumerate()
            .filter_map(|(i, r)| r.as_ref().ok().map(|a| (i, a)))
    }

    pub fn into_results(self) -> Vec<Result<EnqueueAck, JobRejection>> {
        self.results
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueAck {
    pub job_id: String,
    pub deduplicated: bool,
}

impl From<crate::pb::sepp::v1::EnqueueResponse> for EnqueueAck {
    fn from(r: crate::pb::sepp::v1::EnqueueResponse) -> Self {
        Self {
            job_id: r.job_id,
            deduplicated: r.deduplicated,
        }
    }
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum JobRejection {
    #[error("queue {queue:?} is not declared on the server (strict mode)")]
    UnknownQueue { queue: String },
    #[error("payload size {actual} bytes exceeds the queue limit of {limit}")]
    PayloadTooLarge { limit: u64, actual: u64 },
    #[error("payload encoding {encoding:?} is not allowed; accepted: {allowed:?}")]
    EncodingNotAllowed {
        encoding: String,
        allowed: Vec<String>,
    },
    #[error("job_type {job_type:?} is not accepted by this queue; accepted: {allowed:?}")]
    JobTypeNotAllowed {
        job_type: String,
        allowed: Vec<String>,
    },
    #[error("custom map has {actual} entries, exceeding the queue limit of {limit}")]
    CustomEntriesTooMany { limit: u32, actual: u32 },
    #[error("custom map's total size {actual} bytes exceeds the queue limit of {limit}")]
    CustomMapTooLarge { limit: u64, actual: u64 },
    #[error("custom key {key:?} is {actual} bytes, exceeding the limit of {limit}")]
    CustomKeyTooLong {
        key: String,
        limit: u32,
        actual: u64,
    },
    #[error("queue name is {actual} bytes, exceeding the limit of {limit}")]
    QueueNameTooLong { limit: u32, actual: u64 },
    #[error("job_type is {actual} bytes, exceeding the limit of {limit}")]
    JobTypeNameTooLong { limit: u32, actual: u64 },
    #[error("idempotency_key is {actual} bytes, exceeding the limit of {limit}")]
    IdempotencyKeyTooLong { limit: u32, actual: u64 },
    #[error("scheduled_at {actual_ms}ms is beyond max_schedule_horizon_ms ({horizon_ms}ms)")]
    ScheduledTooFar { horizon_ms: u64, actual_ms: i64 },
    #[error("structural validation failed: {message}")]
    InvalidRequest { message: String },
    #[error("server returned an unrecognized rejection variant")]
    Unknown,
}

impl From<crate::pb::sepp::v1::JobRejection> for JobRejection {
    fn from(r: crate::pb::sepp::v1::JobRejection) -> Self {
        use crate::pb::sepp::v1::job_rejection::Reason;
        match r.reason {
            Some(Reason::UnknownQueue(x)) => Self::UnknownQueue { queue: x.queue },
            Some(Reason::PayloadTooLarge(x)) => Self::PayloadTooLarge {
                limit: x.limit,
                actual: x.actual,
            },
            Some(Reason::EncodingNotAllowed(x)) => Self::EncodingNotAllowed {
                encoding: x.encoding,
                allowed: x.allowed,
            },
            Some(Reason::JobTypeNotAllowed(x)) => Self::JobTypeNotAllowed {
                job_type: x.job_type,
                allowed: x.allowed,
            },
            Some(Reason::CustomEntriesTooMany(x)) => Self::CustomEntriesTooMany {
                limit: x.limit,
                actual: x.actual,
            },
            Some(Reason::CustomMapTooLarge(x)) => Self::CustomMapTooLarge {
                limit: x.limit,
                actual: x.actual,
            },
            Some(Reason::CustomKeyTooLong(x)) => Self::CustomKeyTooLong {
                key: x.key,
                limit: x.limit,
                actual: x.actual,
            },
            Some(Reason::QueueNameTooLong(x)) => Self::QueueNameTooLong {
                limit: x.limit,
                actual: x.actual,
            },
            Some(Reason::JobTypeNameTooLong(x)) => Self::JobTypeNameTooLong {
                limit: x.limit,
                actual: x.actual,
            },
            Some(Reason::IdempotencyKeyTooLong(x)) => Self::IdempotencyKeyTooLong {
                limit: x.limit,
                actual: x.actual,
            },
            Some(Reason::ScheduledTooFar(x)) => Self::ScheduledTooFar {
                horizon_ms: x.horizon_ms,
                actual_ms: x.actual_ms,
            },
            Some(Reason::InvalidRequest(x)) => Self::InvalidRequest { message: x.message },
            None => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobValidationError {
    pub index: u32,
    pub rejection: JobRejection,
}

impl From<crate::pb::sepp::v1::JobValidationError> for JobValidationError {
    fn from(e: crate::pb::sepp::v1::JobValidationError) -> Self {
        Self {
            index: e.index,
            rejection: e
                .rejection
                .map(JobRejection::from)
                .unwrap_or(JobRejection::Unknown),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AtomicEnqueueError {
    #[error(transparent)]
    Client(#[from] crate::client::ClientError),
    #[error("atomic batch rejected: {} job(s) failed validation", _0.len())]
    Validation(Vec<JobValidationError>),
}

impl From<tonic::Status> for AtomicEnqueueError {
    fn from(s: tonic::Status) -> Self {
        Self::Client(crate::client::ClientError::from(s))
    }
}

#[derive(Debug, Clone)]
pub struct JobCtx {
    pub id: String,
    pub job_type: String,
    pub priority: Priority,
    pub attempt: u32,
    pub max_attempts: u32,
    pub enqueued_at: SystemTime,
    pub custom: HashMap<String, Primitive>,
    pub trace_context: Option<TraceContext>,
    pub lease_expires_at: SystemTime,
    pub(crate) lease: crate::client::Lease,
}

impl JobCtx {
    pub async fn extend(
        &self,
        extension: Duration,
    ) -> Result<SystemTime, crate::client::LeaseError> {
        self.lease.extend(extension).await
    }
}

impl fmt::Display for JobCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JobCtx {{ id: {}, job_type: {}, attempt: {}/{}, priority: {} }}",
            self.id,
            self.job_type,
            self.attempt,
            self.max_attempts,
            self.priority.get(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub payload: Option<Payload>,
    pub ctx: JobCtx,
}

#[derive(Debug, thiserror::Error)]
pub enum JobConversionError {
    #[error("job is missing required field `{0}`")]
    MissingField(&'static str),
    #[error("job priority {0} is out of range (expected 0-9)")]
    PriorityOutOfRange(u32),
    #[error("job timestamp `{field}` is not a representable time ({value}ms)")]
    InvalidTimestamp { field: &'static str, value: i64 },
    #[error("custom value for key `{0}` has no value set")]
    EmptyCustomValue(String),
}

impl From<crate::pb::sepp::v1::Payload> for Payload {
    fn from(p: crate::pb::sepp::v1::Payload) -> Self {
        Self {
            data: p.data,
            encoding: p.encoding,
        }
    }
}

impl TryFrom<crate::pb::sepp::v1::TraceContext> for TraceContext {
    type Error = TraceContextError;

    fn try_from(tc: crate::pb::sepp::v1::TraceContext) -> Result<Self, Self::Error> {
        let mut ctx = TraceContext::new(tc.traceparent)?;
        if let Some(ts) = tc.tracestate {
            ctx = ctx.with_tracestate(ts);
        }
        Ok(ctx)
    }
}

fn primitive_from_pb(v: crate::pb::sepp::v1::PrimitiveValue) -> Option<Primitive> {
    use crate::pb::sepp::v1::primitive_value::Value;
    Some(match v.value? {
        Value::StringValue(s) => Primitive::String(s),
        Value::DoubleValue(d) => Primitive::Double(d),
        Value::IntValue(i) => Primitive::Int(i),
        Value::BoolValue(b) => Primitive::Bool(b),
    })
}

fn millis_to_system_time(ms: i64) -> Option<SystemTime> {
    let ms = u64::try_from(ms).ok()?;
    SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(ms))
}

pub(crate) fn system_time_to_millis(t: SystemTime) -> i64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn now_millis() -> i64 {
    system_time_to_millis(SystemTime::now())
}

pub(crate) fn job_from_pb(
    client: &crate::client::SeppClient,
    j: crate::pb::sepp::v1::Job,
    worker_id: Option<&str>,
) -> Result<Job, JobConversionError> {
    use JobConversionError as E;

    if j.id.is_empty() {
        return Err(E::MissingField("id"));
    }
    if j.job_type.is_empty() {
        return Err(E::MissingField("job_type"));
    }

    let priority = u8::try_from(j.priority)
        .ok()
        .and_then(|p| Priority::new(p).ok())
        .ok_or(E::PriorityOutOfRange(j.priority))?;

    let enqueued_at = millis_to_system_time(j.enqueued_at).ok_or(E::InvalidTimestamp {
        field: "enqueued_at",
        value: j.enqueued_at,
    })?;
    let lease_expires_at =
        millis_to_system_time(j.lease_expires_at).ok_or(E::InvalidTimestamp {
            field: "lease_expires_at",
            value: j.lease_expires_at,
        })?;

    let mut custom = HashMap::with_capacity(j.custom.len());
    for (k, v) in j.custom {
        let value = primitive_from_pb(v).ok_or_else(|| E::EmptyCustomValue(k.clone()))?;
        custom.insert(k, value);
    }

    // An invalid trace context must not block job delivery: drop it and
    // lose trace continuity rather than failing the whole reservation.
    let trace_context = j
        .trace_context
        .and_then(|tc| TraceContext::try_from(tc).ok());

    let lease = crate::client::Lease::new(
        client.clone(),
        j.id.clone(),
        j.attempt,
        lease_expires_at,
        worker_id.map(String::from),
    );

    Ok(Job {
        payload: j.payload.map(Into::into),
        ctx: JobCtx {
            id: j.id,
            job_type: j.job_type,
            priority,
            attempt: j.attempt,
            max_attempts: j.max_attempts,
            enqueued_at,
            custom,
            trace_context,
            lease_expires_at,
            lease,
        },
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReserveOptions {
    queues: Vec<String>,
    wait_timeout: Duration,
    lease_duration: Duration,
    worker_id: Option<String>,
    max_jobs: Option<u32>,
}

#[derive(Debug, thiserror::Error)]
pub enum ReserveOptionsError {
    #[error("at least one queue must be specified")]
    EmptyQueues,
    #[error("queue name at index {0} must not be empty")]
    EmptyQueueName(usize),
    #[error("lease_duration must be at least 1ms")]
    LeaseDurationTooShort,
    #[error("worker_id must not be empty when set")]
    EmptyWorkerId,
}

impl ReserveOptions {
    pub fn new(
        queues: impl IntoIterator<Item = impl Into<String>>,
        lease_duration: Duration,
    ) -> Result<Self, ReserveOptionsError> {
        let queues: Vec<String> = queues.into_iter().map(Into::into).collect();
        if queues.is_empty() {
            return Err(ReserveOptionsError::EmptyQueues);
        }
        for (i, q) in queues.iter().enumerate() {
            if q.is_empty() {
                return Err(ReserveOptionsError::EmptyQueueName(i));
            }
        }
        if lease_duration.is_zero() {
            return Err(ReserveOptionsError::LeaseDurationTooShort);
        }
        Ok(Self {
            queues,
            wait_timeout: Duration::from_secs(30), // The server will hold the connection open for this long if no job is immediately available
            lease_duration,
            worker_id: None,
            max_jobs: None,
        })
    }

    pub fn with_wait_timeout(mut self, wait: Duration) -> Self {
        self.wait_timeout = wait;
        self
    }

    pub fn wait_timeout(&self) -> Duration {
        self.wait_timeout
    }

    pub fn with_worker_id(mut self, id: impl Into<String>) -> Result<Self, ReserveOptionsError> {
        let id = id.into();
        if id.is_empty() {
            return Err(ReserveOptionsError::EmptyWorkerId);
        }
        self.worker_id = Some(id);
        Ok(self)
    }

    pub fn with_max_jobs(mut self, max: u32) -> Self {
        self.max_jobs = Some(max);
        self
    }
}

impl From<ReserveOptions> for crate::pb::sepp::v1::ReserveRequest {
    fn from(o: ReserveOptions) -> Self {
        Self::from(&o)
    }
}

impl From<&ReserveOptions> for crate::pb::sepp::v1::ReserveRequest {
    fn from(o: &ReserveOptions) -> Self {
        Self {
            queues: o.queues.clone(),
            wait_timeout_ms: o.wait_timeout.as_millis() as u64,
            lease_duration_ms: o.lease_duration.as_millis() as u64,
            worker_id: o.worker_id.clone(),
            max_jobs: o.max_jobs,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerInfo {
    pub version: String,
    pub supported_protocol_versions: Vec<String>,
    pub server_time: SystemTime,
    pub restricts_encodings: bool,
    pub allowed_encodings: Vec<String>,
    pub max_payload_size: u64,
    pub max_custom_entries: u32,
    pub max_custom_total_bytes: u64,
    pub max_custom_key_bytes: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum ServerInfoError {
    #[error("server info is missing required field `{0}`")]
    MissingField(&'static str),
    #[error("server_time_ms is not a representable time ({0}ms)")]
    InvalidServerTime(i64),
}

impl TryFrom<crate::pb::sepp::v1::GetServerInfoResponse> for ServerInfo {
    type Error = ServerInfoError;

    fn try_from(r: crate::pb::sepp::v1::GetServerInfoResponse) -> Result<Self, Self::Error> {
        if r.server_version.is_empty() {
            return Err(ServerInfoError::MissingField("server_version"));
        }
        let server_time = millis_to_system_time(r.server_time_ms)
            .ok_or(ServerInfoError::InvalidServerTime(r.server_time_ms))?;

        Ok(Self {
            version: r.server_version,
            supported_protocol_versions: r.supported_protocol_versions,
            server_time,
            restricts_encodings: r.restricts_encodings,
            allowed_encodings: r.allowed_encodings,
            max_payload_size: r.max_payload_bytes,
            max_custom_entries: r.max_custom_entries,
            max_custom_total_bytes: r.max_custom_total_bytes,
            max_custom_key_bytes: r.max_custom_key_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::sepp::v1 as pb;

    const VALID_TP: &str = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

    fn test_client() -> crate::client::SeppClient {
        let chan = tonic::transport::Endpoint::from_static("http://[::1]:1").connect_lazy();
        crate::client::SeppClient::from_channel(chan)
    }

    #[test]
    fn primitive_from_str() {
        assert_eq!(Primitive::from("hi"), Primitive::String("hi".into()));
    }

    #[test]
    fn primitive_from_string() {
        assert_eq!(
            Primitive::from(String::from("hi")),
            Primitive::String("hi".into())
        );
    }

    #[test]
    fn primitive_from_i64() {
        assert_eq!(Primitive::from(42_i64), Primitive::Int(42));
    }

    #[test]
    fn primitive_from_i32() {
        assert_eq!(Primitive::from(42_i32), Primitive::Int(42));
    }

    #[test]
    fn primitive_from_f64() {
        assert_eq!(Primitive::from(1.5_f64), Primitive::Double(1.5));
    }

    #[test]
    fn primitive_from_bool() {
        assert_eq!(Primitive::from(true), Primitive::Bool(true));
    }

    #[test]
    fn primitive_to_pb_string() {
        let pb: pb::PrimitiveValue = Primitive::String("x".into()).into();
        assert!(
            matches!(pb.value, Some(pb::primitive_value::Value::StringValue(ref s)) if s == "x")
        );
    }

    #[test]
    fn primitive_to_pb_int() {
        let pb: pb::PrimitiveValue = Primitive::Int(7).into();
        assert!(matches!(
            pb.value,
            Some(pb::primitive_value::Value::IntValue(7))
        ));
    }

    #[test]
    fn primitive_to_pb_double() {
        let pb: pb::PrimitiveValue = Primitive::Double(2.5).into();
        assert!(matches!(
            pb.value,
            Some(pb::primitive_value::Value::DoubleValue(d)) if d == 2.5
        ));
    }

    #[test]
    fn primitive_to_pb_bool() {
        let pb: pb::PrimitiveValue = Primitive::Bool(false).into();
        assert!(matches!(
            pb.value,
            Some(pb::primitive_value::Value::BoolValue(false))
        ));
    }

    #[test]
    fn payload_to_pb() {
        let p = Payload {
            data: vec![1, 2, 3],
            encoding: "json".into(),
        };
        let pb: pb::Payload = p.into();
        assert_eq!(pb.data, vec![1, 2, 3]);
        assert_eq!(pb.encoding, "json");
    }

    #[test]
    fn payload_from_pb() {
        let pb = pb::Payload {
            data: vec![9],
            encoding: "raw".into(),
        };
        let p: Payload = pb.into();
        assert_eq!(p.data, vec![9]);
        assert_eq!(p.encoding, "raw");
    }

    #[test]
    fn priority_zero_valid() {
        assert_eq!(Priority::new(0).unwrap().get(), 0);
    }

    #[test]
    fn priority_mid_valid() {
        assert_eq!(Priority::new(5).unwrap().get(), 5);
    }

    #[test]
    fn priority_max_valid() {
        assert_eq!(Priority::new(9).unwrap().get(), 9);
    }

    #[test]
    fn priority_above_max_rejected() {
        assert!(matches!(Priority::new(10), Err(PriorityOutOfRange(10))));
    }

    #[test]
    fn priority_u8_max_rejected() {
        assert!(matches!(
            Priority::new(u8::MAX),
            Err(PriorityOutOfRange(255))
        ));
    }

    #[test]
    fn priority_try_from_ok() {
        let p: Priority = 5u8.try_into().unwrap();
        assert_eq!(p.get(), 5);
    }

    #[test]
    fn priority_try_from_err() {
        assert!(<Priority as TryFrom<u8>>::try_from(11).is_err());
    }

    #[test]
    fn priority_constants() {
        assert_eq!(Priority::MIN.get(), 0);
        assert_eq!(Priority::MAX.get(), 9);
    }

    #[test]
    fn traceparent_valid() {
        assert!(TraceContext::new(VALID_TP).is_ok());
    }

    #[test]
    fn traceparent_wrong_field_count() {
        assert!(matches!(
            TraceContext::new("00-deadbeef-0123"),
            Err(TraceContextError::InvalidTraceparent(_))
        ));
    }

    #[test]
    fn traceparent_bad_version_length() {
        let tp = "0-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        assert!(TraceContext::new(tp).is_err());
    }

    #[test]
    fn traceparent_non_hex_version() {
        let tp = "0g-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        assert!(TraceContext::new(tp).is_err());
    }

    #[test]
    fn traceparent_bad_trace_id_length() {
        let tp = "00-deadbeef-0123456789abcdef-01";
        assert!(TraceContext::new(tp).is_err());
    }

    #[test]
    fn traceparent_non_hex_trace_id() {
        let tp = "00-0123456789abcdeg0123456789abcdef-0123456789abcdef-01";
        assert!(TraceContext::new(tp).is_err());
    }

    #[test]
    fn traceparent_all_zero_trace_id_rejected() {
        let tp = "00-00000000000000000000000000000000-0123456789abcdef-01";
        assert!(TraceContext::new(tp).is_err());
    }

    #[test]
    fn traceparent_bad_span_id_length() {
        let tp = "00-0123456789abcdef0123456789abcdef-deadbeef-01";
        assert!(TraceContext::new(tp).is_err());
    }

    #[test]
    fn traceparent_non_hex_span_id() {
        let tp = "00-0123456789abcdef0123456789abcdef-0123456789abcdeg-01";
        assert!(TraceContext::new(tp).is_err());
    }

    #[test]
    fn traceparent_all_zero_span_id_rejected() {
        let tp = "00-0123456789abcdef0123456789abcdef-0000000000000000-01";
        assert!(TraceContext::new(tp).is_err());
    }

    #[test]
    fn traceparent_bad_flags_length() {
        let tp = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-0";
        assert!(TraceContext::new(tp).is_err());
    }

    #[test]
    fn traceparent_non_hex_flags() {
        let tp = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-0g";
        assert!(TraceContext::new(tp).is_err());
    }

    #[test]
    fn trace_context_with_tracestate() {
        let tc = TraceContext::new(VALID_TP)
            .unwrap()
            .with_tracestate("vendor=abc");
        assert_eq!(tc.traceparent(), VALID_TP);
        assert_eq!(tc.tracestate(), Some("vendor=abc"));
    }

    #[test]
    fn trace_context_without_tracestate() {
        let tc = TraceContext::new(VALID_TP).unwrap();
        assert!(tc.tracestate().is_none());
    }

    #[test]
    fn trace_context_to_pb() {
        let tc = TraceContext::new(VALID_TP).unwrap().with_tracestate("v=1");
        let pb: pb::TraceContext = tc.into();
        assert_eq!(pb.traceparent, VALID_TP);
        assert_eq!(pb.tracestate.as_deref(), Some("v=1"));
    }

    #[test]
    fn trace_context_try_from_pb_ok() {
        let pb = pb::TraceContext {
            traceparent: VALID_TP.into(),
            tracestate: Some("v=1".into()),
        };
        let tc = TraceContext::try_from(pb).unwrap();
        assert_eq!(tc.traceparent(), VALID_TP);
        assert_eq!(tc.tracestate(), Some("v=1"));
    }

    #[test]
    fn trace_context_try_from_pb_propagates_validation_error() {
        let pb = pb::TraceContext {
            traceparent: "garbage".into(),
            tracestate: None,
        };
        assert!(TraceContext::try_from(pb).is_err());
    }

    #[test]
    fn enqueue_request_empty_queue_rejected() {
        assert!(matches!(
            EnqueueRequest::new("", "type"),
            Err(EnqueueRequestBuilderError::EmptyQueue)
        ));
    }

    #[test]
    fn enqueue_request_empty_job_type_rejected() {
        assert!(matches!(
            EnqueueRequest::new("q", ""),
            Err(EnqueueRequestBuilderError::EmptyJobType)
        ));
    }

    #[test]
    fn enqueue_request_to_pb_minimal() {
        let req = EnqueueRequest::new("q", "t").unwrap();
        let pb: pb::EnqueueRequest = req.into();
        assert_eq!(pb.queue, "q");
        assert_eq!(pb.job_type, "t");
        assert!(pb.payload.is_none());
        assert!(pb.idempotency_key.is_none());
        assert!(pb.priority.is_none());
        assert!(pb.max_attempts.is_none());
        assert!(pb.custom.is_empty());
        assert!(pb.trace_context.is_none());
        assert!(pb.scheduled_at.is_none());
    }

    #[test]
    fn enqueue_request_to_pb_all_fields() {
        let mut custom = HashMap::new();
        custom.insert("k".into(), Primitive::Int(1));
        let req = EnqueueRequest::new("q", "t")
            .unwrap()
            .with_payload(Payload {
                data: vec![1],
                encoding: "raw".into(),
            })
            .with_idempotency_key("idem")
            .with_priority(Priority::new(7).unwrap())
            .with_max_attempts(5)
            .with_custom(custom)
            .with_trace_context(TraceContext::new(VALID_TP).unwrap())
            .with_scheduled_at(SystemTime::UNIX_EPOCH + Duration::from_millis(1234));
        let pb: pb::EnqueueRequest = req.into();
        assert_eq!(pb.queue, "q");
        assert_eq!(pb.job_type, "t");
        assert_eq!(pb.payload.as_ref().unwrap().encoding, "raw");
        assert_eq!(pb.payload.as_ref().unwrap().data, vec![1]);
        assert_eq!(pb.idempotency_key.as_deref(), Some("idem"));
        assert_eq!(pb.priority, Some(7));
        assert_eq!(pb.max_attempts, Some(5));
        assert_eq!(pb.custom.len(), 1);
        assert!(pb.trace_context.is_some());
        assert_eq!(pb.scheduled_at, Some(1234));
    }

    #[test]
    fn enqueue_request_scheduled_at_pre_epoch_becomes_none() {
        let req = EnqueueRequest::new("q", "t")
            .unwrap()
            .with_scheduled_at(SystemTime::UNIX_EPOCH - Duration::from_secs(1));
        let pb: pb::EnqueueRequest = req.into();
        assert!(pb.scheduled_at.is_none());
    }

    fn ack(id: &str) -> EnqueueAck {
        EnqueueAck {
            job_id: id.into(),
            deduplicated: false,
        }
    }

    #[test]
    fn batch_outcome_all_succeeded_true_when_empty() {
        let bo = BatchOutcome { results: vec![] };
        assert!(bo.all_succeeded());
    }

    #[test]
    fn batch_outcome_all_succeeded_true() {
        let bo = BatchOutcome {
            results: vec![Ok(ack("a")), Ok(ack("b"))],
        };
        assert!(bo.all_succeeded());
    }

    #[test]
    fn batch_outcome_all_succeeded_false_when_any_err() {
        let bo = BatchOutcome {
            results: vec![Ok(ack("a")), Err(JobRejection::Unknown)],
        };
        assert!(!bo.all_succeeded());
    }

    #[test]
    fn batch_outcome_rejected_indexes() {
        let bo = BatchOutcome {
            results: vec![Ok(ack("a")), Err(JobRejection::Unknown), Ok(ack("c"))],
        };
        let indexes: Vec<usize> = bo.rejected().map(|(i, _)| i).collect();
        assert_eq!(indexes, vec![1]);
    }

    #[test]
    fn batch_outcome_succeeded_indexes() {
        let bo = BatchOutcome {
            results: vec![Ok(ack("a")), Err(JobRejection::Unknown), Ok(ack("c"))],
        };
        let indexes: Vec<usize> = bo.succeeded().map(|(i, _)| i).collect();
        assert_eq!(indexes, vec![0, 2]);
    }

    #[test]
    fn batch_outcome_results_borrow() {
        let bo = BatchOutcome {
            results: vec![Ok(ack("a"))],
        };
        assert_eq!(bo.results().len(), 1);
    }

    #[test]
    fn batch_outcome_into_results_yields_inner() {
        let bo = BatchOutcome {
            results: vec![Ok(ack("a"))],
        };
        assert_eq!(bo.into_results().len(), 1);
    }

    #[test]
    fn enqueue_ack_from_pb() {
        let pb = pb::EnqueueResponse {
            job_id: "abc".into(),
            deduplicated: true,
        };
        let ack = EnqueueAck::from(pb);
        assert_eq!(ack.job_id, "abc");
        assert!(ack.deduplicated);
    }

    fn rej(reason: pb::job_rejection::Reason) -> pb::JobRejection {
        pb::JobRejection {
            reason: Some(reason),
        }
    }

    #[test]
    fn job_rejection_unknown_queue() {
        let pb = rej(pb::job_rejection::Reason::UnknownQueue(pb::UnknownQueue {
            queue: "q".into(),
        }));
        assert!(
            matches!(JobRejection::from(pb), JobRejection::UnknownQueue { queue } if queue == "q")
        );
    }

    #[test]
    fn job_rejection_payload_too_large() {
        let pb = rej(pb::job_rejection::Reason::PayloadTooLarge(
            pb::PayloadTooLarge {
                limit: 10,
                actual: 20,
            },
        ));
        assert!(matches!(
            JobRejection::from(pb),
            JobRejection::PayloadTooLarge {
                limit: 10,
                actual: 20
            }
        ));
    }

    #[test]
    fn job_rejection_encoding_not_allowed() {
        let pb = rej(pb::job_rejection::Reason::EncodingNotAllowed(
            pb::EncodingNotAllowed {
                encoding: "gzip".into(),
                allowed: vec!["json".into()],
            },
        ));
        match JobRejection::from(pb) {
            JobRejection::EncodingNotAllowed { encoding, allowed } => {
                assert_eq!(encoding, "gzip");
                assert_eq!(allowed, vec!["json".to_string()]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn job_rejection_job_type_not_allowed() {
        let pb = rej(pb::job_rejection::Reason::JobTypeNotAllowed(
            pb::JobTypeNotAllowed {
                job_type: "x".into(),
                allowed: vec!["y".into()],
            },
        ));
        match JobRejection::from(pb) {
            JobRejection::JobTypeNotAllowed { job_type, allowed } => {
                assert_eq!(job_type, "x");
                assert_eq!(allowed, vec!["y".to_string()]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn job_rejection_custom_entries_too_many() {
        let pb = rej(pb::job_rejection::Reason::CustomEntriesTooMany(
            pb::CustomEntriesTooMany {
                limit: 5,
                actual: 6,
            },
        ));
        assert!(matches!(
            JobRejection::from(pb),
            JobRejection::CustomEntriesTooMany {
                limit: 5,
                actual: 6
            }
        ));
    }

    #[test]
    fn job_rejection_custom_map_too_large() {
        let pb = rej(pb::job_rejection::Reason::CustomMapTooLarge(
            pb::CustomMapTooLarge {
                limit: 100,
                actual: 200,
            },
        ));
        assert!(matches!(
            JobRejection::from(pb),
            JobRejection::CustomMapTooLarge {
                limit: 100,
                actual: 200
            }
        ));
    }

    #[test]
    fn job_rejection_custom_key_too_long() {
        let pb = rej(pb::job_rejection::Reason::CustomKeyTooLong(
            pb::CustomKeyTooLong {
                key: "k".into(),
                limit: 1,
                actual: 2,
            },
        ));
        match JobRejection::from(pb) {
            JobRejection::CustomKeyTooLong { key, limit, actual } => {
                assert_eq!(key, "k");
                assert_eq!(limit, 1);
                assert_eq!(actual, 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn job_rejection_queue_name_too_long() {
        let pb = rej(pb::job_rejection::Reason::QueueNameTooLong(
            pb::QueueNameTooLong {
                limit: 1,
                actual: 2,
            },
        ));
        assert!(matches!(
            JobRejection::from(pb),
            JobRejection::QueueNameTooLong {
                limit: 1,
                actual: 2
            }
        ));
    }

    #[test]
    fn job_rejection_job_type_name_too_long() {
        let pb = rej(pb::job_rejection::Reason::JobTypeNameTooLong(
            pb::JobTypeNameTooLong {
                limit: 3,
                actual: 4,
            },
        ));
        assert!(matches!(
            JobRejection::from(pb),
            JobRejection::JobTypeNameTooLong {
                limit: 3,
                actual: 4
            }
        ));
    }

    #[test]
    fn job_rejection_idempotency_key_too_long() {
        let pb = rej(pb::job_rejection::Reason::IdempotencyKeyTooLong(
            pb::IdempotencyKeyTooLong {
                limit: 8,
                actual: 9,
            },
        ));
        assert!(matches!(
            JobRejection::from(pb),
            JobRejection::IdempotencyKeyTooLong {
                limit: 8,
                actual: 9
            }
        ));
    }

    #[test]
    fn job_rejection_scheduled_too_far() {
        let pb = rej(pb::job_rejection::Reason::ScheduledTooFar(
            pb::ScheduledTooFar {
                horizon_ms: 60_000,
                actual_ms: 120_000,
            },
        ));
        assert!(matches!(
            JobRejection::from(pb),
            JobRejection::ScheduledTooFar {
                horizon_ms: 60_000,
                actual_ms: 120_000
            }
        ));
    }

    #[test]
    fn job_rejection_invalid_request() {
        let pb = rej(pb::job_rejection::Reason::InvalidRequest(
            pb::InvalidRequest {
                message: "oops".into(),
            },
        ));
        match JobRejection::from(pb) {
            JobRejection::InvalidRequest { message } => assert_eq!(message, "oops"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn job_rejection_none_is_unknown() {
        let pb = pb::JobRejection { reason: None };
        assert!(matches!(JobRejection::from(pb), JobRejection::Unknown));
    }

    #[test]
    fn job_validation_error_from_pb() {
        let pb = pb::JobValidationError {
            index: 3,
            rejection: Some(rej(pb::job_rejection::Reason::UnknownQueue(
                pb::UnknownQueue { queue: "q".into() },
            ))),
        };
        let e = JobValidationError::from(pb);
        assert_eq!(e.index, 3);
        assert!(matches!(e.rejection, JobRejection::UnknownQueue { queue } if queue == "q"));
    }

    #[test]
    fn job_validation_error_missing_rejection_is_unknown() {
        let pb = pb::JobValidationError {
            index: 1,
            rejection: None,
        };
        let e = JobValidationError::from(pb);
        assert!(matches!(e.rejection, JobRejection::Unknown));
    }

    fn valid_job_pb() -> pb::Job {
        pb::Job {
            id: "550e8400-e29b-41d4-a716-446655440000".into(),
            job_type: "send_email".into(),
            payload: None,
            priority: 3,
            trace_context: None,
            enqueued_at: 1_700_000_000_000,
            attempt: 1,
            max_attempts: 5,
            lease_expires_at: 1_700_000_060_000,
            custom: HashMap::new(),
            scheduled_at: None,
        }
    }

    #[tokio::test]
    async fn job_from_pb_happy_path() {
        let client = test_client();
        let mut p = valid_job_pb();
        p.payload = Some(pb::Payload {
            data: vec![1, 2],
            encoding: "json".into(),
        });
        p.custom.insert(
            "k".into(),
            pb::PrimitiveValue {
                value: Some(pb::primitive_value::Value::StringValue("v".into())),
            },
        );
        let job = job_from_pb(&client, p, None).unwrap();
        assert_eq!(job.ctx.id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(job.ctx.job_type, "send_email");
        assert_eq!(job.ctx.priority.get(), 3);
        assert_eq!(job.ctx.attempt, 1);
        assert_eq!(job.ctx.max_attempts, 5);
        assert_eq!(job.payload.as_ref().unwrap().encoding, "json");
        assert_eq!(
            job.ctx.custom.get("k"),
            Some(&Primitive::String("v".into()))
        );
    }

    #[tokio::test]
    async fn job_from_pb_missing_id() {
        let client = test_client();
        let mut p = valid_job_pb();
        p.id.clear();
        assert!(matches!(
            job_from_pb(&client, p, None),
            Err(JobConversionError::MissingField("id"))
        ));
    }

    #[tokio::test]
    async fn job_from_pb_missing_job_type() {
        let client = test_client();
        let mut p = valid_job_pb();
        p.job_type.clear();
        assert!(matches!(
            job_from_pb(&client, p, None),
            Err(JobConversionError::MissingField("job_type"))
        ));
    }

    #[tokio::test]
    async fn job_from_pb_priority_out_of_range() {
        let client = test_client();
        let mut p = valid_job_pb();
        p.priority = 10;
        assert!(matches!(
            job_from_pb(&client, p, None),
            Err(JobConversionError::PriorityOutOfRange(10))
        ));
    }

    #[tokio::test]
    async fn job_from_pb_priority_above_u8() {
        let client = test_client();
        let mut p = valid_job_pb();
        p.priority = 300;
        assert!(matches!(
            job_from_pb(&client, p, None),
            Err(JobConversionError::PriorityOutOfRange(300))
        ));
    }

    #[tokio::test]
    async fn job_from_pb_invalid_enqueued_at() {
        let client = test_client();
        let mut p = valid_job_pb();
        p.enqueued_at = -1;
        assert!(matches!(
            job_from_pb(&client, p, None),
            Err(JobConversionError::InvalidTimestamp {
                field: "enqueued_at",
                value: -1
            })
        ));
    }

    #[tokio::test]
    async fn job_from_pb_invalid_lease_expires_at() {
        let client = test_client();
        let mut p = valid_job_pb();
        p.lease_expires_at = -5;
        assert!(matches!(
            job_from_pb(&client, p, None),
            Err(JobConversionError::InvalidTimestamp {
                field: "lease_expires_at",
                value: -5
            })
        ));
    }

    #[tokio::test]
    async fn job_from_pb_empty_custom_value() {
        let client = test_client();
        let mut p = valid_job_pb();
        p.custom
            .insert("k".into(), pb::PrimitiveValue { value: None });
        match job_from_pb(&client, p, None) {
            Err(JobConversionError::EmptyCustomValue(k)) => assert_eq!(k, "k"),
            _ => panic!("expected EmptyCustomValue"),
        }
    }

    #[tokio::test]
    async fn job_from_pb_drops_invalid_trace_context() {
        let client = test_client();
        let mut p = valid_job_pb();
        p.trace_context = Some(pb::TraceContext {
            traceparent: "garbage".into(),
            tracestate: None,
        });
        let job = job_from_pb(&client, p, None).unwrap();
        assert!(job.ctx.trace_context.is_none());
    }

    #[tokio::test]
    async fn job_from_pb_preserves_valid_trace_context() {
        let client = test_client();
        let mut p = valid_job_pb();
        p.trace_context = Some(pb::TraceContext {
            traceparent: VALID_TP.into(),
            tracestate: Some("v=1".into()),
        });
        let job = job_from_pb(&client, p, None).unwrap();
        let tc = job.ctx.trace_context.as_ref().unwrap();
        assert_eq!(tc.traceparent(), VALID_TP);
        assert_eq!(tc.tracestate(), Some("v=1"));
    }

    #[test]
    fn reserve_opts_empty_queues() {
        let r = ReserveOptions::new(Vec::<String>::new(), Duration::from_secs(1));
        assert!(matches!(r, Err(ReserveOptionsError::EmptyQueues)));
    }

    #[test]
    fn reserve_opts_empty_queue_name_at_index() {
        let r = ReserveOptions::new(["ok", ""], Duration::from_secs(1));
        assert!(matches!(r, Err(ReserveOptionsError::EmptyQueueName(1))));
    }

    #[test]
    fn reserve_opts_zero_lease() {
        let r = ReserveOptions::new(["q"], Duration::ZERO);
        assert!(matches!(r, Err(ReserveOptionsError::LeaseDurationTooShort)));
    }

    #[test]
    fn reserve_opts_with_worker_id_empty_rejected() {
        let opts = ReserveOptions::new(["q"], Duration::from_secs(1)).unwrap();
        assert!(matches!(
            opts.with_worker_id(""),
            Err(ReserveOptionsError::EmptyWorkerId)
        ));
    }

    #[test]
    fn reserve_opts_default_wait_timeout_30s() {
        let opts = ReserveOptions::new(["q"], Duration::from_secs(1)).unwrap();
        assert_eq!(opts.wait_timeout(), Duration::from_secs(30));
    }

    #[test]
    fn reserve_opts_with_wait_timeout_overrides() {
        let opts = ReserveOptions::new(["q"], Duration::from_secs(1))
            .unwrap()
            .with_wait_timeout(Duration::from_millis(500));
        assert_eq!(opts.wait_timeout(), Duration::from_millis(500));
    }

    #[test]
    fn reserve_opts_to_pb_all_fields() {
        let opts = ReserveOptions::new(["q1", "q2"], Duration::from_millis(5_000))
            .unwrap()
            .with_wait_timeout(Duration::from_millis(2_000))
            .with_worker_id("w")
            .unwrap()
            .with_max_jobs(7);
        let pb: pb::ReserveRequest = opts.into();
        assert_eq!(pb.queues, vec!["q1".to_string(), "q2".to_string()]);
        assert_eq!(pb.wait_timeout_ms, 2_000);
        assert_eq!(pb.lease_duration_ms, 5_000);
        assert_eq!(pb.worker_id.as_deref(), Some("w"));
        assert_eq!(pb.max_jobs, Some(7));
    }

    #[test]
    fn reserve_opts_to_pb_by_ref_matches_by_value() {
        let opts = ReserveOptions::new(["q"], Duration::from_millis(1_000)).unwrap();
        let by_ref: pb::ReserveRequest = (&opts).into();
        let by_val: pb::ReserveRequest = opts.into();
        assert_eq!(by_ref, by_val);
    }

    #[test]
    fn millis_to_system_time_zero() {
        assert_eq!(millis_to_system_time(0), Some(SystemTime::UNIX_EPOCH));
    }

    #[test]
    fn millis_to_system_time_negative_is_none() {
        assert!(millis_to_system_time(-1).is_none());
    }

    #[test]
    fn millis_to_system_time_positive() {
        let t = millis_to_system_time(1_700_000_000_000).unwrap();
        assert_eq!(
            t,
            SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_000)
        );
    }

    #[test]
    fn system_time_to_millis_at_epoch_is_zero() {
        assert_eq!(system_time_to_millis(SystemTime::UNIX_EPOCH), 0);
    }

    #[test]
    fn system_time_to_millis_pre_epoch_returns_zero() {
        let t = SystemTime::UNIX_EPOCH - Duration::from_secs(1);
        assert_eq!(system_time_to_millis(t), 0);
    }

    #[test]
    fn system_time_to_millis_round_trip() {
        let t = SystemTime::UNIX_EPOCH + Duration::from_millis(42);
        assert_eq!(system_time_to_millis(t), 42);
    }

    fn valid_server_info_pb() -> pb::GetServerInfoResponse {
        pb::GetServerInfoResponse {
            server_version: "1.2.3".into(),
            supported_protocol_versions: vec!["v1".into()],
            server_time_ms: 1_700_000_000_000,
            restricts_encodings: false,
            allowed_encodings: vec!["json".into()],
            max_payload_bytes: 1024,
            max_custom_entries: 10,
            max_custom_total_bytes: 2048,
            max_custom_key_bytes: 64,
        }
    }

    #[test]
    fn server_info_missing_version() {
        let mut p = valid_server_info_pb();
        p.server_version.clear();
        assert!(matches!(
            ServerInfo::try_from(p),
            Err(ServerInfoError::MissingField("server_version"))
        ));
    }

    #[test]
    fn server_info_invalid_server_time() {
        let mut p = valid_server_info_pb();
        p.server_time_ms = -1;
        assert!(matches!(
            ServerInfo::try_from(p),
            Err(ServerInfoError::InvalidServerTime(-1))
        ));
    }

    #[test]
    fn server_info_happy_path() {
        let info = ServerInfo::try_from(valid_server_info_pb()).unwrap();
        assert_eq!(info.version, "1.2.3");
        assert_eq!(info.supported_protocol_versions, vec!["v1".to_string()]);
        assert_eq!(info.allowed_encodings, vec!["json".to_string()]);
        assert!(!info.restricts_encodings);
        assert_eq!(info.max_payload_size, 1024);
        assert_eq!(info.max_custom_entries, 10);
        assert_eq!(info.max_custom_total_bytes, 2048);
        assert_eq!(info.max_custom_key_bytes, 64);
        assert_eq!(
            info.server_time,
            SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_000)
        );
    }
}
