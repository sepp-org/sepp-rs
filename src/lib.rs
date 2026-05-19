use std::{
    collections::HashMap,
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

// TODO: rethink this approach
#[derive(Debug)]
pub struct JobRejection {
    pub code: String,
    pub message: String,
    pub context: HashMap<String, String>,
}

impl From<crate::pb::sepp::v1::ErrorDetails> for JobRejection {
    fn from(e: crate::pb::sepp::v1::ErrorDetails) -> Self {
        Self {
            code: e.code,
            message: e.message,
            context: e.context,
        }
    }
}

#[derive(Debug)]
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
    ) -> Result<SystemTime, crate::client::ClientError> {
        self.lease.extend(extension).await
    }
}

#[derive(Debug)]
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

    let lease =
        crate::client::Lease::new(client.clone(), j.id.clone(), j.attempt, lease_expires_at);

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

#[derive(Debug, Clone)]
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

#[derive(Debug)]
pub struct ServerInfo {
    pub version: String,
    pub supported_protocol_versions: Vec<String>,
    pub server_time: SystemTime,
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
            allowed_encodings: r.allowed_encodings,
            max_payload_size: r.max_payload_bytes,
            max_custom_entries: r.max_custom_entries,
            max_custom_total_bytes: r.max_custom_total_bytes,
            max_custom_key_bytes: r.max_custom_key_bytes,
        })
    }
}
