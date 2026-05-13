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
        Self { value: Some(value) } // ← wrap, then put Some around it
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
        use opentelemetry::propagation::{Injector, TextMapPropagator};
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

pub struct EnqueueRequest {
    pub queue: String,
    pub job_type: String,
    pub payload: Option<Payload>,
    pub idempotency_key: Option<String>,
    pub priority: Option<Priority>,
    pub max_attempts: Option<u32>,
    pub custom: HashMap<String, Primitive>,
    pub trace_context: Option<TraceContext>,
    pub scheduled_at: Option<SystemTime>,
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
}

pub struct Job {
    pub payload: Option<Payload>,
    pub ctx: JobCtx,
}

pub struct ReserveOptions {
    queues: Vec<String>,
    wait_timeout: Duration,
    lease_duration: Duration,
    worker_id: Option<String>,
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
        })
    }

    pub fn with_wait_timeout(mut self, wait: Duration) -> Self {
        self.wait_timeout = wait;
        self
    }

    pub fn with_worker_id(mut self, id: impl Into<String>) -> Result<Self, ReserveOptionsError> {
        let id = id.into();
        if id.is_empty() {
            return Err(ReserveOptionsError::EmptyWorkerId);
        }
        self.worker_id = Some(id);
        Ok(self)
    }
}

impl From<ReserveOptions> for crate::pb::sepp::v1::ReserveRequest {
    fn from(o: ReserveOptions) -> Self {
        Self {
            queues: o.queues,
            wait_timeout_ms: o.wait_timeout.as_millis() as u64,
            lease_duration_ms: o.lease_duration.as_millis() as u64,
            worker_id: o.worker_id,
        }
    }
}
