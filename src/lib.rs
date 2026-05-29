//! A Rust client for the [sepp](https://github.com/sepp-org/sepp) job queue.
//!
//! This client provides both halves of the job queue API:
//!
//! - **Producers** enqueue jobs with a [`SeppClient`](client::SeppClient) —
//!   one at a time, in best-effort batches or atomic batches.
//! - **Consumers** reserve jobs and report their outcome. The low-level
//!   [`reserve`](client::SeppClient::reserve) /
//!   [`ack`](client::SeppClient::ack) / [`nack`](client::SeppClient::nack)
//!   calls give you full manual control, while the high-level
//!   [`Worker`](worker::Worker) runs the whole reserve → process → ack loop for
//!   you with bounded concurrency, lease auto-extension, graceful shutdown and
//!   metrics.
//!
//! # Quickstart
//!
//! As this crate uses [tonic](https://docs.rs/tonic/latest/tonic/), the client is async only and requires a [tokio runtime](https://docs.rs/tokio/latest/tokio/).
//!
//! Enqueue a job, then run a worker that processes it:
//!
//! ```no_run
//! use std::time::Duration;
//! use sepp_rs::client::SeppClient;
//! use sepp_rs::worker::Worker;
//! use sepp_rs::{EnqueueRequest, Payload};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = SeppClient::connect("http://127.0.0.1:50051").await?;
//!
//!     // Producer: enqueue a job onto the `emails` queue.
//!     let ack = client
//!         .enqueue(
//!             EnqueueRequest::new("emails", "send_welcome")?
//!                 .with_payload(Payload::new(b"{\"user\":42}".to_vec(), "application/json")),
//!         )
//!         .await?;
//!     println!("enqueued job {}", ack.job_id);
//!
//!     // Consumer: process `send_welcome` jobs from the `emails` queue. A handler
//!     // returns `Ok(())` to ack the job, or a `HandlerError` to nack it.
//!     Worker::new(client, ["emails"], Duration::from_secs(30))?
//!         .handle("send_welcome", |payload, ctx| async move {
//!             println!("processing job {}", ctx.id);
//!             Ok(())
//!         })?
//!         .run()
//!         .await;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Concepts
//!
//! **Queues and job types.** Every job is enqueued onto a named queue and
//! tagged with a `job_type`. Workers reserve from one or more queues and
//! dispatch each job to the handler registered for its `job_type`.
//!
//! **[`Payload`](Payload).** Every job can carry an opaque blob of bytes plus an encoding hint.
//! You can use this to transport any data you want as long as producers and workers agree on the encoding.
//! For a primitive key-value map, use [`custom`](EnqueueRequest::with_custom_entry) instead.
//!
//! **Leases and redelivery.** A reserved job is leased to the worker for a
//! bounded duration. The worker must [`ack`](client::SeppClient::ack),
//! [`nack`](client::SeppClient::nack), or [`extend`](client::SeppClient::extend)
//! the lease before it expires; otherwise the server redelivers the job to
//! another worker (with [`attempt`](JobCtx::attempt) incremented) until
//! `max_attempts` is reached and it is dead-lettered. [`Worker`](worker::Worker)
//! can extend leases automatically — see
//! [`with_auto_extend`](worker::Worker::with_auto_extend).
//!
//! # Feature flags
//!
//! - **`opentelemetry`** *(enabled by default)* — emit OpenTelemetry-compatible
//!   `tracing` spans and metrics, and propagate W3C trace context from the
//!   producer's enqueue span to the worker's process span. The host application
//!   still owns the exporter; see the `traced` example.
//! - **`tls`** — enable TLS for the transport via the `tls_*` methods on
//!   [`SeppClientBuilder`](client::SeppClientBuilder).

use std::{
    collections::HashMap,
    fmt,
    time::{Duration, SystemTime},
};

mod pb;

pub mod client;
pub mod worker;

/// A JSON-primitive value stored in a job's [custom](EnqueueRequest::with_custom)
/// metadata map.
///
/// ```
/// use sepp_rs::Primitive;
///
/// let p: Primitive = "hello".into();
/// assert_eq!(p, Primitive::String("hello".to_string()));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum Primitive {
    /// A UTF-8 string.
    String(String),
    /// A 64-bit signed integer.
    Int(i64),
    /// A 64-bit float.
    Double(f64),
    /// A boolean.
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

/// The opaque body of a job, plus an encoding hint.
///
/// The queue never interprets `data`; it only carries the bytes. `encoding`
/// (for example `"application/json"` or `"text/plain"`) is a hint the producer
/// sets and the worker reads to decide how to deserialize the bytes. A queue
/// may restrict which encodings it accepts — see
/// [`JobRejection::EncodingNotAllowed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payload {
    /// The raw payload bytes.
    pub data: Vec<u8>,
    /// Encoding hint for the worker (e.g. a MIME type).
    pub encoding: String,
}

impl Payload {
    /// Creates a payload from its bytes and an encoding hint.
    pub fn new(data: Vec<u8>, encoding: impl Into<String>) -> Self {
        Self {
            data,
            encoding: encoding.into(),
        }
    }
}

impl From<Payload> for crate::pb::sepp::v1::Payload {
    fn from(p: Payload) -> Self {
        Self {
            data: p.data,
            encoding: p.encoding,
        }
    }
}

/// A job priority in the range `0..=9`, where higher values are dequeued first.
///
/// The type makes the valid range unrepresentable-if-invalid: construct one
/// with [`Priority::new`], the `TryFrom<u8>` impl, or one of the
/// [`P0`](Priority::P0)–[`P9`](Priority::P9) constants.
///
/// ```
/// use sepp_rs::Priority;
///
/// assert_eq!(Priority::new(7).unwrap(), Priority::P7);
/// assert!(Priority::new(10).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Priority(u8);

/// Returned by [`Priority::new`] when the value is greater than 9.
#[derive(Debug, thiserror::Error)]
#[error("priority must be 0-9, got {0}")]
pub struct PriorityOutOfRange(pub u8);

impl Priority {
    /// The lowest priority (`0`).
    pub const MIN: Self = Self(0);
    /// The highest priority (`9`).
    pub const MAX: Self = Self(9);

    /// Priority `0` (lowest).
    pub const P0: Self = Self(0);
    /// Priority `1`.
    pub const P1: Self = Self(1);
    /// Priority `2`.
    pub const P2: Self = Self(2);
    /// Priority `3`.
    pub const P3: Self = Self(3);
    /// Priority `4`.
    pub const P4: Self = Self(4);
    /// Priority `5`.
    pub const P5: Self = Self(5);
    /// Priority `6`.
    pub const P6: Self = Self(6);
    /// Priority `7`.
    pub const P7: Self = Self(7);
    /// Priority `8`.
    pub const P8: Self = Self(8);
    /// Priority `9` (highest).
    pub const P9: Self = Self(9);

    /// Creates a priority, returning [`PriorityOutOfRange`] if `value > 9`.
    pub fn new(value: u8) -> Result<Self, PriorityOutOfRange> {
        if value > Self::MAX.0 {
            Err(PriorityOutOfRange(value))
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the priority as a `u8` in `0..=9`.
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

/// A [W3C Trace Context](https://www.w3.org/TR/trace-context/) attached to a
/// job, used to link a producer's trace to the worker that processes the job.
///
/// The `traceparent` is validated on construction. With the `opentelemetry`
/// feature enabled, [`Worker`](worker::Worker) and the client wire this up
/// automatically — you only need this type to bridge to or from an external
/// trace propagation system by hand.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceContext {
    traceparent: String,
    tracestate: Option<String>,
}

/// Returned by [`TraceContext::new`] when the `traceparent` is not a
/// well-formed W3C value.
#[derive(Debug, thiserror::Error)]
pub enum TraceContextError {
    /// The `traceparent` string did not match the `version-trace_id-span_id-flags`
    /// shape; the payload explains which field was wrong.
    #[error("invalid traceparent: {0}")]
    InvalidTraceparent(&'static str),
}

impl TraceContext {
    /// Creates a trace context from a W3C `traceparent`, validating its format.
    ///
    /// The expected shape is `version-trace_id-span_id-flags`, e.g.
    /// `00-<32 hex>-<16 hex>-<2 hex>`.
    pub fn new(traceparent: impl Into<String>) -> Result<Self, TraceContextError> {
        let traceparent = traceparent.into();
        validate_traceparent(&traceparent)?;
        Ok(Self {
            traceparent,
            tracestate: None,
        })
    }

    /// Attaches a W3C `tracestate` (vendor-specific trace data).
    pub fn with_tracestate(mut self, ts: impl Into<String>) -> Self {
        self.tracestate = Some(ts.into());
        self
    }

    /// Returns the W3C `traceparent`.
    pub fn traceparent(&self) -> &str {
        &self.traceparent
    }
    /// Returns the W3C `tracestate`, if one was set.
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }
}

#[cfg(feature = "opentelemetry")]
impl TraceContext {
    /// Captures the current OpenTelemetry context as a `TraceContext`, or
    /// `None` if there is no valid active span.
    ///
    /// *Requires the `opentelemetry` feature.*
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

    /// Installs this trace context as the current OpenTelemetry context for as
    /// long as the returned guard is held.
    ///
    /// *Requires the `opentelemetry` feature.*
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

    /// Decodes this trace context into an OpenTelemetry [`SpanContext`], or
    /// `None` if it does not represent a valid span. Used to add a span *link*
    /// from the worker's process span back to the producer.
    ///
    /// [`SpanContext`]: opentelemetry::trace::SpanContext
    ///
    /// *Requires the `opentelemetry` feature.*
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

/// A job to enqueue, built fluently.
///
/// Start with [`EnqueueRequest::new`] (which requires a queue and a job type)
/// and layer on the optional fields with the `with_*` methods. Pass the result
/// to [`SeppClient::enqueue`](client::SeppClient::enqueue),
/// [`enqueue_batch`](client::SeppClient::enqueue_batch), or
/// [`enqueue_atomic`](client::SeppClient::enqueue_atomic).
///
/// ```
/// use sepp_rs::{EnqueueRequest, Payload, Priority};
///
/// # fn build() -> Result<EnqueueRequest, Box<dyn std::error::Error>> {
/// let req = EnqueueRequest::new("emails", "send_welcome")?
///     .with_payload(Payload::new(b"{}".to_vec(), "application/json"))
///     .with_priority(Priority::P7)
///     .with_idempotency_key("welcome-user-42")
///     .with_max_attempts(5);
/// # Ok(req)
/// # }
/// ```
///
/// Fields left unset fall back to the queue's configured defaults on the
/// server.
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

/// Returned by [`EnqueueRequest::new`] when the queue or job type is empty.
///
/// These are the only two fields validated client-side; all other constraints
/// (size limits, allowed encodings, …) are enforced by the server and surface
/// as a [`JobRejection`].
#[derive(Debug, thiserror::Error)]
pub enum EnqueueRequestBuilderError {
    /// The queue name was empty.
    #[error("queue name must not be empty")]
    EmptyQueue,
    /// The job type was empty.
    #[error("job type must not be empty")]
    EmptyJobType,
}

impl EnqueueRequest {
    /// Begins a request for the given queue and job type.
    ///
    /// Both must be non-empty, or [`EnqueueRequestBuilderError`] is returned.
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

    /// Sets the job's payload.
    pub fn with_payload(mut self, payload: Payload) -> Self {
        self.payload = Some(payload);
        self
    }

    /// Sets a deduplication key. The server drops a duplicate enqueue with the
    /// same key within the queue's dedup window, returning the existing job's
    /// id with [`EnqueueAck::deduplicated`] set.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Overrides the queue's default [`Priority`].
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Overrides the queue's default maximum delivery attempts before the job
    /// is dead-lettered.
    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = Some(attempts);
        self
    }

    /// Replaces the custom metadata map wholesale. See
    /// [`with_custom_entry`](Self::with_custom_entry) to add one key at a time.
    pub fn with_custom(mut self, custom: HashMap<String, Primitive>) -> Self {
        self.custom = custom; // This is cheap enough that Optional is not needed
        self
    }

    /// Inserts a single custom metadata entry. Any type that converts into a
    /// [`Primitive`] (`&str`, `i64`, `bool`, …) is accepted as the value.
    pub fn with_custom_entry(
        mut self,
        key: impl Into<String>,
        value: impl Into<Primitive>,
    ) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }

    /// Attaches a [`TraceContext`] to the job.
    ///
    /// With the `opentelemetry` feature, the current span's context is injected
    /// automatically at enqueue time, so set this only to override that or to
    /// propagate a trace captured elsewhere.
    pub fn with_trace_context(mut self, trace_context: TraceContext) -> Self {
        self.trace_context = Some(trace_context);
        self
    }

    /// Delays the job until the given time. The job is not delivered to any
    /// worker before then. Must be within the server's
    /// [`max_schedule_horizon_ms`](ServerInfo::max_schedule_horizon_ms), or the
    /// job is rejected with [`JobRejection::ScheduledTooFar`].
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

/// Confirmation that a job was accepted by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueAck {
    /// The server-assigned job id (a UUID). When `deduplicated` is `true`, this
    /// is the id of the pre-existing job.
    pub job_id: String,
    /// `true` if an [idempotency key](EnqueueRequest::with_idempotency_key)
    /// matched an existing job, so this enqueue was a no-op.
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

/// Why the server refused a single job.
///
/// Every variant is *deterministic*: re-sending the same job against the same
/// server state produces the same rejection. Transient problems (a storage
/// outage, a dropped connection) are never reported here — they surface as a
/// [`ClientError`](client::ClientError) instead. Most limits behind these
/// variants are advertised up front by [`ServerInfo`], so a producer can
/// validate locally before sending.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum JobRejection {
    /// The server is in [strict mode](ServerInfo::strict_queues) and the target
    /// queue has not been declared.
    #[error("queue {queue:?} is not declared on the server (strict mode)")]
    UnknownQueue { queue: String },
    /// The payload exceeds the queue's
    /// [`max_payload_size`](ServerInfo::max_payload_size).
    #[error("payload size {actual} bytes exceeds the queue limit of {limit}")]
    PayloadTooLarge { limit: u64, actual: u64 },
    /// The queue restricts encodings and the payload's encoding is not on the
    /// allow-list.
    #[error("payload encoding {encoding:?} is not allowed; accepted: {allowed:?}")]
    EncodingNotAllowed {
        encoding: String,
        allowed: Vec<String>,
    },
    /// The queue restricts job types and this one is not on the allow-list.
    #[error("job_type {job_type:?} is not accepted by this queue; accepted: {allowed:?}")]
    JobTypeNotAllowed {
        job_type: String,
        allowed: Vec<String>,
    },
    /// The custom map has more entries than
    /// [`max_custom_entries`](ServerInfo::max_custom_entries).
    #[error("custom map has {actual} entries, exceeding the queue limit of {limit}")]
    CustomEntriesTooMany { limit: u32, actual: u32 },
    /// The custom map's total size exceeds
    /// [`max_custom_total_bytes`](ServerInfo::max_custom_total_bytes).
    #[error("custom map's total size {actual} bytes exceeds the queue limit of {limit}")]
    CustomMapTooLarge { limit: u64, actual: u64 },
    /// A custom key exceeds
    /// [`max_custom_key_bytes`](ServerInfo::max_custom_key_bytes).
    #[error("custom key {key:?} is {actual} bytes, exceeding the limit of {limit}")]
    CustomKeyTooLong {
        key: String,
        limit: u32,
        actual: u64,
    },
    /// The queue name exceeds
    /// [`max_queue_name_bytes`](ServerInfo::max_queue_name_bytes).
    #[error("queue name is {actual} bytes, exceeding the limit of {limit}")]
    QueueNameTooLong { limit: u32, actual: u64 },
    /// The job type exceeds
    /// [`max_job_type_bytes`](ServerInfo::max_job_type_bytes).
    #[error("job_type is {actual} bytes, exceeding the limit of {limit}")]
    JobTypeNameTooLong { limit: u32, actual: u64 },
    /// The idempotency key exceeds
    /// [`max_idempotency_key_bytes`](ServerInfo::max_idempotency_key_bytes).
    #[error("idempotency_key is {actual} bytes, exceeding the limit of {limit}")]
    IdempotencyKeyTooLong { limit: u32, actual: u64 },
    /// [`scheduled_at`](EnqueueRequest::with_scheduled_at) is further out than
    /// [`max_schedule_horizon_ms`](ServerInfo::max_schedule_horizon_ms).
    #[error("scheduled_at {actual_ms}ms is beyond max_schedule_horizon_ms ({horizon_ms}ms)")]
    ScheduledTooFar { horizon_ms: u64, actual_ms: i64 },
    /// The request failed the server's structural validation (e.g. a required
    /// field was missing); `message` carries the detail.
    #[error("structural validation failed: {message}")]
    InvalidRequest { message: String },
    /// The server sent a rejection reason this client version does not
    /// recognize.
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

/// A single job's rejection within an atomic batch, paired with its position
/// in the request so the caller can identify which job failed.
///
/// Collected into [`AtomicEnqueueError::Validation`] when
/// [`enqueue_atomic`](client::SeppClient::enqueue_atomic) rejects a batch.
#[derive(Debug, Clone, PartialEq)]
pub struct JobValidationError {
    /// Zero-based position of the offending job in the submitted batch.
    pub index: u32,
    /// Why that job was rejected.
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

/// The error type of [`enqueue_atomic`](client::SeppClient::enqueue_atomic).
///
/// An atomic batch is all-or-nothing: if any job fails validation, *none* are
/// enqueued and every failure is reported together in [`Validation`].
///
/// [`Validation`]: AtomicEnqueueError::Validation
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AtomicEnqueueError {
    /// The call failed at the transport or protocol level; nothing about
    /// individual jobs is known.
    #[error(transparent)]
    Client(#[from] crate::client::ClientError),
    /// One or more jobs failed validation, so the whole batch was rejected.
    #[error("atomic batch rejected: {} job(s) failed validation", _0.len())]
    Validation(Vec<JobValidationError>),
}

impl From<tonic::Status> for AtomicEnqueueError {
    fn from(s: tonic::Status) -> Self {
        Self::Client(crate::client::ClientError::from(s))
    }
}

/// Everything about a reserved job except its payload: identity, delivery
/// metadata, and the handle needed to manage its lease.
///
/// A handler receives this as `Arc<JobCtx>` alongside the payload. It also
/// carries an internal lease handle, which is why [`extend`](JobCtx::extend)
/// can be called directly on it.
#[derive(Debug, Clone)]
pub struct JobCtx {
    /// Server-assigned job id (a UUID).
    pub id: String,
    /// The job type, used to route to a handler.
    pub job_type: String,
    /// The job's effective priority.
    pub priority: Priority,
    /// Which delivery attempt this is, starting at `1` and incremented on each
    /// redelivery.
    pub attempt: u32,
    /// The maximum attempts before the job is dead-lettered.
    pub max_attempts: u32,
    /// When the producer enqueued the job.
    pub enqueued_at: SystemTime,
    /// Custom metadata the producer attached.
    pub custom: HashMap<String, Primitive>,
    /// The producer's trace context, if any and if it was well-formed.
    pub trace_context: Option<TraceContext>,
    /// When the current lease expires. The job must be acked, nacked, or
    /// extended before this, or it will be redelivered.
    pub lease_expires_at: SystemTime,
    pub(crate) lease: crate::client::Lease,
}

impl JobCtx {
    /// Extends this job's lease by `extension`, measured from now, and returns
    /// the new expiry.
    ///
    /// Use this from inside a handler that needs more time than the original
    /// lease allowed. A [`Worker`](worker::Worker) configured with
    /// [`with_auto_extend`](worker::Worker::with_auto_extend) does this for you.
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

/// A reserved job: its optional [`Payload`] and its [`JobCtx`].
///
/// Returned in batches by [`SeppClient::reserve`](client::SeppClient::reserve).
/// Under a [`Worker`](worker::Worker), the payload and an `Arc<JobCtx>` are
/// passed to your handler directly, so you usually deal with the two halves
/// rather than this struct.
#[derive(Debug, Clone)]
pub struct Job {
    /// The job's payload, if it has one.
    pub payload: Option<Payload>,
    /// The job's identity, delivery metadata, and lease handle.
    pub ctx: JobCtx,
}

/// Returned when a job received from the server cannot be decoded into a
/// [`Job`].
///
/// During [`reserve`](client::SeppClient::reserve) this is logged and the
/// offending job is skipped rather than failing the whole batch, so you
/// normally only encounter it indirectly.
#[derive(Debug, thiserror::Error)]
pub enum JobConversionError {
    /// A required field (named in the payload) was absent or empty.
    #[error("job is missing required field `{0}`")]
    MissingField(&'static str),
    /// The priority was outside `0..=9`.
    #[error("job priority {0} is out of range (expected 0-9)")]
    PriorityOutOfRange(u32),
    /// A timestamp field held a value that is not a representable
    /// [`SystemTime`].
    #[error("job timestamp `{field}` is not a representable time ({value}ms)")]
    InvalidTimestamp { field: &'static str, value: i64 },
    /// A custom map entry was present but carried no value.
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

/// Parameters for a [`reserve`](client::SeppClient::reserve) call.
///
/// Constructed with [`ReserveOptions::new`] (which needs the queues to pull
/// from and the lease duration) and refined with the `with_*` methods. If you
/// use a [`Worker`](worker::Worker), it builds and manages these for you.
#[derive(Debug, Clone, PartialEq)]
pub struct ReserveOptions {
    queues: Vec<String>,
    wait_timeout: Duration,
    lease_duration: Duration,
    worker_id: Option<String>,
    max_jobs: Option<u32>,
}

/// Returned by [`ReserveOptions`] constructors and setters on invalid input.
#[derive(Debug, thiserror::Error)]
pub enum ReserveOptionsError {
    /// No queues were given.
    #[error("at least one queue must be specified")]
    EmptyQueues,
    /// The queue name at this index was empty.
    #[error("queue name at index {0} must not be empty")]
    EmptyQueueName(usize),
    /// The lease duration was zero.
    #[error("lease_duration must be at least 1ms")]
    LeaseDurationTooShort,
    /// A worker id was supplied but empty.
    #[error("worker_id must not be empty when set")]
    EmptyWorkerId,
}

impl ReserveOptions {
    /// Begins reserve options for the given queues and lease duration.
    ///
    /// Queues are polled in order: index 0 first, then index 1, and so on. The
    /// lease duration is how long a returned job is held before it must be
    /// acked, nacked, or extended. The wait timeout defaults to 30s; override
    /// it with [`with_wait_timeout`](Self::with_wait_timeout).
    ///
    /// At least one non-empty queue and a non-zero lease are required.
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

    /// Sets how long the server holds the connection open waiting for a job
    /// before returning empty. The server clamps this to its configured
    /// [`max_wait_timeout_ms`](ServerInfo::max_wait_timeout_ms).
    pub fn with_wait_timeout(mut self, wait: Duration) -> Self {
        self.wait_timeout = wait;
        self
    }

    /// Returns the configured long-poll wait timeout.
    pub fn wait_timeout(&self) -> Duration {
        self.wait_timeout
    }

    /// Sets a stable worker identifier, recorded server-side for observability.
    /// Must be non-empty.
    pub fn with_worker_id(mut self, id: impl Into<String>) -> Result<Self, ReserveOptionsError> {
        let id = id.into();
        if id.is_empty() {
            return Err(ReserveOptionsError::EmptyWorkerId);
        }
        self.worker_id = Some(id);
        Ok(self)
    }

    /// Sets the maximum number of jobs to return in one response (default 1).
    /// The server clamps this to its
    /// [`max_reserve_batch`](ServerInfo::max_reserve_batch).
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

/// The server's version, capabilities, and limits, as returned by
/// [`get_server_info`](client::SeppClient::get_server_info).
///
/// The various `max_*` fields mirror the limits behind the [`JobRejection`]
/// variants. Fetching this once at startup lets a producer validate jobs
/// locally and avoid round-trips that would only be rejected. The limits are
/// the server's *defaults*; an individual queue may be configured more or less
/// strictly.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerInfo {
    /// Server version (a semver string).
    pub version: String,
    /// Protocol versions the server supports.
    pub supported_protocol_versions: Vec<String>,
    /// The server's current wall-clock time, useful for detecting clock skew.
    pub server_time: SystemTime,
    /// If `true`, only encodings in [`allowed_encodings`](Self::allowed_encodings)
    /// are accepted; if `false`, any encoding is accepted.
    pub restricts_encodings: bool,
    /// The accepted encodings when [`restricts_encodings`](Self::restricts_encodings)
    /// is set.
    pub allowed_encodings: Vec<String>,
    /// Maximum payload size in bytes (→ [`JobRejection::PayloadTooLarge`]).
    pub max_payload_size: u64,
    /// Maximum entries in the custom map (→ [`JobRejection::CustomEntriesTooMany`]).
    pub max_custom_entries: u32,
    /// Maximum total bytes across the custom map (→ [`JobRejection::CustomMapTooLarge`]).
    pub max_custom_total_bytes: u64,
    /// Maximum bytes in a single custom key (→ [`JobRejection::CustomKeyTooLong`]).
    pub max_custom_key_bytes: u32,
    /// Maximum bytes in a queue name (→ [`JobRejection::QueueNameTooLong`]).
    pub max_queue_name_bytes: u32,
    /// Maximum bytes in a job type (→ [`JobRejection::JobTypeNameTooLong`]).
    pub max_job_type_bytes: u32,
    /// Maximum bytes in an idempotency key (→ [`JobRejection::IdempotencyKeyTooLong`]).
    pub max_idempotency_key_bytes: u32,
    /// How far ahead a job may be scheduled, in ms (→ [`JobRejection::ScheduledTooFar`]).
    pub max_schedule_horizon_ms: u64,
    /// Maximum jobs in one enqueue batch; larger batches fail the whole request.
    pub max_enqueue_batch: u32,
    /// Maximum jobs returned by one reserve; larger requests are clamped, not rejected.
    pub max_reserve_batch: u32,
    /// Maximum queues one reserve may list; exceeding this is an error.
    pub max_reserve_queues: u32,
    /// Maximum long-poll wait in ms; larger requests are clamped.
    pub max_wait_timeout_ms: u64,
    /// Maximum lease duration in ms; larger requests are clamped.
    pub max_lease_duration_ms: u64,
    /// If `true`, enqueueing to an undeclared queue is rejected with
    /// [`JobRejection::UnknownQueue`]; if `false`, queues are created on demand.
    pub strict_queues: bool,
}

/// Returned when a [`get_server_info`](client::SeppClient::get_server_info)
/// response cannot be decoded into a [`ServerInfo`].
#[derive(Debug, thiserror::Error)]
pub enum ServerInfoError {
    /// A required field (named in the payload) was absent.
    #[error("server info is missing required field `{0}`")]
    MissingField(&'static str),
    /// `server_time_ms` was not a representable [`SystemTime`].
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
            max_queue_name_bytes: r.max_queue_name_bytes,
            max_job_type_bytes: r.max_job_type_bytes,
            max_idempotency_key_bytes: r.max_idempotency_key_bytes,
            max_schedule_horizon_ms: r.max_schedule_horizon_ms,
            max_enqueue_batch: r.max_enqueue_batch,
            max_reserve_batch: r.max_reserve_batch,
            max_reserve_queues: r.max_reserve_queues,
            max_wait_timeout_ms: r.max_wait_timeout_ms,
            max_lease_duration_ms: r.max_lease_duration_ms,
            strict_queues: r.strict_queues,
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
            max_queue_name_bytes: 512,
            max_job_type_bytes: 256,
            max_idempotency_key_bytes: 128,
            max_schedule_horizon_ms: 86_400_000,
            max_enqueue_batch: 100,
            max_reserve_batch: 50,
            max_reserve_queues: 8,
            max_wait_timeout_ms: 30_000,
            max_lease_duration_ms: 60_000,
            strict_queues: true,
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
        assert_eq!(info.max_queue_name_bytes, 512);
        assert_eq!(info.max_job_type_bytes, 256);
        assert_eq!(info.max_idempotency_key_bytes, 128);
        assert_eq!(info.max_schedule_horizon_ms, 86_400_000);
        assert_eq!(info.max_enqueue_batch, 100);
        assert_eq!(info.max_reserve_batch, 50);
        assert_eq!(info.max_reserve_queues, 8);
        assert_eq!(info.max_wait_timeout_ms, 30_000);
        assert_eq!(info.max_lease_duration_ms, 60_000);
        assert!(info.strict_queues);
        assert_eq!(
            info.server_time,
            SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_000)
        );
    }
}
