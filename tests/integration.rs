use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sepp_rs::TraceContext;
use sepp_rs::client::SeppClient;
use sepp_rs::worker::{HandlerError, Worker};
use sepp_rs::{EnqueueRequest, Payload, ReserveOptions};

mod common;

async fn connect(sepp: &common::SeppContainer) -> SeppClient {
    SeppClient::connect(sepp.endpoint())
        .await
        .expect("failed to connect to Sepp")
}

macro_rules! require_sepp {
    ($sepp:expr) => {
        match $sepp {
            Some(s) => s,
            None => return,
        }
    };
}

async fn wait_for_counter(counter: &AtomicU32, target: u32, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while counter.load(Ordering::SeqCst) < target {
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    true
}

#[tokio::test]
async fn test_connect_and_get_server_info() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    let info = client.get_server_info().await.expect("get_server_info");
    assert!(!info.version.is_empty(), "version should be populated");
    assert!(
        !info.supported_protocol_versions.is_empty(),
        "supported_protocol_versions should be populated"
    );
}

#[tokio::test]
async fn test_enqueue_single_job() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    let ack = client
        .enqueue(
            EnqueueRequest::new("emails", "send_welcome")
                .unwrap()
                .with_payload(Payload::new(b"hello".to_vec(), "text/plain")),
        )
        .await
        .expect("enqueue");

    assert!(!ack.job_id.is_empty(), "should get a job id");
    assert!(
        !ack.deduplicated,
        "first enqueue should not be deduplicated"
    );
}

#[tokio::test]
async fn test_enqueue_batch() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    let jobs = vec![
        EnqueueRequest::new("emails", "type_a").unwrap(),
        EnqueueRequest::new("emails", "type_b").unwrap(),
        EnqueueRequest::new("emails", "type_c").unwrap(),
    ];

    let results = client.enqueue_batch(jobs).await.expect("enqueue_batch");
    assert_eq!(results.len(), 3);
    for result in &results {
        assert!(result.is_ok(), "each job should be accepted");
    }
}

#[tokio::test]
async fn test_enqueue_atomic() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    let jobs = vec![
        EnqueueRequest::new("emails", "step_1").unwrap(),
        EnqueueRequest::new("emails", "step_2").unwrap(),
    ];

    let acks = client.enqueue_atomic(jobs).await.expect("enqueue_atomic");
    assert_eq!(acks.len(), 2);
    assert_ne!(acks[0].job_id, acks[1].job_id);
}

#[tokio::test]
async fn test_idempotency() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    let job = EnqueueRequest::new("emails", "idem_test")
        .unwrap()
        .with_idempotency_key("dedup-key-42");

    let first = client.enqueue(job.clone()).await.expect("first enqueue");
    assert!(!first.deduplicated, "first enqueue should be fresh");

    let second = client.enqueue(job).await.expect("second enqueue");
    assert!(second.deduplicated, "second enqueue should be deduplicated");
    assert_eq!(first.job_id, second.job_id, "same job id for dedup");
}

#[tokio::test]
async fn test_reserve_returns_empty_when_no_jobs() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    let opts = ReserveOptions::new(["empty_queue"], Duration::from_secs(1))
        .unwrap()
        .with_wait_timeout(Duration::from_millis(500));

    let result = client.reserve(&opts).await.expect("reserve");
    assert!(result.is_none(), "should return None for empty queue");
}

#[tokio::test]
async fn test_enqueue_reserve_and_ack() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    let enqueued = client
        .enqueue(
            EnqueueRequest::new("integration_ack", "ack_test")
                .unwrap()
                .with_payload(Payload::new(b"data".to_vec(), "application/octet-stream")),
        )
        .await
        .expect("enqueue");

    let opts = ReserveOptions::new(["integration_ack"], Duration::from_secs(30))
        .unwrap()
        .with_wait_timeout(Duration::from_secs(5));

    let jobs = client
        .reserve(&opts)
        .await
        .expect("reserve")
        .expect("should get a job");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].ctx.id, enqueued.job_id);
    assert_eq!(jobs[0].ctx.job_type, "ack_test");
    assert_eq!(jobs[0].ctx.attempt, 1);
    assert!(jobs[0].payload.is_some());

    client.ack(&jobs[0].ctx).await.expect("ack");

    let opts = ReserveOptions::new(["integration_ack"], Duration::from_secs(1))
        .unwrap()
        .with_wait_timeout(Duration::from_millis(500));
    let result = client.reserve(&opts).await.expect("reserve");
    assert!(result.is_none(), "job should not be available after ack");
}

#[tokio::test]
async fn test_nack_retry_and_reserve() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    client
        .enqueue(EnqueueRequest::new("integration_nack", "nack_test").unwrap())
        .await
        .expect("enqueue");

    let opts = ReserveOptions::new(["integration_nack"], Duration::from_secs(30))
        .unwrap()
        .with_wait_timeout(Duration::from_secs(5));

    let jobs = client
        .reserve(&opts)
        .await
        .expect("reserve")
        .expect("should get a job");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].ctx.attempt, 1);

    use sepp_rs::client::RetryDirective;
    let dead_lettered = client
        .nack(&jobs[0].ctx, RetryDirective::Default, "test retry")
        .await
        .expect("nack");
    assert!(!dead_lettered, "should not be dead-lettered on first nack");

    let jobs = client
        .reserve(&opts)
        .await
        .expect("reserve")
        .expect("should get the job again");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].ctx.attempt, 2, "attempt should increment");

    client.ack(&jobs[0].ctx).await.expect("ack");
}

#[tokio::test]
async fn test_extend_lease() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    client
        .enqueue(EnqueueRequest::new("integration_extend", "extend_test").unwrap())
        .await
        .expect("enqueue");

    // Reserve with a short lease, extend by a larger amount. The server
    // replaces the lease with `now + extension`, so the new expiry must
    // exceed the original only when extension > original lease.
    let opts = ReserveOptions::new(["integration_extend"], Duration::from_secs(2))
        .unwrap()
        .with_wait_timeout(Duration::from_secs(5));

    let jobs = client
        .reserve(&opts)
        .await
        .expect("reserve")
        .expect("should get a job");

    let original_expiry = jobs[0].ctx.lease_expires_at;

    let new_expiry = client
        .extend(&jobs[0].ctx, Duration::from_secs(10))
        .await
        .expect("extend");

    assert!(
        new_expiry > original_expiry,
        "extending by 10s should push expiry beyond a 2s lease"
    );

    client.ack(&jobs[0].ctx).await.expect("ack");
}

#[tokio::test]
async fn test_trace_context_propagates_through_enqueue_and_reserve() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let tc = TraceContext::new(traceparent)
        .unwrap()
        .with_tracestate("vendor=test");

    client
        .enqueue(
            EnqueueRequest::new("traced", "traced_job")
                .unwrap()
                .with_trace_context(tc),
        )
        .await
        .expect("enqueue");

    let opts = ReserveOptions::new(["traced"], Duration::from_secs(30))
        .unwrap()
        .with_wait_timeout(Duration::from_secs(5));

    let jobs = client
        .reserve(&opts)
        .await
        .expect("reserve")
        .expect("should get a job");

    assert_eq!(jobs.len(), 1);
    let job_tc = jobs[0]
        .ctx
        .trace_context
        .as_ref()
        .expect("trace context should be present on reserved job");

    assert_eq!(job_tc.traceparent(), traceparent);
    assert_eq!(job_tc.tracestate(), Some("vendor=test"));

    client.ack(&jobs[0].ctx).await.expect("ack");
}

#[tokio::test]
async fn test_trace_context_absent_when_not_set() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    client
        .enqueue(EnqueueRequest::new("traced_none", "no_tc").unwrap())
        .await
        .expect("enqueue");

    let opts = ReserveOptions::new(["traced_none"], Duration::from_secs(30))
        .unwrap()
        .with_wait_timeout(Duration::from_secs(5));

    let jobs = client
        .reserve(&opts)
        .await
        .expect("reserve")
        .expect("should get a job");

    assert!(jobs[0].ctx.trace_context.is_none());

    client.ack(&jobs[0].ctx).await.expect("ack");
}

#[tokio::test]
async fn test_worker_processes_jobs() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;
    let queue = "worker_basic";

    client
        .enqueue(EnqueueRequest::new(queue, "greet").unwrap())
        .await
        .unwrap();
    client
        .enqueue(EnqueueRequest::new(queue, "greet").unwrap())
        .await
        .unwrap();

    let processed = Arc::new(AtomicU32::new(0));
    {
        let p = processed.clone();
        let worker = Worker::new(client.clone(), [queue], Duration::from_secs(10))
            .unwrap()
            .with_max_in_flight(2)
            .handle("greet", move |_payload, _ctx| {
                let p = p.clone();
                async move {
                    p.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .unwrap();

        let shutdown = worker.shutdown_handle();
        tokio::spawn(async move { worker.run().await });

        assert!(
            wait_for_counter(&processed, 2, Duration::from_secs(10)).await,
            "worker should process both jobs within timeout"
        );

        shutdown.shutdown();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn test_worker_handler_returns_error_nacks_job() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;
    let queue = "worker_nack";

    // A job the handler will nack, followed by a job it will process successfully
    client
        .enqueue(EnqueueRequest::new(queue, "flaky").unwrap())
        .await
        .unwrap();
    client
        .enqueue(EnqueueRequest::new(queue, "ok").unwrap())
        .await
        .unwrap();

    let ok_processed = Arc::new(AtomicU32::new(0));
    {
        let p = ok_processed.clone();
        let worker = Worker::new(client.clone(), [queue], Duration::from_secs(10))
            .unwrap()
            .with_max_in_flight(2)
            .handle("flaky", |_payload, _ctx| async move {
                Err(HandlerError::permanent("simulated failure"))
            })
            .unwrap()
            .handle("ok", move |_payload, _ctx| {
                let p = p.clone();
                async move {
                    p.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .unwrap();

        let shutdown = worker.shutdown_handle();
        tokio::spawn(async move { worker.run().await });

        assert!(
            wait_for_counter(&ok_processed, 1, Duration::from_secs(10)).await,
            "worker should process the ok job even after nacking the flaky one"
        );

        shutdown.shutdown();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn test_worker_catch_all_handler() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;
    let queue = "worker_catchall";

    client
        .enqueue(EnqueueRequest::new(queue, "unknown_type").unwrap())
        .await
        .unwrap();

    let caught = Arc::new(AtomicU32::new(0));
    {
        let c = caught.clone();
        let worker = Worker::new(client.clone(), [queue], Duration::from_secs(10))
            .unwrap()
            .with_catch_all_handler(move |_payload, ctx| {
                let c = c.clone();
                async move {
                    assert_eq!(ctx.job_type, "unknown_type");
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            });

        let shutdown = worker.shutdown_handle();
        tokio::spawn(async move { worker.run().await });

        assert!(
            wait_for_counter(&caught, 1, Duration::from_secs(10)).await,
            "catch-all handler should process job with unregistered type"
        );

        shutdown.shutdown();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn test_worker_panicking_handler_is_recovered() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;
    let queue = "worker_panic";

    client
        .enqueue(EnqueueRequest::new(queue, "will_panic").unwrap())
        .await
        .unwrap();
    client
        .enqueue(EnqueueRequest::new(queue, "normal").unwrap())
        .await
        .unwrap();

    let normal_processed = Arc::new(AtomicU32::new(0));
    {
        let p = normal_processed.clone();
        let worker = Worker::new(client.clone(), [queue], Duration::from_secs(10))
            .unwrap()
            .with_max_in_flight(2)
            .handle("will_panic", |_payload, _ctx| async move {
                panic!("handler crashed");
            })
            .unwrap()
            .handle("normal", move |_payload, _ctx| {
                let p = p.clone();
                async move {
                    p.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .unwrap();

        let shutdown = worker.shutdown_handle();
        tokio::spawn(async move { worker.run().await });

        assert!(
            wait_for_counter(&normal_processed, 1, Duration::from_secs(10)).await,
            "worker should process normal job even after a handler panicked"
        );

        shutdown.shutdown();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn test_worker_with_auto_extend_keeps_lease_alive() {
    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;
    let queue = "worker_extend";

    client
        .enqueue(EnqueueRequest::new(queue, "slow").unwrap())
        .await
        .unwrap();

    let done = Arc::new(AtomicU32::new(0));
    {
        let d = done.clone();
        let worker = Worker::new(client.clone(), [queue], Duration::from_secs(2))
            .unwrap()
            .with_auto_extend()
            .with_max_in_flight(1)
            .handle("slow", move |_payload, _ctx| {
                let d = d.clone();
                async move {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    d.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .unwrap();

        let shutdown = worker.shutdown_handle();
        tokio::spawn(async move { worker.run().await });

        assert!(
            wait_for_counter(&done, 1, Duration::from_secs(15)).await,
            "auto-extend should keep lease alive for the slow handler"
        );

        shutdown.shutdown();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[cfg(feature = "opentelemetry")]
#[tokio::test]
async fn test_otel_trace_context_from_active_span_roundtrips_through_server() {
    let _ = tracing_subscriber::fmt().try_init();

    use opentelemetry::trace::Span as _;
    use opentelemetry::trace::TraceContextExt as _;
    use opentelemetry::trace::Tracer as _;
    use opentelemetry::trace::TracerProvider as _;

    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("sepp-rs-integration-test");

    let (tc, trace_id) = {
        let span = tracer.start("producer-test-span");
        let trace_id = span.span_context().trace_id();
        let cx = opentelemetry::Context::current_with_span(span);
        let _guard = cx.attach();

        let tc = sepp_rs::TraceContext::from_current_otel()
            .expect("should capture trace context from an active OTel span");

        let trace_id_hex = trace_id.to_string();
        assert!(
            tc.traceparent().contains(&trace_id_hex),
            "traceparent {} should contain trace id {}",
            tc.traceparent(),
            trace_id_hex
        );

        // _guard drops → detaches context; cx drops → span drops → exported
        (tc, trace_id)
    };

    let sepp = require_sepp!(common::start_sepp().await);
    let client = connect(&sepp).await;

    client
        .enqueue(
            EnqueueRequest::new("otel_sdk", "traced")
                .unwrap()
                .with_trace_context(tc.clone()),
        )
        .await
        .unwrap();

    let opts = ReserveOptions::new(["otel_sdk"], Duration::from_secs(30))
        .unwrap()
        .with_wait_timeout(Duration::from_secs(5));

    let jobs = client
        .reserve(&opts)
        .await
        .expect("reserve")
        .expect("should get a job");

    let job_tc = jobs[0]
        .ctx
        .trace_context
        .as_ref()
        .expect("trace context should survive the roundtrip");

    assert_eq!(job_tc.traceparent(), tc.traceparent());

    let span_context = job_tc
        .otel_span_context()
        .expect("reserved job should yield a valid SpanContext");
    assert!(span_context.is_valid());
    assert_eq!(span_context.trace_id(), trace_id);

    client.ack(&jobs[0].ctx).await.expect("ack");

    let exported = exporter.get_finished_spans().unwrap();
    assert_eq!(exported.len(), 1, "one producer span should be exported");
    assert_eq!(exported[0].span_context.trace_id(), trace_id);
}
