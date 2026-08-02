use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sepp_rs::TraceContext;
use sepp_rs::client::SeppClient;
use sepp_rs::worker::{HandlerError, Worker};
use sepp_rs::{EnqueueRequest, Payload, ReserveOptions};

mod common;

async fn connect(sepp: &common::SeppServer) -> SeppClient {
    SeppClient::connect(sepp.endpoint())
        .await
        .expect("failed to connect to Sepp")
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

/// A name unique per call, so tests that run against a shared server (via
/// `SEPP_TEST_ADDR`) do not trip over each other's state or over reruns.
fn unique_name(prefix: &str) -> String {
    use std::sync::atomic::AtomicU32;
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!(
        "{prefix}_{}_{}_{nanos}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[tokio::test]
async fn test_connect_and_get_server_info() {
    let sepp = common::start_sepp().await;
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
    let sepp = common::start_sepp().await;
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
    let sepp = common::start_sepp().await;
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
    let sepp = common::start_sepp().await;
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
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;

    let job = EnqueueRequest::new("emails", "idem_test")
        .unwrap()
        .with_idempotency_key(unique_name("dedup-key"));

    let first = client.enqueue(job.clone()).await.expect("first enqueue");
    assert!(!first.deduplicated, "first enqueue should be fresh");

    let second = client.enqueue(job).await.expect("second enqueue");
    assert!(second.deduplicated, "second enqueue should be deduplicated");
    assert_eq!(first.job_id, second.job_id, "same job id for dedup");
}

#[tokio::test]
async fn test_reserve_returns_empty_when_no_jobs() {
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;

    let opts = ReserveOptions::new(["empty_queue"], Duration::from_secs(1))
        .unwrap()
        .with_wait_timeout(Duration::from_millis(500));

    let result = client.reserve(&opts).await.expect("reserve");
    assert!(result.is_none(), "should return None for empty queue");
}

#[tokio::test]
async fn test_enqueue_reserve_and_ack() {
    let sepp = common::start_sepp().await;
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
    let sepp = common::start_sepp().await;
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
    let sepp = common::start_sepp().await;
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
    let sepp = common::start_sepp().await;
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
    let sepp = common::start_sepp().await;
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
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;
    let queue = unique_name("worker_basic");

    client
        .enqueue(EnqueueRequest::new(queue.as_str(), "greet").unwrap())
        .await
        .unwrap();
    client
        .enqueue(EnqueueRequest::new(queue.as_str(), "greet").unwrap())
        .await
        .unwrap();

    let processed = Arc::new(AtomicU32::new(0));
    let p = processed.clone();
    let worker = Worker::new(client.clone(), [queue.clone()], Duration::from_secs(10))
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
    let run = tokio::spawn(worker.run());

    assert!(
        wait_for_counter(&processed, 2, Duration::from_secs(10)).await,
        "worker should process both jobs within timeout"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should drain within the timeout")
        .expect("worker task should not panic");
}

#[tokio::test]
async fn test_worker_handler_returns_error_nacks_job() {
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;
    let queue = unique_name("worker_nack");

    // A job the handler will nack, followed by a job it will process successfully
    client
        .enqueue(EnqueueRequest::new(queue.as_str(), "flaky").unwrap())
        .await
        .unwrap();
    client
        .enqueue(EnqueueRequest::new(queue.as_str(), "ok").unwrap())
        .await
        .unwrap();

    let ok_processed = Arc::new(AtomicU32::new(0));
    let p = ok_processed.clone();
    let worker = Worker::new(client.clone(), [queue.clone()], Duration::from_secs(10))
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
    let run = tokio::spawn(worker.run());

    assert!(
        wait_for_counter(&ok_processed, 1, Duration::from_secs(10)).await,
        "worker should process the ok job even after nacking the flaky one"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should drain within the timeout")
        .expect("worker task should not panic");

    // The flaky job was nacked as a permanent failure, so it must have been
    // dead-lettered rather than left in the queue for redelivery.
    let opts = ReserveOptions::new([queue.as_str()], Duration::from_secs(1))
        .unwrap()
        .with_wait_timeout(Duration::from_millis(500));
    assert!(
        client.reserve(&opts).await.expect("reserve").is_none(),
        "permanent-failure job should be dead-lettered, not redelivered"
    );
}

#[tokio::test]
async fn test_worker_catch_all_handler() {
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;
    let queue = unique_name("worker_catchall");

    client
        .enqueue(EnqueueRequest::new(queue.as_str(), "unknown_type").unwrap())
        .await
        .unwrap();

    let caught = Arc::new(AtomicU32::new(0));
    let c = caught.clone();
    let worker = Worker::new(client.clone(), [queue.clone()], Duration::from_secs(10))
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
    let run = tokio::spawn(worker.run());

    assert!(
        wait_for_counter(&caught, 1, Duration::from_secs(10)).await,
        "catch-all handler should process job with unregistered type"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should drain within the timeout")
        .expect("worker task should not panic");
}

#[tokio::test]
async fn test_worker_panicking_handler_is_recovered() {
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;
    let queue = unique_name("worker_panic");

    client
        .enqueue(EnqueueRequest::new(queue.as_str(), "will_panic").unwrap())
        .await
        .unwrap();
    client
        .enqueue(EnqueueRequest::new(queue.as_str(), "normal").unwrap())
        .await
        .unwrap();

    let normal_processed = Arc::new(AtomicU32::new(0));
    let p = normal_processed.clone();
    let worker = Worker::new(client.clone(), [queue.clone()], Duration::from_secs(10))
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
    let run = tokio::spawn(worker.run());

    assert!(
        wait_for_counter(&normal_processed, 1, Duration::from_secs(10)).await,
        "worker should process normal job even after a handler panicked"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should drain within the timeout")
        .expect("worker task should not panic");

    // The panicked job was nacked with an attempt-based delay (2s for attempt
    // 1), so it must come back for a second attempt — not be lost or
    // dead-lettered.
    let opts = ReserveOptions::new([queue.as_str()], Duration::from_secs(5))
        .unwrap()
        .with_wait_timeout(Duration::from_secs(10));
    let jobs = client
        .reserve(&opts)
        .await
        .expect("reserve")
        .expect("panicked job should be redelivered after the nack backoff");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].ctx.job_type, "will_panic");
    assert_eq!(
        jobs[0].ctx.attempt, 2,
        "attempt should have incremented once"
    );

    client.ack(&jobs[0].ctx).await.expect("ack");
}

#[tokio::test]
async fn test_worker_with_auto_extend_keeps_lease_alive() {
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;
    let queue = unique_name("worker_extend");

    client
        .enqueue(EnqueueRequest::new(queue.as_str(), "slow").unwrap())
        .await
        .unwrap();

    let done = Arc::new(AtomicU32::new(0));
    let d = done.clone();
    let worker = Worker::new(client.clone(), [queue.clone()], Duration::from_secs(2))
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
    let run = tokio::spawn(worker.run());

    assert!(
        wait_for_counter(&done, 1, Duration::from_secs(15)).await,
        "auto-extend should keep lease alive for the slow handler"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should drain within the timeout")
        .expect("worker task should not panic");
}

#[tokio::test]
async fn test_worker_shutdown_drains_in_flight_job() {
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;
    let queue = unique_name("worker_drain");

    client
        .enqueue(EnqueueRequest::new(queue.as_str(), "slow").unwrap())
        .await
        .unwrap();

    let started = Arc::new(AtomicU32::new(0));
    let s = started.clone();
    let worker = Worker::new(client.clone(), [queue.clone()], Duration::from_secs(30))
        .unwrap()
        .handle("slow", move |_payload, _ctx| {
            let s = s.clone();
            async move {
                s.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok(())
            }
        })
        .unwrap();

    let shutdown = worker.shutdown_handle();
    let run = tokio::spawn(worker.run());

    assert!(
        wait_for_counter(&started, 1, Duration::from_secs(10)).await,
        "handler should have started"
    );

    // Shut down while the handler is mid-job: run() must wait for it to
    // finish (and ack) before returning.
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should drain within the timeout")
        .expect("worker task should not panic");

    // The drained job was acked, so nothing is left to reserve.
    let opts = ReserveOptions::new([queue.as_str()], Duration::from_secs(1))
        .unwrap()
        .with_wait_timeout(Duration::from_millis(500));
    assert!(
        client.reserve(&opts).await.expect("reserve").is_none(),
        "the in-flight job should have been acked during the drain"
    );
}

#[tokio::test]
async fn test_worker_unhandled_job_type_is_nacked_with_backoff() {
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;
    let queue = unique_name("worker_nohandler");

    client
        .enqueue(
            EnqueueRequest::new(queue.as_str(), "unhandled")
                .unwrap()
                .with_max_attempts(3),
        )
        .await
        .unwrap();

    // A worker that handles a different job type and has no catch-all.
    let worker = Worker::new(client.clone(), [queue.clone()], Duration::from_secs(10))
        .unwrap()
        .with_wait_timeout(Duration::from_millis(500))
        .handle("other", |_payload, _ctx| async move { Ok(()) })
        .unwrap();

    let shutdown = worker.shutdown_handle();
    let run = tokio::spawn(worker.run());

    // Give the worker time to reserve the job and nack it. The nack carries an
    // attempt-based delay, so within this window the worker must not re-reserve
    // it and burn through its attempts.
    tokio::time::sleep(Duration::from_secs(1)).await;
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should shut down within the timeout")
        .expect("worker task should not panic");

    // The job must still exist for a capable worker: redelivered after the
    // backoff (2s for attempt 1), on attempt 2 — neither dead-lettered nor
    // repeatedly nacked.
    let opts = ReserveOptions::new([queue.as_str()], Duration::from_secs(5))
        .unwrap()
        .with_wait_timeout(Duration::from_secs(10));
    let jobs = client
        .reserve(&opts)
        .await
        .expect("reserve")
        .expect("unhandled job should be redelivered after the nack backoff");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].ctx.job_type, "unhandled");
    assert_eq!(
        jobs[0].ctx.attempt, 2,
        "the worker should have nacked the unhandled job exactly once"
    );

    client.ack(&jobs[0].ctx).await.expect("ack");
}

#[tokio::test]
async fn test_worker_auto_extend_lease_lost_aborts_handler() {
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;
    let queue = unique_name("worker_leaselost");

    client
        .enqueue(EnqueueRequest::new(queue.as_str(), "hang").unwrap())
        .await
        .unwrap();

    let completed = Arc::new(AtomicU32::new(0));
    let c = completed.clone();
    // A 1s lease whose first heartbeat only fires after 3s: by then the lease
    // has expired and the job been re-reserved below, so the extend must fail
    // and the handler must be aborted to avoid double processing.
    // max_in_flight = 1 makes the hanging handler hold the worker's only
    // permit, so the worker cannot race the test for the expired job.
    let worker = Worker::new(client.clone(), [queue.clone()], Duration::from_secs(1))
        .unwrap()
        .with_max_in_flight(1)
        .with_auto_extend_interval(Duration::from_secs(3))
        .handle("hang", move |_payload, _ctx| {
            let c = c.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(6)).await;
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();

    let shutdown = worker.shutdown_handle();
    let run = tokio::spawn(worker.run());

    // Let the worker's lease expire, then steal the job.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let opts = ReserveOptions::new([queue.as_str()], Duration::from_secs(30))
        .unwrap()
        .with_wait_timeout(Duration::from_secs(5));
    let jobs = client
        .reserve(&opts)
        .await
        .expect("reserve")
        .expect("expired lease should make the job redeliverable");
    assert_eq!(jobs[0].ctx.attempt, 2);

    // At ~3s the worker's heartbeat fires, learns the lease is gone, and
    // aborts the handler; the aborted handler must not block the drain and
    // must never run to completion.
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should shut down promptly after aborting the handler")
        .expect("worker task should not panic");
    assert_eq!(
        completed.load(Ordering::SeqCst),
        0,
        "the aborted handler must not have completed"
    );

    client.ack(&jobs[0].ctx).await.expect("ack");
}

#[tokio::test]
async fn test_worker_max_in_flight_caps_concurrency() {
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;
    let queue = unique_name("worker_cap");

    for _ in 0..4 {
        client
            .enqueue(EnqueueRequest::new(queue.as_str(), "work").unwrap())
            .await
            .unwrap();
    }

    let current = Arc::new(AtomicU32::new(0));
    let max_seen = Arc::new(AtomicU32::new(0));
    let processed = Arc::new(AtomicU32::new(0));
    let (cur, max, done) = (current.clone(), max_seen.clone(), processed.clone());
    let worker = Worker::new(client.clone(), [queue.clone()], Duration::from_secs(10))
        .unwrap()
        .with_max_in_flight(2)
        .with_wait_timeout(Duration::from_millis(500))
        .handle("work", move |_payload, _ctx| {
            let (cur, max, done) = (cur.clone(), max.clone(), done.clone());
            async move {
                let now = cur.fetch_add(1, Ordering::SeqCst) + 1;
                max.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(400)).await;
                cur.fetch_sub(1, Ordering::SeqCst);
                done.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();

    let shutdown = worker.shutdown_handle();
    let run = tokio::spawn(worker.run());

    assert!(
        wait_for_counter(&processed, 4, Duration::from_secs(15)).await,
        "worker should process all four jobs within the timeout"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should drain within the timeout")
        .expect("worker task should not panic");

    assert!(
        max_seen.load(Ordering::SeqCst) <= 2,
        "no more than max_in_flight handlers may run concurrently, saw {}",
        max_seen.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn test_worker_retry_after_delays_redelivery() {
    let sepp = common::start_sepp().await;
    let client = connect(&sepp).await;
    let queue = unique_name("worker_retry_after");

    client
        .enqueue(EnqueueRequest::new(queue.as_str(), "flaky").unwrap())
        .await
        .unwrap();

    let deliveries = Arc::new(AtomicU32::new(0));
    let delivery_times = Arc::new(std::sync::Mutex::new(Vec::<tokio::time::Instant>::new()));
    let (count, times) = (deliveries.clone(), delivery_times.clone());
    let worker = Worker::new(client.clone(), [queue.clone()], Duration::from_secs(10))
        .unwrap()
        .with_wait_timeout(Duration::from_millis(500))
        .handle("flaky", move |_payload, ctx| {
            let (count, times) = (count.clone(), times.clone());
            async move {
                times.lock().unwrap().push(tokio::time::Instant::now());
                count.fetch_add(1, Ordering::SeqCst);
                if ctx.attempt == 1 {
                    Err(HandlerError::retry_after(
                        "not ready yet",
                        Duration::from_secs(2),
                    ))
                } else {
                    Ok(())
                }
            }
        })
        .unwrap();

    let shutdown = worker.shutdown_handle();
    let run = tokio::spawn(worker.run());

    assert!(
        wait_for_counter(&deliveries, 2, Duration::from_secs(15)).await,
        "the job should be redelivered after the requested delay"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should drain within the timeout")
        .expect("worker task should not panic");

    let times = delivery_times.lock().unwrap();
    let gap = times[1] - times[0];
    assert!(
        gap >= Duration::from_millis(1800),
        "redelivery came {gap:?} after the first attempt, before the requested 2s delay"
    );
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

    let sepp = common::start_sepp().await;
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
