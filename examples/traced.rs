//! End-to-end roundtrip *with OpenTelemetry tracing*.
//!
//! This is the example to run when you want to see the producer's "publish"
//! span and the worker's "process" span show up as linked spans in a tracing
//! backend.
//!
//! Prerequisites — two things must be running:
//!
//!   1. A Sepp server. Point at it with `SEPP_ADDR` (default
//!      `http://127.0.0.1:50051`).
//!
//!   2. An OTLP collector. Point at it with `OTEL_ENDPOINT` (default
//!      `http://localhost:4317`). The quickest one to stand up is Jaeger v2:
//!
//!          docker run --rm --name jaeger \
//!            -p 16686:16686 -p 4317:4317 -p 4318:4318 \
//!            jaegertracing/jaeger:latest
//!
//! Run it (the `opentelemetry` feature is on by default):
//!
//!     cargo run --example traced
//!
//! Then open your tracing backend, pick the `sepp-rs-example` service, and open
//! the most recent `sepp-rs.process` trace. That span carries a *link* back to
//! the `sepp-rs.enqueue` span — that is the producer→worker linkage.

use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use sepp_rs::client::SeppClient;
use sepp_rs::worker::Worker;
use sepp_rs::{EnqueueRequest, Payload};

const QUEUE: &str = "traced-example";
const JOB_TYPE: &str = "greeting";

/// Build an OTLP exporter and install it as a `tracing` layer. The returned
/// provider must be kept alive until shutdown, and `shutdown()` must be called
/// before the process exits or buffered spans are lost.
fn init_telemetry() -> SdkTracerProvider {
    let endpoint =
        std::env::var("OTEL_ENDPOINT").unwrap_or_else(|_| "http://localhost:4317".to_string());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("build OTLP span exporter");

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name("sepp-rs-example")
                .build(),
        )
        .build();

    // `with_tracer` is the bridge: every `tracing` span (including sepp-rs's
    // `sepp-rs.enqueue` / `sepp-rs.process` spans) becomes an OpenTelemetry span.
    let otel_layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("sepp-rs-example"));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,sepp_rs=debug")),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .init();

    provider
}

#[tokio::main]
async fn main() {
    let provider = init_telemetry();

    let result = roundtrip().await;

    // Flush spans to the collector before exiting.
    if let Err(e) = provider.shutdown() {
        eprintln!("tracer shutdown failed: {e}");
    }
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("SEPP_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let client = SeppClient::connect(addr).await?;

    // 1. Enqueue a job. `enqueue` -> `enqueue_batch` opens the `sepp-rs.enqueue`
    //    span and stamps its trace context onto the job.
    let job = EnqueueRequest::new(QUEUE, JOB_TYPE)?
        .with_payload(Payload::new(b"hello, sepp".to_vec(), "text/plain"));
    let ack = client.enqueue(job).await?;
    println!("enqueued job {}", ack.job_id);

    // 2. Run a worker. `process_job` opens the `sepp-rs.process` span and links
    //    it back to the enqueue span recovered from the job's trace context.
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<String>(1);

    let worker = Worker::new(client.clone(), [QUEUE], Duration::from_secs(30))?.handle(
        JOB_TYPE,
        move |payload, ctx| {
            let done_tx = done_tx.clone();
            async move {
                let body = payload
                    .map(|p| String::from_utf8_lossy(&p.data).into_owned())
                    .unwrap_or_default();
                // This event is recorded inside the `sepp-rs.process` span.
                tracing::info!(job_id = %ctx.id, payload = %body, "handling job");
                let _ = done_tx.send(ctx.id.clone()).await;
                Ok(())
            }
        },
    )?;
    // Take the shutdown handle before `run()` consumes the worker.
    let shutdown = worker.shutdown_handle();
    let worker_task = tokio::spawn(worker.run());

    // 3. Wait for the job to be processed, then drain the worker: after
    //    `shutdown()` it stops reserving, finishes (and acks) in-flight jobs,
    //    and `run()` returns.
    let processed = tokio::time::timeout(Duration::from_secs(15), done_rx.recv()).await;
    shutdown.shutdown();
    worker_task.await?;

    match processed {
        Ok(Some(id)) => {
            println!("roundtrip OK — job {id} processed; check the trace in your backend");
            Ok(())
        }
        Ok(None) => Err("worker stopped before processing the job".into()),
        Err(_) => Err("timed out waiting for the job to be processed".into()),
    }
}
