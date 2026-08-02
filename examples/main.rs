//! End-to-end roundtrip: enqueue one job, then run a worker that processes it.
//!
//! Requires a running Sepp server. Point at it with `SEPP_ADDR`
//! (default `http://127.0.0.1:50051`):
//!
//!     cargo run --example main

use std::time::Duration;

use sepp_rs::client::SeppClient;
use sepp_rs::worker::Worker;
use sepp_rs::{EnqueueRequest, Payload};

const QUEUE: &str = "example";
const JOB_TYPE: &str = "greeting";

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,sepp_rs=debug")),
        )
        .init();

    let addr = std::env::var("SEPP_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let client = SeppClient::connect(addr).await?;

    // 1. Enqueue a single job.
    let job = EnqueueRequest::new(QUEUE, JOB_TYPE)?
        .with_payload(Payload::new(b"hello, sepp".to_vec(), "text/plain"));

    let ack = client.enqueue(job).await?;
    println!("enqueued job {}", ack.job_id);

    // 2. Launch a worker. The handler sends the job id back over a channel so
    //    the example can finish instead of looping in `run` forever.
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<String>(1);

    let worker = Worker::new(client.clone(), [QUEUE], Duration::from_secs(30))?.handle(
        JOB_TYPE,
        move |payload, ctx| {
            let done_tx = done_tx.clone();
            async move {
                let body = payload
                    .map(|p| String::from_utf8_lossy(&p.data).into_owned())
                    .unwrap_or_default();
                println!("worker: processing job {} — payload {body:?}", ctx.id);
                let _ = done_tx.send(ctx.id.clone()).await;
                Ok(())
            }
        },
    )?;

    // Take the shutdown handle before `run()` consumes the worker.
    let shutdown = worker.shutdown_handle();
    let worker_task = tokio::spawn(worker.run());

    // Shut down cleanly on ctrl-c too.
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                shutdown.shutdown();
            }
        });
    }

    // 3. Wait for the job to be processed, with a timeout.
    let processed = tokio::time::timeout(Duration::from_secs(15), done_rx.recv()).await;

    // 4. Drain the worker: after `shutdown()` it stops reserving, finishes
    //    (and acks) in-flight jobs, and `run()` returns — so the job is not
    //    redelivered on the next run.
    shutdown.shutdown();
    worker_task.await?;

    match processed {
        Ok(Some(id)) => {
            println!("roundtrip OK — job {id} was enqueued and processed");
            Ok(())
        }
        Ok(None) => Err("worker stopped before processing the job".into()),
        Err(_) => Err("timed out waiting for the job to be processed".into()),
    }
}
