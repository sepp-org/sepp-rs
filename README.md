<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/sepp-org/sepp-rs/HEAD/media/sepp-rust-paper-200.png">
    <img alt="sepp-rs" src="https://raw.githubusercontent.com/sepp-org/sepp-rs/HEAD/media/sepp-rust-ink-200.png" width="128" height="128">
  </picture>

  <h1>sepp-rs</h1>

  <p>
    <strong>The official Rust client for <a href="https://github.com/sepp-org/sepp">sepp</a>,</strong>
    <br/>
    a small, language-agnostic durable job queue.
  </p>

  <p>
    <a href="https://github.com/sepp-org/sepp-rs/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/sepp-org/sepp-rs/ci.yml?branch=master&labelColor=181512"></a>
    <a href="https://crates.io/crates/sepp-rs"><img alt="crates.io" src="https://img.shields.io/crates/v/sepp-rs?labelColor=181512"></a>
    <a href="https://docs.rs/sepp-rs"><img alt="docs.rs" src="https://img.shields.io/docsrs/sepp-rs?labelColor=181512"></a>
    <a href="LICENSE"><img alt="license" src="https://img.shields.io/crates/l/sepp-rs?color=ec6a2e&labelColor=181512"></a>
  </p>

  <p>
    <a href="https://docs.rs/sepp-rs">API docs</a>
    ·
    <a href="https://sepp-org.github.io/sepp/docs/">sepp docs</a>
    ·
    <a href="https://buf.build/sepp-org/sepp-proto/docs/main%3Asepp.v1">Protocol</a>
    ·
    <a href="https://github.com/sepp-org/sepp-rs/issues">Issues</a>
  </p>
</div>

## Functionality

- **Producers** — enqueue jobs one at a time, in best-effort batches, or atomically. Supports idempotency keys, priorities, scheduled delivery and custom metadata.
- **Consumers** — a high-level `Worker` runs the whole reserve → process → ack loop for you with bounded concurrency, automatic lease extension and graceful shutdown, or drop down to the raw `reserve` / `ack` / `nack` / `extend` calls for full control.
- **Observability** — with the default `opentelemetry` feature, the client emits `tracing` spans and metrics and propagates W3C trace context from the producer's enqueue span to the worker's process span. The host application owns the exporter.
- **Typed errors** — deterministic server rejections (payload too large, unknown queue, …) are separate from transient transport errors, so retry logic stays simple.

The client is async-only and requires a [tokio](https://docs.rs/tokio) runtime.

## Install

```sh
cargo add sepp-rs
```

## Quickstart

Enqueue a job, then run a worker that processes it (requires a running [sepp server](https://github.com/sepp-org/sepp)):

```rust
use std::time::Duration;
use sepp_rs::client::SeppClient;
use sepp_rs::worker::Worker;
use sepp_rs::{EnqueueRequest, Payload};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = SeppClient::connect("http://127.0.0.1:50051").await?;

    // Producer: enqueue a job onto the `emails` queue.
    let ack = client
        .enqueue(
            EnqueueRequest::new("emails", "send_welcome")?
                .with_payload(Payload::new(b"{\"user\":42}".to_vec(), "application/json")),
        )
        .await?;
    println!("enqueued job {}", ack.job_id);

    // Consumer: process `send_welcome` jobs from the `emails` queue. A handler
    // returns `Ok(())` to ack the job, or a `HandlerError` to nack it.
    Worker::new(client, ["emails"], Duration::from_secs(30))?
        .handle("send_welcome", |payload, ctx| async move {
            println!("processing job {}", ctx.id);
            Ok(())
        })?
        .run()
        .await;

    Ok(())
}
```

Runnable versions live in [`examples/`](examples/), including [`traced.rs`](examples/traced.rs) which wires up an OTLP exporter for end-to-end distributed tracing.

## Feature flags

- `opentelemetry` *(default)* — OpenTelemetry-compatible tracing spans, metrics and automatic trace context propagation.
- `tls` — TLS for the gRPC transport.

## Docs

The full API reference is on [docs.rs](https://docs.rs/sepp-rs). For running and configuring the sepp server itself, see the [sepp docs site](https://sepp-org.github.io/sepp/docs/).

## License

sepp-rs is licensed under the MIT License. See [LICENSE](LICENSE) for details.
