# metrics233 Rust SDK

Bounded Rust instrumentation for [metrics233-server](https://github.com/neko233-com/metrics233-server), using its native `POST /api/v1/ingest` JSON protocol. Package and library name: `metrics233`. Rust 1.88+, Tokio runtime. The transport reuses reqwest 0.12 and rustls already present in the Rust game gateway; it adds no separate HTTP/TLS stack, native OpenSSL, exporter, tracing framework or automatic host collector.

Install the `metrics233` crate from crates.io:

```sh
cargo add metrics233@0.1.0
cargo add tokio --features rt-multi-thread,macros
```

```rust,no_run
use metrics233::{Client, Identity};
use std::time::Duration;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let root = std::env::var("METRICS233_URL")?;
let token = std::env::var("METRICSPANEL_INGEST_TOKEN")?;
let metrics = Client::builder(root)
    .with_token(token)
    .with_identity(Identity::new("gateway-1")
        .with_service("game-gateway")
        .with_role("gateway")
        .with_environment("test")
        .with_instance("node-1"))
    .with_labels([("implementation", "rust"), ("channel", "test")])
    .build()?;

metrics.start(Duration::from_secs(15))?;
metrics.counter("game_gateway_connections_total", 1.0)?;
metrics.gauge("game_gateway_connections", 32.0)?;
metrics.with_labels([("result", "success")])?
    .counter("game_gateway_login_total", 1.0)?;
metrics.flush().await?;
metrics.shutdown().await?;
# Ok(()) }
```

The SDK never reads environment variables itself. The example reads an **ingest-only** token from caller-controlled private configuration. `METRICSPANEL_INGEST_TOKEN` is separate from management credentials, requires 32–8192 printable ASCII characters without whitespace, and grants only the server's five explicit ingestion POST endpoints. Source-IP access policies still apply. Do not distribute management tokens to game servers. Browser administration uses the server's configured account/session authorization.

Root URLs use HTTPS with normal trusted-root and hostname validation. Redirects, automatic transport retries and environment HTTP proxies are disabled. A root path such as `https://monitoring.example.com/metrics` becomes `/metrics/api/v1/ingest`. URL credentials, query strings and fragments are rejected. For private PKI, `with_ca_certificate_pem(pem)?` adds a trusted CA without disabling validation. `allow_http_loopback()` is an explicit development exception limited to localhost/numeric loopback; remote cleartext HTTP remains rejected. There is no insecure TLS switch. Error and Client Debug output omit tokens, URLs and response bodies.

## Lifecycle and delivery

- `build()` validates configuration but does not connect, record automatically or spawn background work. Recording methods are synchronous and thread-safe; `Client::clone` and `with_labels` views share one registry, queue and lifecycle.
- `start(interval)` explicitly starts one worker in the caller's active Tokio runtime. It flushes immediately and waits the interval after each completed round. A second start returns `AlreadyRunning`. Intervals are 100 ms–24 h. Inspect `stats().last_error` for periodic failures.
- `flush().await` serializes sends. Its default **5-second total deadline includes waiting for another flush, all batches, retries and Retry-After**. Dropping/cancelling the future retains the unacknowledged body and releases the send gate. Callers may wrap it in a shorter `tokio::time::timeout`.
- `shutdown().await` rejects new recordings, stops the worker and flushes within the configured total deadline. Success is idempotent across all clones. On failure it stays closing and retains data: call shutdown again to retry. Await it before ending the Tokio runtime. Dropping a client does not perform a hidden final send; in-memory state does not survive process termination.
- Every HTTP batch requires a complete 2xx JSON `{ "accepted": N }` matching its entire sample count. Previously acknowledged batches are removed immediately and never replayed. Failed or ambiguously delivered batches retain exact bytes/timestamps and are delivered before a new snapshot. Network failures, 429 and 5xx allow up to two additional retries; other statuses, malformed or partial acknowledgements fail immediately. Retry-After seconds/dates persist across separate flush calls (untrusted remote delays capped at seven days).

This is bounded at-least-once delivery during the process lifetime, with no disk spool or distributed exactly-once guarantee. An acknowledgement lost after server storage can cause retransmission of the same timestamp. The server's series/timestamp semantics handle replacement; a successful local ACK is never intentionally resent.

## Counters, gauges and atomic batches

`counter(name, delta)` accepts a finite nonnegative delta and retains its cumulative process-local value after every flush, including failures. When sampling an existing absolute counter, calculate its delta in the caller and handle source resets; do not repeatedly add the absolute snapshot. `gauge(name, value)` keeps the latest finite value. A metric name cannot mix counter, gauge and raw recording kinds, even with different labels.

```rust,no_run
# use metrics233::{Client, Sample};
# fn example(metrics: &Client) -> Result<(), metrics233::Error> {
metrics.batch()
    .counter("game_requests_total", 1.0)
    .gauge("game_players_online", 12.0)
    .record(Sample::new("game_round_duration_seconds", 4.2))
    .commit()?;
# Ok(()) }
```

Batch commit validates all operations/capacity/counter overflow before applying any change, with a maximum 10,000 operations. `record` queues an individual sample; a zero timestamp is fixed to registration time in Unix milliseconds, or use `Sample::with_timestamp` explicitly. Negative timestamps or timestamps more than five minutes in the future are rejected before queueing, matching the server contract. Counter/gauge snapshots receive flush time. Label views merge base identity with local dimensions; raw Sample labels can override view labels, so use only caller-controlled stable metadata.

| Bound | Default | Configurable limit |
| --- | --- | --- |
| Lifetime distinct series (raw included) | 1,000 | 1–100,000 |
| Pending raw records, including in-flight records | 1,000 | 1–100,000 |
| Samples in an HTTP batch | 500 | 1–10,000 |
| Encoded HTTP body | 4 MiB | Fixed |
| ACK body | 64 KiB | Fixed |
| Labels per series | 32 | Fixed |
| Label name/value | 100/512 UTF-8 bytes | Fixed |
| Metric name | 200 ASCII bytes | Fixed |
| Additional retries | 2 | 0–5 |
| Total flush/shutdown deadline | 5 seconds | >0–300 seconds |

Use `with_limits(Limits { max_series, max_pending, batch_size })`, `with_timeout(Duration)` and `with_retry(count, initial_backoff)` on the builder. Counter/gauge updates coalesce while a failed delivery is retained. Series slots remain allocated for the client lifetime, so a stream of ever-changing labels eventually returns `SeriesLimit` rather than growing forever. These are count and encoded-body bounds; total memory also includes registry labels, snapshots, TLS/HTTP buffers and caller memory.

## Identity and business dimensions

`Identity` emits `serverId`, `service`, `role`, `environment` and `instance`; set only the fields known by the caller. The SDK does not guess a production environment, discover an IP or read gateway configuration. Use actual server configuration for serverId. `role` is typically `gateway` or `logic`; `implementation` can be `rust` or `go`. Keep labels finite and stable: region/country/channel/result categories are suitable; player UID, request ID and individual player IP are not.

If operationally necessary, caller-reported **server** address can be a stable `ip` label. It is an assertion by the sender, not proof of origin. The server's canonical observed peer/source IP is derived independently from the connection and configured trusted proxies and cannot be authenticated by a sample label. Aggregate player distribution by bounded country/region; never export raw player identifiers or addresses.

## Verification

```text
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo test --locked --doc
cargo package --locked --allow-dirty
```

The tests use real local HTTP and TLS listeners with synthetic credentials, exercise certificate/hostname rejection, stable retries, no credential forwarding on redirects, atomic bounds, partial ACKs, cancellation, concurrent recording and explicit lifecycle. The crate does not test against or send data to a production endpoint. Registry availability, repository publication, cargo publication and a clean external consumer restore are separate delivery checks.

An optional real-server contract test starts a separate local executable and temporary database. Set `METRICS233_TEST_SERVER_BIN` to a verified local server binary and run `cargo test --locked --features real-server-tests --test real_server`. It creates its own synthetic management/ingest tokens and checks actual SDK ingestion, stored values/identity, missing/wrong-token 401 and ingest-only query/source-list 403. It never connects to an existing server, reads a real `.env` or sends a notification. This opt-in feature is a test harness, not a runtime feature requirement.

