mod support;
use metrics233::{Client, Error, Identity, Limits, Sample};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use support::{Fixture, Reply, TOKEN};

#[test]
fn secure_configuration_and_explicit_runtime() {
    for root in [
        "http://example.com",
        "https://user:pass@example.com",
        "https://example.com?token=x",
        "https://example.com#fragment",
        "file:///tmp/metrics",
    ] {
        assert!(Client::builder(root)
            .with_token(TOKEN)
            .allow_http_loopback()
            .build()
            .is_err());
    }
    for token in [
        "",
        "short",
        "contains whitespace cannot be accepted",
        "a\nheader-injection-synthetic-token-long",
    ] {
        assert!(Client::builder("https://example.com")
            .with_token(token)
            .build()
            .is_err());
    }
    let client = Client::builder("https://example.com")
        .with_token(TOKEN)
        .build()
        .unwrap();
    assert_eq!(
        client.start(Duration::from_secs(1)),
        Err(Error::RuntimeRequired)
    );
    assert!(!format!("{client:?}").contains(TOKEN));
}

#[tokio::test]
async fn https_identity_auth_and_constructor_no_side_effects() {
    let fixture = Fixture::https(|request, _| Reply::ack(request));
    let client = Client::builder(format!("{}/monitoring/", fixture.root()))
        .with_token(TOKEN)
        .with_identity(
            Identity::new("gateway-1")
                .with_service("game")
                .with_role("gateway")
                .with_environment("test")
                .with_instance("node-1"),
        )
        .with_ca_certificate_pem(fixture.ca.as_bytes())
        .unwrap()
        .build()
        .unwrap();
    client
        .with_labels([("channel", "test")])
        .unwrap()
        .counter("game_events_total", 3.0)
        .unwrap();
    assert_eq!(fixture.requests.lock().unwrap().len(), 0);
    client.flush().await.unwrap();
    let requests = fixture.requests.lock().unwrap();
    let request = &requests[0];
    assert_eq!(request.path, "/monitoring/api/v1/ingest");
    assert_eq!(request.headers["authorization"], format!("Bearer {TOKEN}"));
    assert_eq!(request.sni.as_deref(), Some("localhost"));
    assert_eq!(request.headers["accept-encoding"], "identity");
    assert_eq!(request.samples()[0].labels["serverId"], "gateway-1");
    assert_eq!(request.samples()[0].labels["channel"], "test");
}

#[tokio::test]
async fn tls_rejects_unknown_ca_and_wrong_hostname() {
    let fixture = Fixture::https(|request, _| Reply::ack(request));
    let untrusted = Client::builder(fixture.root())
        .with_token(TOKEN)
        .with_retry(0, Duration::ZERO)
        .build()
        .unwrap();
    untrusted.gauge("test_value", 1.0).unwrap();
    assert_eq!(untrusted.flush().await, Err(Error::Network));
    let wrong_host = Client::builder(format!("https://127.0.0.1:{}", fixture.address.port()))
        .with_token(TOKEN)
        .with_ca_certificate_pem(fixture.ca.as_bytes())
        .unwrap()
        .with_retry(0, Duration::ZERO)
        .build()
        .unwrap();
    wrong_host.gauge("test_value", 1.0).unwrap();
    assert_eq!(wrong_host.flush().await, Err(Error::Network));
    assert!(fixture.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn redirects_never_forward_bearer() {
    let destination = Fixture::http(|request, _| Reply::ack(request));
    let target = destination.root();
    let fixture = Fixture::http(move |_, _| {
        Reply::new(307, "secret-response-never-exposed").header("Location", &target)
    });
    let client = fixture.client();
    client.counter("test_total", 1.0).unwrap();
    let error = client.flush().await.unwrap_err();
    assert_eq!(error, Error::Http(307));
    assert!(!error.to_string().contains("secret"));
    assert!(destination.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cumulative_counter_latest_gauge_and_raw_timestamps() {
    let fixture = Fixture::http(|request, _| Reply::ack(request));
    let client = fixture.client();
    client
        .batch()
        .counter("events_total", 2.0)
        .counter("events_total", 3.0)
        .gauge("online", 1.0)
        .gauge("online", 7.0)
        .record(Sample::new("raw_value", 4.0).with_timestamp(12345))
        .commit()
        .unwrap();
    client.flush().await.unwrap();
    client.counter("events_total", 1.0).unwrap();
    client.flush().await.unwrap();
    let requests = fixture.requests.lock().unwrap();
    let first = requests[0].samples();
    let second = requests[1].samples();
    assert_eq!(
        first
            .iter()
            .find(|s| s.name == "events_total")
            .unwrap()
            .value,
        5.0
    );
    assert_eq!(
        second
            .iter()
            .find(|s| s.name == "events_total")
            .unwrap()
            .value,
        6.0
    );
    assert_eq!(
        first.iter().find(|s| s.name == "online").unwrap().value,
        7.0
    );
    assert_eq!(
        first
            .iter()
            .find(|s| s.name == "raw_value")
            .unwrap()
            .timestamp,
        12345
    );
    assert!(!second.iter().any(|s| s.name == "raw_value"));
}

#[tokio::test]
async fn partial_ack_never_replays_successful_batches() {
    let failing = Arc::new(AtomicBool::new(true));
    let flag = failing.clone();
    let fixture = Fixture::http(move |request, index| {
        if index == 1 && flag.load(Ordering::Relaxed) {
            Reply::new(503, "unavailable")
        } else {
            Reply::ack(request)
        }
    });
    let client = Client::builder(fixture.root())
        .allow_http_loopback()
        .with_token(TOKEN)
        .with_retry(0, Duration::ZERO)
        .with_limits(Limits {
            max_series: 20,
            max_pending: 20,
            batch_size: 2,
        })
        .build()
        .unwrap();
    for i in 0..5 {
        client
            .record(Sample::new("events", i as f64).with_timestamp(100 + i))
            .unwrap();
    }
    assert_eq!(client.flush().await, Err(Error::Http(503)));
    assert_eq!(client.stats().unwrap().pending_records, 3);
    failing.store(false, Ordering::Relaxed);
    client.flush().await.unwrap();
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[1].body, requests[2].body);
    assert_eq!(requests[0].samples()[0].timestamp, 100);
    assert_eq!(requests[3].samples()[0].timestamp, 104);
    assert_eq!(client.stats().unwrap().pending_records, 0);
}

#[tokio::test]
async fn malformed_or_partial_success_retains_exact_body() {
    for ack in [
        "{}",
        "{\"accepted\":0}",
        "{\"accepted\":1.5}",
        "{\"accepted\":-1}",
        "not-json",
    ] {
        let ack = ack.to_owned();
        let fixture = Fixture::http(move |request, index| {
            if index == 0 {
                Reply::new(200, ack.clone())
            } else {
                Reply::ack(request)
            }
        });
        let client = fixture.client();
        client.record(Sample::new("event", 4.0)).unwrap();
        assert_eq!(client.flush().await, Err(Error::Protocol));
        client.flush().await.unwrap();
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests[0].body, requests[1].body);
    }
}

#[tokio::test]
async fn network_and_http_retry_keep_payload_stable() {
    let fixture = Fixture::http(|request, index| {
        if index == 0 {
            Reply::new(503, "synthetic")
        } else {
            Reply::ack(request)
        }
    });
    let client = Client::builder(fixture.root())
        .with_token(TOKEN)
        .allow_http_loopback()
        .with_retry(1, Duration::from_millis(1))
        .build()
        .unwrap();
    client.record(Sample::new("event", 1.0)).unwrap();
    client.flush().await.unwrap();
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body, requests[1].body);
}

#[tokio::test]
async fn retry_after_survives_separate_flush_and_honors_deadline() {
    let fixture = Fixture::http(|request, index| {
        if index == 0 {
            Reply::new(429, "backpressure").header("Retry-After", "1")
        } else {
            Reply::ack(request)
        }
    });
    let client = Client::builder(fixture.root())
        .with_token(TOKEN)
        .allow_http_loopback()
        .with_retry(0, Duration::ZERO)
        .with_timeout(Duration::from_millis(80))
        .build()
        .unwrap();
    client.record(Sample::new("event", 1.0)).unwrap();
    assert_eq!(client.flush().await, Err(Error::Http(429)));
    assert_eq!(client.flush().await, Err(Error::Timeout));
    assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    tokio::time::sleep(Duration::from_millis(950)).await;
    client.flush().await.unwrap();
}

#[tokio::test]
async fn bounds_and_atomic_validation_include_inflight_raw() {
    let fixture = Fixture::http(|_, _| Reply::new(503, "fail"));
    let client = Client::builder(fixture.root())
        .with_token(TOKEN)
        .allow_http_loopback()
        .with_retry(0, Duration::ZERO)
        .with_limits(Limits {
            max_series: 2,
            max_pending: 2,
            batch_size: 1,
        })
        .build()
        .unwrap();
    assert!(client
        .batch()
        .counter("ok_total", 1.0)
        .counter("bad name", 1.0)
        .commit()
        .is_err());
    assert_eq!(client.stats().unwrap().series, 0);
    client.record(Sample::new("raw", 1.0)).unwrap();
    client.record(Sample::new("raw", 2.0)).unwrap();
    assert_eq!(client.flush().await, Err(Error::Http(503)));
    assert_eq!(
        client.record(Sample::new("raw", 3.0)),
        Err(Error::QueueFull)
    );
    assert_eq!(client.gauge("raw", 3.0), Err(Error::TypeConflict));
    client.counter("second", 1.0).unwrap();
    assert_eq!(client.gauge("third", 1.0), Err(Error::SeriesLimit));
    assert!(client.gauge("nan", f64::NAN).is_err());
    assert!(client.counter("negative", -1.0).is_err());
    assert!(client
        .record(Sample::new("future", 1.0).with_timestamp(i64::MAX))
        .is_err());
    assert!(client.with_labels([("playerId", "x".repeat(513))]).is_err());
    let mut batch = client.batch();
    for _ in 0..10001 {
        batch = batch.counter("second", 0.0);
    }
    assert_eq!(batch.commit(), Err(Error::BatchLimit));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_recording_flush_and_periodic_shutdown() {
    let fixture = Fixture::http(|request, _| Reply::ack(request));
    let client = fixture.client();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..1000 {
                client.counter("events_total", 1.0).unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    client.start(Duration::from_millis(100)).unwrap();
    assert_eq!(
        client.start(Duration::from_secs(1)),
        Err(Error::AlreadyRunning)
    );
    fixture.wait_requests(1).await;
    client.shutdown().await.unwrap();
    client.shutdown().await.unwrap();
    assert_eq!(client.counter("events_total", 1.0), Err(Error::Closed));
    assert_eq!(client.flush().await, Err(Error::Closed));
    assert!(client.stats().unwrap().closed);
    assert!(!client.stats().unwrap().running);
    assert!(fixture
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.samples()[0].value == 8000.0));
}

#[tokio::test]
async fn cancelled_flush_preserves_body_and_gate_releases() {
    let fixture = Fixture::http(|request, index| {
        if index == 0 {
            Reply::ack(request).delay(Duration::from_millis(200))
        } else {
            Reply::ack(request)
        }
    });
    let client = fixture.client();
    client.record(Sample::new("event", 1.0)).unwrap();
    let clone = client.clone();
    let task = tokio::spawn(async move { clone.flush().await });
    fixture.wait_requests(1).await;
    task.abort();
    let _ = task.await;
    client.flush().await.unwrap();
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests[0].body, requests[1].body);
}

#[tokio::test]
async fn failed_shutdown_can_retry_without_reopening_recording() {
    let fixture = Fixture::http(|request, index| {
        if index == 0 {
            Reply::new(503, "fail")
        } else {
            Reply::ack(request)
        }
    });
    let client = fixture.client();
    client.record(Sample::new("event", 1.0)).unwrap();
    assert_eq!(client.shutdown().await, Err(Error::Http(503)));
    assert!(client.stats().unwrap().closing);
    assert_eq!(client.gauge("new", 1.0), Err(Error::Closed));
    client.shutdown().await.unwrap();
    assert!(client.stats().unwrap().closed);
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests[0].body, requests[1].body);
}

#[tokio::test]
async fn dropped_connection_retries_but_auth_and_validation_failures_do_not() {
    let fixture = Fixture::http(|request, index| {
        if index == 0 {
            Reply::new(0, "")
        } else {
            Reply::ack(request)
        }
    });
    let client = Client::builder(fixture.root())
        .with_token(TOKEN)
        .allow_http_loopback()
        .with_retry(1, Duration::ZERO)
        .build()
        .unwrap();
    client.record(Sample::new("event", 1.0)).unwrap();
    client.flush().await.unwrap();
    {
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].body, requests[1].body);
    }
    for status in [400, 401, 403] {
        let fixture = Fixture::http(move |_, _| Reply::new(status, "secret response"));
        let client = Client::builder(fixture.root())
            .with_token(TOKEN)
            .allow_http_loopback()
            .with_retry(2, Duration::ZERO)
            .build()
            .unwrap();
        client.record(Sample::new("event", 1.0)).unwrap();
        assert_eq!(client.flush().await, Err(Error::Http(status)));
        assert_eq!(fixture.requests.lock().unwrap().len(), 1);
        assert_eq!(client.stats().unwrap().pending_records, 1);
    }
}

#[tokio::test]
async fn oversized_ack_is_bounded_and_retained() {
    let fixture = Fixture::http(|request, index| {
        if index == 0 {
            Reply::new(200, vec![b' '; 65537])
        } else {
            Reply::ack(request)
        }
    });
    let client = fixture.client();
    client.record(Sample::new("event", 1.0)).unwrap();
    assert_eq!(client.flush().await, Err(Error::Protocol));
    client.flush().await.unwrap();
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests[0].body, requests[1].body);
}

#[tokio::test]
async fn encoded_batches_split_at_four_mib_without_losing_samples() {
    let fixture = Fixture::http(|request, _| Reply::ack(request));
    let client = Client::builder(fixture.root())
        .with_token(TOKEN)
        .allow_http_loopback()
        .with_limits(Limits {
            max_series: 1000,
            max_pending: 1000,
            batch_size: 1000,
        })
        .build()
        .unwrap();
    let labels = (0..32)
        .map(|i| (format!("dimension_{i}"), "x".repeat(512)))
        .collect::<Vec<_>>();
    let view = client.with_labels(labels).unwrap();
    for i in 0..300 {
        view.record(Sample::new("wide_record", i as f64).with_timestamp(100 + i))
            .unwrap();
    }
    client.flush().await.unwrap();
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests.iter().map(|r| r.samples().len()).sum::<usize>(),
        300
    );
    assert!(requests.iter().all(|r| r.body.len() <= 4 * 1024 * 1024));
    assert_eq!(client.stats().unwrap().pending_records, 0);
}

#[tokio::test]
async fn updates_wait_for_retained_counter_gauge_snapshot() {
    let fixture = Fixture::http(|request, index| {
        if index == 0 {
            Reply::new(503, "fail")
        } else {
            Reply::ack(request)
        }
    });
    let client = fixture.client();
    client
        .batch()
        .counter("events_total", 2.0)
        .gauge("online", 3.0)
        .commit()
        .unwrap();
    assert_eq!(client.flush().await, Err(Error::Http(503)));
    client
        .batch()
        .counter("events_total", 1.0)
        .gauge("online", 4.0)
        .record(Sample::new("new_record", 7.0))
        .commit()
        .unwrap();
    client.flush().await.unwrap();
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].body, requests[1].body);
    let fresh = requests[2].samples();
    assert_eq!(
        fresh
            .iter()
            .find(|s| s.name == "events_total")
            .unwrap()
            .value,
        3.0
    );
    assert_eq!(
        fresh.iter().find(|s| s.name == "online").unwrap().value,
        4.0
    );
    assert_eq!(fresh.iter().filter(|s| s.name == "new_record").count(), 1);
}
