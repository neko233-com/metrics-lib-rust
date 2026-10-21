//! Opt-in contract test against a caller-supplied local official server binary.
#![cfg(feature = "real-server-tests")]
use metrics233::{Client, Identity, Sample};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Server {
    process: Child,
    directory: PathBuf,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[tokio::test]
async fn native_ingest_only_token_and_query_contract() {
    let binary = std::env::var_os("METRICS233_TEST_SERVER_BIN")
        .expect("set METRICS233_TEST_SERVER_BIN to a local metrics233-server executable");
    let admin = "synthetic-management-token-contract-32-chars";
    let ingest = "synthetic-reporting-token-contract-32-chars";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let root = format!("http://{address}");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "metrics233-rust-contract-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&directory).unwrap();
    let mut command = Command::new(binary);
    command
        .current_dir(&directory)
        .env_clear()
        .env("METRICSPANEL_TOKEN", admin)
        .env("METRICSPANEL_INGEST_TOKEN", ingest)
        .args([
            "serve",
            "--listen",
            &address.to_string(),
            "--auth",
            "token",
            "--root-url",
            &root,
            "--storage",
            "sqlite",
            "--db",
            "metrics.db",
            "--identity-db",
            "identity.db",
            "--plugins-dir",
            "plugins",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    if let Some(system) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", system);
    }
    let process = command.spawn().expect("start isolated official server");
    let mut server = Server { process, directory };
    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            assert!(
                server.process.try_wait().unwrap().is_none(),
                "isolated server exited before health"
            );
            if http
                .get(format!("{root}/api/health"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    for token in [None, Some("synthetic-wrong-token-contract-32-chars")] {
        let mut request = http
            .post(format!("{root}/api/v1/ingest"))
            .header("Content-Type", "application/json")
            .body("{\"samples\":[]}");
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        assert_eq!(request.send().await.unwrap().status().as_u16(), 401);
    }
    let metrics = Client::builder(&root)
        .allow_http_loopback()
        .with_token(ingest)
        .with_identity(Identity::new("contract-gateway-1").with_role("gateway"))
        .build()
        .unwrap();
    metrics
        .batch()
        .counter("rust_contract_events_total", 3.0)
        .gauge("rust_contract_online", 7.0)
        .record(Sample::new("rust_contract_record", 5.0))
        .commit()
        .unwrap();
    metrics.flush().await.unwrap();
    for route in [
        "/api/v1/query?metric=rust_contract_online",
        "/api/v1/reporting-sources",
    ] {
        assert_eq!(
            http.get(format!("{root}{route}"))
                .bearer_auth(ingest)
                .send()
                .await
                .unwrap()
                .status()
                .as_u16(),
            403
        );
    }
    for (metric, expected) in [
        ("rust_contract_events_total", 3.0),
        ("rust_contract_online", 7.0),
        ("rust_contract_record", 5.0),
    ] {
        let response = http
            .get(format!("{root}/api/v1/query"))
            .bearer_auth(admin)
            .query(&[("metric", metric)])
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let body: serde_json::Value =
            serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(body["series"].as_array().unwrap().len(), 1);
        assert_eq!(
            body["series"][0]["labels"]["serverId"],
            "contract-gateway-1"
        );
        assert!(body["series"][0]["points"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["value"].as_f64() == Some(expected)));
    }
    metrics.shutdown().await.unwrap();
}
