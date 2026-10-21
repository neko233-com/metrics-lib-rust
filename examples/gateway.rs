use metrics233::{Client, Identity};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The application decides when and how to read private configuration.
    let root = std::env::var("METRICS233_URL")?;
    let token = std::env::var("METRICSPANEL_INGEST_TOKEN")?;
    let server_id = std::env::var("SERVER_ID")?;
    let metrics = Client::builder(root)
        .with_token(token)
        .with_identity(
            Identity::new(server_id)
                .with_service("game-gateway")
                .with_role("gateway"),
        )
        .with_labels([("implementation", "rust")])
        .build()?;

    metrics.start(Duration::from_secs(15))?;
    // Call these at real lifecycle events; never label by player UID/IP.
    metrics
        .batch()
        .counter("game_gateway_connections_total", 1.0)
        .gauge("game_gateway_connections", 1.0)
        .commit()?;
    metrics.flush().await?;
    metrics.shutdown().await?;
    Ok(())
}
