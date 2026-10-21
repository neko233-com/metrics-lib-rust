//! Explicitly managed, bounded instrumentation for metrics233-server.
//!
//! Building a [`Client`] performs no network activity and spawns no tasks. Record
//! cumulative counters and latest-value gauges, then explicitly [`Client::flush`],
//! or opt into periodic delivery with [`Client::start`]. Always await
//! [`Client::shutdown`] before terminating the caller's Tokio runtime.
//!
//! ```no_run
//! use metrics233::{Client, Identity};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), metrics233::Error> {
//! let token = std::env::var("METRICSPANEL_INGEST_TOKEN").unwrap_or_default();
//! let metrics = Client::builder("https://monitoring.example.com")
//!     .with_token(token)
//!     .with_identity(Identity::new("gateway-1").with_service("game-gateway").with_role("gateway"))
//!     .build()?;
//! metrics.counter("game_gateway_connections_total", 1.0)?;
//! metrics.gauge("game_gateway_connections", 4.0)?;
//! metrics.start(Duration::from_secs(15))?;
//! metrics.shutdown().await?;
//! # Ok(()) }
//! ```

mod client;
mod config;
mod delivery;
mod model;

pub use client::{Batch, Client, Stats};
pub use config::{Builder, Limits};
pub use model::{Error, Identity, Labels, Sample};
