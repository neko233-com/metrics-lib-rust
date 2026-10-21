use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// Low-cardinality dimensions; at most 32 per series, values at most 512 bytes.
pub type Labels = BTreeMap<String, String>;

/// A native sample. A zero timestamp is fixed to registration time (Unix ms).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Sample {
    pub name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: Labels,
    pub value: f64,
    pub timestamp: i64,
}

impl Sample {
    pub fn new(name: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            value,
            timestamp: 0,
        }
    }

    pub fn with_timestamp(mut self, unix_millis: i64) -> Self {
        self.timestamp = unix_millis;
        self
    }

    pub fn with_labels<K: Into<String>, V: Into<String>>(
        mut self,
        labels: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        self.labels
            .extend(labels.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }
}

/// Stable process identity. Never use a player ID or per-player IP here.
#[derive(Clone, Default, Debug)]
pub struct Identity {
    pub(crate) labels: Labels,
}

impl Identity {
    pub fn new(server_id: impl Into<String>) -> Self {
        Self::default().field("serverId", server_id)
    }
    pub fn with_service(self, value: impl Into<String>) -> Self {
        self.field("service", value)
    }
    pub fn with_role(self, value: impl Into<String>) -> Self {
        self.field("role", value)
    }
    pub fn with_environment(self, value: impl Into<String>) -> Self {
        self.field("environment", value)
    }
    pub fn with_instance(self, value: impl Into<String>) -> Self {
        self.field("instance", value)
    }
    fn field(mut self, name: &str, value: impl Into<String>) -> Self {
        self.labels.insert(name.to_owned(), value.into());
        self
    }
}

/// Errors deliberately omit tokens, endpoint URLs and arbitrary response bodies.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    InvalidConfiguration(&'static str),
    InvalidSample(&'static str),
    SeriesLimit,
    QueueFull,
    TypeConflict,
    BatchLimit,
    Closed,
    AlreadyRunning,
    RuntimeRequired,
    StateUnavailable,
    Timeout,
    Network,
    Http(u16),
    Protocol,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(field) => {
                write!(f, "metrics233: invalid configuration ({field})")
            }
            Self::InvalidSample(field) => write!(f, "metrics233: invalid sample ({field})"),
            Self::Http(status) => write!(f, "metrics233: ingest returned HTTP {status}"),
            other => write!(
                f,
                "metrics233: {}",
                match other {
                    Self::SeriesLimit => "series limit reached",
                    Self::QueueFull => "pending record limit reached",
                    Self::TypeConflict => "metric recording type conflict",
                    Self::BatchLimit => "batch operation limit reached",
                    Self::Closed => "client is closing or closed",
                    Self::AlreadyRunning => "periodic delivery is already running",
                    Self::RuntimeRequired => "an active Tokio runtime is required",
                    Self::StateUnavailable => "state lock unavailable",
                    Self::Timeout => "delivery deadline exceeded",
                    Self::Network => "ingest network request failed",
                    Self::Protocol => "invalid ingest acknowledgement",
                    _ => "request failed",
                }
            ),
        }
    }
}

impl std::error::Error for Error {}

pub(crate) fn validate_name(value: &str, metric: bool) -> bool {
    let limit = if metric { 200 } else { 100 };
    !value.is_empty()
        && value.len() <= limit
        && value.bytes().enumerate().all(|(i, b)| {
            b.is_ascii_alphabetic()
                || b == b'_'
                || (metric && b == b':')
                || (i > 0 && b.is_ascii_digit())
        })
}

pub(crate) fn validate_labels(labels: &Labels) -> Result<(), Error> {
    if labels.len() > 32 {
        return Err(Error::InvalidSample("label count"));
    }
    for (key, value) in labels {
        if !validate_name(key, false)
            || key == "__name__"
            || value.len() > 512
            || value.chars().any(char::is_control)
        {
            return Err(Error::InvalidSample("labels"));
        }
    }
    Ok(())
}

pub(crate) fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
