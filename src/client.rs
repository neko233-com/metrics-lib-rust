use crate::config::{Builder, Config};
use crate::model::{now_millis, validate_labels, validate_name, Error, Labels, Sample};
use bytes::Bytes;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Counter,
    Gauge,
    Raw,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Key {
    pub name: String,
    pub labels: Labels,
}

#[derive(Clone)]
pub(crate) struct Entry {
    pub kind: Kind,
    pub value: f64,
}

pub(crate) struct Queued {
    pub sample: Sample,
    pub raw: bool,
}
pub(crate) struct Delivery {
    pub samples: VecDeque<Queued>,
    pub raw_remaining: usize,
    pub body: Option<Bytes>,
    pub count: usize,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Active,
    Closing,
    Closed,
}
struct Worker {
    stop: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

pub(crate) struct State {
    pub entries: BTreeMap<Key, Entry>,
    pub names: BTreeMap<String, Kind>,
    pub pending: VecDeque<Sample>,
    pub delivery: Option<Delivery>,
    pub phase: Phase,
    worker: Option<Worker>,
    pub last_error: Option<Error>,
    pub retry_not_before: Option<Instant>,
}

pub(crate) struct Inner {
    pub config: Config,
    pub state: Mutex<State>,
    pub gate: tokio::sync::Mutex<()>,
    shutdown_gate: tokio::sync::Mutex<()>,
}

impl Inner {
    pub fn lock(&self) -> Result<MutexGuard<'_, State>, Error> {
        self.state.lock().map_err(|_| Error::StateUnavailable)
    }
}

/// A cheap concurrent clone or label view. All clones share registry/lifecycle.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
    pub(crate) labels: Labels,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

/// Safe diagnostic counters; no endpoint, token, labels or response data.
#[derive(Clone, Debug)]
pub struct Stats {
    pub series: usize,
    pub pending_records: usize,
    pub pending_delivery_samples: usize,
    pub running: bool,
    pub closing: bool,
    pub closed: bool,
    pub last_error: Option<Error>,
}

impl Client {
    pub fn builder(root_url: impl Into<String>) -> Builder {
        Builder::new(root_url)
    }

    pub(crate) fn from_config(config: Config, labels: Labels) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                state: Mutex::new(State {
                    entries: BTreeMap::new(),
                    names: BTreeMap::new(),
                    pending: VecDeque::new(),
                    delivery: None,
                    phase: Phase::Active,
                    worker: None,
                    last_error: None,
                    retry_not_before: None,
                }),
                gate: tokio::sync::Mutex::new(()),
                shutdown_gate: tokio::sync::Mutex::new(()),
            }),
            labels,
        }
    }

    pub fn with_labels<K: Into<String>, V: Into<String>>(
        &self,
        labels: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self, Error> {
        let mut view = self.clone();
        view.labels
            .extend(labels.into_iter().map(|(k, v)| (k.into(), v.into())));
        validate_labels(&view.labels)?;
        Ok(view)
    }

    /// Add a finite nonnegative delta to a process-lifetime cumulative counter.
    pub fn counter(&self, name: impl Into<String>, delta: f64) -> Result<(), Error> {
        self.commit(vec![Operation::Counter(name.into(), delta)])
    }
    /// Replace the most recent gauge value for this name/label series.
    pub fn gauge(&self, name: impl Into<String>, value: f64) -> Result<(), Error> {
        self.commit(vec![Operation::Gauge(name.into(), value)])
    }
    /// Queue an individual timestamped raw sample; does not coalesce records.
    pub fn record(&self, sample: Sample) -> Result<(), Error> {
        self.commit(vec![Operation::Record(sample)])
    }
    pub fn batch(&self) -> Batch {
        Batch {
            client: self.clone(),
            operations: vec![],
            overflow: false,
        }
    }

    fn commit(&self, operations: Vec<Operation>) -> Result<(), Error> {
        let mut state = self.inner.lock()?;
        if state.phase != Phase::Active {
            return Err(Error::Closed);
        }
        let mut proposed = BTreeMap::<Key, Entry>::new();
        let mut names = BTreeMap::new();
        let mut records = Vec::new();
        let now = now_millis();
        for op in operations {
            let (name, value, kind, mut labels, timestamp) = match op {
                Operation::Counter(name, value) => {
                    (name, value, Kind::Counter, self.labels.clone(), now)
                }
                Operation::Gauge(name, value) => {
                    (name, value, Kind::Gauge, self.labels.clone(), now)
                }
                Operation::Record(sample) => {
                    let mut labels = self.labels.clone();
                    labels.extend(sample.labels);
                    (
                        sample.name,
                        sample.value,
                        Kind::Raw,
                        labels,
                        if sample.timestamp == 0 {
                            now
                        } else {
                            sample.timestamp
                        },
                    )
                }
            };
            if !validate_name(&name, true)
                || !value.is_finite()
                || timestamp <= 0
                || timestamp > now.saturating_add(300_000)
                || (kind == Kind::Counter && value < 0.0)
            {
                return Err(Error::InvalidSample("name/value/timestamp"));
            }
            validate_labels(&labels)?;
            if state
                .names
                .get(&name)
                .or_else(|| names.get(&name))
                .is_some_and(|existing| *existing != kind)
            {
                return Err(Error::TypeConflict);
            }
            names.insert(name.clone(), kind);
            let key = Key {
                name: name.clone(),
                labels: std::mem::take(&mut labels),
            };
            let old = proposed.get(&key).or_else(|| state.entries.get(&key));
            let next = if kind == Kind::Counter {
                old.map_or(0.0, |v| v.value) + value
            } else {
                value
            };
            if !next.is_finite() {
                return Err(Error::InvalidSample("counter overflow"));
            }
            if kind == Kind::Raw {
                records.push(Sample {
                    name,
                    labels: key.labels.clone(),
                    value,
                    timestamp,
                });
            }
            proposed.insert(key, Entry { kind, value: next });
        }
        let added = proposed
            .keys()
            .filter(|key| !state.entries.contains_key(*key))
            .count();
        if state.entries.len() + added > self.inner.config.limits.max_series {
            return Err(Error::SeriesLimit);
        }
        let in_flight = state.delivery.as_ref().map_or(0, |d| d.raw_remaining);
        if state.pending.len() + in_flight + records.len() > self.inner.config.limits.max_pending {
            return Err(Error::QueueFull);
        }
        state.entries.extend(proposed);
        state.names.extend(names);
        state.pending.extend(records);
        Ok(())
    }

    pub fn stats(&self) -> Result<Stats, Error> {
        let state = self.inner.lock()?;
        Ok(Stats {
            series: state.entries.len(),
            pending_records: state.pending.len()
                + state.delivery.as_ref().map_or(0, |d| d.raw_remaining),
            pending_delivery_samples: state.delivery.as_ref().map_or(0, |d| d.samples.len()),
            running: state.worker.is_some(),
            closing: state.phase == Phase::Closing,
            closed: state.phase == Phase::Closed,
            last_error: state.last_error.clone(),
        })
    }

    /// Enable one background worker explicitly. First flush is immediate;
    /// subsequent rounds wait interval after completion (no catch-up burst).
    pub fn start(&self, interval: Duration) -> Result<(), Error> {
        if interval < Duration::from_millis(100) || interval > Duration::from_secs(86400) {
            return Err(Error::InvalidConfiguration("periodic interval"));
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeRequired)?;
        let mut state = self.inner.lock()?;
        if state.phase != Phase::Active {
            return Err(Error::Closed);
        }
        if state.worker.is_some() {
            return Err(Error::AlreadyRunning);
        }
        let weak = Arc::downgrade(&self.inner);
        let (stop, mut receiver) = watch::channel(false);
        let handle = runtime.spawn(async move {
            loop {
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                let client = Client {
                    inner,
                    labels: Labels::new(),
                };
                tokio::select! { biased;
                    _ = receiver.changed() => break,
                    _ = client.flush() => {}
                }
                drop(client);
                tokio::select! { biased;
                    _ = receiver.changed() => break,
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        });
        state.worker = Some(Worker { stop, handle });
        Ok(())
    }

    /// Flush retained delivery first, then one current registry snapshot.
    /// Cancellation preserves the current unacknowledged body/timestamps.
    pub async fn flush(&self) -> Result<(), Error> {
        self.flush_mode(false).await
    }

    pub(crate) async fn flush_mode(&self, closing: bool) -> Result<(), Error> {
        let result = tokio::time::timeout(self.inner.config.timeout, self.flush_inner(closing))
            .await
            .unwrap_or(Err(Error::Timeout));
        if let Ok(mut state) = self.inner.lock() {
            state.last_error = result.as_ref().err().cloned();
        }
        result
    }

    /// Stop recording and the worker, then flush. A failed shutdown preserves
    /// data in closing state; call shutdown again to retry. Success is idempotent.
    pub async fn shutdown(&self) -> Result<(), Error> {
        tokio::time::timeout(self.inner.config.timeout, self.shutdown_inner())
            .await
            .unwrap_or(Err(Error::Timeout))
    }

    async fn shutdown_inner(&self) -> Result<(), Error> {
        let _shutdown = self.inner.shutdown_gate.lock().await;
        let worker = {
            let mut state = self.inner.lock()?;
            if state.phase == Phase::Closed {
                return Ok(());
            }
            state.phase = Phase::Closing;
            state.worker.take()
        };
        if let Some(worker) = worker {
            let _ = worker.stop.send(true);
            let _ = worker.handle.await;
        }
        self.flush_mode(true).await?;
        self.inner.lock()?.phase = Phase::Closed;
        Ok(())
    }
}

enum Operation {
    Counter(String, f64),
    Gauge(String, f64),
    Record(Sample),
}

/// An atomic in-memory registration batch (up to 10,000 operations). The
/// consuming commit makes accidental double commits impossible in safe Rust.
pub struct Batch {
    client: Client,
    operations: Vec<Operation>,
    overflow: bool,
}
impl Batch {
    fn push(mut self, op: Operation) -> Self {
        if self.operations.len() >= 10_000 {
            self.overflow = true;
        } else {
            self.operations.push(op);
        }
        self
    }
    pub fn counter(self, name: impl Into<String>, delta: f64) -> Self {
        self.push(Operation::Counter(name.into(), delta))
    }
    pub fn gauge(self, name: impl Into<String>, value: f64) -> Self {
        self.push(Operation::Gauge(name.into(), value))
    }
    pub fn record(self, sample: Sample) -> Self {
        self.push(Operation::Record(sample))
    }
    pub fn commit(self) -> Result<(), Error> {
        if self.overflow {
            return Err(Error::BatchLimit);
        }
        self.client.commit(self.operations)
    }
}
