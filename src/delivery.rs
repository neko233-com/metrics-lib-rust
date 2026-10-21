use crate::client::{Client, Delivery, Kind, Phase, Queued};
use crate::model::{now_millis, Error, Sample};
use bytes::Bytes;
use serde::Deserialize;
use std::collections::VecDeque;
use std::time::{Duration, SystemTime};
use tokio::time::Instant;

const MAX_BODY: usize = 4 * 1024 * 1024;
const MAX_ACK: usize = 64 * 1024;

impl Client {
    pub(crate) async fn flush_inner(&self, closing: bool) -> Result<(), Error> {
        let _gate = self.inner.gate.lock().await;
        let retained = {
            let mut state = self.inner.lock()?;
            if state.phase == Phase::Closed {
                return if closing { Ok(()) } else { Err(Error::Closed) };
            }
            if state.phase == Phase::Closing && !closing {
                return Err(Error::Closed);
            }
            let retained = state.delivery.is_some();
            if !retained {
                snapshot(&mut state);
            }
            retained
        };
        self.flush_delivery().await?;
        if retained {
            {
                let mut state = self.inner.lock()?;
                snapshot(&mut state);
            }
            self.flush_delivery().await?;
        }
        Ok(())
    }

    async fn flush_delivery(&self) -> Result<(), Error> {
        loop {
            let (body, count) = {
                let mut state = self.inner.lock()?;
                let Some(delivery) = state.delivery.as_mut() else {
                    return Ok(());
                };
                if delivery.body.is_none() {
                    let mut body = b"{\"samples\":[".to_vec();
                    let mut count = 0;
                    for queued in delivery
                        .samples
                        .iter()
                        .take(self.inner.config.limits.batch_size)
                    {
                        let sample = serde_json::to_vec(&queued.sample)
                            .map_err(|_| Error::InvalidSample("JSON encoding"))?;
                        if body.len() + usize::from(count > 0) + sample.len() + 2 > MAX_BODY {
                            break;
                        }
                        if count > 0 {
                            body.push(b',');
                        }
                        body.extend(sample);
                        count += 1;
                    }
                    if count == 0 {
                        return Err(Error::InvalidSample("encoded sample exceeds 4 MiB"));
                    }
                    body.extend(b"]}");
                    delivery.body = Some(Bytes::from(body));
                    delivery.count = count;
                }
                (
                    delivery.body.clone().ok_or(Error::StateUnavailable)?,
                    delivery.count,
                )
            };
            self.send(body, count).await?;
            // No await between successful ACK and removal. Already acknowledged
            // batches cannot be replayed by a later flush or cancellation.
            let mut state = self.inner.lock()?;
            let delivery = state.delivery.as_mut().ok_or(Error::StateUnavailable)?;
            for _ in 0..count {
                if delivery.samples.pop_front().is_some_and(|q| q.raw) {
                    delivery.raw_remaining -= 1;
                }
            }
            delivery.body = None;
            delivery.count = 0;
            if delivery.samples.is_empty() {
                state.delivery = None;
            }
        }
    }

    async fn send(&self, body: Bytes, count: usize) -> Result<(), Error> {
        let config = &self.inner.config;
        for attempt in 0..=config.retries {
            let not_before = self.inner.lock()?.retry_not_before;
            if let Some(until) = not_before {
                tokio::time::sleep_until(until).await;
            }
            let request = config
                .http
                .post(config.endpoint.clone())
                .header(reqwest::header::AUTHORIZATION, config.authorization.clone())
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .header(reqwest::header::ACCEPT_ENCODING, "identity")
                .body(body.clone());
            let response = request.send().await;
            let mut delay = config
                .backoff
                .saturating_mul(1u32 << attempt)
                .min(Duration::from_secs(5));
            let error = match response {
                Err(error) => {
                    if error.is_timeout() {
                        Error::Timeout
                    } else {
                        Error::Network
                    }
                }
                Ok(mut response) => {
                    let status = response.status().as_u16();
                    if (200..300).contains(&status) {
                        if response
                            .content_length()
                            .is_some_and(|n| n > MAX_ACK as u64)
                        {
                            return Err(Error::Protocol);
                        }
                        let mut data = Vec::new();
                        while let Some(chunk) =
                            response.chunk().await.map_err(|_| Error::Protocol)?
                        {
                            if data.len() + chunk.len() > MAX_ACK {
                                return Err(Error::Protocol);
                            }
                            data.extend_from_slice(&chunk);
                        }
                        #[derive(Deserialize)]
                        struct Ack {
                            accepted: usize,
                        }
                        let ack: Ack =
                            serde_json::from_slice(&data).map_err(|_| Error::Protocol)?;
                        return if ack.accepted == count {
                            Ok(())
                        } else {
                            Err(Error::Protocol)
                        };
                    }
                    if status != 429 && !(500..600).contains(&status) {
                        return Err(Error::Http(status));
                    }
                    if let Some(value) = response
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                    {
                        let retry_after = parse_retry_after(value);
                        delay = delay.max(retry_after);
                        if !retry_after.is_zero() {
                            let until = Instant::now() + retry_after;
                            let mut state = self.inner.lock()?;
                            state.retry_not_before =
                                Some(state.retry_not_before.map_or(until, |old| old.max(until)));
                        }
                    }
                    Error::Http(status)
                }
            };
            if attempt == config.retries {
                return Err(error);
            }
            tokio::time::sleep(delay).await;
        }
        Err(Error::Network)
    }
}

fn snapshot(state: &mut crate::client::State) {
    let now = now_millis();
    let mut samples = VecDeque::new();
    for (key, entry) in &state.entries {
        if entry.kind != Kind::Raw {
            samples.push_back(Queued {
                sample: Sample {
                    name: key.name.clone(),
                    labels: key.labels.clone(),
                    value: entry.value,
                    timestamp: now,
                },
                raw: false,
            });
        }
    }
    let raw_remaining = state.pending.len();
    samples.extend(
        state
            .pending
            .drain(..)
            .map(|sample| Queued { sample, raw: true }),
    );
    if !samples.is_empty() {
        state.delivery = Some(Delivery {
            samples,
            raw_remaining,
            body: None,
            count: 0,
        });
    }
}

fn parse_retry_after(value: &str) -> Duration {
    // Bound an untrusted remote scheduling value while honoring normal dates.
    const MAX: Duration = Duration::from_secs(7 * 86400);
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Duration::from_secs(seconds).min(MAX);
    }
    httpdate::parse_http_date(value)
        .ok()
        .and_then(|time| time.duration_since(SystemTime::now()).ok())
        .unwrap_or_default()
        .min(MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_after_seconds_dates_and_bounds() {
        assert_eq!(parse_retry_after("2"), Duration::from_secs(2));
        assert!(
            parse_retry_after(&httpdate::fmt_http_date(
                SystemTime::now() + Duration::from_secs(120)
            )) > Duration::from_secs(118)
        );
        assert_eq!(parse_retry_after("invalid"), Duration::ZERO);
        assert_eq!(
            parse_retry_after("99999999999"),
            Duration::from_secs(7 * 86400)
        );
    }
}
