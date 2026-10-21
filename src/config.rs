use crate::client::Client;
use crate::model::{validate_labels, Error, Identity, Labels};
use reqwest::{Certificate, Url};
use std::net::IpAddr;
use std::time::Duration;

/// Registry, pending raw-record and per-request bounds. Raw records in flight
/// still count toward max_pending. Series are bounded for the entire lifetime.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_series: usize,
    pub max_pending: usize,
    pub batch_size: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_series: 1000,
            max_pending: 1000,
            batch_size: 500,
        }
    }
}

/// Side-effect-free configuration; build does not connect or start a task.
pub struct Builder {
    root: String,
    token: String,
    pub(crate) labels: Labels,
    pub(crate) timeout: Duration,
    pub(crate) retries: u8,
    pub(crate) backoff: Duration,
    pub(crate) limits: Limits,
    loopback_http: bool,
    certificates: Vec<Certificate>,
}

pub(crate) struct Config {
    pub endpoint: Url,
    pub authorization: reqwest::header::HeaderValue,
    pub http: reqwest::Client,
    pub timeout: Duration,
    pub retries: u8,
    pub backoff: Duration,
    pub limits: Limits,
}

impl Builder {
    pub(crate) fn new(root: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            token: String::new(),
            labels: Labels::new(),
            timeout: Duration::from_secs(5),
            retries: 2,
            backoff: Duration::from_millis(100),
            limits: Limits::default(),
            loopback_http: false,
            certificates: vec![],
        }
    }
    /// Use an ingest-only token; the SDK never reads environment variables.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = token.into();
        self
    }
    pub fn with_identity(mut self, identity: Identity) -> Self {
        self.labels.extend(identity.labels);
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
    /// End-to-end deadline of each flush/shutdown, including gate/retry waits.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    /// Additional retries (0..=5), only for network errors, 429 and 5xx.
    pub fn with_retry(mut self, retries: u8, backoff: Duration) -> Self {
        self.retries = retries;
        self.backoff = backoff;
        self
    }
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    /// Explicit development-only HTTP exception for numeric loopback/localhost.
    /// Remote HTTP endpoints remain rejected; there is no insecure TLS option.
    pub fn allow_http_loopback(mut self) -> Self {
        self.loopback_http = true;
        self
    }
    /// Add a private CA without disabling normal certificate/hostname validation.
    pub fn with_ca_certificate_pem(mut self, pem: &[u8]) -> Result<Self, Error> {
        self.certificates.push(
            Certificate::from_pem(pem)
                .map_err(|_| Error::InvalidConfiguration("CA certificate"))?,
        );
        Ok(self)
    }
    pub fn build(self) -> Result<Client, Error> {
        if self.token.len() < 32
            || self.token.len() > 8192
            || !self.token.bytes().all(|b| (0x21..=0x7e).contains(&b))
        {
            return Err(Error::InvalidConfiguration("token"));
        }
        validate_labels(&self.labels)?;
        if self.labels.iter().any(|(k, v)| {
            matches!(
                k.as_str(),
                "serverId" | "service" | "role" | "environment" | "instance"
            ) && v.is_empty()
        }) {
            return Err(Error::InvalidConfiguration("identity"));
        }
        if self.timeout.is_zero()
            || self.timeout > Duration::from_secs(300)
            || self.retries > 5
            || self.backoff > Duration::from_secs(60)
        {
            return Err(Error::InvalidConfiguration("delivery deadline/retry"));
        }
        let l = self.limits;
        if !(1..=100_000).contains(&l.max_series)
            || !(1..=100_000).contains(&l.max_pending)
            || !(1..=10_000).contains(&l.batch_size)
        {
            return Err(Error::InvalidConfiguration("capacity"));
        }
        let mut endpoint =
            Url::parse(&self.root).map_err(|_| Error::InvalidConfiguration("root URL"))?;
        let host = endpoint
            .host_str()
            .ok_or(Error::InvalidConfiguration("root URL host"))?;
        let loopback = host.eq_ignore_ascii_case("localhost")
            || host
                .trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        if (endpoint.scheme() != "https"
            && !(self.loopback_http && endpoint.scheme() == "http" && loopback))
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(Error::InvalidConfiguration("secure root URL"));
        }
        endpoint.set_path(&format!(
            "{}/api/v1/ingest",
            endpoint.path().trim_end_matches('/')
        ));
        let mut authorization =
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", self.token))
                .map_err(|_| Error::InvalidConfiguration("token"))?;
        authorization.set_sensitive(true);
        let mut http = reqwest::Client::builder()
            .use_rustls_tls()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .referer(false)
            .https_only(!self.loopback_http)
            .user_agent(concat!("metrics233-rust/", env!("CARGO_PKG_VERSION")))
            .timeout(self.timeout)
            .pool_max_idle_per_host(1);
        // Disable automatic decoding even when a consuming application unifies
        // reqwest gzip/brotli/deflate features into this dependency graph.
        http = http.no_gzip().no_brotli().no_deflate().no_zstd();
        for certificate in self.certificates {
            http = http.add_root_certificate(certificate);
        }
        let config = Config {
            endpoint,
            authorization,
            http: http
                .build()
                .map_err(|_| Error::InvalidConfiguration("HTTP transport"))?,
            timeout: self.timeout,
            retries: self.retries,
            backoff: self.backoff,
            limits: self.limits,
        };
        Ok(Client::from_config(config, self.labels))
    }
}
