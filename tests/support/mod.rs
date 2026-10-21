#![allow(dead_code)]
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub const TOKEN: &str = "fixture-token-synthetic-32-characters";

#[derive(Clone, Debug)]
pub struct Request {
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub sni: Option<String>,
}
impl Request {
    pub fn samples(&self) -> Vec<metrics233::Sample> {
        serde_json::from_slice::<serde_json::Value>(&self.body).unwrap()["samples"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| serde_json::from_value(v.clone()).unwrap())
            .collect()
    }
}

pub struct Reply {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub delay: Duration,
}
impl Reply {
    pub fn ack(request: &Request) -> Self {
        Self::new(200, format!("{{\"accepted\":{}}}", request.samples().len()))
    }
    pub fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            body: body.into(),
            headers: vec![],
            delay: Duration::ZERO,
        }
    }
    pub fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }
    pub fn delay(mut self, duration: Duration) -> Self {
        self.delay = duration;
        self
    }
}

pub struct Fixture {
    pub address: SocketAddr,
    pub requests: Arc<Mutex<Vec<Request>>>,
    pub ca: String,
    pub tls: bool,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Fixture {
    pub fn http(handler: impl Fn(&Request, usize) -> Reply + Send + Sync + 'static) -> Self {
        Self::new(false, handler)
    }
    pub fn https(handler: impl Fn(&Request, usize) -> Reply + Send + Sync + 'static) -> Self {
        Self::new(true, handler)
    }
    fn new(tls: bool, handler: impl Fn(&Request, usize) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (config, ca) = if tls {
            let cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let config = rustls::ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![CertificateDer::from(cert.cert.der().to_vec())],
                    PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into(),
                )
                .unwrap();
            (Some(Arc::new(config)), cert.cert.pem())
        } else {
            (None, String::new())
        };
        let work_requests = requests.clone();
        let work_stop = stop.clone();
        let handler = Arc::new(handler);
        let handle = thread::spawn(move || {
            let mut children = Vec::new();
            while !work_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let config = config.clone();
                        let requests = work_requests.clone();
                        let handler = handler.clone();
                        children.push(thread::spawn(move || {
                            stream.set_nonblocking(false).unwrap();
                            stream
                                .set_read_timeout(Some(Duration::from_secs(3)))
                                .unwrap();
                            stream
                                .set_write_timeout(Some(Duration::from_secs(3)))
                                .unwrap();
                            if let Some(config) = config {
                                let Ok(connection) = rustls::ServerConnection::new(config) else {
                                    return;
                                };
                                let mut stream = rustls::StreamOwned::new(connection, stream);
                                while stream.conn.is_handshaking() {
                                    if stream.conn.complete_io(&mut stream.sock).is_err() {
                                        return;
                                    }
                                }
                                let sni = stream.conn.server_name().map(str::to_owned);
                                serve(&mut stream, sni, requests, &*handler);
                            } else {
                                let mut stream = stream;
                                serve(&mut stream, None, requests, &*handler);
                            }
                        }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(_) => break,
                }
            }
            for child in children {
                child.join().unwrap();
            }
        });
        Self {
            address,
            requests,
            ca,
            tls,
            stop,
            handle: Some(handle),
        }
    }
    pub fn root(&self) -> String {
        format!(
            "{}://{}:{}",
            if self.tls { "https" } else { "http" },
            if self.tls { "localhost" } else { "127.0.0.1" },
            self.address.port()
        )
    }
    pub fn client(&self) -> metrics233::Client {
        let mut builder = metrics233::Client::builder(self.root())
            .with_token(TOKEN)
            .with_retry(0, Duration::ZERO);
        if self.tls {
            builder = builder.with_ca_certificate_pem(self.ca.as_bytes()).unwrap();
        } else {
            builder = builder.allow_http_loopback();
        }
        builder.build().unwrap()
    }
    pub async fn wait_requests(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.requests.lock().unwrap().len() < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}

fn serve<T: Read + Write>(
    stream: &mut T,
    sni: Option<String>,
    requests: Arc<Mutex<Vec<Request>>>,
    handler: &dyn Fn(&Request, usize) -> Reply,
) {
    let Ok(mut request) = read_request(stream) else {
        return;
    };
    request.sni = sni;
    let index = {
        let mut requests = requests.lock().unwrap();
        let index = requests.len();
        requests.push(request.clone());
        index
    };
    let reply = handler(&request, index);
    if reply.status == 0 {
        return;
    }
    thread::sleep(reply.delay);
    let mut head = format!(
        "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n",
        reply.status,
        reply.body.len()
    );
    for (key, value) in reply.headers {
        head.push_str(&format!("{key}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&reply.body);
    let _ = stream.flush();
}
fn read_request<T: Read>(stream: &mut T) -> std::io::Result<Request> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() > 16384 {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        let mut byte = [0];
        stream.read_exact(&mut byte)?;
        head.push(byte[0]);
    }
    let text = String::from_utf8(head).map_err(|_| std::io::ErrorKind::InvalidData)?;
    let mut lines = text.split("\r\n");
    let first = lines.next().unwrap_or_default();
    let path = first.split_whitespace().nth(1).unwrap_or_default().into();
    let mut headers = BTreeMap::new();
    for line in lines.filter(|v| !v.is_empty()) {
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.to_ascii_lowercase(), value.trim().into());
        }
    }
    let length = headers
        .get("content-length")
        .and_then(|v: &String| v.parse::<usize>().ok())
        .unwrap_or(0);
    if length > 4 * 1024 * 1024 {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body)?;
    Ok(Request {
        path,
        headers,
        body,
        sni: None,
    })
}
