# metrics233 Rust SDK

This independent Rust crate sends bounded native JSON to metrics233-server. Keep construction free of network activity and background tasks. Lifecycle must remain explicit through start, flush and shutdown.

Use the existing reqwest 0.12 + rustls transport shared with the first Rust gateway consumer. Keep TLS certificate verification, disable redirects and environment proxies, and never include endpoint/token/response bodies in errors or Debug output. Do not read environment variables implicitly.

Counter is cumulative; gauge is latest value; raw records retain timestamps. Preserve unacknowledged encoded batches, never replay acknowledged batches, and keep series, pending records, labels and HTTP payload bounded. Tests must cover real local HTTP/TLS and concurrency/lifecycle boundaries.

Use only synthetic credentials and loopback/example destinations. No production addresses, player identifiers, per-player IP labels, webhook URLs or credentials belong here. Git commits, pushes, releases and cargo publish are owned by the parent task.

