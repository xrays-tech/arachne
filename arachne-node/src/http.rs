//! Hyper HTTP/1.1 server for `/readyz`, `/metrics`, `/kv/*`, and `/members*`.
//!
//! Replaces the hand-rolled blocking server (propsol rev U, U1). The old one
//! used a nonblocking accept loop with a 20ms `WouldBlock` sleep and served one
//! request per TCP connection (no keep-alive), which floored single-client latency
//! at ~20ms and capped single-client throughput at ~45 ops/s. Hyper + tokio:
//!
//! * `TcpListener::accept().await` is a real epoll wait — the idle poll is gone;
//! * HTTP/1.1 **keep-alive** is on by default (one connection services many
//!   requests, dropping the per-request TCP setup);
//! * one task per connection, bounded by a [`Semaphore`];
//! * the request ceiling is preserved via `http1().max_buf_size(...)` and the
//!   stall timeout via `header_read_timeout`, so oversized/slow clients cannot
//!   wedge the server.
//!
//! The handler is **async** (boxed future on the trait): the node
//! implementation in `main` awaits the KV client directly instead of bridging with
//! `rt.block_on`. The path handed to the handler is the **raw** request target
//! including the `?query` (the handler splits it, as before).

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::service::Service;
use hyper::{Request, Response, Uri};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

/// Max concurrent connections (mirrors the pre-upgrade bounded model).
const MAX_CONNECTIONS: usize = 256;
/// Cap on the request (line + headers) in bytes — preserves the old 8 KiB
/// request-line ceiling, so oversized input yields a 431.
const MAX_BUF_SIZE: usize = 8 * 1024;
/// Read timeout for an idle/header read — preserves the old 5s stall timeout.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A single HTTP response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// Numeric status code.
    pub status: u16,
    /// Reason phrase (static).
    pub reason: &'static str,
    /// `Content-Type` header value.
    pub content_type: &'static str,
    /// Response body bytes.
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// `200 OK` with the given content type and body.
    pub fn ok(content_type: &'static str, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            reason: "OK",
            content_type,
            body: body.into(),
        }
    }

    /// `200 OK` with a plain-text body.
    pub fn text(body: impl Into<Vec<u8>>) -> Self {
        Self::ok("text/plain; charset=utf-8", body)
    }

    /// `503 Service Unavailable` with a plain-text body.
    pub fn unavailable(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 503,
            reason: "Service Unavailable",
            content_type: "text/plain; charset=utf-8",
            body: body.into(),
        }
    }

    /// `404 Not Found`.
    pub fn not_found() -> Self {
        Self {
            status: 404,
            reason: "Not Found",
            content_type: "text/plain; charset=utf-8",
            body: b"not found".to_vec(),
        }
    }

    /// `405 Method Not Allowed`.
    pub fn method_not_allowed() -> Self {
        Self {
            status: 405,
            reason: "Method Not Allowed",
            content_type: "text/plain; charset=utf-8",
            body: b"method not allowed".to_vec(),
        }
    }

    /// `409 Conflict` with an explanatory body.
    ///
    /// Used for "the cluster is in a state where this request cannot be
    /// honoured yet" (no leader, a configuration change already pending,
    /// a leadership handover needed first) — all of which a retry can fix.
    pub fn conflict(body: &str) -> Self {
        Self {
            status: 409,
            reason: "Conflict",
            content_type: "text/plain; charset=utf-8",
            body: body.as_bytes().to_vec(),
        }
    }

    /// `400 Bad Request`.
    pub fn bad_request() -> Self {
        Self {
            status: 400,
            reason: "Bad Request",
            content_type: "text/plain; charset=utf-8",
            body: b"bad request".to_vec(),
        }
    }

    /// `431 Request Header Fields Too Large`.
    pub fn request_line_too_long() -> Self {
        Self {
            status: 431,
            reason: "Request Header Fields Too Large",
            content_type: "text/plain; charset=utf-8",
            body: b"request line too long".to_vec(),
        }
    }
}

/// Routes a request to a response.
///
/// Async so the node implementation can await the KV client directly. `path` is
/// the raw request target (including any `?query`); the handler splits it.
pub trait HttpHandler: Send + Sync + 'static {
    /// Handle a request for `method` + `path` (with any `?query` included).
    fn handle(
        &self,
        method: &str,
        path: &str,
    ) -> Pin<Box<dyn Future<Output = HttpResponse> + Send + '_>>;
}

/// Build a hyper response from an [`HttpResponse`].
fn to_hyper(resp: HttpResponse) -> Response<Full<Bytes>> {
    let builder = Response::builder().status(resp.status);
    // Preserve the exact content type the node advertises.
    if let Ok(res) = builder
        .header("Content-Type", resp.content_type)
        .body(Full::new(Bytes::from(resp.body)))
    {
        res
    } else {
        // Status/header values are static and valid; this is unreachable in
        // practice. Build without the content-type as a safe fallback.
        Response::builder()
            .status(500)
            .body(Full::new(Bytes::from_static(b"internal error")))
            .expect("static fallback response")
    }
}

async fn dispatch<H: HttpHandler>(handler: &H, method: &str, uri: &Uri) -> HttpResponse {
    // `uri.path()` drops the query; the node handler expects the full target so it
    // can split `?stale=1` itself. Reconstruct the target from path + query.
    let target = match uri.query() {
        Some(q) => format!("{}?{q}", uri.path()),
        None => uri.path().to_string(),
    };
    handler.handle(method, &target).await
}

/// Adapter from [`HttpHandler`] to hyper's `Service` trait.
struct HandlerService<H>(Arc<H>);

impl<H: HttpHandler> Service<Request<Incoming>> for HandlerService<H> {
    type Response = Response<Full<Bytes>>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let handler = Arc::clone(&self.0);
        Box::pin(async move {
            // Preserve the old dispatcher's method whitelist before the handler
            // runs: only the KV surface methods (GET/PUT/DELETE) and the
            // membership ops method (POST) reach the handler; every other method is
            // a 405. The old `dispatch_line` enforced this; the hyper rewrite
            // routed *all* methods to the handler, so a `PATCH` was echoed
            // instead of rejected.
            let method = req.method().as_str().to_string();
            if !matches!(method.as_str(), "GET" | "PUT" | "DELETE" | "POST") {
                return Ok(to_hyper(HttpResponse::method_not_allowed()));
            }
            let uri = req.uri().clone();
            let resp = dispatch(handler.as_ref(), &method, &uri).await;
            Ok(to_hyper(resp))
        })
    }
}

/// Serve connections until `shutdown` becomes true, on the current tokio runtime.
///
/// Returns when the shutdown flag is observed; in-flight connections are allowed to
/// finish (graceful). The caller aborts outstanding connection tasks if it wants a
/// hard stop.
pub async fn serve<H: HttpHandler>(
    listener: TcpListener,
    handler: Arc<H>,
    shutdown: Arc<tokio::sync::watch::Sender<bool>>,
) -> std::io::Result<()> {
    let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut shutdown_rx = shutdown.subscribe();
    loop {
        // Race the shutdown watch against the accept half with a biased
        // select: the watch wins whenever both are ready, and — crucially —
        // shutdown is observed even while parked in `accept()` with an idle
        // listener. The original loop only checked the flag *after* an accept
        // returned, so signaling shutdown with no new connection left the
        // server task parked in `accept()` forever (graceful shutdown hung).
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                // `changed()` resolves once per send; the value re-check makes
                // the false->true transition what actually stops the server.
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                // Shutdown may have been signaled concurrently — do not spin
                // up fresh work while stopping.
                if *shutdown.borrow() {
                    break;
                }
                if let Ok(permit) = semaphore.clone().try_acquire_owned() {
                    let handler = Arc::clone(&handler);
                    let _permit = permit;
                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        let service = HandlerService(handler);
                        // Per-connection hyper HTTP/1.1 server via hyper-util's auto
                        // builder (executor + timer wired): keep-alive (default),
                        // request ceiling + stall timeout preserved.
                        let mut auto_builder = auto::Builder::new(TokioExecutor::new());
                        let mut builder = auto_builder.http1();
                        // `header_read_timeout` requires an explicit timer (hyper panics
                        // with "no timer set" otherwise).
                        builder.timer(TokioTimer::new());
                        builder.max_buf_size(MAX_BUF_SIZE);
                        builder.header_read_timeout(HEADER_READ_TIMEOUT);
                        if let Err(e) = builder.serve_connection(io, service).await {
                            eprintln!("[http] connection error: {e}");
                        }
                    });
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct TestHandler;

    impl HttpHandler for TestHandler {
        fn handle(
            &self,
            method: &str,
            path: &str,
        ) -> Pin<Box<dyn Future<Output = HttpResponse> + Send + '_>> {
            let method = method.to_string();
            let path = path.to_string();
            Box::pin(async move {
                match path.as_str() {
                    // Read-only, exactly like the node's real handler: the
                    // dispatcher now lets `POST` through (the membership ops
                    // surface needs it), so "this endpoint is GET-only" is the
                    // handler's job, not the dispatcher's.
                    "/readyz" => {
                        if method != "GET" {
                            return HttpResponse::method_not_allowed();
                        }
                        HttpResponse::text("ready\n")
                    }
                    "/metrics" => {
                        if method != "GET" {
                            return HttpResponse::method_not_allowed();
                        }
                        HttpResponse::ok("text/plain; version=0.0.4", "arachne_term 1\n")
                    }
                    // Echo the method + path so tests can prove `PUT`/`DELETE`/
                    // `POST` are routed to the handler (not rejected as `405`).
                    "/kv/x" => HttpResponse::text(format!("{method} {path}\n")),
                    _ => HttpResponse::not_found(),
                }
            })
        }
    }

    async fn start_server(
    ) -> (SocketAddr, Arc<tokio::sync::watch::Sender<bool>>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, _) = tokio::sync::watch::channel(false);
        let shut = Arc::new(tx);
        let s2 = Arc::clone(&shut);
        let handler = Arc::new(TestHandler);
        let handle = tokio::spawn(async move {
            let _ = serve(listener, handler, s2).await;
        });
        (addr, shut, handle)
    }

    async fn request(addr: SocketAddr, raw: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        stream.write_all(raw.as_bytes()).await.expect("write request");
        stream.flush().await.ok();
        // Hyper keeps the connection alive after the response (no EOF), so read
        // exactly the response head + Content-Length body instead of waiting for
        // the server to close.
        read_response(&mut stream).await
    }

    /// Read one hyper response: headers (through the blank line), then exactly
    /// the `Content-Length` body bytes.
    async fn read_response(stream: &mut tokio::net::TcpStream) -> String {
        // Read until the end of the header block.
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        while buf.len() < 64 * 1024 {
            let n = stream.read(&mut byte).await.expect("read head");
            if n == 0 {
                break;
            }
            buf.push(byte[0]);
            if buf.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        // Extract Content-Length.
        let head = String::from_utf8_lossy(&buf).into_owned();
        let content_length = head
            .split("\r\n")
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split_once(':').map(|(_, v)| v.trim().parse::<usize>().ok()))
            .flatten()
            .unwrap_or(0);
        let mut body = vec![0u8; content_length];
        let mut read = 0;
        while read < content_length {
            let n = stream.read(&mut body[read..]).await.expect("read body");
            if n == 0 {
                break;
            }
            read += n;
        }
        format!("{head}{}", String::from_utf8_lossy(&body))
    }

    #[tokio::test]
    async fn serves_readyz_and_metrics_and_errors() {
        let (addr, shutdown, handle) = start_server().await;

        let ready = request(addr, "GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(ready.starts_with("HTTP/1.1 200"), "got: {ready}");
        assert!(ready.ends_with("ready\n"), "got: {ready}");

        let metrics = request(addr, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(metrics.starts_with("HTTP/1.1 200"), "got: {metrics}");
        assert!(metrics.contains("arachne_term 1"), "got: {metrics}");

        let missing = request(addr, "GET /nope HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(missing.starts_with("HTTP/1.1 404"), "got: {missing}");

        let wrong_method = request(addr, "POST /readyz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(wrong_method.starts_with("HTTP/1.1 405"), "got: {wrong_method}");

        let bare = request(addr, "GET /readyz HTTP/1.1\r\n\r\n").await;
        assert!(bare.starts_with("HTTP/1.1 200"), "got: {bare}");

        shutdown.send(true).ok();
        handle.await.ok();
    }

    #[tokio::test]
    async fn put_and_delete_reach_the_handler() {
        let (addr, shutdown, handle) = start_server().await;

        let put = request(addr, "PUT /kv/x HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(put.contains("PUT /kv/x"), "got: {put}");

        let delete = request(addr, "DELETE /kv/x HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(delete.contains("DELETE /kv/x"), "got: {delete}");

        let post = request(addr, "POST /kv/x HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(post.contains("POST /kv/x"), "got: {post}");

        let unknown = request(addr, "PATCH /kv/x HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(unknown.starts_with("HTTP/1.1 405"), "got: {unknown}");

        shutdown.send(true).ok();
        handle.await.ok();
    }

    #[tokio::test]
    async fn a_client_that_delays_before_sending_still_gets_a_response() {
        let (addr, shutdown, handle) = start_server().await;

        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        tokio::time::sleep(Duration::from_millis(300)).await;
        stream
            .write_all(b"GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .expect("write");
        stream.flush().await.ok();
        let response = read_response(&mut stream).await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        assert!(response.ends_with("ready\n"), "got: {response}");

        shutdown.send(true).ok();
        handle.await.ok();
    }

    #[tokio::test]
    async fn shutdown_with_an_idle_listener_terminates_the_server() {
        // Regression: signaling shutdown with no inbound connection must stop
        // the accept loop promptly. The pre-fix loop only checked the shutdown
        // flag *after* `accept()` returned, so an idle listener left the server
        // task parked in `accept()` forever and graceful shutdown hung.
        let (_addr, shutdown, handle) = start_server().await;
        // Yield once so the spawned `serve` task runs and creates its watch
        // subscriber before we signal; otherwise `send` has no receiver and the
        // value is dropped (a send-before-subscribe race).
        tokio::task::yield_now().await;
        shutdown.send(true).ok();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("server task must terminate once shutdown is signaled")
            .ok();
    }

    #[tokio::test]
    async fn an_oversized_request_line_is_rejected() {
        let (addr, shutdown, handle) = start_server().await;

        let big_path = "a".repeat(9000); // well over the 8 KiB cap
        let raw = format!("GET /{big_path} HTTP/1.1\r\nHost: x\r\n\r\n");
        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        stream.write_all(raw.as_bytes()).await.expect("write");
        stream.flush().await.ok();
        // hyper rejects an oversized head with its own 431 before our handler
        // runs; it then closes the connection, so a lenient read is enough.
        let response = read_response(&mut stream).await;
        assert!(
            response.starts_with("HTTP/1.1 431") || response.starts_with("HTTP/1.1 400"),
            "got: {response:?}"
        );

        let ready = request(addr, "GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(ready.starts_with("HTTP/1.1 200"), "got: {ready}");

        shutdown.send(true).ok();
        handle.await.ok();
    }
}
