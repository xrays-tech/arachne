//! A minimal, dependency-free HTTP/1.1 server for `/readyz` and `/metrics`.
//!
//! Deliberately hand-rolled (per the M0 decision): it speaks just enough HTTP
//! to serve two GET endpoints and is exercised by unit tests over a real
//! loopback socket. It is not a general-purpose server (no keep-alive, no
//! body parsing, no chunked encoding).

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};

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

    /// `400 Bad Request`.
    pub fn bad_request() -> Self {
        Self {
            status: 400,
            reason: "Bad Request",
            content_type: "text/plain; charset=utf-8",
            body: b"bad request".to_vec(),
        }
    }

    fn write_to(&self, stream: &mut TcpStream) -> std::io::Result<()> {
        let head = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.status,
            self.reason,
            self.content_type,
            self.body.len()
        );
        stream.write_all(head.as_bytes())?;
        stream.write_all(&self.body)?;
        stream.flush()
    }
}

/// Routes a parsed request to a response.
pub trait HttpHandler: Send + Sync + 'static {
    /// Handle a `GET` (or other) request for `path`.
    fn handle(&self, method: &str, path: &str) -> HttpResponse;
}

/// Serve requests until `shutdown` is set. **Blocking** — run it on a
/// dedicated thread.
pub fn serve<H: HttpHandler>(
    listener: &TcpListener,
    handler: &H,
    shutdown: &AtomicBool,
) -> std::io::Result<()> {
    listener.set_nonblocking(true)?;
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                // Best-effort per connection: a client that vanishes must not
                // take the server down.
                let _ = handle_connection(&mut stream, handler);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Read one request line off `stream`, dispatch it, and write the response.
pub fn handle_connection<H: HttpHandler>(
    stream: &mut TcpStream,
    handler: &H,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(()); // client closed before sending anything
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    let response = if method.is_empty() || path.is_empty() {
        HttpResponse::bad_request()
    } else if method != "GET" {
        HttpResponse::method_not_allowed()
    } else {
        handler.handle(method, path)
    };
    response.write_to(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;

    struct TestHandler;

    impl HttpHandler for TestHandler {
        fn handle(&self, _method: &str, path: &str) -> HttpResponse {
            match path {
                "/readyz" => HttpResponse::text("ready\n"),
                "/metrics" => HttpResponse::ok("text/plain; version=0.0.4", "arachne_term 1\n"),
                _ => HttpResponse::not_found(),
            }
        }
    }

    /// Start a server on an ephemeral port; return its address and a shutdown
    /// flag. The thread ends when the flag is set.
    fn start() -> (SocketAddr, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        let shutdown = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || {
            let _ = serve(&listener, &TestHandler, &flag);
        });
        (addr, shutdown, handle)
    }

    fn request(addr: SocketAddr, raw: &str) -> String {
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(std::time::Duration::from_millis(500)))
            .expect("set timeout");
        stream.write_all(raw.as_bytes()).expect("write request");
        let mut response = String::new();
        let _ = std::io::Read::read_to_string(&mut stream, &mut response);
        response
    }

    #[test]
    fn serves_readyz_and_metrics_and_errors() {
        let (addr, shutdown, handle) = start();

        let ready = request(addr, "GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n");
        assert!(ready.starts_with("HTTP/1.1 200 OK"), "got: {ready}");
        assert!(ready.ends_with("ready\n"), "got: {ready}");

        let metrics = request(addr, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
        assert!(metrics.starts_with("HTTP/1.1 200 OK"), "got: {metrics}");
        assert!(metrics.contains("arachne_term 1"), "got: {metrics}");

        let missing = request(addr, "GET /nope HTTP/1.1\r\nHost: x\r\n\r\n");
        assert!(missing.starts_with("HTTP/1.1 404 Not Found"), "got: {missing}");

        let wrong_method = request(addr, "POST /readyz HTTP/1.1\r\nHost: x\r\n\r\n");
        assert!(
            wrong_method.starts_with("HTTP/1.1 405 Method Not Allowed"),
            "got: {wrong_method}"
        );

        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join();
    }
}
