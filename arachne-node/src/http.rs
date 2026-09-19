//! A minimal, dependency-free HTTP/1.1 server for `/readyz` and `/metrics`.
//!
//! Deliberately hand-rolled (per the M0 decision): it speaks just enough HTTP
//! to serve two GET endpoints and is exercised by unit tests over a real
//! loopback socket. It is not a general-purpose server (no keep-alive, no
//! body parsing, no chunked encoding).
//!
//! Robustness notes (M0 final gate):
//! * Each accepted connection is forced into **blocking** mode with a bounded
//!   **read timeout**, so a client that connects and then stalls times out
//!   instead of wedging the sequential accept loop (which would in turn wedge
//!   `main`'s `http_thread.join()` and block SIGINT handling).
//! * The request line is read with a **hard cap** (no unbounded `read_line`),
//!   and the remaining header block is **drained** before the response is
//!   written, so a slow client is never reset while it is still sending.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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

    /// `431 Request Line Too Long`.
    pub fn request_line_too_long() -> Self {
        Self {
            status: 431,
            reason: "Request Line Too Long",
            content_type: "text/plain; charset=utf-8",
            body: b"request line too long".to_vec(),
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

/// Read timeout for a client connection. A client that connects and then
/// stalls while sending its request is dropped after this instead of wedging
/// the sequential accept loop.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Hard cap on the request line length (bytes). A line longer than this is
/// rejected (431) instead of being read unbounded.
const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
/// The maximum number of bytes we drain after the request line (the remainder
/// of a too-long line plus the header block) before responding. Bounds the
/// work a misbehaving client can force on us.
const MAX_DRAIN_BYTES: usize = 64 * 1024;

/// The result of reading a request line off the wire.
enum RequestLine {
    /// A complete (`\n`-terminated) request line within the cap.
    Line(String),
    /// The client closed the connection before sending a complete line.
    Closed,
    /// The request line exceeded the cap before a newline arrived.
    TooLong,
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
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Read one request line of at most `max` bytes (no unbounded `read_line`).
///
/// Returns [`RequestLine::Closed`] on EOF before a complete line,
/// [`RequestLine::TooLong`] if the line exceeds the cap, and
/// [`RequestLine::Line`] with the (newline-stripped) line otherwise.
fn read_request_line(reader: &mut impl BufRead, max: usize) -> std::io::Result<RequestLine> {
    let mut buf: Vec<u8> = Vec::with_capacity(64);
    loop {
        // Enforce the cap before reading so we never read past it.
        if buf.len() >= max {
            return Ok(RequestLine::TooLong);
        }
        let mut byte = [0u8; 1];
        let n = reader.read(&mut byte)?;
        if n == 0 {
            return Ok(RequestLine::Closed);
        }
        let b = byte[0];
        if b == b'\n' {
            // Complete line; strip the trailing LF and any preceding CR.
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            return Ok(RequestLine::Line(String::from_utf8_lossy(&buf).into_owned()));
        }
        buf.push(b);
    }
}

/// Drain the rest of a request — the remainder of a too-long line and/or the
/// header block — up to the blank line that ends the headers, or `max` bytes,
/// or EOF/timeout, without storing any of it.
///
/// Finishing the read before we write the response keeps a slow or
/// misbehaving client from being reset while it is still sending. The blank
/// line is the first *empty* line (a `\r\n` with no content), so a header-less
/// request (`\r\n` right after the request line) is handled correctly.
fn drain_request(reader: &mut impl BufRead, max: usize) -> std::io::Result<()> {
    let mut read = 0usize;
    let mut line_empty = true; // the current line has no content yet
    loop {
        if read >= max {
            return Ok(()); // cap reached
        }
        let mut byte = [0u8; 1];
        let n = reader.read(&mut byte)?;
        if n == 0 {
            return Ok(()); // EOF
        }
        read += 1;
        match byte[0] {
            b'\n' => {
                // End of line. An empty line is the blank line that ends the
                // header block.
                if line_empty {
                    return Ok(());
                }
                line_empty = true; // start a new line
            }
            b'\r' => {
                // Part of a CRLF; not line content by itself.
            }
            _ => {
                line_empty = false; // any other byte is line content
            }
        }
    }
}

/// Route a parsed request line to a response.
///
/// `GET`, `PUT`, and `DELETE` are dispatched to the handler (the KV write/read
/// surface uses all three). Every other method is rejected with `405 Method
/// Not Allowed` *before* the handler runs. An empty method or path is a `400`
/// Bad Request.
fn dispatch_line<H: HttpHandler>(line: &str, handler: &H) -> HttpResponse {
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    if method.is_empty() || path.is_empty() {
        HttpResponse::bad_request()
    } else if matches!(method, "GET" | "PUT" | "DELETE") {
        handler.handle(method, path)
    } else {
        HttpResponse::method_not_allowed()
    }
}

/// Read one request (a capped request line plus the header block), dispatch
/// it, and write the response.
///
/// The stream is forced into blocking mode with a bounded read timeout, so a
/// client that connects and then stalls (or sends a too-long request line)
/// times out instead of wedging the sequential accept loop.
pub fn handle_connection<H: HttpHandler>(
    stream: &mut TcpStream,
    handler: &H,
) -> std::io::Result<()> {
    // The accepted stream may have inherited `O_NONBLOCK` from the listener;
    // make it blocking and bound reads so a stuck client times out rather
    // than wedging the server.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    // Read from a clone (so the original stays free for the response). The
    // blocking flag is per-fd and is inherited by the clone, but the read
    // timeout is per-instance, so set it on the clone we actually read from.
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    reader.get_mut().set_read_timeout(Some(READ_TIMEOUT))?;

    let response = match read_request_line(&mut reader, MAX_REQUEST_LINE_BYTES) {
        Ok(RequestLine::Closed) => return Ok(()), // client closed before a request
        Ok(RequestLine::TooLong) => {
            // Best-effort drain (so we are not reset mid-send), then reject.
            let _ = drain_request(&mut reader, MAX_DRAIN_BYTES);
            HttpResponse::request_line_too_long()
        }
        Ok(RequestLine::Line(line)) => {
            // Drain the header block before responding, so a slow client is
            // not reset while it is still sending. A stall/timeout means there
            // is no valid request to answer.
            if drain_request(&mut reader, MAX_DRAIN_BYTES).is_err() {
                return Ok(());
            }
            dispatch_line(&line, handler)
        }
        Err(_) => return Ok(()), // read error/timeout: drop the connection
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
        fn handle(&self, method: &str, path: &str) -> HttpResponse {
            match path {
                "/readyz" => HttpResponse::text("ready\n"),
                "/metrics" => HttpResponse::ok("text/plain; version=0.0.4", "arachne_term 1\n"),
                // Echo the method + path so tests can prove `PUT`/`DELETE` are
                // routed to the handler (not rejected as `405`).
                "/kv/x" => HttpResponse::text(format!("{method} {path}\n")),
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
            .set_read_timeout(Some(Duration::from_millis(2000)))
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

        // A header-less request is valid HTTP and must still be served.
        let bare = request(addr, "GET /readyz HTTP/1.1\r\n\r\n");
        assert!(bare.starts_with("HTTP/1.1 200 OK"), "got: {bare}");

        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join();
    }

    /// `GET`, `PUT`, and `DELETE` are all routed to the handler (the KV
    /// write/read surface uses all three); any other method is rejected with
    /// `405` before the handler runs. This is the regression test for the bug
    /// where `dispatch_line` rejected every non-`GET` method with `405`.
    #[test]
    fn put_and_delete_reach_the_handler() {
        let (addr, shutdown, handle) = start();

        let put = request(addr, "PUT /kv/x HTTP/1.1\r\nHost: x\r\n\r\n");
        assert!(put.starts_with("HTTP/1.1 200 OK"), "got: {put}");
        assert!(put.ends_with("PUT /kv/x\n"), "got: {put}");

        let delete = request(addr, "DELETE /kv/x HTTP/1.1\r\nHost: x\r\n\r\n");
        assert!(delete.starts_with("HTTP/1.1 200 OK"), "got: {delete}");
        assert!(delete.ends_with("DELETE /kv/x\n"), "got: {delete}");

        // A method the server does not serve is still rejected with 405.
        let unknown = request(addr, "PATCH /kv/x HTTP/1.1\r\nHost: x\r\n\r\n");
        assert!(
            unknown.starts_with("HTTP/1.1 405 Method Not Allowed"),
            "got: {unknown}"
        );

        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join();
    }

    /// A client that connects and then pauses before sending its request must
    /// still receive a correct response: the server waits (up to the read
    /// timeout) instead of dropping the connection.
    #[test]
    fn a_client_that_delays_before_sending_still_gets_a_response() {
        let (addr, shutdown, handle) = start();

        let mut stream = TcpStream::connect(addr).expect("connect");
        // Pause well below the 5s read timeout, then send the request.
        std::thread::sleep(Duration::from_millis(300));
        stream
            .write_all(b"GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n")
            .expect("write request");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set timeout");
        let mut response = String::new();
        let _ = std::io::Read::read_to_string(&mut stream, &mut response);
        assert!(response.starts_with("HTTP/1.1 200 OK"), "got: {response}");
        assert!(response.ends_with("ready\n"), "got: {response}");

        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join();
    }

    /// A request line longer than the cap is rejected (431) without a panic,
    /// and the server keeps serving subsequent requests.
    #[test]
    fn an_oversized_request_line_is_rejected() {
        let (addr, shutdown, handle) = start();

        let big_path = "a".repeat(9000); // well over the 8 KiB cap
        let raw = format!("GET /{big_path} HTTP/1.1\r\nHost: x\r\n\r\n");
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set timeout");
        stream.write_all(raw.as_bytes()).expect("write request");
        let mut response = String::new();
        let _ = std::io::Read::read_to_string(&mut stream, &mut response);
        assert!(
            response.starts_with("HTTP/1.1 431") || response.starts_with("HTTP/1.1 400"),
            "got: {response:?}"
        );

        // The server must still be alive and able to serve a normal request.
        let ready = request(addr, "GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n");
        assert!(ready.starts_with("HTTP/1.1 200 OK"), "got: {ready}");

        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join();
    }
}
