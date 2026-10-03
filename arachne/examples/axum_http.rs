//! An axum HTTP wrapper around the [`arachne::server`] facade.
//!
//! The companion to [`facade`](crate::examples::facade): instead of a caller
//! hand-rolling a tokio runtime and calling `.block_on(...)` to drive the four
//! facade methods (`set` / `get` / `get_stale` / `delete`), the `#[tokio::main]`
//! attribute (supplied by the `tokio` dependency that axum pulls in) provides the
//! runtime, and the same four operations are exposed as plain HTTP endpoints.
//! The caller side is just `curl`.
//!
//! Endpoints (mirroring the reference node's `/kv` semantics, but routed
//! straight through the in-process facade rather than the tonic/Hyper server):
//!
//! - `PUT /kv/<key>/<value>`  -> `Arachne::set`          ; responds `ok`
//! - `GET  /kv/<key>`         -> `Arachne::get`          ; 404 if absent
//! - `GET  /kv/<key>?stale=1` -> `Arachne::get_stale`    ; 404 if absent
//! - `DELETE /kv/<key>`       -> `Arachne::delete`       ; responds `ok`
//! - `GET  /readyz`           -> node alive               ; responds `ready`
//!
//! The node is the same single in-process singleton as in [`facade`]: one
//! global node, elected leader on the first tick (singleton quorum-free reads),
//! running on a dedicated thread. The HTTP server merely relays to it over
//! channels, so it is a faithful demonstration of the facade without any
//! embedding-side runtime ownership.
//!
//! Note: this uses axum's **default** body type (`axum::body::Body`). In axum the
//! `Router` is generic over the *state* (not the body); the body is selected
//! per `Service<Request<B>>` impl where `B: http_body_util::Body + Send + 'static`.
//! The default `Body` satisfies that bound, so no custom body type is needed.
//!
//! Error mapping (see [`status_for`]):
//! - `NotLeader`            -> 409 Conflict
//! - `InvalidArgument`      -> 400 Bad Request
//! - `QuorumUnavailable` /
//!   `Timeout` / `Busy` /
//!   `ShuttingDown` /
//!   `DataDirLocked` /
//!   `NotInitialized`       -> 503 Service Unavailable
//! - anything else          -> 500 Internal Server Error
//!
//! ```sh
//! cargo run --example axum_http
//! # prints: arachne HTTP server (facade) listening on 127.0.0.1:XXXXX
//! # then, in another terminal:
//! curl -X PUT  http://127.0.0.1:XXXXX/kv/hello/world
//! curl       http://127.0.0.1:XXXXX/kv/hello
//! curl -X DELETE http://127.0.0.1:XXXXX/kv/hello
//! curl       http://127.0.0.1:XXXXX/readyz
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use arachne::server::{Arachne, ArachneError, WalConfig};

use axum::body::Body;
use axum::extract::{Path, Request};
use axum::http::{StatusCode, Response};
use axum::{routing::{delete, get, post}, Router};
use tokio::net::TcpListener;
use tokio::signal::ctrl_c;
use tokio::time::sleep;

/// The temp dir of the in-process node, held across the process lifetime so the
/// example can clean it up on shutdown.
struct Server {
    dir: PathBuf,
}

/// Build the HTTP router. Handlers route to the `Arachne` facade statics.
fn build_router() -> Router {
    Router::new()
        .route("/kv/:key/:value", post(handle_put))
        .route("/kv/:key", get(handle_get))
        .route("/kv/:key", delete(handle_delete))
        .route("/readyz", get(handle_ready))
}

/// Build a text/plain response with the given status and body.
fn text_response(status: StatusCode, body: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        // axum's `Body` has `From<String>` (owned) but only `From<&'static str>`;
        // an owned `String` keeps the response self-contained.
        .body(Body::from(body.to_string()))
        .expect("valid status + text body always build a valid response")
}

/// `PUT /kv/<key>/<value>` -> linearizable write.
async fn handle_put(path: Path<(String, String)>) -> Response<Body> {
    let (key, value) = &path.0;
    match Arachne::set(key.as_bytes(), value.as_bytes()).await {
        Ok(()) => text_response(StatusCode::OK, "ok"),
        Err(e) => text_response(status_for(&e), &e.to_string()),
    }
}

/// `GET /kv/<key>` (linear) and `GET /kv/<key>?stale=1` (weak).
///
/// The stale variant is selected from the query string (no serde needed).
async fn handle_get(key: Path<String>, req: Request<Body>) -> Response<Body> {
    let stale = req
        .uri()
        .query()
        .map_or(false, |q| q.split('&').any(|p| p == "stale=1"));

    let value = if stale {
        Arachne::get_stale(key.0.as_bytes()).await
    } else {
        Arachne::get(key.0.as_bytes()).await
    };
    match value {
        Ok(Some(v)) => text_response(StatusCode::OK, &to_text(&v)),
        Ok(None) => text_response(StatusCode::NOT_FOUND, "key not found"),
        Err(e) => text_response(status_for(&e), &e.to_string()),
    }
}

/// `DELETE /kv/<key>` -> linearizable delete.
async fn handle_delete(key: Path<String>) -> Response<Body> {
    match Arachne::delete(key.0.as_bytes()).await {
        Ok(()) => text_response(StatusCode::OK, "ok"),
        Err(e) => text_response(status_for(&e), &e.to_string()),
    }
}

/// `GET /readyz` -> node alive.
async fn handle_ready() -> Response<Body> {
    text_response(StatusCode::OK, "ready")
}

/// Map an [`ArachneError`] to an HTTP status code.
fn status_for(e: &ArachneError) -> StatusCode {
    match e {
        ArachneError::NotLeader { .. } => StatusCode::CONFLICT,
        ArachneError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
        ArachneError::QuorumUnavailable
        | ArachneError::Timeout
        | ArachneError::Busy
        | ArachneError::ShuttingDown
        | ArachneError::DataDirLocked
        | ArachneError::NotInitialized
        | ArachneError::AlreadyInitialized => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Encode a byte value for a text/plain response: UTF-8 if it is, otherwise a
/// compact size annotation (so the response is always printable).
fn to_text(v: &[u8]) -> String {
    match std::str::from_utf8(v) {
        Ok(s) => s.to_string(),
        Err(_) => format!("non-utf8 ({} bytes)", v.len()),
    }
}

/// Poll until the singleton node has elected itself leader. Pre-election reads
/// fail fast with `NotLeader` / `QuorumUnavailable`; once the node is leader, a
/// read for a key that has not yet been written resolves to `None`.
async fn wait_for_leader() -> Result<(), Box<dyn std::error::Error>> {
    let election = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match Arachne::get(b"__ready__").await {
                Ok(v) => {
                    assert!(v.is_none(), "unexpected pre-write value");
                    return Ok(());
                }
                Err(ArachneError::NotLeader { .. })
                | Err(ArachneError::QuorumUnavailable) => {
                    sleep(Duration::from_millis(10)).await;
                }
                Err(e) => {
                    // Pin the async block's (inferred) return type to
                    // `Result<(), Box<dyn Error>>` so the `Ok(())` arm and the
                    // `Err` arm agree (an async block has no `-> Type` syntax).
                    let err: Box<dyn std::error::Error> =
                        format!("unexpected read error during election: {e}").into();
                    return Err(err);
                }
            }
        }
    });

    match election.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err("node never became ready within 5s".into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // (1) One node per process — the facade keeps a single global node.
    let dir = std::env::temp_dir().join(format!(
        "arachne-axum-example-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir)?;

    Arachne::new(1, &dir, WalConfig::default())?;

    // (2) Wait until the node has elected itself leader.
    wait_for_leader().await?;
    println!("node elected leader (facade singleton)");

    // (3) Serve the facade over axum.
    let server = Server { dir };
    let app = build_router();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let listener = TcpListener::bind(addr).await?;
    let bound: SocketAddr = listener.local_addr()?;
    println!("arachne HTTP server (facade) listening on {bound}");

    // Run the HTTP server until Ctrl+C, then shut the node down cleanly.
    tokio::select! {
        _ = ctrl_c() => {
            println!("\nreceived Ctrl+C, shutting down");
        }
        result = axum::serve(listener, app) => {
            // The server only completes on an internal error; bail if so.
            result?;
        }
    }

    // (4) Shut down: frees the static slot and releases the WAL data-dir lock.
    Arachne::shutdown()?;
    println!("facade lifecycle complete");
    std::fs::remove_dir_all(&server.dir).ok();

    Ok(())
}
