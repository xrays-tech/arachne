//! Arachne node binary.
//!
//! Loads a TOML config, runs an Arachne node (WAL + raft + KV state machine via
//! the lib runtime actor), serves `/readyz`, `/metrics`, and the KV write/read
//! endpoints over a hand-rolled HTTP server, and shuts down gracefully on
//! SIGINT/SIGTERM.
//!
//! KV endpoints (M1 cross-process surface):
//! * `PUT /kv/<key>/<value>` — write; `200` on the leader. In a multi-process
//!   deployment a **non-leader returns `503` (quorum unavailable)**: this build
//!   has no cross-process client redirect, so a follower write collapses to a
//!   bounded quorum failure (it never hangs). `409` (with a leader hint) is only
//!   produced when a peer's handle is registered in-process; cross-process
//!   `409` + hint is deferred to task (D).
//! * `GET /kv/<key>` — linearizable **ReadIndex** read on the leader (propsol
//!   §5.4); a non-leader in a multi-process deployment returns `503` (no
//!   cross-process redirect), matching the `PUT` note above.
//! * `GET /kv/<key>?stale=1` — stale local read (works on any node).
//! * `DELETE /kv/<key>` — delete.
//!
//! Values are taken verbatim from the path (URL-safe ASCII) for M1; body
//! parsing is not implemented yet.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arachne::client::Handle;
use arachne::ArachneError;
use arachne_node::config::{parse_config, Config};
use arachne_node::http::{self, HttpHandler, HttpResponse};
use arachne_node::metrics::Metrics;
use arachne_node::node::Arachne;

const USAGE: &str = "usage: arachne-node --config <path.toml>";

/// HTTP handler: readiness, metrics, and the KV endpoints.
struct NodeHttp {
    metrics: Arc<Metrics>,
    /// A handle to the tokio runtime, so the (blocking) HTTP thread can drive
    /// the async client calls to completion.
    rt: tokio::runtime::Handle,
    /// This node's client handle.
    kv: Handle,
}

impl HttpHandler for NodeHttp {
    fn handle(&self, method: &str, path: &str) -> HttpResponse {
        let (path_only, query) = match path.split_once('?') {
            Some((p, q)) => (p, q),
            None => (path, ""),
        };
        if let Some(rest) = path_only.strip_prefix("/kv/") {
            return self.handle_kv(method, rest, query);
        }
        match path_only {
            "/readyz" => {
                if self.metrics.is_ready() {
                    HttpResponse::text("ready\n")
                } else {
                    HttpResponse::unavailable("not ready\n")
                }
            }
            "/metrics" => HttpResponse::ok("text/plain; version=0.0.4", self.metrics.render()),
            _ => HttpResponse::not_found(),
        }
    }
}

impl NodeHttp {
    fn handle_kv(&self, method: &str, rest: &str, query: &str) -> HttpResponse {
        match method {
            "GET" => {
                let key = rest.as_bytes();
                let result = if query.contains("stale=1") {
                    self.rt.block_on(self.kv.get_stale(key))
                } else {
                    self.rt.block_on(self.kv.get(key))
                };
                match result {
                    Ok(Some(value)) => HttpResponse::text(value),
                    Ok(None) => HttpResponse::not_found(),
                    Err(e) => Self::map_error(e),
                }
            }
            "PUT" => {
                let Some((key, value)) = rest.split_once('/') else {
                    return HttpResponse::bad_request();
                };
                match self.rt.block_on(self.kv.put(key.as_bytes(), value.as_bytes())) {
                    Ok(()) => HttpResponse::text("ok\n"),
                    Err(e) => Self::map_error(e),
                }
            }
            "DELETE" => match self.rt.block_on(self.kv.delete(rest.as_bytes())) {
                Ok(()) => HttpResponse::text("ok\n"),
                Err(e) => Self::map_error(e),
            },
            _ => HttpResponse::method_not_allowed(),
        }
    }

    fn map_error(error: ArachneError) -> HttpResponse {
        match error {
            ArachneError::NotLeader { leader_hint } => {
                let body = match leader_hint {
                    Some((id, addr)) => format!("not leader; leader={id} addr={addr}\n"),
                    None => "not leader; no leader known\n".to_string(),
                };
                HttpResponse {
                    status: 409,
                    reason: "Conflict",
                    content_type: "text/plain; charset=utf-8",
                    body: body.into_bytes(),
                }
            }
            ArachneError::QuorumUnavailable => HttpResponse::unavailable("quorum unavailable\n"),
            ArachneError::ShuttingDown => HttpResponse::unavailable("shutting down\n"),
            other => {
                let detail = format!("{other}\n");
                let status = match other {
                    ArachneError::InvalidArgument(_) => 400,
                    _ => 500,
                };
                let reason = if status == 400 {
                    "Bad Request"
                } else {
                    "Internal Server Error"
                };
                HttpResponse {
                    status,
                    reason,
                    content_type: "text/plain; charset=utf-8",
                    body: detail.into_bytes(),
                }
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("{USAGE}");
        return Ok(());
    }

    let config_path = match parse_args(&args) {
        Ok(path) => path,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };

    let text = std::fs::read_to_string(&config_path)
        .map_err(|e| format!("cannot read config `{config_path}`: {e}"))?;
    let config = parse_config(&text)?;

    let logger = build_logger(config.node_id.as_str());
    let metrics = Arc::new(Metrics::new());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(config, metrics, logger))
}

fn parse_args(args: &[String]) -> Result<String, String> {
    let mut config_path: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                let next = args
                    .get(i + 1)
                    .ok_or_else(|| format!("`--config` requires a path\n{USAGE}"))?;
                config_path = Some(next.clone());
                i += 2;
            }
            other => return Err(format!("unknown argument `{other}`\n{USAGE}")),
        }
    }
    config_path.ok_or_else(|| format!("missing `--config`\n{USAGE}"))
}

async fn run(
    config: Config,
    metrics: Arc<Metrics>,
    logger: slog::Logger,
) -> Result<(), Box<dyn std::error::Error>> {
    let node = Arachne::open(&config, Arc::clone(&metrics), &logger).await?;

    // Hand-rolled HTTP server on its own blocking thread. The handler drives the
    // async client calls with `rt.block_on`, which is valid here because the
    // HTTP thread has no runtime context of its own.
    let listener = std::net::TcpListener::bind(config.http_listen)?;
    let http_shutdown = Arc::new(AtomicBool::new(false));
    let handler = Arc::new(NodeHttp {
        metrics: Arc::clone(&metrics),
        rt: tokio::runtime::Handle::current(),
        kv: node.handle(),
    });
    let http_thread = {
        let http_shutdown = Arc::clone(&http_shutdown);
        std::thread::spawn(move || {
            if let Err(e) = http::serve(&listener, handler.as_ref(), &http_shutdown) {
                eprintln!("http server error: {e}");
            }
        })
    };

    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c() => {}
        _ = async {
            #[cfg(unix)]
            {
                let _ = sigterm.recv().await;
            }
            #[cfg(not(unix))]
            {
                std::future::pending::<()>().await;
            }
        } => {}
    }

    // Graceful shutdown: stop the HTTP server, then stop the runtime actor
    // (which drops the WAL and releases the data-dir lock).
    http_shutdown.store(true, Ordering::Relaxed);
    let _ = http_thread.join();
    node.shutdown().await;
    Ok(())
}

fn build_logger(node_id: &str) -> slog::Logger {
    // M0/M1: the raft logger is a no-op sink. Structured terminal logging lands
    // later; the node reports status through `/readyz` and `/metrics`.
    use slog::Drain;
    slog::Logger::root(slog::Discard.fuse(), slog::o!("node" => node_id.to_string()))
}
