//! Arachne node binary.
//!
//! Loads a TOML config, runs a single-node Arachne cluster (WAL + raft + KV
//! state machine), serves `/readyz` and `/metrics` over a hand-rolled HTTP
//! server, and shuts down gracefully on SIGINT/SIGTERM.
//!
//! M0 uses the local placeholder transport (no peers); the tonic transport
//! lands at M1.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arachne_node::config::{parse_config, Config};
use arachne_node::http::{self, HttpHandler, HttpResponse};
use arachne_node::metrics::Metrics;
use arachne_node::node::Arachne;

const USAGE: &str = "usage: arachne-node --config <path.toml>";

/// HTTP handler: `/readyz` (readiness) and `/metrics` (Prometheus text).
struct NodeHttp {
    metrics: Arc<Metrics>,
    ready: Arc<AtomicBool>,
}

impl HttpHandler for NodeHttp {
    fn handle(&self, _method: &str, path: &str) -> HttpResponse {
        match path {
            "/readyz" => {
                if self.ready.load(Ordering::Relaxed) {
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
    let ready = Arc::new(AtomicBool::new(false));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(config, metrics, ready, logger))
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
    ready: Arc<AtomicBool>,
    logger: slog::Logger,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut node = Arachne::open(&config, Arc::clone(&metrics), &logger)?;

    // Hand-rolled HTTP server on its own blocking thread.
    let listener = std::net::TcpListener::bind(config.http_listen)?;
    let http_shutdown = Arc::new(AtomicBool::new(false));
    let handler = Arc::new(NodeHttp {
        metrics: Arc::clone(&metrics),
        ready: Arc::clone(&ready),
    });
    let http_thread = {
        let http_shutdown = Arc::clone(&http_shutdown);
        std::thread::spawn(move || {
            if let Err(e) = http::serve(&listener, handler.as_ref(), &http_shutdown) {
                eprintln!("http server error: {e}");
            }
        })
    };

    let period = Duration::from_millis(config.heartbeat_interval_ms.max(1));
    let mut tick = tokio::time::interval(period);

    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    // The loop exits either on a fatal drive-loop error (fail-stop: the
    // process must exit non-zero) or on a signal (graceful: exit 0). Carry
    // that distinction out of the loop.
    let drive_result: Result<(), Box<dyn std::error::Error>> = loop {
        tokio::select! {
            biased;
            _ = tick.tick() => {
                match node.tick().await {
                    Ok(()) => {
                        if node.is_ready() {
                            ready.store(true, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        // A drive-loop failure is fatal (fail-stop discipline):
                        // report it and exit non-zero.
                        eprintln!("drive loop error: {e}");
                        break Err(Box::new(e));
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => break Ok(()),
            _ = async {
                #[cfg(unix)]
                {
                    let _ = sigterm.recv().await;
                }
                #[cfg(not(unix))]
                {
                    std::future::pending::<()>().await;
                }
            } => break Ok(()),
        }
    };

    // Graceful shutdown: stop the HTTP server, then drop the node (which
    // releases the WAL data-dir lock).
    http_shutdown.store(true, Ordering::Relaxed);
    let _ = http_thread.join();
    drop(node);
    drive_result
}

fn build_logger(node_id: &str) -> slog::Logger {
    // M0: the raft logger is a no-op sink. `slog-term`'s terminal decorator is
    // not `Send + Sync` in the form `Logger::root` requires, and the node does
    // not yet emit structured logs. Structured terminal logging lands at M1
    // alongside the tonic transport; until then the node reports status
    // through `/readyz` and `/metrics`.
    use slog::Drain;
    slog::Logger::root(slog::Discard.fuse(), slog::o!("node" => node_id.to_string()))
}
