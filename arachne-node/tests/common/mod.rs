//! Shared helpers for the `arachne-node` integration tests.
//!
//! Gate C forbids real-time imports in `tests/`, so every wait here is
//! count-based (`STEP_MS` sleeps, no clock reading).
//!
//! `#![allow(dead_code)]`: each test binary compiles this module and uses only
//! a subset of the helpers.

#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

/// Poll step for every bounded wait loop in the node integration tests.
pub const STEP_MS: u64 = 50;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The next port candidate for this process.
///
/// `bind(:0)`-then-release looks like the obvious way to find a free port, but
/// it hands *the same free port* to two tests running in parallel, and anything
/// else on the machine can take it before the node binds. That is what made the
/// process-level tests fail under CI load while the node was still starting
/// (handoff §1.31).
///
/// Instead every test process draws from its own band (derived from its pid) with
/// a monotonic counter, so two tests in the same process can never be given the
/// same candidate.
static PORT_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_port_candidate() -> u16 {
    // 200 bands of 200 ports, starting at 40000.
    const BANDS: u64 = 200;
    const PER_BAND: u64 = 200;
    let band = (std::process::id() as u64) % BANDS;
    let n = PORT_COUNTER.fetch_add(1, Ordering::Relaxed) % PER_BAND;
    (40_000 + band * PER_BAND + n) as u16
}

/// Claim a loopback port for a test node.
///
/// Candidates come from this process's own band, and each is probed by binding
/// it (then releasing it) so a port that is genuinely busy is skipped. The
/// release-then-rebind window still exists, but it can no longer hand the same
/// port to two tests of the same process — which was the failure mode.
pub fn alloc_port() -> u16 {
    for _ in 0..200 {
        let candidate = next_port_candidate();
        if TcpListener::bind(("127.0.0.1", candidate)).is_ok() {
            return candidate;
        }
    }
    panic!("no free port in this process's band");
}

/// Parse an HTTP/1.1 response: the numeric status from the first line and the
/// body after the blank line. Returns `None` on a malformed/empty response.
pub fn parse_response(response: &str) -> Option<(u16, String)> {
    let (head, body) = response.split_once("\r\n\r\n")?;
    let first_line = head.lines().next()?;
    let status = first_line.split_whitespace().nth(1)?.parse::<u16>().ok()?;
    Some((status, body.to_string()))
}

/// Issue a single HTTP request over a fresh loopback connection and read the
/// full response. `None` if connect/write/read fails (the node is not up yet,
/// the connection was reset, or the read timed out) — callers treat that as
/// "retry later". The 2s read timeout guarantees a stuck request never hangs.
pub fn http_request(addr: SocketAddr, method: &str, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(addr).ok()?;
    let _ = stream.set_read_timeout(Some(core::time::Duration::from_millis(2000)));
    let request = format!("{method} {path} HTTP/1.1\r\nHost: x\r\n\r\n");
    stream.write_all(request.as_bytes()).ok()?;
    let mut response = String::new();
    Read::read_to_string(&mut stream, &mut response).ok()?;
    parse_response(&response)
}

/// A unique temp directory, removed on drop (even on panic).
pub struct TempTree {
    pub root: PathBuf,
}

impl TempTree {
    pub fn new(tag: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "arachne-node-{tag}-{}-{}",
            std::process::id(),
            DIR_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).expect("create temp tree");
        Self { root }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
}

impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Render a **single-node** config to `path` and return the TOML text.
pub fn write_single_node_config(
    path: &Path,
    cluster_id: &str,
    node_id: &str,
    listen_port: u16,
    http_port: u16,
    data_dir: &Path,
) -> String {
    let toml = format!(
        "cluster_id = {cluster_id:?}\n\
         node_id = {node_id:?}\n\
         listen = \"127.0.0.1:{listen_port}\"\n\
         data_dir = {data_dir:?}\n\
         http_listen = \"127.0.0.1:{http_port}\"\n\
         initial_cluster = [{node_id:?}]\n\
         heartbeat_interval_ms = 20\n\
         election_timeout_ms = 200\n\
         rpc_timeout_ms = 100\n",
    );
    std::fs::write(path, &toml).expect("write config");
    toml
}

/// Spawn `arachne-node --config <config_path>`, appending stdout+stderr to
/// `log_path` (one shared append handle, so the two streams interleave intact).
pub fn spawn_node(config_path: &Path, log_path: &Path) -> Child {
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .expect("open node log");
    let log_err = log_file.try_clone().expect("clone node log");
    Command::new(env!("CARGO_BIN_EXE_arachne-node"))
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_err))
        .spawn()
        .expect("spawn node process")
}

/// Read a captured node log (empty if it does not exist).
pub fn read_log(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// Wait (bounded) for a child to exit; `None` if it is still running.
pub fn wait_exit(child: &mut Child, polls: usize) -> Option<ExitStatus> {
    for _ in 0..polls {
        if let Some(status) = child.try_wait().expect("try_wait") {
            return Some(status);
        }
        std::thread::sleep(core::time::Duration::from_millis(STEP_MS));
    }
    None
}

/// Wait (bounded) until `addr` answers `GET path` with status `want`.
pub fn wait_http_status(addr: SocketAddr, path: &str, want: u16, polls: usize) -> bool {
    for _ in 0..polls {
        if http_request(addr, "GET", path)
            .map(|(status, _)| status == want)
            .unwrap_or(false)
        {
            return true;
        }
        std::thread::sleep(core::time::Duration::from_millis(STEP_MS));
    }
    false
}

/// Wait (bounded) until `addr` answers `GET /readyz` with 200.
pub fn wait_ready(addr: SocketAddr, polls: usize) -> bool {
    wait_http_status(addr, "/readyz", 200, polls)
}

/// Send `SIGTERM` to `pid` (unix only). Used to exercise graceful shutdown.
#[cfg(unix)]
pub fn send_sigterm(pid: u32) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("run kill(1)");
    assert!(status.success(), "kill -TERM {pid} must succeed");
}
