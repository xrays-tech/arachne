//! L3 (multi-process) acceptance for M1: three **real** `arachne-node`
//! processes form a tonic cluster and are driven over HTTP.
//!
//! Each process is spawned from the compiled binary with its own TOML config
//! (a distinct raft `listen` port, HTTP port, and WAL dir). The test then:
//!
//! 1. waits until all three report `ready` (a leader is known),
//! 2. writes a key/value through whichever node is the leader (followers
//!    answer `409`/`503` — expected, and must not hang),
//! 3. reads the value back from the leader, and
//! 4. verifies the write **replicates** to all three via stale reads.
//!
//! Gate C forbids real-time and network imports in `tests/`, so all waiting
//! uses `core::time::Duration` + `std::thread::sleep` and every wait loop is
//! bounded (count-based, no `Instant`). A `Drop` guard kills every child and
//! removes the temp tree even if a later step panics.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

const NUM_NODES: usize = 3;

/// Per-node poll step (and the granularity of every bounded wait loop).
const STEP_MS: u64 = 50;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Claim a free loopback port (bind → read → release). The node process then
/// binds that port; the (tiny) race window on loopback is acceptable for a
/// single-test harness.
fn alloc_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a probe listener");
    let port = listener.local_addr().expect("probe local addr").port();
    drop(listener);
    port
}

/// Parse an HTTP/1.1 response: the numeric status from the first line and the
/// body after the blank line. Returns `None` on a malformed/empty response.
fn parse_response(response: &str) -> Option<(u16, String)> {
    let (head, body) = response.split_once("\r\n\r\n")?;
    let first_line = head.lines().next()?;
    let status = first_line.split_whitespace().nth(1)?.parse::<u16>().ok()?;
    Some((status, body.to_string()))
}

/// Issue a single HTTP request over a fresh loopback connection and read the
/// full response. `None` if connect/write/read fails (the node is not up yet,
/// the connection was reset, or the read timed out) — callers treat that as
/// "retry later". The 2s read timeout guarantees a stuck request never hangs.
fn http_request(addr: SocketAddr, method: &str, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(addr).ok()?;
    let _ = stream.set_read_timeout(Some(core::time::Duration::from_millis(2000)));
    let request = format!("{method} {path} HTTP/1.1\r\nHost: x\r\n\r\n");
    stream.write_all(request.as_bytes()).ok()?;
    let mut response = String::new();
    Read::read_to_string(&mut stream, &mut response).ok()?;
    parse_response(&response)
}

/// Trailing log lines from every node (a debug aid printed on failure).
fn log_tails(root: &Path, states: &[bool; NUM_NODES]) -> String {
    let mut out = String::new();
    for i in 0..NUM_NODES {
        let log = root.join(format!("n{}.log", i + 1));
        let content = std::fs::read_to_string(&log).unwrap_or_default();
        let tail: Vec<&str> = content
            .lines()
            .rev()
            .take(25)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        out.push_str(&format!("--- n{} (state={}) ---\n", i + 1, states[i]));
        out.push_str(&tail.join("\n"));
        out.push('\n');
    }
    out
}

/// Kill all child processes and remove the temp tree on drop (even on panic).
struct Cleanup {
    root: PathBuf,
    children: Vec<Child>,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        for child in self.children.iter_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// True if `addr` answers `GET /readyz` with HTTP 200 (a leader is known).
fn is_ready(addr: SocketAddr) -> bool {
    http_request(addr, "GET", "/readyz")
        .map(|(status, _)| status == 200)
        .unwrap_or(false)
}

#[test]
fn three_processes_form_a_cluster_and_replicate_over_http() {
    let root = std::env::temp_dir().join(format!(
        "arachne-node-multi-{}-{}",
        std::process::id(),
        DIR_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).expect("create temp root");
    let mut cleanup = Cleanup {
        root: root.clone(),
        children: Vec::new(),
    };

    let raft_ports: Vec<u16> = (0..NUM_NODES).map(|_| alloc_port()).collect();
    let http_ports: Vec<u16> = (0..NUM_NODES).map(|_| alloc_port()).collect();

    // Write a config and spawn a process for each node.
    for i in 0..NUM_NODES {
        let node_id = format!("n{}", i + 1);
        let data_dir = root.join(&node_id);
        std::fs::create_dir_all(&data_dir).expect("create node data dir");

        // Each entry is a string (`"n1=127.0.0.1:PORT"`) — the `id=addr` form
        // must be quoted, as `=` and `:` are not valid in a bare TOML array.
        let cluster = (0..NUM_NODES)
            .map(|j| format!("\"n{}=127.0.0.1:{}\"", j + 1, raft_ports[j]))
            .collect::<Vec<_>>()
            .join(",");
        let toml = format!(
            "cluster_id = \"m1-multiproc\"\n\
             node_id = \"{node_id}\"\n\
             listen = \"127.0.0.1:{}\"\n\
             data_dir = \"{}\"\n\
             http_listen = \"127.0.0.1:{}\"\n\
             initial_cluster = [{}]\n\
             heartbeat_interval_ms = 20\n\
             election_timeout_ms = 200\n\
             rpc_timeout_ms = 100\n",
            raft_ports[i],
            data_dir.display(),
            http_ports[i],
            cluster,
        );
        let cfg = root.join(format!("{node_id}.toml"));
        std::fs::write(&cfg, toml).expect("write node config");

        let log = root.join(format!("{node_id}.log"));
        // One append-mode handle, cloned for stdout and stderr. `File::create`
        // truncates, so two independent handles on the same path would each
        // truncate and garble the failure diagnostics; a single shared append
        // stream keeps stdout and stderr interleaved and intact.
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .expect("open node log");
        let log_err = log_file.try_clone().expect("clone node log");
        let child = Command::new(env!("CARGO_BIN_EXE_arachne-node"))
            .arg("--config")
            .arg(&cfg)
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_err))
            .spawn()
            .expect("spawn node process");
        cleanup.children.push(child);
    }

    let http_addrs: Vec<SocketAddr> = http_ports
        .iter()
        .map(|p| format!("127.0.0.1:{p}").parse().expect("http addr"))
        .collect();

    // 1. Ready: all three report 200 on /readyz (a leader is known).
    let mut ready = [false; NUM_NODES];
    for _ in 0..300 {
        for i in 0..NUM_NODES {
            if !ready[i] && is_ready(http_addrs[i]) {
                ready[i] = true;
            }
        }
        if ready.iter().all(|&r| r) {
            break;
        }
        std::thread::sleep(core::time::Duration::from_millis(STEP_MS));
    }
    assert!(
        ready.iter().all(|&r| r),
        "all three nodes must become ready (a leader must be known); log tails:\n{}",
        log_tails(&root, &ready)
    );

    // 2. Write: retry until the (current) leader accepts the PUT. Followers
    //    answer 409 (not leader) / 503 (no quorum) — expected, and the bounded
    //    window (plus the per-request read timeout) means a slow node never
    //    wedges the test.
    let mut writer: Option<usize> = None;
    for _ in 0..200 {
        for i in 0..NUM_NODES {
            if http_request(http_addrs[i], "PUT", "/kv/k/v")
                .map(|(status, _)| status == 200)
                .unwrap_or(false)
            {
                writer = Some(i);
                break;
            }
        }
        if writer.is_some() {
            break;
        }
        std::thread::sleep(core::time::Duration::from_millis(STEP_MS));
    }
    assert!(
        writer.is_some(),
        "a leader must accept the PUT within the window; log tails:\n{}",
        log_tails(&root, &ready)
    );
    let writer = writer.expect("a leader accepted the PUT");

    // 3. Read back from the writer (a linearizable ReadIndex read on the leader).
    let mut read_ok = false;
    for _ in 0..100 {
        if let Some((status, body)) = http_request(http_addrs[writer], "GET", "/kv/k") {
            if status == 200 && body.trim() == "v" {
                read_ok = true;
                break;
            }
        }
        std::thread::sleep(core::time::Duration::from_millis(STEP_MS));
    }
    assert!(
        read_ok,
        "the writer must read back `v`; log tails:\n{}",
        log_tails(&root, &ready)
    );

    // 4. Replication: all three eventually serve the value via a stale read.
    let mut replicated = [false; NUM_NODES];
    for _ in 0..200 {
        for i in 0..NUM_NODES {
            if !replicated[i]
                && http_request(http_addrs[i], "GET", "/kv/k?stale=1")
                    .map(|(status, body)| status == 200 && body.trim() == "v")
                    .unwrap_or(false)
            {
                replicated[i] = true;
            }
        }
        if replicated.iter().all(|&r| r) {
            break;
        }
        std::thread::sleep(core::time::Duration::from_millis(STEP_MS));
    }
    assert!(
        replicated.iter().all(|&r| r),
        "all three nodes must replicate `v` via stale reads; log tails:\n{}",
        log_tails(&root, &replicated)
    );
}
