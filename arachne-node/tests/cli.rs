//! Integration: the `arachne-node` binary's CLI surface and startup-validation
//! checklist (test-plan §3.3), independent of any cluster.
//!
//! Covered:
//! * `--help` succeeds; missing/unknown/valueless arguments exit `2`;
//! * an unreadable config file and a config-validation failure fail-start
//!   (exit `1`) with a diagnostic on stderr;
//! * a second process on a **locked** data dir fails start (`… is locked`);
//! * a data dir whose `META` names a different `cluster_id` fails start;
//! * `SIGTERM` is a **graceful** shutdown (exit 0) that releases the dir lock;
//! * `/readyz` and `/metrics` endpoint behavior.
//!
//! Gate C forbids real-time imports in `tests/`, so every wait is count-based.

mod common;

use std::net::SocketAddr;
use std::process::Command;

use common::{
    alloc_port, http_request, read_log, spawn_node, wait_ready, write_single_node_config, TempTree,
};
#[cfg(unix)]
use common::{send_sigterm, wait_exit};

const BIN: &str = env!("CARGO_BIN_EXE_arachne-node");

/// `--help` succeeds; a missing/unknown/valueless argument exits `2` with a
/// diagnostic (the parse-error exit code, distinct from a fail-start).
#[test]
fn help_and_argument_errors() {
    let help = Command::new(BIN).arg("--help").output().expect("run --help");
    assert!(help.status.success(), "--help must exit 0");
    let stdout = String::from_utf8_lossy(&help.stdout);
    assert!(
        stdout.contains("usage: arachne-node --config"),
        "--help must print usage; got: {stdout:?}"
    );

    let none = Command::new(BIN).output().expect("run with no args");
    assert_eq!(none.status.code(), Some(2), "no args must exit 2");
    assert!(
        String::from_utf8_lossy(&none.stderr).contains("missing `--config`"),
        "got: {:?}",
        String::from_utf8_lossy(&none.stderr)
    );

    let unknown = Command::new(BIN)
        .arg("--bogus")
        .output()
        .expect("run --bogus");
    assert_eq!(unknown.status.code(), Some(2), "unknown arg must exit 2");
    assert!(
        String::from_utf8_lossy(&unknown.stderr).contains("unknown argument `--bogus`"),
        "got: {:?}",
        String::from_utf8_lossy(&unknown.stderr)
    );

    let dangling = Command::new(BIN)
        .arg("--config")
        .output()
        .expect("run --config");
    assert_eq!(dangling.status.code(), Some(2), "a valueless --config must exit 2");
    assert!(
        String::from_utf8_lossy(&dangling.stderr).contains("requires a path"),
        "got: {:?}",
        String::from_utf8_lossy(&dangling.stderr)
    );
}

/// A config path that cannot be read is a fail-start (exit `1`), not a panic.
#[test]
fn unreadable_config_fails_start() {
    let missing = std::env::temp_dir().join("arachne-node-no-such-config.toml");
    let out = Command::new(BIN)
        .arg("--config")
        .arg(&missing)
        .output()
        .expect("run with a missing config");
    assert!(!out.status.success(), "a missing config must fail start");
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("cannot read config"), "got: {stderr:?}");
}

/// A config whose profile constraints are violated (`rpc_timeout_ms` is not
/// shorter than `election_timeout_ms`) is rejected before any node is built.
#[test]
fn invalid_config_fails_start() {
    let tree = TempTree::new("badcfg");
    let data = tree.path("data");
    std::fs::create_dir_all(&data).expect("create data dir");
    let cfg = tree.path("bad.toml");
    let toml = format!(
        "cluster_id = \"cli-badcfg\"\n\
         node_id = \"n1\"\n\
         listen = \"127.0.0.1:{}\"\n\
         data_dir = {data:?}\n\
         http_listen = \"127.0.0.1:{}\"\n\
         initial_cluster = [\"n1\"]\n\
         heartbeat_interval_ms = 20\n\
         election_timeout_ms = 100\n\
         rpc_timeout_ms = 200\n",
        alloc_port(),
        alloc_port()
    );
    std::fs::write(&cfg, toml).expect("write config");

    let out = Command::new(BIN)
        .arg("--config")
        .arg(&cfg)
        .output()
        .expect("run with an invalid config");
    assert!(!out.status.success(), "an invalid config must fail start");
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("rpc_timeout_ms"), "got: {stderr:?}");
    assert!(stderr.contains("election_timeout_ms"), "got: {stderr:?}");
}

/// A second process opening the same data dir must fail-start on the file lock
/// (propsol §6.2, "flock → META 一致性"): the first process holds the `LOCK`.
#[test]
fn second_process_on_a_locked_data_dir_fails_start() {
    let tree = TempTree::new("flock");
    let data = tree.path("data");
    std::fs::create_dir_all(&data).expect("create data dir");

    let http = alloc_port();
    let cfg_a = tree.path("a.toml");
    write_single_node_config(&cfg_a, "cli-flock", "n1", alloc_port(), http, &data);
    let mut child = spawn_node(&cfg_a, &tree.path("a.log"));
    let addr: SocketAddr = format!("127.0.0.1:{http}").parse().expect("addr");
    assert!(
        wait_ready(addr, 300),
        "node A must become ready; log:\n{}",
        read_log(&tree.path("a.log"))
    );

    // A second process on the same data dir (different ports) must be refused.
    let cfg_b = tree.path("b.toml");
    write_single_node_config(&cfg_b, "cli-flock", "n1", alloc_port(), alloc_port(), &data);
    let out = Command::new(BIN)
        .arg("--config")
        .arg(&cfg_b)
        .output()
        .expect("run the second process");
    assert!(
        !out.status.success(),
        "a second process must not open a locked data dir"
    );
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("locked"), "got: {stderr:?}");

    let _ = child.kill();
    let _ = child.wait();
}

/// `SIGTERM` is a graceful shutdown: the process exits 0, and the data-dir lock
/// is released afterwards (proved by a fresh process opening the same dir).
#[cfg(unix)]
#[test]
fn sigterm_shuts_down_gracefully_and_releases_the_lock() {
    let tree = TempTree::new("sigterm");
    let data = tree.path("data");
    std::fs::create_dir_all(&data).expect("create data dir");

    let http = alloc_port();
    let cfg = tree.path("n.toml");
    write_single_node_config(&cfg, "cli-sigterm", "n1", alloc_port(), http, &data);
    let mut child = spawn_node(&cfg, &tree.path("n.log"));
    let addr: SocketAddr = format!("127.0.0.1:{http}").parse().expect("addr");
    assert!(
        wait_ready(addr, 300),
        "the node must become ready; log:\n{}",
        read_log(&tree.path("n.log"))
    );

    send_sigterm(child.id());
    let status = wait_exit(&mut child, 200).expect("the node must exit on SIGTERM");
    assert!(status.success(), "SIGTERM must be a graceful (exit-0) shutdown");

    // The lock is released: a fresh process on the same dir starts (and, since
    // its META matches, becomes ready).
    let http2 = alloc_port();
    let cfg2 = tree.path("n2.toml");
    write_single_node_config(&cfg2, "cli-sigterm", "n1", alloc_port(), http2, &data);
    let mut child2 = spawn_node(&cfg2, &tree.path("n2.log"));
    let addr2: SocketAddr = format!("127.0.0.1:{http2}").parse().expect("addr");
    let restarted = wait_ready(addr2, 300);
    let log = read_log(&tree.path("n2.log"));
    let _ = child2.kill();
    let _ = child2.wait();
    assert!(
        restarted,
        "the data-dir lock must be released after a graceful shutdown; log:\n{log}"
    );
}

/// A data dir whose `META` records a different `cluster_id` must fail-start
/// (propsol §6.2), so a node cannot silently join the wrong cluster.
#[cfg(unix)]
#[test]
fn meta_cluster_id_mismatch_fails_start() {
    let tree = TempTree::new("meta");
    let data = tree.path("data");
    std::fs::create_dir_all(&data).expect("create data dir");

    // First run writes META with `cluster_id = "cli-meta-a"`.
    let http = alloc_port();
    let cfg_a = tree.path("a.toml");
    write_single_node_config(&cfg_a, "cli-meta-a", "n1", alloc_port(), http, &data);
    let mut child = spawn_node(&cfg_a, &tree.path("a.log"));
    let addr: SocketAddr = format!("127.0.0.1:{http}").parse().expect("addr");
    assert!(
        wait_ready(addr, 300),
        "node A must become ready; log:\n{}",
        read_log(&tree.path("a.log"))
    );
    send_sigterm(child.id());
    let status = wait_exit(&mut child, 200).expect("node A must exit on SIGTERM");
    assert!(status.success());

    // Same data dir, different cluster_id -> fail-start.
    let cfg_b = tree.path("b.toml");
    write_single_node_config(&cfg_b, "cli-meta-b", "n1", alloc_port(), alloc_port(), &data);
    let out = Command::new(BIN)
        .arg("--config")
        .arg(&cfg_b)
        .output()
        .expect("run with a mismatched cluster id");
    assert!(
        !out.status.success(),
        "a META cluster_id mismatch must fail start"
    );
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("META cluster_id mismatch"),
        "got: {stderr:?}"
    );
}

/// `/readyz` and `/metrics` behave as documented: readiness flips to 200 once a
/// leader is known, metrics render the documented Prometheus types, an unknown
/// path is `404`, and an unsupported method is `405`.
#[test]
fn readyz_and_metrics_are_served() {
    let tree = TempTree::new("endpoints");
    let data = tree.path("data");
    std::fs::create_dir_all(&data).expect("create data dir");

    let http = alloc_port();
    let cfg = tree.path("n.toml");
    write_single_node_config(&cfg, "cli-endpoints", "n1", alloc_port(), http, &data);
    let mut child = spawn_node(&cfg, &tree.path("n.log"));
    let addr: SocketAddr = format!("127.0.0.1:{http}").parse().expect("addr");
    assert!(
        wait_ready(addr, 300),
        "/readyz must become 200 once a leader is known; log:\n{}",
        read_log(&tree.path("n.log"))
    );

    let (status, body) = http_request(addr, "GET", "/metrics").expect("/metrics must answer");
    assert_eq!(status, 200, "got body: {body:?}");
    for needle in [
        "# TYPE arachne_term gauge",
        "# TYPE arachne_commit_index gauge",
        "# TYPE arachne_applied_index gauge",
        "# TYPE arachne_read_index_timeout_total counter",
        "# TYPE arachne_read_index_pending gauge",
    ] {
        assert!(
            body.contains(needle),
            "/metrics must expose `{needle}`; got:\n{body}"
        );
    }

    assert_eq!(
        http_request(addr, "GET", "/nope").map(|(s, _)| s),
        Some(404),
        "an unknown path must be 404"
    );
    assert_eq!(
        http_request(addr, "POST", "/readyz").map(|(s, _)| s),
        Some(405),
        "an unsupported method must be 405"
    );

    let _ = child.kill();
    let _ = child.wait();
}

/// `force-recovery` refuses without the explicit `--i-know-data-loss`
/// confirmation (exit 2) and never touches the data dir.
#[test]
fn force_recovery_requires_the_double_confirm() {
    let out = Command::new(BIN)
        .args(["force-recovery", "--config", "/nonexistent/n.toml"])
        .output()
        .expect("run force-recovery");
    assert_eq!(out.status.code(), Some(2), "a missing confirm must exit 2");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--i-know-data-loss"),
        "got: {:?}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `force-recovery --help` prints the subcommand usage and exits 0.
#[test]
fn force_recovery_help_succeeds() {
    let out = Command::new(BIN)
        .args(["force-recovery", "--help"])
        .output()
        .expect("run force-recovery --help");
    assert!(out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("force-recovery --config"),
        "got: {:?}",
        String::from_utf8_lossy(&out.stdout)
    );
}

/// End to end (propsol §6.1): an acked write survives force-recovery, the
/// cluster id rotates, and the emitted single-voter config brings the node back
/// up with the data intact.
#[cfg(unix)]
#[test]
fn force_recovery_preserves_an_acked_write_and_rotates_the_cluster_id() {
    let tree = TempTree::new("force-recovery");
    let data = tree.path("data");
    std::fs::create_dir_all(&data).expect("create data dir");

    let http = alloc_port();
    let cfg = tree.path("n.toml");
    write_single_node_config(&cfg, "cli-force-recovery", "n1", alloc_port(), http, &data);
    let mut child = spawn_node(&cfg, &tree.path("n.log"));
    let addr: SocketAddr = format!("127.0.0.1:{http}").parse().expect("addr");
    assert!(
        wait_ready(addr, 300),
        "the node must become ready; log:\n{}",
        read_log(&tree.path("n.log"))
    );

    // An acked write (committed + applied).
    assert_eq!(
        http_request(addr, "PUT", "/kv/keep/v").map(|(status, _)| status),
        Some(200)
    );

    send_sigterm(child.id());
    let status = wait_exit(&mut child, 200).expect("node must exit on SIGTERM");
    assert!(status.success());

    // Recover: rotate the cluster id and emit a single-voter config.
    let out_cfg = tree.path("recovered.toml");
    let out = Command::new(BIN)
        .args(["force-recovery", "--config"])
        .arg(&cfg)
        .args(["--i-know-data-loss", "--out-config"])
        .arg(&out_cfg)
        .output()
        .expect("run force-recovery");
    assert!(
        out.status.success(),
        "force-recovery must succeed; stderr: {:?}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("discarded entries   : 0"),
        "the committed log must not be truncated; got:\n{stdout}"
    );

    let recovered = std::fs::read_to_string(&out_cfg).expect("read the recovered config");
    assert!(
        recovered.contains("initial_cluster = [\"n1\"]"),
        "the recovered config must be a single-voter cluster; got:\n{recovered}"
    );
    assert!(
        !recovered.contains("cluster_id = \"cli-force-recovery\""),
        "the cluster id must rotate by default; got:\n{recovered}"
    );

    // Start with the recovered config: the acked write is still served.
    let mut child2 = spawn_node(&out_cfg, &tree.path("n2.log"));
    assert!(
        wait_ready(addr, 300),
        "the recovered node must become ready; log:\n{}",
        read_log(&tree.path("n2.log"))
    );
    let mut read_ok = false;
    for _ in 0..100 {
        if let Some((200, body)) = http_request(addr, "GET", "/kv/keep") {
            if body.trim() == "v" {
                read_ok = true;
                break;
            }
        }
        std::thread::sleep(core::time::Duration::from_millis(50));
    }
    assert!(
        read_ok,
        "the acked write must survive force-recovery; log:\n{}",
        read_log(&tree.path("n2.log"))
    );
    let _ = child2.kill();
    let _ = child2.wait();
}

/// Force-recovery on a **locked** data dir (the node is still running) must
/// fail (exit 1) rather than corrupt a live directory.
#[cfg(unix)]
#[test]
fn force_recovery_refuses_a_locked_data_dir() {
    let tree = TempTree::new("force-recovery-locked");
    let data = tree.path("data");
    std::fs::create_dir_all(&data).expect("create data dir");

    let http = alloc_port();
    let cfg = tree.path("n.toml");
    write_single_node_config(&cfg, "cli-fr-locked", "n1", alloc_port(), http, &data);
    let mut child = spawn_node(&cfg, &tree.path("n.log"));
    let addr: SocketAddr = format!("127.0.0.1:{http}").parse().expect("addr");
    assert!(
        wait_ready(addr, 300),
        "the node must become ready; log:\n{}",
        read_log(&tree.path("n.log"))
    );

    let out = Command::new(BIN)
        .args(["force-recovery", "--config"])
        .arg(&cfg)
        .arg("--i-know-data-loss")
        .output()
        .expect("run force-recovery");
    assert!(!out.status.success(), "a locked data dir must be refused");
    assert_eq!(out.status.code(), Some(1), "a lock refusal is a runtime failure");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("locked"),
        "got: {:?}",
        String::from_utf8_lossy(&out.stderr)
    );

    let _ = child.kill();
    let _ = child.wait();
}

/// The membership ops subcommands: usage errors, then the full path — CLI →
/// operator endpoint → actor → durable state, read back over `/members`
/// (propsol §5.3, rev S S5).
#[test]
fn membership_ops_commands_and_endpoints() {
    // Usage errors exit 2 and print the usage line.
    for bad in [
        vec!["add-learner", "--config"],
        vec!["promote", "--config", "x.toml"],
        vec!["remove", "--config", "x.toml", "--id", "abc"],
        vec!["members", "--nope"],
    ] {
        let out = Command::new(BIN)
            .args(&bad)
            .output()
            .unwrap_or_else(|e| panic!("run {bad:?}: {e}"));
        assert_eq!(
            out.status.code(),
            Some(2),
            "{bad:?} must be a usage error; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(String::from_utf8_lossy(&out.stderr).contains("usage: arachne-node"));
    }

    let help = Command::new(BIN)
        .args(["add-learner", "--help"])
        .output()
        .expect("run --help");
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("add-learner <raft-id>"));

    // A live single-node cluster to operate on.
    let tree = TempTree::new("members-ops");
    let data = tree.path("data");
    std::fs::create_dir_all(&data).expect("create data dir");
    let http = alloc_port();
    let cfg = tree.path("n.toml");
    write_single_node_config(&cfg, "cli-members", "n1", alloc_port(), http, &data);
    let mut child = spawn_node(&cfg, &tree.path("n.log"));
    let addr: SocketAddr = format!("127.0.0.1:{http}").parse().expect("addr");
    assert!(
        wait_ready(addr, 300),
        "/readyz must become 200; log:\n{}",
        read_log(&tree.path("n.log"))
    );

    // The read starts from the bootstrap configuration.
    let (status, body) = http_request(addr, "GET", "/members").expect("/members must answer");
    assert_eq!(status, 200, "got body: {body:?}");
    assert!(body.contains("voters=1"), "got body: {body:?}");
    assert!(body.contains("learners=\n") || body.ends_with("learners=\n"));

    // Add a learner through the CLI (a separate process, as an operator would).
    let add = Command::new(BIN)
        .args([
            "add-learner",
            "--config",
            cfg.to_str().expect("utf-8 path"),
            "--id",
            "2",
        ])
        .output()
        .expect("run add-learner");
    assert!(
        add.status.success(),
        "add-learner must succeed; stdout: {} stderr: {}",
        String::from_utf8_lossy(&add.stdout),
        String::from_utf8_lossy(&add.stderr)
    );

    // A `200` means applied, so the read-back must already show it.
    let (status, body) = http_request(addr, "GET", "/members").expect("/members must answer");
    assert_eq!(status, 200);
    assert!(
        body.contains("learners=2"),
        "the learner must be durable once the command returns; got body: {body:?}"
    );

    // Promoting a learner that never answered is a precondition failure: the
    // CLI reports it and exits non-zero rather than claiming success.
    let promote = Command::new(BIN)
        .args([
            "promote",
            "--config",
            cfg.to_str().expect("utf-8 path"),
            "--id",
            "2",
        ])
        .output()
        .expect("run promote");
    assert_eq!(promote.status.code(), Some(1), "a refused change exits 1");
    let stderr = String::from_utf8_lossy(&promote.stderr);
    assert!(
        stderr.contains("412") && stderr.contains("not caught up"),
        "the refusal must be reported with its status; got: {stderr:?}"
    );

    // The state machine is untouched by all of this: a write still works.
    let (status, _) = http_request(addr, "PUT", "/kv/k/v").expect("put");
    assert_eq!(status, 200);

    let _ = child.kill();
    let _ = child.wait();
}
