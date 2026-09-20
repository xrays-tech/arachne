//! `arachne-node force-recovery` — the operator-gated disaster-recovery command
//! (propsol §6.1).
//!
//! This command rewrites a node's data dir so it can be started as a
//! **single-voter** cluster at its committed point. It is destructive: it
//! discards the uncommitted log tail and (by default) rotates the `cluster_id`
//! so a surviving old majority can never reconnect (propsol §6.1, the
//! anti-split-brain property — safer than `etcd --force-new-cluster`).
//!
//! # Preconditions
//!
//! 1. The data dir is locked by nobody (the old process is stopped) — enforced
//!    by the storage layer's data-dir lock.
//! 2. `--i-know-data-loss` is given explicitly (double confirmation).
//! 3. A reachability pre-check probes the other `initial_cluster` members
//!    (3s). A reachable peer is reported as a warning (the operator has already
//!    accepted data loss by passing the flag).
//!
//! # M1 membership boundary
//!
//! Membership is **bootstrap-only** until M3 persists a `ConfState`: the
//! recovered node's single-voter membership comes from the `initial_cluster` of
//! the config it is then started with. This command therefore does not persist
//! membership; instead it prints (or, with `--out-config`, writes) the
//! single-voter config that makes the postcondition true.
//!
//! # Exit codes
//!
//! * `0` — recovery succeeded (or `--help`);
//! * `1` — a runtime failure (unreadable config, recovery failure, I/O);
//! * `2` — a usage or precondition failure (bad args, missing
//!   `--i-know-data-loss`).

use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use arachne::storage::{ForceRecoveryReport, WalConfig, WalStorage};

use crate::config::{parse_config, Config};

/// Usage string for the `force-recovery` subcommand.
pub const USAGE: &str = "usage: arachne-node force-recovery --config <path.toml> \
--i-know-data-loss [--keep-cluster-id] [--out-config <path.toml>]";

/// The parsed `force-recovery` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForceRecoveryArgs {
    /// The node config to read (node id, data dir, membership, profile).
    pub config_path: String,
    /// The mandatory double confirmation.
    pub i_know_data_loss: bool,
    /// Keep the existing `cluster_id` instead of rotating it (propsol
    /// `--keep-cluster-id`).
    pub keep_cluster_id: bool,
    /// Where to write a ready-to-use single-voter config, if requested.
    pub out_config: Option<String>,
}

/// A `force-recovery` failure, tagged with its exit-code class.
#[derive(Debug)]
pub enum Refusal {
    /// A usage or precondition failure (exit code 2).
    Usage(String),
    /// A runtime failure (exit code 1).
    Failed(String),
}

/// Parse the arguments following `force-recovery`.
pub fn parse_args(args: &[String]) -> Result<ForceRecoveryArgs, String> {
    let mut config_path: Option<String> = None;
    let mut i_know_data_loss = false;
    let mut keep_cluster_id = false;
    let mut out_config: Option<String> = None;

    let mut i = 2; // args[0] = binary, args[1] = "force-recovery"
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                let next = args
                    .get(i + 1)
                    .ok_or_else(|| format!("`--config` requires a path\n{USAGE}"))?;
                config_path = Some(next.clone());
                i += 2;
            }
            "--i-know-data-loss" => {
                i_know_data_loss = true;
                i += 1;
            }
            "--keep-cluster-id" => {
                keep_cluster_id = true;
                i += 1;
            }
            "--out-config" => {
                let next = args
                    .get(i + 1)
                    .ok_or_else(|| format!("`--out-config` requires a path\n{USAGE}"))?;
                out_config = Some(next.clone());
                i += 2;
            }
            other => return Err(format!("unknown argument `{other}`\n{USAGE}")),
        }
    }

    Ok(ForceRecoveryArgs {
        config_path: config_path.ok_or_else(|| format!("missing `--config`\n{USAGE}"))?,
        i_know_data_loss,
        keep_cluster_id,
        out_config,
    })
}

/// Run the subcommand, returning the process exit code.
pub fn main(args: &[String]) -> i32 {
    match run(args) {
        Ok(()) => 0,
        Err(Refusal::Usage(message)) => {
            eprintln!("{message}");
            2
        }
        Err(Refusal::Failed(message)) => {
            eprintln!("force-recovery failed: {message}");
            1
        }
    }
}

fn run(args: &[String]) -> Result<(), Refusal> {
    let parsed = parse_args(args).map_err(Refusal::Usage)?;
    if !parsed.i_know_data_loss {
        return Err(Refusal::Usage(format!(
            "refusing: force-recovery is destructive (it may discard committed \
             data and rotates the cluster id) and requires the explicit \
             `--i-know-data-loss` confirmation\n{USAGE}"
        )));
    }

    let text = std::fs::read_to_string(&parsed.config_path).map_err(|e| {
        Refusal::Failed(format!("cannot read config `{}`: {e}", parsed.config_path))
    })?;
    let config =
        parse_config(&text).map_err(|e| Refusal::Failed(format!("invalid config: {e}")))?;

    // Reachability pre-check (propsol §6.1 precondition 2). The operator has
    // already confirmed data loss, so a reachable peer is a loud warning, not a
    // refusal.
    let reachable = probe_peers(&config);
    if !reachable.is_empty() {
        eprintln!(
            "WARNING: old cluster member(s) still reachable: {} — force-recovery \
             while the old cluster may be alive risks a split brain. The rotated \
             cluster_id prevents reconnection, but the old cluster must be \
             destroyed separately.",
            reachable.join(", ")
        );
    }

    let new_cluster_id = if parsed.keep_cluster_id {
        None
    } else {
        Some(format!("{}-recovered-{}", config.cluster_id, now_millis()))
    };

    let report = WalStorage::force_recovery(
        &config.data_dir,
        config.node_id.as_str(),
        new_cluster_id,
        WalConfig {
            fsync_policy: config.profile_config.fsync_policy,
            segment_bytes: config.profile_config.wal_segment_bytes,
        },
        now_millis(),
    )
    .map_err(|e| Refusal::Failed(format!("{e}")))?;

    report_recovery(&config, &report);

    match &parsed.out_config {
        Some(out) => {
            let toml = render_single_voter_config(&config, &report.cluster_id);
            std::fs::write(out, toml)
                .map_err(|e| Refusal::Failed(format!("cannot write `--out-config {out}`: {e}")))?;
            println!("wrote single-voter config: {out}");
        }
        None => {
            println!(
                "next: start this node with a config whose `cluster_id = {:?}` and \
                 `initial_cluster = [{:?}]` (a single-voter cluster), or re-run with \
                 `--out-config <path>` to have one written.",
                report.cluster_id,
                config.node_id.as_str()
            );
        }
    }
    Ok(())
}

/// Probe every OTHER member of the bootstrap cluster; return the reachable ones
/// as `"<node id> <addr>"` strings. A 3s connect timeout per peer (propsol
/// §6.1).
fn probe_peers(config: &Config) -> Vec<String> {
    let timeout = Duration::from_secs(3);
    let mut reachable = Vec::new();
    for id in &config.initial_cluster {
        if *id == config.node_id {
            continue;
        }
        let Some(addr) = config.addresses.get(id) else {
            continue;
        };
        if peer_is_reachable(*addr, timeout) {
            reachable.push(format!("{id} {addr}"));
        }
    }
    reachable
}

/// Whether a TCP connection to `addr` succeeds within `timeout`.
fn peer_is_reachable(addr: SocketAddr, timeout: Duration) -> bool {
    TcpStream::connect_timeout(&addr, timeout).is_ok()
}

fn report_recovery(config: &Config, report: &ForceRecoveryReport) {
    println!("force-recovery complete for node {:?}", config.node_id.as_str());
    println!("  previous cluster_id : {}", report.previous_cluster_id);
    println!("  new cluster_id      : {}", report.cluster_id);
    println!("  term                : {} (bumped)", report.term);
    println!("  commit (= applied)  : {}", report.commit);
    println!("  discarded entries   : {}", report.discarded_entries);
}

/// Render a ready-to-run **single-voter** config for the recovered node.
///
/// `initial_cluster = [self]` is what makes the reset membership real at M1
/// (membership is bootstrap-only until M3 persists `ConfState`).
fn render_single_voter_config(config: &Config, cluster_id: &str) -> String {
    let pc = &config.profile_config;
    format!(
        "# Generated by `arachne-node force-recovery` (propsol §6.1).\n\
         # Single-voter cluster reset at the recovered commit point.\n\
         cluster_id = {cluster_id:?}\n\
         node_id = {node_id:?}\n\
         listen = \"{listen}\"\n\
         data_dir = {data_dir:?}\n\
         http_listen = \"{http_listen}\"\n\
         initial_cluster = [{node_id:?}]\n\
         heartbeat_interval_ms = {heartbeat}\n\
         election_timeout_ms = {election}\n\
         rpc_timeout_ms = {rpc}\n",
        node_id = config.node_id.as_str(),
        listen = config.listen,
        data_dir = config.data_dir.display().to_string(),
        http_listen = config.http_listen,
        heartbeat = pc.heartbeat_interval_ms,
        election = pc.election_timeout_ms,
        rpc = pc.rpc_timeout_ms,
    )
}

/// The current wall-clock time in milliseconds (the binary edge may read it;
/// the core never does).
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        std::iter::once("arachne-node".to_string())
            .chain(std::iter::once("force-recovery".to_string()))
            .chain(list.iter().map(|s| s.to_string()))
            .collect()
    }

    #[test]
    fn parses_all_flags() {
        let parsed = parse_args(&args(&[
            "--config",
            "n.toml",
            "--i-know-data-loss",
            "--keep-cluster-id",
            "--out-config",
            "out.toml",
        ]))
        .expect("valid args");
        assert_eq!(
            parsed,
            ForceRecoveryArgs {
                config_path: "n.toml".into(),
                i_know_data_loss: true,
                keep_cluster_id: true,
                out_config: Some("out.toml".into()),
            }
        );
    }

    #[test]
    fn requires_config() {
        let err = parse_args(&args(&["--i-know-data-loss"])).expect_err("missing --config");
        assert!(err.contains("missing `--config`"), "got: {err}");
    }

    #[test]
    fn rejects_unknown_arguments() {
        let err = parse_args(&args(&["--config", "n.toml", "--yolo"]))
            .expect_err("unknown argument");
        assert!(err.contains("unknown argument `--yolo`"), "got: {err}");
    }

    #[test]
    fn dangling_flag_is_rejected() {
        let err = parse_args(&args(&["--config"])).expect_err("dangling --config");
        assert!(err.contains("requires a path"), "got: {err}");
    }

    /// Without `--i-know-data-loss` the command refuses with a usage error
    /// (exit-code class 2) before touching the data dir.
    #[test]
    fn refuses_without_the_double_confirm() {
        let err = run(&args(&["--config", "/nonexistent/n.toml"]))
            .expect_err("must refuse without --i-know-data-loss");
        match err {
            Refusal::Usage(message) => {
                assert!(message.contains("--i-know-data-loss"), "got: {message}");
            }
            Refusal::Failed(message) => panic!("expected a usage refusal, got: {message}"),
        }
    }

    /// A config that cannot be read is a runtime failure (exit-code class 1).
    #[test]
    fn missing_config_is_a_runtime_failure() {
        let err = run(&args(&[
            "--config",
            "/nonexistent/arachne-not-here.toml",
            "--i-know-data-loss",
        ]))
        .expect_err("must fail on a missing config");
        match err {
            Refusal::Failed(message) => assert!(message.contains("cannot read config")),
            Refusal::Usage(message) => panic!("expected a runtime failure, got usage: {message}"),
        }
    }
}
