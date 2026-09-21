//! `arachne-node add-learner | promote | remove | transfer-leader` — the
//! membership ops surface (propsol §5.3, §5.6; rev S S5).
//!
//! # Why an HTTP client rather than a library call
//!
//! These commands run as a **separate process** from the node (that is what
//! operational tooling looks like: `arachne-node promote --config node3.toml
//! --id 4`), so they cannot hold the in-process `Handle`. They talk to the
//! target node's operator endpoints instead:
//!
//! * `POST /members/add-learner/<id>`
//! * `POST /members/promote/<id>`
//! * `POST /members/remove/<id>`
//! * `POST /members/transfer-leader/<id>`
//! * `GET  /members` (the read-back, used by `status`)
//!
//! A `200` means the change is **applied** (durable), not merely proposed, so
//! reading `/members` immediately afterwards shows the new configuration.
//!
//! # Targeting the leader
//!
//! A member change must reach the leader. This tool does **not** follow hints,
//! because a hint carries the node's raft address rather than its operator
//! address; it reports the leader it was told about (in the `409` body) and
//! exits non-zero so the operator points the command at that node. Removing the
//! *current* leader is the exception that needs no leader at all: the node
//! answers "requires transfer", the CLI performs the handover and retries (see
//! [`run`]).
//!
//! # Exit codes
//!
//! `0` success, `1` the node refused or was unreachable, `2` usage error —
//! matching `force-recovery` and the plain `--config` startup path.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;

use crate::config::parse_config;

/// Usage string for the membership subcommands.
pub const USAGE: &str = "usage: arachne-node <command> --config <path.toml> --id <raft-id> [--http <ip:port>]\n\
     \n\
     commands:\n\
     \x20 add-learner <raft-id>        add a node as a non-voting learner\n\
     \x20 promote <raft-id>            promote a caught-up learner to voter\n\
     \x20 remove <raft-id>             remove a member (a leader hands over first)\n\
     \x20 transfer-leader <raft-id>    hand leadership to another voter\n\
     \x20 members                      print the node's applied membership\n\
     \n\
     --http overrides the config's http_listen (use it when that port is 0).";

/// One membership operation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Op {
    /// Add a learner.
    AddLearner,
    /// Promote a learner to voter.
    Promote,
    /// Remove a member.
    Remove,
    /// Hand leadership over.
    TransferLeader,
    /// Read the applied membership.
    Status,
}

impl Op {
    /// The command name as typed.
    pub fn name(self) -> &'static str {
        match self {
            Op::AddLearner => "add-learner",
            Op::Promote => "promote",
            Op::Remove => "remove",
            Op::TransferLeader => "transfer-leader",
            Op::Status => "members",
        }
    }

    /// The `/members/...` path for this op (`/members` for a read).
    fn path(self, id: u64) -> String {
        match self {
            Op::Status => "/members".to_string(),
            Op::AddLearner => format!("/members/add-learner/{id}"),
            Op::Promote => format!("/members/promote/{id}"),
            Op::Remove => format!("/members/remove/{id}"),
            Op::TransferLeader => format!("/members/transfer-leader/{id}"),
        }
    }

    /// Whether this op is a read.
    fn is_read(self) -> bool {
        matches!(self, Op::Status)
    }

    /// Parse a command name as typed on the command line.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "add-learner" => Some(Op::AddLearner),
            "promote" => Some(Op::Promote),
            "remove" => Some(Op::Remove),
            "transfer-leader" => Some(Op::TransferLeader),
            "members" => Some(Op::Status),
            _ => None,
        }
    }
}

/// The parsed invocation.
#[derive(Debug)]
pub struct Args {
    /// Which operation to perform.
    pub op: Op,
    /// The node config to read the operator address from.
    pub config: PathBuf,
    /// The target's raft id (`None` for the read).
    pub id: Option<u64>,
    /// Explicit operator address, overriding the config.
    pub http: Option<SocketAddr>,
}

/// Parse the arguments following the subcommand name (which is `args[1]`).
///
/// # Errors
///
/// A message describing the usage problem.
pub fn parse_args(op: Op, args: &[String]) -> Result<Args, String> {
    let mut config: Option<PathBuf> = None;
    let mut id: Option<u64> = None;
    let mut http: Option<SocketAddr> = None;

    let mut i = 2; // args[0] = binary, args[1] = command
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                let value = args.get(i + 1).ok_or("--config needs a path")?;
                config = Some(PathBuf::from(value));
                i += 2;
            }
            "--id" => {
                let value = args.get(i + 1).ok_or("--id needs a raft id")?;
                id = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("--id must be a raft id, got {value:?}"))?,
                );
                i += 2;
            }
            "--http" => {
                let value = args.get(i + 1).ok_or("--http needs an address")?;
                http = Some(
                    value
                        .parse::<SocketAddr>()
                        .map_err(|_| format!("--http must be ip:port, got {value:?}"))?,
                );
                i += 2;
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }

    let config = config.ok_or("--config is required")?;
    // A read needs no target; a change always does.
    if !op.is_read() && id.is_none() {
        return Err(format!("{} needs --id", op.name()));
    }
    Ok(Args {
        op,
        config,
        id,
        http,
    })
}

/// Where to send the request: the config's `http_listen`, or `--http`.
fn operator_addr(args: &Args) -> Result<SocketAddr, String> {
    if let Some(addr) = args.http {
        return Ok(addr);
    }
    let text = std::fs::read_to_string(&args.config)
        .map_err(|e| format!("cannot read {}: {e}", args.config.display()))?;
    let config = parse_config(&text).map_err(|e| format!("{e}"))?;
    if config.http_listen.port() == 0 {
        return Err(format!(
            "{}: http_listen has an ephemeral port (0); pass --http <ip:port>",
            args.config.display()
        ));
    }
    Ok(config.http_listen)
}

/// POST (or GET) one request and return `(status, body)`.
fn request(addr: SocketAddr, op: Op, id: u64) -> Result<(u16, String), String> {
    let method = if op.is_read() { "GET" } else { "POST" };
    let mut stream = TcpStream::connect(addr)
        .map_err(|e| format!("cannot reach the node at {addr}: {e}"))?;
    let _ = stream.set_read_timeout(Some(core::time::Duration::from_secs(30)));
    let req = format!(
        "{method} {} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n",
        op.path(id)
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("cannot send the request: {e}"))?;
    let mut response = String::new();
    Read::read_to_string(&mut stream, &mut response)
        .map_err(|e| format!("cannot read the response: {e}"))?;

    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or("malformed response from the node")?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or("malformed status line from the node")?;
    Ok((status, body.to_string()))
}

/// Run one membership command. Returns the process exit code.
pub fn run(args: &Args) -> i32 {
    let addr = match operator_addr(args) {
        Ok(addr) => addr,
        Err(message) => {
            eprintln!("{message}");
            return 1;
        }
    };
    let id = args.id.unwrap_or(0);

    match request(addr, args.op, id) {
        Ok((200, body)) => {
            print!("{body}");
            0
        }
        // Removing the current leader is a two-step operation and the node
        // refuses to guess: it answers "requires transfer", and the client
        // performs the handover and retries against the new leader. That is
        // the same composition `Handle::remove_member` does in-process, done
        // here across processes.
        Ok((409, body))
            if args.op == Op::Remove && body.contains("requires a transfer") =>
        {
            eprintln!("leader removal requested on the leader; handing over first");
            match request(addr, Op::TransferLeader, 0) {
                // `transfer-leader/0` means "any other voter": the node picks.
                Ok((200, _)) => match request(addr, args.op, id) {
                    Ok((200, body)) => {
                        print!("{body}");
                        0
                    }
                    Ok((status, body)) => {
                        eprintln!("remove failed after the handover: {status} {body}");
                        1
                    }
                    Err(message) => {
                        eprintln!("{message}");
                        1
                    }
                },
                Ok((status, body)) => {
                    eprintln!("the handover failed: {status} {body}");
                    1
                }
                Err(message) => {
                    eprintln!("{message}");
                    1
                }
            }
        }
        Ok((status, body)) => {
            eprintln!("the node refused ({status}): {body}");
            1
        }
        Err(message) => {
            eprintln!("{message}");
            1
        }
    }
}

/// Entry point for the membership subcommands: `args[1]` is the command name.
pub fn main(args: &[String]) -> i32 {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("{USAGE}");
        return 0;
    }
    let Some(op) = args.get(1).and_then(|name| Op::from_name(name)) else {
        eprintln!("{USAGE}");
        return 2;
    };
    match parse_args(op, args) {
        Ok(parsed) => run(&parsed),
        Err(message) => {
            eprintln!("{message}");
            eprintln!("{USAGE}");
            2
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(parts: &[&str]) -> Vec<String> {
        std::iter::once("arachne-node".to_string())
            .chain(parts.iter().map(|p| p.to_string()))
            .collect()
    }

    #[test]
    fn every_command_name_parses_back() {
        for op in [
            Op::AddLearner,
            Op::Promote,
            Op::Remove,
            Op::TransferLeader,
            Op::Status,
        ] {
            assert_eq!(Op::from_name(op.name()), Some(op));
        }
        assert_eq!(Op::from_name("nonsense"), None);
    }

    #[test]
    fn paths_match_the_operator_surface() {
        assert_eq!(Op::AddLearner.path(4), "/members/add-learner/4");
        assert_eq!(Op::Promote.path(4), "/members/promote/4");
        assert_eq!(Op::Remove.path(4), "/members/remove/4");
        assert_eq!(Op::TransferLeader.path(4), "/members/transfer-leader/4");
        assert_eq!(Op::Status.path(0), "/members");
    }

    #[test]
    fn a_change_requires_an_id_but_a_read_does_not() {
        let parsed = parse_args(Op::Status, &argv(&["members", "--config", "n.toml"]))
            .expect("a read needs no id");
        assert_eq!(parsed.id, None);

        let err = parse_args(Op::Promote, &argv(&["promote", "--config", "n.toml"]))
            .expect_err("a change must name its target");
        assert!(err.contains("--id"), "unexpected message: {err}");

        let err = parse_args(
            Op::Promote,
            &argv(&["promote", "--config", "n.toml", "--id", "abc"]),
        )
        .expect_err("the id must be numeric");
        assert!(err.contains("raft id"), "unexpected message: {err}");
    }

    #[test]
    fn unknown_and_valueless_arguments_are_rejected() {
        assert!(
            parse_args(Op::Remove, &argv(&["remove", "--config"]))
                .expect_err("--config without a value")
                .contains("--config needs a path")
        );
        assert!(
            parse_args(Op::Remove, &argv(&["remove", "--wat"]))
                .expect_err("unknown flag")
                .contains("unknown argument")
        );
        assert!(
            parse_args(Op::Remove, &argv(&["remove", "--id", "2"]))
                .expect_err("missing --config")
                .contains("--config is required")
        );
    }

    #[test]
    fn http_override_is_parsed() {
        let parsed = parse_args(
            Op::Remove,
            &argv(&[
                "remove",
                "--config",
                "n.toml",
                "--id",
                "2",
                "--http",
                "127.0.0.1:9000",
            ]),
        )
        .expect("valid");
        assert_eq!(parsed.http, Some("127.0.0.1:9000".parse().expect("addr")));
    }
}
