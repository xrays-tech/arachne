#!/usr/bin/env python3
"""Arachne 3-node benchmark driver (host side, stdlib only).

Talks only to the published HTTP ports of the docker/bench cluster:
  PUT    /kv/<key>/<value>   write (200 on leader, 409+hint elsewhere)
  GET    /kv/<key>           linearizable read
  GET    /kv/<key>?stale=1   stale local read (any node)
  GET    /readyz             readiness of the HTTP server

Usage:
  python3 driver/bench.py --ports 8001,8002,8003            # from the host
  python3 bench.py --hosts node1,node2,node3 --ports 8001,8002,8003  # in-cluster

--json: emit a structured JSON report to stdout for stable parsing (the
regression-gate script `scripts/check-perf-baseline.sh` consumes this). Human
text lines are re-routed to stderr so stdout carries *only* the JSON object.
Default (no --json) behaviour is unchanged: all output is the familiar text
report on stdout.
"""

import argparse
import concurrent.futures as futures
import http.client
import json
import statistics
import sys
import time

VALUE = "v"
HOST = "127.0.0.1"
HOSTS_BY_PORT = {}

# --json mode: stdout holds the JSON report only; human text goes to stderr.
_JSON_MODE = False
RESULTS = []


def log(msg):
    """Human-facing line. stdout when text mode, stderr in --json mode."""
    if _JSON_MODE:
        print(msg, file=sys.stderr, flush=True)
    else:
        print(msg, flush=True)


def record(name, tag, ops, workers, samples, n):
    """Accumulate one benchmark's metrics for the JSON report."""
    if not _JSON_MODE:
        return
    rps = len(samples) / (sum(samples) / 1e9) if samples else 0.0
    RESULTS.append({
        "name": name,
        "tag": tag,
        "ops": ops,
        "workers": workers,
        "rps": round(float(rps), 1),
        "p50_ms": round(float(pct(samples, 0.5)), 3),
        "p99_ms": round(float(pct(samples, 0.99)), 3),
        "ok": len(samples),
        "attempted": n,
    })


def req(method, port, path, timeout=15.0):
    conn = http.client.HTTPConnection(
        HOSTS_BY_PORT.get(port, HOST), port, timeout=timeout)
    t0 = time.perf_counter_ns()
    try:
        conn.request(method, path)
        resp = conn.getresponse()
        body = resp.read()
        ok = resp.status == 200
        status = resp.status
    except Exception as e:  # connection refused / reset / timeout
        ok, status, body = False, 0, str(e).encode()
    finally:
        conn.close()
    dt = time.perf_counter_ns() - t0
    return ok, status, body.decode("utf-8", "replace"), dt


def put(port, key, value=VALUE, timeout=15.0):
    return req("PUT", port, f"/kv/{key}/{value}", timeout)


def get(port, key, stale=False, timeout=15.0):
    path = f"/kv/{key}" + ("?stale=1" if stale else "")
    return req("GET", port, path, timeout)


def pct(xs, q):
    if not xs:
        return 0.0
    s = sorted(xs)
    rank = max(1, int(round(q * (len(s) - 1))))
    return s[min(rank, len(s) - 1)] / 1e6  # ms


def report(tag, samples, n):
    ops = len(samples)
    if n > 0 and samples:
        rps = ops / (sum(samples) / 1e9)
    else:
        rps = 0.0
    # Preserve the existing human line verbatim (stdout by default).
    line = (
        f"[bench] {tag}: {rps:.0f} ops/s  p50 {pct(samples, .5):.2f}ms "
        f"p99 {pct(samples, .99):.2f}ms  ({ops} ok / {n} attempted)"
    )
    log(line)
    if _JSON_MODE:
        pass  # name/workers/ops are recorded by the callers for fidelity
    return rps


def discover_leader(ports, tries=90, pause=0.2):
    t0 = time.perf_counter()
    for _ in range(tries):
        for p in ports:
            ok, _, _, _ = put(p, "probe-all")
            if ok:
                return p, time.perf_counter() - t0
        time.sleep(pause)
    raise SystemExit(f"no leader elected on {ports} within timeout")


def run_put_benchmark(leader, workers, per_worker, tag):
    def job(w):
        out = []
        for i in range(per_worker):
            ok, st, _, dt = put(leader, f"{tag}-{w}-{i}")
            if ok:
                out.append(dt)
        return out

    with futures.ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(job, range(workers)))
    samples = [d for r in results for d in r]
    report(f"put ({tag}, {workers}w)", samples, workers * per_worker)
    if _JSON_MODE:
        record(f"put-{tag}-{workers}w", f"put ({tag}, {workers}w)",
               "put", workers, samples, workers * per_worker)
    return samples


def run_get_benchmark(port, key, stale, tag, n, workers=1):
    def job(seed):
        out = []
        for i in range(seed, n, workers):
            ok, _, _, dt = get(port, key, stale=stale)
            if ok:
                out.append(dt)
        return out

    if workers == 1:
        samples = job(0)
    else:
        with futures.ThreadPoolExecutor(max_workers=workers) as ex:
            results = list(ex.map(job, range(workers)))
        samples = [d for r in results for d in r]
    report(f"get ({tag})", samples, n)
    if _JSON_MODE:
        mode = "stale" if stale else "linear"
        record(f"get-{mode}-{workers}w", f"get ({tag})", "get", workers, samples, n)
    return samples


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ports", default="8001,8002,8003",
                    help="comma-separated HTTP ports")
    ap.add_argument("--hosts", default="127.0.0.1",
                    help="comma-separated node hosts (in-cluster: node1,node2,node3)")
    ap.add_argument("--json", action="store_true",
                    help="emit a structured JSON report to stdout (human text to stderr)")
    ap.add_argument("--fast", action="store_true",
                    help="run only the critical items (put 1-way + linear read 1-way)")
    ap.add_argument("--n", type=int, default=100,
                    help="op count for each critical benchmark (default 100 puts / 100 reads; "
                         "used only when --fast)")
    args = ap.parse_args()
    global HOST, HOSTS_BY_PORT, _JSON_MODE
    _JSON_MODE = args.json
    fast = args.fast
    n = args.n
    host_list = [h for h in args.hosts.split(",") if h]
    ports = [int(x) for x in args.ports.split(",")]
    if len(host_list) == len(ports):
        HOSTS_BY_PORT = dict(zip(ports, host_list))
        log(f"=== arachne 3-node docker benchmark ===")
        log(f"per-port hosts {HOSTS_BY_PORT}; value={VALUE!r} (path-encoded)")
    else:
        HOST = host_list[0]
        log(f"=== arachne 3-node docker benchmark ===")
        log(f"host {HOST}; ports {ports}; value={VALUE!r} (path-encoded)")

    leader, wait = discover_leader(ports)
    log(f"leader found on port {leader} after {wait:.2f}s of polling")

    # Replication sanity: write one key, stale-read it on every node.
    seed = "seed"
    ok, st, body, _ = put(leader, seed)
    if not ok:
        log(f"seed write failed: status {st} {body}")
        sys.exit(1)
    for p in ports:
        for _ in range(100):
            o2, _, b2, _ = get(p, seed, stale=True)
            if o2 and b2.strip().startswith(VALUE):
                log(f"replication check: {seed!r} visible on port {p}")
                break
            time.sleep(0.2)
        else:
            log(f"replication check FAILED: {seed!r} not visible on port {p}")

    if fast:
        # --fast: critical items only, controlled duration (put 1-way + linear
        # read 1-way). Keeps the same single-client methodology as the full run.
        run_put_benchmark(leader, 1, n, "seq")
        for _ in range(400):
            o, _, _, _ = get(leader, seed)
            if o:
                break
            time.sleep(0.05)
        run_get_benchmark(leader, seed, stale=False, tag="linear", n=n)
    else:
        # Writes. Sequential first (the honest single-client ceiling), then a
        # concurrent burst (does not beat the actor's serialized durability).
        run_put_benchmark(leader, 1, 100, "seq")
        run_put_benchmark(leader, 4, 30, "con4")

        # Reads on the leader: linearizable, then stale — single client and a
        # 4-way burst (the single-client numbers include the HTTP server's ~20 ms
        # idle-accept poll, so the burst shows the aggregate ceiling).
        for _ in range(1200):
            o, _, _, _ = get(leader, seed)
            if o:
                break
            time.sleep(0.05)
        run_get_benchmark(leader, seed, stale=False, tag="linear", n=400)
        run_get_benchmark(leader, seed, stale=False, tag="linear-con4", n=400, workers=4)
        run_get_benchmark(leader, seed, stale=True, tag="stale", n=400)
        run_get_benchmark(leader, seed, stale=True, tag="stale-con4", n=400, workers=4)

    log("=== done ===")

    if _JSON_MODE:
        payload = {
            "cluster": "arachne",
            "schema": 1,
            "leader_port": leader,
            "value": VALUE,
            "results": RESULTS,
        }
        # stdout carries *only* the JSON object in --json mode.
        sys.stdout.write(json.dumps(payload, indent=2) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    sys.exit(main())
