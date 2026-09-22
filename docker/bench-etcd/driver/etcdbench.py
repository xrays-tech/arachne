#!/usr/bin/env python3
"""etcd 3-node benchmark driver (host of docker/bench-etcd, stdlib only).

Talks to etcd's gRPC-gateway JSON endpoints over HTTP/1.1:
  POST /v3/kv/put    {"key": b64, "value": b64}
  POST /v3/kv/range  {"key": b64[, "linearizable": true]}

Two transport modes:
  * fresh  — one TCP connection per request (exactly what the arachne node's
             HTTP server forces; this is the apples-to-apples comparison);
  * keepalive — one connection reused (the realistic etcd-client pattern,
             bonus context).

Usage:
  python3 etcdbench.py --hosts etcd1,etcd2,etcd3 --ports 2379,2380,2381
"""

import argparse
import base64
import concurrent.futures as futures
import http.client
import sys
import time

VALUE = b"v"


def b64(b):
    if isinstance(b, str):
        b = b.encode()
    return base64.b64encode(b).decode()


class Cli:
    """Minimal HTTP/1.1 client; keepalive=None -> fresh connection per op."""

    def __init__(self, host, port, keepalive):
        self.host, self.port = host, port
        self.keepalive = keepalive
        self.conn = None

    def _conn(self):
        if self.keepalive and self.conn is not None:
            return self.conn
        self.conn = http.client.HTTPConnection(self.host, self.port, timeout=20)
        return self.conn

    def post(self, path, body):
        c = self._conn()
        t0 = time.perf_counter_ns()
        ok, st, data = False, 0, b""
        try:
            c.request("POST", path, body=body,
                      headers={"Content-Type": "application/json"})
            r = c.getresponse()
            data = r.read()
            st = r.status
            ok = st == 200 and b'"header"' in data
        except Exception:
            if self.conn is not None:
                try:
                    self.conn.close()
                except Exception:
                    pass
            self.conn = None
        dt = time.perf_counter_ns() - t0
        return ok, st, data, dt

    def put(self, key, value=VALUE):
        body = '{"key":"%s","value":"%s"}' % (b64(key), b64(value))
        return self.post("/v3/kv/put", body.encode())

    def range(self, key, linearizable):
        body = '{"key":"%s"%s}' % (b64(key),
                                   ', "linearizable": true' if linearizable else "")
        return self.post("/v3/kv/range", body.encode())


def pct(xs, q):
    if not xs:
        return 0.0
    s = sorted(xs)
    rank = max(1, int(round(q * (len(s) - 1))))
    return s[min(rank, len(s) - 1)] / 1e6  # ms


def report(tag, samples, n):
    if samples:
        rps = len(samples) / (sum(samples) / 1e9)
    else:
        rps = 0.0
    print(
        f"[etcd-bench] {tag}: {rps:.0f} ops/s  p50 {pct(samples, .5):.2f}ms "
        f"p99 {pct(samples, .99):.2f}ms  ({len(samples)} ok / {n} attempted)"
    )


def discover(hosts, ports):
    for h, p in zip(hosts, ports):
        ok, _, _, _ = Cli(h, p, keepalive=False).put(b"discover")
        if ok:
            print(f"[etcd-bench] primary member {h}:{p} accepts writes")
            return h, p
    raise SystemExit("no etcd member accepted a write")


def bench_put_sequential(host, port, keepalive, n):
    cli = Cli(host, port, keepalive)
    samples = []
    for i in range(n):
        ok, _, _, dt = cli.put(f"k{i}")
        if ok:
            samples.append(dt)
    report(f"put {'keepalive' if keepalive else 'fresh'} 1w", samples, n)
    return samples


def bench_put_concurrent(host, port, workers, per_worker):
    def job(_):
        out = []
        for i in range(per_worker):
            ok, _, _, dt = Cli(host, port, keepalive=False).put(f"c{i}")
            if ok:
                out.append(dt)
        return out

    with futures.ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(job, range(workers)))
    samples = [d for r in results for d in r]
    report(f"put fresh {workers}w", samples, workers * per_worker)


def bench_range(host, port, key, linearizable, keepalive, n):
    cli = Cli(host, port, keepalive)
    samples = []
    for _ in range(n):
        ok, _, _, dt = cli.range(key, linearizable)
        if ok:
            samples.append(dt)
    kind = "linearizable" if linearizable else "serializable"
    report(f"range({kind}) {'keepalive' if keepalive else 'fresh'} 1w",
           samples, n)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hosts", default="127.0.0.1",
                    help="comma-separated member hosts")
    ap.add_argument("--ports", default="2379,2380,2381",
                    help="comma-separated client ports (one per host)")
    args = ap.parse_args()
    hosts = [h for h in args.hosts.split(",") if h]
    ports = [int(x) for x in args.ports.split(",")]
    print("[etcd-bench] === etcd 3-node benchmark ===")

    host, port = discover(hosts, ports)

    bench_put_sequential(host, port, keepalive=False, n=100)
    bench_put_concurrent(host, port, workers=4, per_worker=30)
    bench_put_sequential(host, port, keepalive=True, n=100)

    bench_range(host, port, b"k0", linearizable=False, keepalive=False, n=400)
    bench_range(host, port, b"k0", linearizable=True, keepalive=False, n=400)
    bench_range(host, port, b"k0", linearizable=True, keepalive=True, n=400)

    print("[etcd-bench] === done ===")


if __name__ == "__main__":
    sys.exit(main())
