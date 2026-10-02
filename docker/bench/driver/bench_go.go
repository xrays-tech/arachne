// bench_go.go — Go 客户端基准（host 侧，连已发布端口）。
//
// 用途（Lane 3）：用 Go（goroutine 并发、http 连接池、无 GIL）重测 arachne
// 与 etcd 的并发线性读，排除 Python 客户端（GIL/HTTP 解析/调度）残余影响，
// 判定 arachne 4w ~2043 是 server 真实上限还是 client 残余。
//
// 与 Python driver 同口径（docker/bench/driver/bench.py、docker/bench-etcd/driver/etcdbench.py）：
//   - strided workers：worker w 处理 range(w, n, workers)；
//   - 每 worker 一条持久连接（keep-alive，等价 Python 每 worker 一条 HTTPConnection）；
//   - dt = 发请求到响应体读完的墙钟（ns）；
//   - rps = 成功数 / (Σdt / 1e9)；p50/p99 用与 Python 一致的 rank 公式。
//
// 协议可插拔：
//   - --protocol arachne：GET /kv/<key>[?stale=1]，写 PUT /kv/<key>/<value>；
//     线性读只打 leader（自动发现：PUT 200 的端口）。
//   - --protocol etcd：线性读 POST /v3/kv/range {"key": b64, "linearizable": true}
//     （成功 = 200 且 body 含 "header"，与 etcdbench.py 一致），写 POST /v3/kv/put
//     {"key": b64, "value": b64}；任意成员可写、线性读。
//
// 用法（host 上连已映射端口；driver 跑在 host，非容器内）：
//   go run ./bench_go.go --protocol arachne --workers 1,2,4,8 --n 400 --mode linear
//   go run ./bench_go.go --protocol etcd    --workers 1,2,4,8 --n 400 --mode linear
//   go run ./bench_go.go --protocol arachne --workers 1 --n 400 --mode put
//   --keep-alive=false 切 fresh（每请求新建连接）。
//
// 输出文本行：
//   [go-bench] linear 4w: 2400 ops/s  p50 0.41ms p99 1.20ms  (400 ok / 400 attempted)
//   [go-bench] === done ===
//
// 编译：本地 go build（host darwin/arm64，直接连 host 映射端口；driver 不上容器，
// 故无需跨架构交叉编译）。
package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// cfg 基准配置。
type cfg struct {
	protocol  string // arachne | etcd
	work      []int  // read 并发梯度
	n         int    // 总 op（strided 到各 worker）
	key       string // 读/写 key
	mode      string // linear | stale | put
	keepAlive bool   // 每 worker 一条持久连接
	hosts     []string
	ports     []int
	seed      bool
}

// parseCLI 解析命令行参数并填默认值。
func parseCLI() cfg {
	var c cfg
	var rawW, rawH, rawP string
	flag.StringVar(&c.protocol, "protocol", "arachne", "backend: arachne (GET /kv/<key>) or etcd (POST /v3/kv/range)")
	flag.StringVar(&rawW, "workers", "1,2,4,8", "read concurrency gradient, e.g. '1,2,4,8'")
	flag.IntVar(&c.n, "n", 400, "total read ops (strided across workers)")
	flag.StringVar(&c.key, "key", "", "read/write key (default per-protocol: arachne=seed, etcd=k0)")
	flag.StringVar(&c.mode, "mode", "linear", "linear | stale | put")
	flag.BoolVar(&c.keepAlive, "keep-alive", true, "keep-alive model (default on); use --keep-alive=false for per-request conn")
	flag.StringVar(&rawH, "hosts", "", "comma-separated hosts (default 127.0.0.1)")
	flag.StringVar(&rawP, "ports", "", "comma-separated ports (default per-protocol)")
	flag.BoolVar(&c.seed, "seed", true, "seed the read key once before reads (default on)")
	flag.Parse()

	if flag.NArg() > 0 {
		log.Fatal("unexpected positional args")
	}
	if c.protocol != "arachne" && c.protocol != "etcd" {
		log.Fatalf("invalid --protocol %q (want arachne|etcd)", c.protocol)
	}
	switch c.mode {
	case "linear", "stale", "put":
	default:
		log.Fatalf("invalid --mode %q (want linear|stale|put)", c.mode)
	}

	if c.key == "" {
		if c.protocol == "etcd" {
			c.key = "k0"
		} else {
			c.key = "seed"
		}
	}
	c.work = intList(rawW)
	if len(c.work) == 0 {
		c.work = []int{1}
	}
	if c.n < 1 {
		c.n = 400
	}
	if rawH == "" {
		c.hosts = []string{"127.0.0.1"}
	} else {
		c.hosts = splitNonEmpty(rawH, ",")
	}
	if rawP == "" {
		if c.protocol == "etcd" {
			c.ports = []int{2379, 2480, 2580}
		} else {
			c.ports = []int{8001, 8002, 8003}
		}
	} else {
		c.ports = intList(rawP)
	}
	return c
}

// splitNonEmpty 按 sep 切分并去空。
func splitNonEmpty(s, sep string) []string {
	parts := strings.Split(s, sep)
	var out []string
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// intList 解析逗号分隔正整数。
func intList(s string) []int {
	var out []int
	for _, p := range splitNonEmpty(s, ",") {
		if v := atoiTrim(strings.TrimSpace(p)); v > 0 {
			out = append(out, v)
		}
	}
	return out
}

func atoiTrim(s string) int {
	if len(s) == 0 {
		return 0
	}
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// b64enc 标准库 base64 编码（预缓存避免热路径重复）。
var b64cache = make(map[string]string)

func b64enc(b string) string {
	if v, ok := b64cache[b]; ok {
		return v
	}
	v := base64.StdEncoding.EncodeToString([]byte(b))
	b64cache[b] = v
	return v
}

// newClient 返回每 worker 独立的 *http.Client（独立 Transport/连接池），
// 保证"每 worker 一条持久连接、互不串行化"（keep-alive）；--keep-alive=false
// 则 DisableKeepAlives 每请求新建连接（等价 Python 的 fresh 模型）。
func newClient(keepAlive bool) *http.Client {
	tr := &http.Transport{
		DisableKeepAlives:   !keepAlive,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 1,
		MaxConnsPerHost:     4,
		IdleConnTimeout:     60 * time.Second,
	}
	return &http.Client{
		Transport: tr,
		Timeout:   15 * time.Second,
	}
}

// do 发一次 HTTP 请求并读全响应体，返回 (status, body, dtNs)。
func do(client *http.Client, method, url string, body, contentType string) (int, []byte, int64) {
	t0 := time.Now()
	var req *http.Request
	if body != "" {
		req, _ = http.NewRequest(method, url, strings.NewReader(body))
	} else {
		req, _ = http.NewRequest(method, url, nil)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, time.Since(t0).Nanoseconds()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, time.Since(t0).Nanoseconds()
}

// readOp 一次线性/陈旧读（协议相关），返回 (ok, dtNs)。
func readOp(client *http.Client, host string, port int, key string, c cfg, stale bool) (bool, int64) {
	if c.protocol == "etcd" {
		body := fmt.Sprintf(`{"key":"%s","linearizable":true}`, b64enc(key))
		status, b, dt := do(client, "POST",
			fmt.Sprintf("http://%s:%d/v3/kv/range", host, port), body, "application/json")
		return status == 200 && strings.Contains(string(b), "header"), dt
	}
	url := fmt.Sprintf("http://%s:%d/kv/%s", host, port, key)
	if stale {
		url += "?stale=1"
	}
	status, _, dt := do(client, "GET", url, "", "")
	return status == 200, dt
}

// putOp 一次写（协议相关），返回 (ok, dtNs)。
func putOp(client *http.Client, host string, port int, key string, c cfg) (bool, int64) {
	if c.protocol == "etcd" {
		body := fmt.Sprintf(`{"key":"%s","value":"%s"}`, b64enc(key), b64enc("v"))
		status, _, dt := do(client, "POST",
			fmt.Sprintf("http://%s:%d/v3/kv/put", host, port), body, "application/json")
		return status == 200, dt
	}
	status, _, dt := do(client, "PUT", fmt.Sprintf("http://%s:%d/kv/%s/v", host, port, key), "", "")
	return status == 200, dt
}

// runRead 对给定 worker 数 w 运行一次 read 基准，返回样本集（仅 ok 的 dtNs）。
func runRead(c cfg, host string, port int, w int, stale bool) []int64 {
	clients := make([]http.Client, w)
	for i := range clients {
		clients[i] = *newClient(c.keepAlive)
	}
	samples := make([]int64, 0, c.n)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for worker := 0; worker < w; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			client := &clients[worker]
			for i := worker; i < c.n; i += w {
				if ok, dt := readOp(client, host, port, c.key, c, stale); ok {
					mu.Lock()
					samples = append(samples, dt)
					mu.Unlock()
				}
			}
		}(worker)
	}
	wg.Wait()
	return samples
}

// runPut 对给定 worker 数 w 运行一次 put 基准（每个 op 唯一 key），返回样本集。
func runPut(c cfg, host string, port int, w int, n int, tag string) []int64 {
	clients := make([]http.Client, w)
	for i := range clients {
		clients[i] = *newClient(c.keepAlive)
	}
	samples := make([]int64, 0, n)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for worker := 0; worker < w; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			client := &clients[worker]
			for i := worker; i < n; i += w {
				key := fmt.Sprintf("%s-%d-%d", tag, worker, i)
				if ok, dt := putOp(client, host, port, key, c); ok {
					mu.Lock()
					samples = append(samples, dt)
					mu.Unlock()
				}
			}
		}(worker)
	}
	wg.Wait()
	return samples
}

// pct 与 Python pct() 一致的百分位数：rank = max(1, round(q*(n-1)))。
func pct(xs []int64, q float64) int64 {
	n := len(xs)
	if n == 0 {
		return 0
	}
	s := make([]int64, n)
	copy(s, xs)
	sort.Slice(s, func(a, b int) bool { return s[a] < s[b] })
	rank := int(math.Round(q * float64(n-1)))
	if rank < 1 {
		rank = 1
	}
	if rank > n-1 {
		rank = n - 1
	}
	return s[rank]
}

// report 打印一行结果（与 Python 同格式，仅前缀为 [go-bench]）。
func report(tag string, samples []int64, attempted int) {
	ops := len(samples)
	var rps float64
	if ops > 0 {
		var sum int64
		for _, s := range samples {
			sum += s
		}
		rps = float64(ops) / (float64(sum) / 1e9)
	}
	p50 := float64(pct(samples, 0.5)) / 1e6
	p99 := float64(pct(samples, 0.99)) / 1e6
	fmt.Printf("[go-bench] %s: %.0f ops/s  p50 %.2fms p99 %.2fms  (%d ok / %d attempted)\n",
		tag, rps, p50, p99, ops, attempted)
}

// discover 返回首个接受写的目标端口（arachne=leader，etcd=任意可写成员）。
func discover(c cfg) (string, int) {
	for i := 0; i < len(c.ports); i++ {
		port := c.ports[i]
		host := "127.0.0.1"
		if i < len(c.hosts) {
			host = c.hosts[i]
		}
		client := *newClient(c.keepAlive)
		if c.protocol == "etcd" {
			body := fmt.Sprintf(`{"key":"%s","value":"%s"}`, b64enc("probe"), b64enc("v"))
			status, _, _ := do(&client, "POST", fmt.Sprintf("http://%s:%d/v3/kv/put", host, port), body, "application/json")
			if status == 200 {
				log.Printf("[go-bench] target etcd member %s:%d accepts writes", host, port)
				return host, port
			}
		} else {
			status, _, _ := do(&client, "PUT", fmt.Sprintf("http://%s:%d/kv/probe-all/v", host, port), "", "")
			if status == 200 {
				log.Printf("[go-bench] leader found on %s:%d", host, port)
				return host, port
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Fatal("no leader/member accepted writes")
	return "", 0
}

// seedWrite 在 read 前写入 key（确保读 key 存在）。
func seedWrite(c cfg, host string, port int) bool {
	client := *newClient(c.keepAlive)
	if c.protocol == "etcd" {
		status, _, _ := do(&client, "POST", fmt.Sprintf("http://%s:%d/v3/kv/put", host, port), fmt.Sprintf(`{"key":"%s","value":"%s"}`, b64enc(c.key), b64enc("v")), "application/json")
		return status == 200
	}
	status, _, _ := do(&client, "PUT", fmt.Sprintf("http://%s:%d/kv/%s/v", host, port, c.key), "", "")
	return status == 200
}

func main() {
	c := parseCLI()
	log.Printf("[go-bench] protocol=%s mode=%s workers=%v n=%d key=%q keepalive=%v",
		c.protocol, c.mode, c.work, c.n, c.key, c.keepAlive)

	host, port := discover(c)

	if c.seed && (c.mode == "linear" || c.mode == "stale") {
		if !seedWrite(c, host, port) {
			log.Printf("[go-bench] warning: seed write failed on %s:%d (proceeding; reads may fail)", host, port)
		}
	}

	if c.mode == "put" {
		samples := runPut(c, host, port, 1, c.n, "put")
		report("put 1w", samples, c.n)
		log.Println("[go-bench] === done ===")
		return
	}
	stale := c.mode == "stale"
	for _, w := range c.work {
		tag := fmt.Sprintf("%s %dw", c.mode, w)
		samples := runRead(c, host, port, w, stale)
		report(tag, samples, c.n)
	}
	log.Println("[go-bench] === done ===")
}
