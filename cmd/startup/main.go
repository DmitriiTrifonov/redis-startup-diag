// startup reproduces "service starts, go-redis cluster client gets i/o timeouts"
// with a real go-redis ClusterClient and full visibility into what it does:
//
//   - every TCP dial the pool makes (count, in-flight, latency, errors)
//   - every successful connection init (HELLO/AUTH/SETINFO) via OnConnect
//   - command latency and errors bucketed by cause
//   - pool stats (TotalConns, Idle, Misses, Timeouts) once per second
//   - go-redis internal log lines with timestamps
//
// Toggle PoolSize / MinIdleConns / number of workers / warmup mode and compare
// runs. Default operation is read-only GET of non-existent keys, so it is safe
// to point at a production cluster (it still adds load and connections!).
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/DmitriiTrifonov/redis-startup-diag/internal/diag"
)

type config struct {
	addrs        []string
	user, pass   string
	useTLS       bool
	poolSize     int
	minIdle      int
	maxActive    int
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	poolTimeout  time.Duration
	workers      int
	ramp         time.Duration
	duration     time.Duration
	report       time.Duration
	warmup       string
	warmupWait   time.Duration
	write        bool
	keyPrefix    string
	internalLog  bool
	hookDelay    time.Duration
	opPause      time.Duration
}

// ---------------------------------------------------------------------------
// instrumentation state

var (
	dialStarted  atomic.Int64
	dialInflight atomic.Int64
	dialMaxInfl  atomic.Int64
	dialOK       atomic.Int64
	connInit     atomic.Int64
	dialLat      diag.Latencies
	dialErrs     diag.Counter
	cmdLat       diag.Latencies
	cmdRes       diag.Counter
	opsTotal     atomic.Int64
)

func main() {
	cfg := parseFlags()

	printEnv(cfg)

	if cfg.internalLog {
		redis.SetLogger(tsLogger{})
	}

	opt := &redis.ClusterOptions{
		Addrs:          cfg.addrs,
		Username:       cfg.user,
		Password:       cfg.pass,
		PoolSize:       cfg.poolSize,
		MinIdleConns:   cfg.minIdle,
		MaxActiveConns: cfg.maxActive,
		DialTimeout:    cfg.dialTimeout,
		ReadTimeout:    cfg.readTimeout,
		WriteTimeout:   cfg.writeTimeout,
		PoolTimeout:    cfg.poolTimeout,
		Dialer:         instrumentedDialer(cfg),
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			connInit.Add(1)
			return nil
		},
	}
	if cfg.useTLS {
		opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}

	rdb := redis.NewClusterClient(opt)
	defer rdb.Close()
	rdb.AddHook(cmdHook{delay: cfg.hookDelay})
	if cfg.hookDelay > 0 {
		// Simulate slow interceptors (tracing/metrics) on every node client too,
		// like redisotel does via OnNewNode.
		rdb.OnNewNode(func(c *redis.Client) { c.AddHook(cmdHook{delay: cfg.hookDelay, nodeOnly: true}) })
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := warmup(ctx, cfg, rdb); err != nil {
		fmt.Printf("%s warmup FAILED: %v\n", diag.Since(), err)
	}

	runCtx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()

	var wg sync.WaitGroup
	reportDone := make(chan struct{})
	go reporter(runCtx, cfg, rdb, reportDone)

	fmt.Printf("%s starting %d workers (ramp=%s)\n", diag.Since(), cfg.workers, cfg.ramp)
	for i := 0; i < cfg.workers; i++ {
		if cfg.ramp > 0 && cfg.workers > 1 {
			time.Sleep(cfg.ramp / time.Duration(cfg.workers))
		}
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(runCtx, cfg, rdb, id)
		}(i)
	}
	wg.Wait()
	cancel()
	<-reportDone
	finalSummary()
}

func parseFlags() config {
	var c config
	var addrs string
	flag.StringVar(&addrs, "addrs", diag.Env("REDIS_ADDRS", ""), "seed nodes host:port, comma separated (env REDIS_ADDRS)")
	flag.StringVar(&c.user, "user", diag.Env("REDIS_USERNAME", ""), "ACL username (env REDIS_USERNAME)")
	flag.StringVar(&c.pass, "password", diag.Env("REDIS_PASSWORD", ""), "password (env REDIS_PASSWORD)")
	flag.BoolVar(&c.useTLS, "tls", false, "use TLS (InsecureSkipVerify, diagnostics only)")
	flag.IntVar(&c.poolSize, "pool-size", 10000, "ClusterOptions.PoolSize (per node!). 0 = go-redis default")
	flag.IntVar(&c.minIdle, "min-idle", 0, "ClusterOptions.MinIdleConns (per node)")
	flag.IntVar(&c.maxActive, "max-active", 0, "ClusterOptions.MaxActiveConns (per node), 0 = unlimited")
	flag.DurationVar(&c.dialTimeout, "dial-timeout", 5*time.Second, "DialTimeout")
	flag.DurationVar(&c.readTimeout, "read-timeout", 3*time.Second, "ReadTimeout")
	flag.DurationVar(&c.writeTimeout, "write-timeout", 3*time.Second, "WriteTimeout")
	flag.DurationVar(&c.poolTimeout, "pool-timeout", 0, "PoolTimeout, 0 = ReadTimeout+1s")
	flag.IntVar(&c.workers, "workers", 2000, "concurrent goroutines issuing commands (simulates incoming load)")
	flag.DurationVar(&c.ramp, "ramp", 0, "spread worker start over this duration (0 = all at once, worst case)")
	flag.DurationVar(&c.duration, "duration", 30*time.Second, "how long to run")
	flag.DurationVar(&c.report, "report", time.Second, "report interval")
	flag.StringVar(&c.warmup, "warmup", "none", "none | tcp (your current approach: raw dial+AUTH per seed) | ping (ForEachShard PING via the client)")
	flag.DurationVar(&c.warmupWait, "warmup-timeout", 30*time.Second, "give up warmup after this long")
	flag.BoolVar(&c.write, "write", false, "also SET keys (with 60s TTL). Default is read-only GET")
	flag.StringVar(&c.keyPrefix, "key-prefix", "redis-startup-diag:", "key prefix")
	flag.BoolVar(&c.internalLog, "internal-log", true, "print go-redis internal logs with timestamps")
	flag.DurationVar(&c.hookDelay, "hook-delay", 0, "artificial delay inside a command hook (simulates heavy interceptors)")
	flag.DurationVar(&c.opPause, "op-pause", 0, "pause between ops in each worker (0 = as fast as possible)")
	flag.Parse()
	c.addrs = diag.SplitAddrs(addrs)
	if len(c.addrs) == 0 {
		fmt.Fprintln(os.Stderr, "no addresses: pass -addrs or REDIS_ADDRS")
		os.Exit(2)
	}
	return c
}

func printEnv(cfg config) {
	ps := cfg.poolSize
	note := ""
	if ps == 0 {
		ps = 5 * runtime.GOMAXPROCS(0) // ClusterOptions default in recent v9; older v9: 10*GOMAXPROCS
		note = " (go-redis default, depends on GOMAXPROCS!)"
	}
	fmt.Printf("%s go=%s GOMAXPROCS=%d NumCPU=%d\n", diag.Since(), runtime.Version(), runtime.GOMAXPROCS(0), runtime.NumCPU())
	fmt.Printf("%s seeds=%v pool_size/node=%d%s min_idle/node=%d max_active/node=%d\n", diag.Since(), cfg.addrs, ps, note, cfg.minIdle, cfg.maxActive)
	fmt.Printf("%s dial_timeout=%s read_timeout=%s workers=%d warmup=%s write=%v\n", diag.Since(), cfg.dialTimeout, cfg.readTimeout, cfg.workers, cfg.warmup, cfg.write)
}

// ---------------------------------------------------------------------------
// dialer / hooks / logger

func instrumentedDialer(cfg config) func(ctx context.Context, network, addr string) (net.Conn, error) {
	d := &net.Dialer{Timeout: cfg.dialTimeout, KeepAlive: 5 * time.Minute}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialStarted.Add(1)
		n := dialInflight.Add(1)
		for {
			m := dialMaxInfl.Load()
			if n <= m || dialMaxInfl.CompareAndSwap(m, n) {
				break
			}
		}
		t := time.Now()
		conn, err := d.DialContext(ctx, network, addr)
		dialLat.Add(time.Since(t))
		dialInflight.Add(-1)
		if err != nil {
			dialErrs.Inc(diag.Classify(err), err)
			return nil, err
		}
		dialOK.Add(1)
		if cfg.useTLS {
			host, _, _ := net.SplitHostPort(addr)
			return tls.Client(conn, &tls.Config{InsecureSkipVerify: true, ServerName: host}), nil
		}
		return conn, nil
	}
}

type cmdHook struct {
	delay    time.Duration
	nodeOnly bool
}

func (h cmdHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h cmdHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.delay > 0 {
			time.Sleep(h.delay)
		}
		if h.nodeOnly {
			return next(ctx, cmd)
		}
		t := time.Now()
		err := next(ctx, cmd)
		cmdLat.Add(time.Since(t))
		cmdRes.Inc(diag.Classify(err), err)
		return err
	}
}

func (h cmdHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

type tsLogger struct{}

func (tsLogger) Printf(_ context.Context, format string, v ...interface{}) {
	fmt.Printf("%s [go-redis] %s\n", diag.Since(), fmt.Sprintf(format, v...))
}

// ---------------------------------------------------------------------------
// warmup strategies

func warmup(ctx context.Context, cfg config, rdb *redis.ClusterClient) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.warmupWait)
	defer cancel()
	t := time.Now()
	defer func() {
		fmt.Printf("%s warmup=%s took %s\n", diag.Since(), cfg.warmup, time.Since(t).Round(time.Millisecond))
	}()

	switch cfg.warmup {
	case "none":
		return nil

	case "tcp":
		// Same idea as the user's current workaround: separate raw connection
		// to each seed, AUTH, close. Does NOT fill the go-redis pool.
		for _, addr := range cfg.addrs {
			for attempt := 1; ; attempt++ {
				err := rawAuth(ctx, cfg, addr)
				fmt.Printf("%s warmup tcp %s attempt=%d err=%v\n", diag.Since(), addr, attempt, err)
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
		return nil

	case "ping":
		// Recommended: go through the client itself. Loads cluster topology,
		// opens one connection per master, retries with backoff.
		backoff := 100 * time.Millisecond
		for attempt := 1; ; attempt++ {
			err := rdb.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
				return c.Ping(ctx).Err()
			})
			fmt.Printf("%s warmup ping attempt=%d err=%v\n", diag.Since(), attempt, err)
			if err == nil {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			time.Sleep(backoff)
			if backoff < 2*time.Second {
				backoff *= 2
			}
		}
	}
	return fmt.Errorf("unknown warmup mode %q", cfg.warmup)
}

func rawAuth(ctx context.Context, cfg config, addr string) error {
	d := &net.Dialer{Timeout: cfg.dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	var c net.Conn = conn
	if cfg.useTLS {
		c = tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
	}
	_ = c.SetDeadline(time.Now().Add(cfg.readTimeout))
	cmd := "*1\r\n$4\r\nPING\r\n"
	if cfg.pass != "" {
		if cfg.user != "" {
			cmd = fmt.Sprintf("*3\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(cfg.user), cfg.user, len(cfg.pass), cfg.pass)
		} else {
			cmd = fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(cfg.pass), cfg.pass)
		}
	}
	if _, err := c.Write([]byte(cmd)); err != nil {
		return err
	}
	buf := make([]byte, 256)
	n, err := c.Read(buf)
	if err != nil {
		return err
	}
	if n > 0 && buf[0] == '-' {
		return fmt.Errorf("redis: %s", buf[1:n])
	}
	return nil
}

// ---------------------------------------------------------------------------
// load + reporting

func worker(ctx context.Context, cfg config, rdb *redis.ClusterClient, id int) {
	rnd := rand.New(rand.NewSource(int64(id) + time.Now().UnixNano()))
	for ctx.Err() == nil {
		key := fmt.Sprintf("%s%d", cfg.keyPrefix, rnd.Intn(1<<20))
		if cfg.write && rnd.Intn(2) == 0 {
			_ = rdb.Set(ctx, key, "x", time.Minute).Err()
		} else {
			_ = rdb.Get(ctx, key).Err()
		}
		opsTotal.Add(1)
		if cfg.opPause > 0 {
			time.Sleep(cfg.opPause)
		}
	}
}

func reporter(ctx context.Context, cfg config, rdb *redis.ClusterClient, done chan struct{}) {
	defer close(done)
	tk := time.NewTicker(cfg.report)
	defer tk.Stop()
	fmt.Printf("\n%-9s | %-58s | %-44s | %s\n", "time", "dials: started/ok/inflight(max) lat", "pool: total idle miss timeout", "commands")
	for {
		select {
		case <-ctx.Done():
			reportLine(rdb)
			return
		case <-tk.C:
			reportLine(rdb)
		}
	}
}

func reportLine(rdb *redis.ClusterClient) {
	ps := rdb.PoolStats()
	dl := dialLat.Drain()
	cl := cmdLat.Drain()
	fmt.Printf("%s | started=%d ok=%d init=%d infl=%d(%d) %s | total=%d idle=%d miss=%d timeouts=%d | %s | %s\n",
		diag.Since(),
		dialStarted.Load(), dialOK.Load(), connInit.Load(), dialInflight.Load(), dialMaxInfl.Load(),
		shortSummary(dl),
		ps.TotalConns, ps.IdleConns, ps.Misses, ps.Timeouts,
		diag.FormatCounts(cmdRes.Drain()),
		shortSummary(cl),
	)
	if e := dialErrs.Drain(); len(e) > 0 {
		fmt.Printf("%s |   dial errors: %s\n", diag.Since(), diag.FormatCounts(e))
	}
}

func shortSummary(d []time.Duration) string {
	if len(d) == 0 {
		return "lat[-]"
	}
	return "lat[" + diag.Summary(d) + "]"
}

func finalSummary() {
	fmt.Printf("\n--- summary ---\n")
	fmt.Printf("ops=%d dials_started=%d dials_ok=%d conn_init_ok=%d max_inflight_dials=%d\n",
		opsTotal.Load(), dialStarted.Load(), dialOK.Load(), connInit.Load(), dialMaxInfl.Load())
	fmt.Println("example errors (first seen per bucket):")
	for k, v := range cmdRes.Examples() {
		fmt.Printf("  cmd  %-14s %s\n", k, v)
	}
	for k, v := range dialErrs.Examples() {
		fmt.Printf("  dial %-14s %s\n", k, v)
	}
	fmt.Println(`
How to read:
  * max_inflight_dials in the hundreds/thousands  -> cold pool opens a connection per concurrent request (H4)
  * dials_ok >> conn_init_ok                      -> TCP fine, HELLO/AUTH handshake fails/times out (H5, H6)
  * dial lat steps at ~1s/~3s                     -> SYN drops: backlog or conntrack (H5, H7)
  * pool timeouts with few dial errors            -> pool waits on slow dials; compare with smaller -pool-size (H4)
  * errors vanish with -warmup ping or -ramp 5s   -> it is the startup burst, not the network itself
  * errors vanish only after a fixed delay        -> network readiness (H1), see netprobe`)
}
