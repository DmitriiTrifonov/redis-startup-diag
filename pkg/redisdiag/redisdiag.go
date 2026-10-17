// Package redisdiag is drop-in instrumentation for a go-redis v9 ClusterClient
// inside your own service. It is meant for environments where you cannot exec
// into pods: what it learns goes to the service's normal logs and can be read
// over HTTP (Handler) or gRPC (module pkg/grpcdiag) on an admin port.
//
// Per Redis node it tracks:
//   - every dial: count, in flight (and the max), latency, failures by cause
//   - pool statistics: connections opened on demand, pool timeouts, total/idle
//   - command errors by cause
//   - go-redis internal log lines (cluster state reloads, node failures)
//
// and keeps a ring buffer of recent notable events with timestamps.
//
// Usage, right after creating the client and BEFORE the first command
// (OnNewNode only sees nodes created after it is registered):
//
//	rdb := redis.NewClusterClient(opt)
//	diag := redisdiag.InstrumentCluster(rdb, redisdiag.Options{Logger: slog.Default()})
//	defer diag.Stop()
//	adminMux.Handle("/debug/redis/", diag.Handler())
package redisdiag

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/DmitriiTrifonov/redis-startup-diag/internal/diag"
)

type Options struct {
	Logger *slog.Logger
	// StatsEvery is the log reporting interval. Default 5s.
	StatsEvery time.Duration
	// SlowDial: successful dials slower than this are logged. Default 200ms.
	SlowDial time.Duration
	// Verbose logs a stats line every interval even if nothing changed.
	Verbose bool
	// DisableInternalLog keeps go-redis internal logs where they are.
	// By default they are routed to Logger and the event buffer (global setting!).
	DisableInternalLog bool
	// Events is the size of the recent-events ring buffer. Default 500.
	Events int
}

// Instrument holds the collected state. Safe for concurrent use.
type Instrument struct {
	opt     Options
	log     *slog.Logger
	rdb     *redis.ClusterClient
	started time.Time

	mu    sync.Mutex
	nodes map[string]*nodeStats

	evMu   sync.Mutex
	events []Event
	evNext int
	evFull bool

	probing atomic.Bool
	cancel  context.CancelFunc
}

type nodeStats struct {
	addr      string
	client    *redis.Client
	createdAt time.Time

	inflight    atomic.Int64
	maxInflight atomic.Int64
	dials       atomic.Int64
	dialFails   atomic.Int64
	dialLat     reservoir

	mu       sync.Mutex
	dialErrs map[string]int64 // cumulative, by cause
	cmdErrs  map[string]int64 // cumulative, by cause
	lastErr  map[string]string

	// for interval logging
	prev        redis.PoolStats
	prevDialErr map[string]int64
	prevCmdErr  map[string]int64
}

// Event is one notable thing that happened.
type Event struct {
	Time    time.Time `json:"time"`
	Kind    string    `json:"kind"` // dial_failed, slow_dial, node_created, pool_stats, go_redis_log
	Addr    string    `json:"addr,omitempty"`
	Message string    `json:"message"`
}

// InstrumentCluster attaches hooks and starts the log reporter.
func InstrumentCluster(rdb *redis.ClusterClient, opt Options) *Instrument {
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	if opt.StatsEvery <= 0 {
		opt.StatsEvery = 5 * time.Second
	}
	if opt.SlowDial <= 0 {
		opt.SlowDial = 200 * time.Millisecond
	}
	if opt.Events <= 0 {
		opt.Events = 500
	}
	in := &Instrument{
		opt:     opt,
		log:     opt.Logger.With("component", "redisdiag"),
		rdb:     rdb,
		started: time.Now(),
		nodes:   map[string]*nodeStats{},
		events:  make([]Event, opt.Events),
	}
	if !opt.DisableInternalLog {
		redis.SetLogger(internalLogger{in})
	}
	rdb.OnNewNode(func(c *redis.Client) {
		addr := c.Options().Addr
		ns := &nodeStats{
			addr: addr, client: c, createdAt: time.Now(),
			dialErrs: map[string]int64{}, cmdErrs: map[string]int64{}, lastErr: map[string]string{},
			prevDialErr: map[string]int64{}, prevCmdErr: map[string]int64{},
		}
		in.mu.Lock()
		in.nodes[addr] = ns
		in.mu.Unlock()
		in.event("node_created", addr, "node client created")
		in.log.Info("redis node client created", "addr", addr)
		c.AddHook(nodeHook{in: in, ns: ns})
	})

	ctx, cancel := context.WithCancel(context.Background())
	in.cancel = cancel
	go in.loop(ctx)
	return in
}

// Stop stops the periodic reporter.
func (in *Instrument) Stop() { in.cancel() }

func (in *Instrument) event(kind, addr, msg string) {
	in.evMu.Lock()
	in.events[in.evNext] = Event{Time: time.Now(), Kind: kind, Addr: addr, Message: msg}
	in.evNext = (in.evNext + 1) % len(in.events)
	if in.evNext == 0 {
		in.evFull = true
	}
	in.evMu.Unlock()
}

// Events returns recent events, oldest first.
func (in *Instrument) Events() []Event {
	in.evMu.Lock()
	defer in.evMu.Unlock()
	var out []Event
	if in.evFull {
		out = append(out, in.events[in.evNext:]...)
	}
	return append(out, in.events[:in.evNext]...)
}

func (in *Instrument) nodeList() []*nodeStats {
	in.mu.Lock()
	nodes := make([]*nodeStats, 0, len(in.nodes))
	for _, n := range in.nodes {
		nodes = append(nodes, n)
	}
	in.mu.Unlock()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].addr < nodes[j].addr })
	return nodes
}

// ---------------------------------------------------------------------------
// periodic log line

func (in *Instrument) loop(ctx context.Context) {
	t := time.NewTicker(in.opt.StatsEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			in.report()
		}
	}
}

func (in *Instrument) report() {
	for _, n := range in.nodeList() {
		ps := *n.client.PoolStats()
		newConns := int64(ps.Misses) - int64(n.prev.Misses)
		timeouts := int64(ps.Timeouts) - int64(n.prev.Timeouts)
		n.prev = ps
		n.mu.Lock()
		dialErrs := delta(n.dialErrs, n.prevDialErr)
		cmdErrs := delta(n.cmdErrs, n.prevCmdErr)
		n.mu.Unlock()

		quiet := newConns == 0 && timeouts == 0 && len(dialErrs) == 0 && len(cmdErrs) == 0
		if quiet && !in.opt.Verbose {
			continue
		}
		level := slog.LevelInfo
		if timeouts > 0 || len(dialErrs) > 0 || len(cmdErrs) > 0 {
			level = slog.LevelWarn
		}
		msg := fmt.Sprintf("new_conns=%d pool_timeouts=%d total=%d idle=%d dial_errors=[%s] cmd_errors=[%s]",
			newConns, timeouts, ps.TotalConns, ps.IdleConns, diag.FormatCounts(dialErrs), diag.FormatCounts(cmdErrs))
		if level == slog.LevelWarn || newConns >= 50 {
			in.event("pool_stats", n.addr, msg)
		}
		in.log.Log(context.Background(), level, "redis pool stats",
			"addr", n.addr,
			"interval", in.opt.StatsEvery.String(),
			"new_conns", newConns, // pool misses = connections opened on demand
			"pool_timeouts", timeouts,
			"total_conns", ps.TotalConns,
			"idle_conns", ps.IdleConns,
			"dials_in_flight", n.inflight.Load(),
			"dials_in_flight_max", n.maxInflight.Load(),
			"dial_errors", diag.FormatCounts(dialErrs),
			"cmd_errors", diag.FormatCounts(cmdErrs),
		)
	}
}

// delta returns cur-prev for changed keys and updates prev. Caller holds the lock.
func delta(cur, prev map[string]int64) map[string]int {
	out := map[string]int{}
	for k, v := range cur {
		if d := v - prev[k]; d > 0 {
			out[k] = int(d)
		}
		prev[k] = v
	}
	return out
}

// ---------------------------------------------------------------------------
// hooks

type nodeHook struct {
	in *Instrument
	ns *nodeStats
}

func (h nodeHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		ns := h.ns
		ns.dials.Add(1)
		infl := ns.inflight.Add(1)
		for {
			m := ns.maxInflight.Load()
			if infl <= m || ns.maxInflight.CompareAndSwap(m, infl) {
				break
			}
		}
		t := time.Now()
		conn, err := next(ctx, network, addr)
		d := time.Since(t)
		ns.inflight.Add(-1)
		ns.dialLat.add(d)
		if err != nil {
			cls := diag.Classify(err)
			ns.dialFails.Add(1)
			ns.mu.Lock()
			ns.dialErrs[cls]++
			ns.lastErr["dial_"+cls] = err.Error()
			ns.mu.Unlock()
			h.in.event("dial_failed", addr, fmt.Sprintf("took=%s cause=%s in_flight=%d err=%v", d, cls, infl, err))
			h.in.log.Warn("redis dial failed", "addr", addr, "took", d.String(), "cause", cls, "dials_in_flight", infl, "err", err)
		} else if d >= h.in.opt.SlowDial {
			h.in.event("slow_dial", addr, fmt.Sprintf("took=%s in_flight=%d", d, infl))
			h.in.log.Warn("redis slow dial", "addr", addr, "took", d.String(), "dials_in_flight", infl)
		}
		return conn, err
	}
}

func (h nodeHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		h.cmdErr(err)
		return err
	}
}

func (h nodeHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		h.cmdErr(err)
		return err
	}
}

func (h nodeHook) cmdErr(err error) {
	if err == nil {
		return
	}
	cls := diag.Classify(err)
	if cls == "ok(nil)" || cls == "setinfo_unsupported" {
		// redis.Nil is not an error; CLIENT SETINFO failing on Redis < 7.2 is
		// part of connection init and ignored by go-redis itself.
		return
	}
	h.ns.mu.Lock()
	h.ns.cmdErrs[cls]++
	h.ns.lastErr["cmd_"+cls] = err.Error()
	h.ns.mu.Unlock()
}

type internalLogger struct{ in *Instrument }

func (l internalLogger) Printf(_ context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.in.event("go_redis_log", "", msg)
	l.in.log.Warn("go-redis: " + msg)
}

// ---------------------------------------------------------------------------
// snapshot

// Snapshot is the full current state, JSON-friendly.
type Snapshot struct {
	Time       time.Time      `json:"time"`
	Uptime     string         `json:"uptime"`
	GoVersion  string         `json:"go_version"`
	GOMAXPROCS int            `json:"gomaxprocs"`
	NumCPU     int            `json:"num_cpu"`
	Goroutines int            `json:"goroutines"`
	Options    OptionsView    `json:"client_options"`
	Nodes      []NodeSnapshot `json:"nodes"`
}

// OptionsView is the subset of ClusterOptions that matters here. No secrets.
type OptionsView struct {
	Seeds           []string `json:"seeds"`
	PoolSize        int      `json:"pool_size_per_node"`
	MinIdleConns    int      `json:"min_idle_conns"`
	MaxActiveConns  int      `json:"max_active_conns"`
	DialTimeout     string   `json:"dial_timeout"`
	ReadTimeout     string   `json:"read_timeout"`
	WriteTimeout    string   `json:"write_timeout"`
	PoolTimeout     string   `json:"pool_timeout"`
	ConnMaxIdleTime string   `json:"conn_max_idle_time"`
	MaxRetries      int      `json:"max_retries"`
	ReadOnly        bool     `json:"read_only"`
	RouteByLatency  bool     `json:"route_by_latency"`
	RouteRandomly   bool     `json:"route_randomly"`
	TLS             bool     `json:"tls"`
}

type NodeSnapshot struct {
	Addr              string            `json:"addr"`
	ClientAge         string            `json:"client_age"`
	TotalConns        uint32            `json:"total_conns"`
	IdleConns         uint32            `json:"idle_conns"`
	NewConnsTotal     uint32            `json:"new_conns_total"` // pool misses
	PoolHits          uint32            `json:"pool_hits"`
	PoolTimeouts      uint32            `json:"pool_timeouts"`
	StaleConns        uint32            `json:"stale_conns"`
	DialsTotal        int64             `json:"dials_total"`
	DialFailures      int64             `json:"dial_failures"`
	DialsInFlight     int64             `json:"dials_in_flight"`
	DialsInFlightMax  int64             `json:"dials_in_flight_max"`
	DialLatencyRecent string            `json:"dial_latency_recent"`
	DialErrors        map[string]int64  `json:"dial_errors"`
	CmdErrors         map[string]int64  `json:"cmd_errors"`
	LastErrors        map[string]string `json:"last_errors"`
}

func (in *Instrument) Snapshot() Snapshot {
	o := in.rdb.Options()
	s := Snapshot{
		Time:       time.Now(),
		Uptime:     time.Since(in.started).Round(time.Millisecond).String(),
		GoVersion:  runtime.Version(),
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		NumCPU:     runtime.NumCPU(),
		Goroutines: runtime.NumGoroutine(),
		Options: OptionsView{
			Seeds: o.Addrs, PoolSize: o.PoolSize, MinIdleConns: o.MinIdleConns, MaxActiveConns: o.MaxActiveConns,
			DialTimeout: o.DialTimeout.String(), ReadTimeout: o.ReadTimeout.String(), WriteTimeout: o.WriteTimeout.String(),
			PoolTimeout: o.PoolTimeout.String(), ConnMaxIdleTime: o.ConnMaxIdleTime.String(), MaxRetries: o.MaxRetries,
			ReadOnly: o.ReadOnly, RouteByLatency: o.RouteByLatency, RouteRandomly: o.RouteRandomly, TLS: o.TLSConfig != nil,
		},
	}
	for _, n := range in.nodeList() {
		ps := n.client.PoolStats()
		n.mu.Lock()
		ns := NodeSnapshot{
			Addr:              n.addr,
			ClientAge:         time.Since(n.createdAt).Round(time.Second).String(),
			TotalConns:        ps.TotalConns,
			IdleConns:         ps.IdleConns,
			NewConnsTotal:     ps.Misses,
			PoolHits:          ps.Hits,
			PoolTimeouts:      ps.Timeouts,
			StaleConns:        ps.StaleConns,
			DialsTotal:        n.dials.Load(),
			DialFailures:      n.dialFails.Load(),
			DialsInFlight:     n.inflight.Load(),
			DialsInFlightMax:  n.maxInflight.Load(),
			DialLatencyRecent: diag.Summary(n.dialLat.values()),
			DialErrors:        copyMap(n.dialErrs),
			CmdErrors:         copyMap(n.cmdErrs),
			LastErrors:        copyStrMap(n.lastErr),
		}
		n.mu.Unlock()
		s.Nodes = append(s.Nodes, ns)
	}
	return s
}

func copyMap(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyStrMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// reservoir keeps the last 1024 durations.
type reservoir struct {
	mu   sync.Mutex
	buf  [1024]time.Duration
	n    int
	full bool
}

func (r *reservoir) add(d time.Duration) {
	r.mu.Lock()
	r.buf[r.n] = d
	r.n = (r.n + 1) % len(r.buf)
	if r.n == 0 {
		r.full = true
	}
	r.mu.Unlock()
}

func (r *reservoir) values() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.full {
		return append([]time.Duration(nil), r.buf[:]...)
	}
	return append([]time.Duration(nil), r.buf[:r.n]...)
}
