// Package diag holds small helpers shared by the diagnostic commands:
// latency recording, error classification and environment parsing.
package diag

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// ProcessStart is captured as early as possible so every report can show
// time since the process started (i.e. roughly since the container started).
var ProcessStart = time.Now()

// Since returns a compact "+1.234s" string relative to ProcessStart.
func Since() string {
	return fmt.Sprintf("+%7.3fs", time.Since(ProcessStart).Seconds())
}

// Latencies is a concurrency-safe bag of durations with percentile helpers.
type Latencies struct {
	mu sync.Mutex
	d  []time.Duration
}

func (l *Latencies) Add(d time.Duration) {
	l.mu.Lock()
	l.d = append(l.d, d)
	l.mu.Unlock()
}

// Drain returns the collected durations and resets the bag.
func (l *Latencies) Drain() []time.Duration {
	l.mu.Lock()
	out := l.d
	l.d = nil
	l.mu.Unlock()
	return out
}

// Snapshot returns a copy without resetting.
func (l *Latencies) Snapshot() []time.Duration {
	l.mu.Lock()
	out := append([]time.Duration(nil), l.d...)
	l.mu.Unlock()
	return out
}

// Summary formats count / p50 / p95 / p99 / max.
func Summary(d []time.Duration) string {
	if len(d) == 0 {
		return "n=0"
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	p := func(q float64) time.Duration {
		i := int(float64(len(s)-1) * q)
		return s[i]
	}
	return fmt.Sprintf("n=%d p50=%s p95=%s p99=%s max=%s",
		len(s), r(p(0.50)), r(p(0.95)), r(p(0.99)), r(s[len(s)-1]))
}

func r(d time.Duration) time.Duration {
	switch {
	case d >= time.Second:
		return d.Round(time.Millisecond)
	case d >= time.Millisecond:
		return d.Round(10 * time.Microsecond)
	default:
		return d.Round(time.Microsecond)
	}
}

// Classify maps an error to a short, stable bucket name. The buckets are
// chosen so that each one points to a different hypothesis in README.md.
func Classify(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, redis.Nil) {
		return "ok(nil)"
	}
	msg := err.Error()
	switch {
	case errors.Is(err, redis.ErrPoolTimeout) || strings.Contains(msg, "connection pool timeout"):
		return "pool_timeout" // waited for a free conn in our own pool
	case strings.Contains(msg, "max number of clients"):
		return "maxclients" // Redis refused: maxclients reached
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused" // nothing listening / RST on SYN
	case errors.Is(err, syscall.ECONNRESET):
		return "reset"
	case errors.Is(err, syscall.EADDRNOTAVAIL):
		return "no_local_port" // ephemeral ports exhausted
	case errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH):
		return "unreachable" // no route (network not ready?)
	case errors.Is(err, io.EOF):
		return "eof"
	case strings.Contains(msg, "unknown subcommand 'setinfo'"):
		return "setinfo_unsupported" // CLIENT SETINFO on Redis < 7.2, ignored by go-redis
	case strings.HasPrefix(msg, "MOVED "):
		return "moved" // slot moved (failover / resharding), handled by the cluster client
	case strings.HasPrefix(msg, "ASK "):
		return "ask"
	case strings.HasPrefix(msg, "TRYAGAIN"):
		return "tryagain"
	case strings.HasPrefix(msg, "LOADING"):
		return "loading" // node is loading the dataset (after restart / full sync)
	case strings.HasPrefix(msg, "READONLY"):
		return "readonly" // write sent to a replica
	case strings.HasPrefix(msg, "MASTERDOWN"):
		return "masterdown"
	case strings.Contains(msg, "CLUSTERDOWN"):
		return "clusterdown"
	case strings.Contains(msg, "NOAUTH") || strings.Contains(msg, "WRONGPASS"):
		return "auth"
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded):
		return classifyTimeout(msg)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return classifyTimeout(msg)
	}
	var de *net.DNSError
	if errors.As(err, &de) {
		return "dns"
	}
	return "other"
}

func classifyTimeout(msg string) string {
	switch {
	case strings.Contains(msg, "dial"):
		return "timeout_dial" // SYN not answered in time
	case strings.Contains(msg, "read"):
		return "timeout_read" // connected, but reply did not arrive in time
	case strings.Contains(msg, "write"):
		return "timeout_write"
	}
	return "timeout"
}

// Counter is a concurrency-safe map of bucket -> count.
type Counter struct {
	mu sync.Mutex
	m  map[string]int
	ex map[string]string // one example error message per bucket
}

func (c *Counter) Inc(bucket string, err error) {
	c.mu.Lock()
	if c.m == nil {
		c.m = map[string]int{}
		c.ex = map[string]string{}
	}
	c.m[bucket]++
	if err != nil {
		if _, ok := c.ex[bucket]; !ok {
			c.ex[bucket] = err.Error()
		}
	}
	c.mu.Unlock()
}

// Drain returns bucket counts and resets them (examples are kept).
func (c *Counter) Drain() map[string]int {
	c.mu.Lock()
	out := c.m
	c.m = map[string]int{}
	c.mu.Unlock()
	return out
}

func (c *Counter) Examples() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]string, len(c.ex))
	for k, v := range c.ex {
		out[k] = v
	}
	return out
}

// FormatCounts renders a map as "a=1 b=2" with stable ordering.
func FormatCounts(m map[string]int) string {
	if len(m) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%d", k, m[k])
	}
	return b.String()
}

// Env returns the environment variable or a default.
func Env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// SplitAddrs splits a comma/space separated list of host:port.
func SplitAddrs(s string) []string {
	f := strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' })
	out := f[:0]
	for _, a := range f {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}
