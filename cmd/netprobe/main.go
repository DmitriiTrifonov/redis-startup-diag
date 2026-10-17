// netprobe checks the network path from a freshly started pod to every Redis
// node WITHOUT go-redis. It answers two questions:
//
//  1. Is the network ready right after container start? (sequential mode)
//     It dials every node again and again from t=0 and prints when the first
//     successful dial happened and how long the first dials took.
//
//  2. What happens when many connections are opened at once? (burst mode)
//     It opens N connections to every node simultaneously, which is what a
//     cold go-redis pool does under load, and shows how many of them time out.
//
// Run it as the very first thing in a pod (e.g. as the container command or an
// initContainer) so it observes the same conditions as your service.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/DmitriiTrifonov/redis-startup-diag/internal/diag"
)

type config struct {
	addrs    []string
	user     string
	password string
	useTLS   bool
	auth     bool
	timeout  time.Duration
	interval time.Duration
	duration time.Duration
	burst    int
	hold     time.Duration
}

func main() {
	var cfg config
	var addrs string
	flag.StringVar(&addrs, "addrs", diag.Env("REDIS_ADDRS", ""), "comma separated host:port list of ALL cluster nodes (env REDIS_ADDRS)")
	flag.StringVar(&cfg.user, "user", diag.Env("REDIS_USERNAME", ""), "ACL username (env REDIS_USERNAME)")
	flag.StringVar(&cfg.password, "password", diag.Env("REDIS_PASSWORD", ""), "password (env REDIS_PASSWORD)")
	flag.BoolVar(&cfg.useTLS, "tls", false, "use TLS (InsecureSkipVerify, diagnostics only)")
	flag.BoolVar(&cfg.auth, "auth", true, "after TCP connect send AUTH (if password set) and PING")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Second, "dial and per-command timeout")
	flag.DurationVar(&cfg.interval, "interval", 250*time.Millisecond, "pause between sequential rounds")
	flag.DurationVar(&cfg.duration, "duration", 15*time.Second, "how long to run sequential rounds (0 = skip)")
	flag.IntVar(&cfg.burst, "burst", 0, "after sequential rounds open N connections per node at once (0 = skip)")
	flag.DurationVar(&cfg.hold, "hold", 2*time.Second, "keep burst connections open this long before closing")
	flag.Parse()

	cfg.addrs = diag.SplitAddrs(addrs)
	if len(cfg.addrs) == 0 {
		fmt.Fprintln(os.Stderr, "no addresses: pass -addrs or REDIS_ADDRS")
		os.Exit(2)
	}

	fmt.Printf("%s netprobe start, nodes=%d timeout=%s auth=%v\n", diag.Since(), len(cfg.addrs), cfg.timeout, cfg.auth)

	if cfg.duration > 0 {
		sequential(cfg)
	}
	if cfg.burst > 0 {
		burst(cfg)
	}
}

// ---------------------------------------------------------------------------
// sequential mode

type nodeState struct {
	firstOK     string
	attempts    int
	fails       int
	firstResult string
	dials       []time.Duration
}

func sequential(cfg config) {
	fmt.Printf("\n=== sequential: one connection at a time, every %s for %s ===\n", cfg.interval, cfg.duration)
	state := map[string]*nodeState{}
	for _, a := range cfg.addrs {
		state[a] = &nodeState{}
	}
	deadline := time.Now().Add(cfg.duration)
	for round := 0; time.Now().Before(deadline); round++ {
		for _, addr := range cfg.addrs {
			res := probeOnce(cfg, addr)
			st := state[addr]
			st.attempts++
			if st.attempts == 1 {
				st.firstResult = res.String()
			}
			if res.err != nil {
				st.fails++
			} else {
				st.dials = append(st.dials, res.dial)
				if st.firstOK == "" {
					st.firstOK = res.at
				}
			}
			// Print every failure, the first round and every 10th round.
			if res.err != nil || round == 0 || round%10 == 0 {
				fmt.Printf("%s round=%-3d %s\n", res.at, round, res.String())
			}
		}
		time.Sleep(cfg.interval)
	}

	fmt.Printf("\n--- sequential summary ---\n")
	for _, addr := range cfg.addrs {
		st := state[addr]
		fmt.Printf("%-24s attempts=%d fails=%d first_ok_at=%s\n", addr, st.attempts, st.fails, orDash(st.firstOK))
		fmt.Printf("%-24s first attempt: %s\n", "", st.firstResult)
		if len(st.dials) > 1 {
			fmt.Printf("%-24s dial after warm: %s\n", "", diag.Summary(st.dials[1:]))
		}
	}
	fmt.Println(`
How to read:
  * fails only in the first seconds, then 0       -> pod network/policy not ready at start (H1)
  * first dial much slower than the rest          -> first-packet cost on the path: ARP/conntrack/SDN/DNS (H2, H3)
  * dns=... large on hostnames                    -> resolv.conf ndots / DNS (H3)
  * everything fast and clean here                -> the network is fine for single conns; look at burst mode / go-redis (H4-H8)`)
}

type result struct {
	at      string
	addr    string
	dns     time.Duration
	dial    time.Duration
	cmd     time.Duration
	err     error
	errStep string
}

func (r result) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-24s", r.addr)
	if r.dns > 0 {
		fmt.Fprintf(&b, " dns=%s", r.dns.Round(time.Microsecond))
	}
	fmt.Fprintf(&b, " dial=%s", r.dial.Round(time.Microsecond))
	if r.cmd > 0 {
		fmt.Fprintf(&b, " auth+ping=%s", r.cmd.Round(time.Microsecond))
	}
	if r.err != nil {
		fmt.Fprintf(&b, " FAIL[%s:%s] %v", r.errStep, diag.Classify(r.err), r.err)
	} else {
		b.WriteString(" ok")
	}
	return b.String()
}

func probeOnce(cfg config, addr string) result {
	res := result{at: diag.Since(), addr: addr}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		res.err, res.errStep = err, "parse"
		return res
	}
	target := addr
	if net.ParseIP(host) == nil {
		t := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
		ips, err := net.DefaultResolver.LookupHost(ctx, host)
		cancel()
		res.dns = time.Since(t)
		if err != nil {
			res.err, res.errStep = err, "dns"
			return res
		}
		target = net.JoinHostPort(ips[0], port)
	}

	t := time.Now()
	conn, err := (&net.Dialer{Timeout: cfg.timeout}).Dial("tcp", target)
	res.dial = time.Since(t)
	if err != nil {
		res.err, res.errStep = err, "dial"
		return res
	}
	defer conn.Close()

	if !cfg.auth {
		return res
	}
	if cfg.useTLS {
		tc := tls.Client(conn, &tls.Config{InsecureSkipVerify: true, ServerName: host})
		conn = tc
	}
	t = time.Now()
	err = authPing(conn, cfg)
	res.cmd = time.Since(t)
	if err != nil {
		res.err, res.errStep = err, "auth+ping"
	}
	return res
}

// authPing speaks raw RESP so that this tool does not depend on go-redis at all.
func authPing(conn net.Conn, cfg config) error {
	_ = conn.SetDeadline(time.Now().Add(cfg.timeout))
	rd := bufio.NewReader(conn)
	if cfg.password != "" {
		args := []string{"AUTH", cfg.password}
		if cfg.user != "" {
			args = []string{"AUTH", cfg.user, cfg.password}
		}
		if err := send(conn, rd, args...); err != nil {
			return fmt.Errorf("AUTH: %w", err)
		}
	}
	if err := send(conn, rd, "PING"); err != nil {
		return fmt.Errorf("PING: %w", err)
	}
	return nil
}

func send(conn net.Conn, rd *bufio.Reader, args ...string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := conn.Write([]byte(b.String())); err != nil {
		return err
	}
	line, err := rd.ReadString('\n')
	if err != nil {
		return err
	}
	if strings.HasPrefix(line, "-") {
		return fmt.Errorf("redis: %s", strings.TrimSpace(line[1:]))
	}
	return nil
}

// ---------------------------------------------------------------------------
// burst mode

func burst(cfg config) {
	fmt.Printf("\n=== burst: %d simultaneous connections per node (%d total) ===\n", cfg.burst, cfg.burst*len(cfg.addrs))
	type agg struct {
		dial, cmd diag.Latencies
		errs      diag.Counter
	}
	per := map[string]*agg{}
	for _, a := range cfg.addrs {
		per[a] = &agg{}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var open []net.Conn
	start := make(chan struct{})
	for _, addr := range cfg.addrs {
		for i := 0; i < cfg.burst; i++ {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				<-start
				a := per[addr]
				t := time.Now()
				conn, err := (&net.Dialer{Timeout: cfg.timeout}).Dial("tcp", addr)
				a.dial.Add(time.Since(t))
				if err != nil {
					a.errs.Inc("dial_"+diag.Classify(err), err)
					return
				}
				if cfg.auth {
					var c net.Conn = conn
					if cfg.useTLS {
						c = tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
					}
					t = time.Now()
					err = authPing(c, cfg)
					a.cmd.Add(time.Since(t))
					if err != nil {
						a.errs.Inc("cmd_"+diag.Classify(err), err)
						conn.Close()
						return
					}
				}
				a.errs.Inc("ok", nil)
				mu.Lock()
				open = append(open, conn)
				mu.Unlock()
			}(addr)
		}
	}
	t0 := time.Now()
	close(start)
	wg.Wait()
	fmt.Printf("%s burst finished in %s, open=%d\n", diag.Since(), time.Since(t0).Round(time.Millisecond), len(open))

	for _, addr := range cfg.addrs {
		a := per[addr]
		fmt.Printf("%-24s %s\n", addr, diag.FormatCounts(a.errs.Drain()))
		fmt.Printf("%-24s dial      %s\n", "", diag.Summary(a.dial.Snapshot()))
		if cfg.auth {
			fmt.Printf("%-24s auth+ping %s\n", "", diag.Summary(a.cmd.Snapshot()))
		}
		for k, v := range a.errs.Examples() {
			fmt.Printf("%-24s example %s: %s\n", "", k, v)
		}
	}
	time.Sleep(cfg.hold)
	for _, c := range open {
		c.Close()
	}
	fmt.Println(`
How to read:
  * dial p99/max ~1s or ~3s (SYN retransmit steps) -> listen/SYN backlog overflow on the VM or drops on the path (H5, H7)
  * dial_timeout_dial on many conns               -> same as above, or conntrack table full on the k8s node (H7)
  * cmd_maxclients                                -> Redis maxclients / ulimit -n reached (H6)
  * cmd_timeout_read with fast dial               -> Redis event loop busy with handshakes (AUTH/HELLO) (H5)
  * dial_no_local_port                            -> ephemeral port exhaustion in the pod (H7)
  * clean at the burst size your pool really uses -> burst is not the problem; check H1-H3 and go-redis behaviour`)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
