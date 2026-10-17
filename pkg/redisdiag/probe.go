package redisdiag

import (
	"context"
	"errors"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/DmitriiTrifonov/redis-startup-diag/internal/diag"
)

// ErrProbeRunning is returned when another probe is in progress.
var ErrProbeRunning = errors.New("redisdiag: probe already running")

// ProbeResult answers "is it the network, the pool or Redis?" from inside the
// service, for every node of the current topology.
type ProbeResult struct {
	Time        time.Time    `json:"time"`
	Took        string       `json:"took"`
	TopologyOK  bool         `json:"topology_ok"`
	TopologyErr string       `json:"topology_error,omitempty"`
	TopologyRTT string       `json:"topology_rtt"`
	Nodes       []NodeResult `json:"nodes"`
}

type NodeResult struct {
	Addr string `json:"addr"`
	Role string `json:"role"` // from CLUSTER SLOTS: master / replica / seed

	// Fresh TCP connection, bypassing the pool: tells network/backlog problems
	// apart from pool problems.
	TCPDial    string `json:"tcp_dial"`
	TCPDialErr string `json:"tcp_dial_error,omitempty"`

	// PING through the client's pool, as the service would do it.
	PoolPing    string `json:"pool_ping"`
	PoolPingErr string `json:"pool_ping_error,omitempty"`

	// A few server-side counters (INFO clients/stats/replication). Empty if ACL forbids INFO.
	Server  map[string]string `json:"server,omitempty"`
	InfoErr string            `json:"info_error,omitempty"`
}

var infoFields = []string{
	"role", "connected_clients", "maxclients", "blocked_clients",
	"total_connections_received", "rejected_connections", "instantaneous_ops_per_sec",
	"sync_full", "sync_partial_err", "latest_fork_usec", "master_link_status", "connected_slaves",
}

// Probe checks every node once. Only one probe runs at a time.
// timeout applies to each individual operation.
func (in *Instrument) Probe(ctx context.Context, timeout time.Duration) (ProbeResult, error) {
	if !in.probing.CompareAndSwap(false, true) {
		return ProbeResult{}, ErrProbeRunning
	}
	defer in.probing.Store(false)
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	start := time.Now()
	res := ProbeResult{Time: start}

	// 1. Topology as the cluster reports it now.
	roles := map[string]string{}
	tctx, cancel := context.WithTimeout(ctx, timeout)
	t := time.Now()
	slots, err := in.rdb.ClusterSlots(tctx).Result()
	cancel()
	res.TopologyRTT = time.Since(t).Round(time.Microsecond).String()
	if err != nil {
		res.TopologyErr = err.Error()
	} else {
		res.TopologyOK = true
		for _, s := range slots {
			for i, n := range s.Nodes {
				if i == 0 {
					roles[n.Addr] = "master"
				} else if _, ok := roles[n.Addr]; !ok {
					roles[n.Addr] = "replica"
				}
			}
		}
	}
	for _, a := range in.rdb.Options().Addrs {
		if _, ok := roles[a]; !ok {
			roles[a] = "seed"
		}
	}

	// 2. Node clients the cluster client already has, by address.
	clients := map[string]*redis.Client{}
	for _, n := range in.nodeList() {
		clients[n.addr] = n.client
	}

	// 3. Probe every address concurrently (one goroutine per node, not a burst).
	addrs := make([]string, 0, len(roles))
	for a := range roles {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	out := make([]NodeResult, len(addrs))
	var wg sync.WaitGroup
	for i, addr := range addrs {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			out[i] = in.probeNode(ctx, addr, roles[addr], clients[addr], timeout)
		}(i, addr)
	}
	wg.Wait()
	res.Nodes = out
	res.Took = time.Since(start).Round(time.Millisecond).String()
	return res, nil
}

func (in *Instrument) probeNode(ctx context.Context, addr, role string, c *redis.Client, timeout time.Duration) NodeResult {
	r := NodeResult{Addr: addr, Role: role}

	t := time.Now()
	conn, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, "tcp", addr)
	r.TCPDial = time.Since(t).Round(time.Microsecond).String()
	if err != nil {
		r.TCPDialErr = diag.Classify(err) + ": " + err.Error()
	} else {
		conn.Close()
	}

	if c == nil {
		r.PoolPingErr = "no node client yet (client never routed to this node)"
		return r
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	t = time.Now()
	err = c.Ping(pctx).Err()
	r.PoolPing = time.Since(t).Round(time.Microsecond).String()
	cancel()
	if err != nil {
		r.PoolPingErr = diag.Classify(err) + ": " + err.Error()
		return r
	}

	ictx, cancel := context.WithTimeout(ctx, timeout)
	info, err := c.Info(ictx, "clients", "stats", "replication").Result()
	cancel()
	if err != nil {
		r.InfoErr = err.Error()
		return r
	}
	r.Server = map[string]string{}
	for _, line := range strings.Split(info, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}
		for _, f := range infoFields {
			if k == f {
				r.Server[k] = v
			}
		}
	}
	return r
}
