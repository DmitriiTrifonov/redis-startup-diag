// envreport prints the pod-side facts that matter for Redis connection storms
// and estimates how many connections your fleet can open in the worst case.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
)

func main() {
	poolSize := flag.Int("pool-size", 10000, "PoolSize per node in your service")
	nodes := flag.Int("nodes", 6, "number of Redis nodes the client talks to (masters, + replicas if ReadOnly/RouteByLatency)")
	pods := flag.Int("pods", 3, "replicas of the service (all services sharing the cluster, summed)")
	maxclients := flag.Int("maxclients", 10000, "Redis maxclients per node (CONFIG GET maxclients)")
	flag.Parse()

	fmt.Println("== Go runtime ==")
	fmt.Printf("go=%s GOMAXPROCS=%d NumCPU=%d GOMAXPROCS(env)=%q\n",
		runtime.Version(), runtime.GOMAXPROCS(0), runtime.NumCPU(), os.Getenv("GOMAXPROCS"))
	if runtime.GOMAXPROCS(0) == runtime.NumCPU() {
		fmt.Println("  note: GOMAXPROCS == NumCPU. If the pod has a CPU limit lower than the node's cores,")
		fmt.Println("        go-redis default PoolSize (k*GOMAXPROCS) is sized for the whole node. (H4)")
	}

	fmt.Println("\n== cgroup limits ==")
	show("/sys/fs/cgroup/cpu.max")
	show("/sys/fs/cgroup/memory.max")
	show("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	show("/sys/fs/cgroup/cpu/cpu.cfs_period_us")

	fmt.Println("\n== file descriptors ==")
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err == nil {
		fmt.Printf("RLIMIT_NOFILE soft=%d hard=%d\n", rl.Cur, rl.Max)
		need := uint64(*poolSize) * uint64(*nodes)
		if need > rl.Cur {
			fmt.Printf("  WARNING: pool can grow to %d sockets per pod > soft limit %d (\"too many open files\")\n", need, rl.Cur)
		}
	}

	fmt.Println("\n== pod network namespace sysctls ==")
	for _, p := range []string{
		"/proc/sys/net/core/somaxconn",
		"/proc/sys/net/ipv4/ip_local_port_range",
		"/proc/sys/net/ipv4/tcp_syn_retries",
		"/proc/sys/net/ipv4/tcp_fin_timeout",
		"/proc/sys/net/ipv4/tcp_tw_reuse",
		"/proc/sys/net/netfilter/nf_conntrack_count",
		"/proc/sys/net/netfilter/nf_conntrack_max",
	} {
		show(p)
	}

	fmt.Println("\n== DNS ==")
	show("/etc/resolv.conf")

	fmt.Println("\n== worst-case connection math ==")
	perPod := *poolSize * *nodes
	perNode := *poolSize * *pods
	fmt.Printf("per pod:  pool_size(%d) x nodes(%d) = %d sockets\n", *poolSize, *nodes, perPod)
	fmt.Printf("per Redis node: pool_size(%d) x pods(%d) = %d clients vs maxclients=%d\n", *poolSize, *pods, perNode, *maxclients)
	if perNode > *maxclients {
		fmt.Println("  WARNING: fleet can exceed maxclients -> 'ERR max number of clients reached' (H6)")
	}
	fmt.Printf("ephemeral ports: one pod->one node needs <=%d local ports; range above must be wider\n", *poolSize)
	fmt.Println("conntrack on a k8s node: sum of all pods on that node x nodes; compare with nf_conntrack_max on the NODE (H7)")
}

func show(path string) {
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	v := strings.TrimSpace(string(b))
	if strings.Contains(v, "\n") {
		fmt.Printf("%s:\n", path)
		for _, l := range strings.Split(v, "\n") {
			fmt.Printf("    %s\n", l)
		}
		return
	}
	fmt.Printf("%s = %s\n", path, v)
}
