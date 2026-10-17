#!/usr/bin/env bash
# Run on the Kubernetes NODE where the freshly started pod lives
# (ssh, or `kubectl debug node/<node> -it --image=nicolaka/netshoot` and `chroot /host`).
# Run before and right after the rollout and diff.
#
# Usage: ./node-check.sh <redis-port> [pod-ip]
set -u
PORT=${1:-6379}
POD_IP=${2:-}
hr() { printf '\n==== %s ====\n' "$*"; }

hr "time / node"
date -Is; hostname

hr "conntrack table usage (H7)"
echo "count=$(cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null) max=$(cat /proc/sys/net/netfilter/nf_conntrack_max 2>/dev/null)"

hr "conntrack per-cpu stats: insert_failed / drop / early_drop (H7, SNAT race)"
if command -v conntrack >/dev/null; then
  conntrack -S | grep -oE '(drop|insert_failed|early_drop|clash_resolve)=[0-9]+' | awk -F= '{a[$1]+=$2} END{for(k in a) print k, a[k]}'
  hr "conntrack entries towards redis port $PORT by state"
  conntrack -L -p tcp --dport "$PORT" 2>/dev/null | awk '{print $4}' | sort | uniq -c
else
  echo "conntrack tool not installed (apt install conntrack / use netshoot image)"
fi

hr "SNAT / ephemeral ports (H7)"
sysctl net.ipv4.ip_local_port_range net.ipv4.tcp_tw_reuse 2>/dev/null
ss -tan "dport = :$PORT" | awk 'NR>1{s[$1]++} END{for(k in s) print k, s[k]}'

hr "node TCP counters: retransmits, SYN retries, drops (H2, H5, H7)"
nstat -az TcpRetransSegs TcpExtTCPSynRetrans TcpExtTCPTimeouts TcpAttemptFails \
         TcpExtListenDrops IpExtInNoRoutes 2>/dev/null

hr "dmesg: conntrack table full / drops (H7)"
dmesg -T 2>/dev/null | grep -Ei 'conntrack|table full|dropping packet' | tail -20

hr "CNI hints (H1)"
ls /etc/cni/net.d/ 2>/dev/null
command -v calicoctl >/dev/null && calicoctl node status 2>/dev/null | head -20
command -v cilium >/dev/null && cilium status --brief 2>/dev/null
if [ -n "$POD_IP" ]; then
  hr "routes / neighbour for pod $POD_IP"
  ip route get "$POD_IP" 2>/dev/null
  ip neigh | grep -F "$POD_IP"
fi

cat <<'EOF'

==== packet capture (run during a rollout) ====
# On the node, capture only SYNs/RSTs to redis so the file stays small:
#   tcpdump -ni any -w /tmp/redis-start.pcap "tcp port PORT and (tcp[tcpflags] & (tcp-syn|tcp-rst) != 0)"
# Then look for:
#   * the same 4-tuple SYN repeated after ~1s, ~3s  -> SYN dropped somewhere (H1, H2, H5, H7)
#   * SYN leaves the node but no SYN-ACK comes back -> VM side / path (H2, H5)
#   * SYN never leaves the node                     -> local policy / conntrack (H1, H7)
# For a pod-only view: kubectl debug -it <pod> --image=nicolaka/netshoot --target=<container> -- tcpdump -ni eth0 ...
EOF
