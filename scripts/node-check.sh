#!/usr/bin/env bash
# Kubernetes NODE side checks. Everything goes to stdout, so it works without
# `kubectl exec`/`cp`/`debug`: run it as a Job pinned to the node
# (deploy/k8s/node-diag-job.yaml) and read `kubectl logs`, or over ssh.
#
# Needs the host network namespace (hostNetwork: true), and for the capture
# part NET_RAW (tcpdump) and NET_ADMIN (conntrack -S). Tools: tcpdump,
# conntrack, nstat, ss (all present in the nicolaka/netshoot image).
#
# Usage: node-check.sh <redis-port>
#   CAPTURE_SECONDS=120  also capture SYN/SYN-ACK/RST to the redis port and
#                        print a per-second summary (no pcap file needed)
#   REPEAT_EVERY=10      print counters again every N seconds during capture
set -u
PORT=${1:-6379}
CAPTURE_SECONDS=${CAPTURE_SECONDS:-0}
REPEAT_EVERY=${REPEAT_EVERY:-0}
hr() { printf '\n==== %s ====\n' "$*"; }

counters() {
  hr "$(date +%H:%M:%S) conntrack usage (H7)"
  echo "count=$(cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null) max=$(cat /proc/sys/net/netfilter/nf_conntrack_max 2>/dev/null)"
  if command -v conntrack >/dev/null; then
    echo -n "conntrack -S sums: "
    conntrack -S 2>/dev/null | grep -oE '(drop|insert_failed|early_drop|clash_resolve)=[0-9]+' |
      awk -F= '{a[$1]+=$2} END{for(k in a) printf "%s=%d ", k, a[k]; print ""}'
  fi
  hr "$(date +%H:%M:%S) node TCP counters (H2, H5, H7)"
  nstat -az TcpRetransSegs TcpExtTCPSynRetrans TcpExtTCPTimeouts TcpAttemptFails \
           TcpActiveOpens TcpExtListenDrops 2>/dev/null | grep -v '^#'
  hr "$(date +%H:%M:%S) sockets to redis port $PORT by state"
  ss -tan "dport = :$PORT" 2>/dev/null | awk 'NR>1{s[$1]++} END{for(k in s) print k, s[k]}'
}

hr "time / node"
date -Is; hostname; uname -r

hr "sysctls"
sysctl net.netfilter.nf_conntrack_max net.ipv4.ip_local_port_range net.ipv4.tcp_tw_reuse \
       net.ipv4.tcp_syn_retries 2>/dev/null

counters

hr "dmesg: conntrack table full / drops (H7)  (needs CAP_SYSLOG or kernel.dmesg_restrict=0)"
dmesg -T 2>/dev/null | grep -Ei 'conntrack|table full|dropping packet' | tail -20 || true

hr "CNI config present on this node (H1)"
ls /etc/cni/net.d/ /host/etc/cni/net.d/ 2>/dev/null

if [ "$CAPTURE_SECONDS" -gt 0 ]; then
  hr "capture: SYN / SYN-ACK / RST to and from port $PORT for ${CAPTURE_SECONDS}s"
  echo "columns: second syn syn_retrans synack rst  (syn_retrans = same 4-tuple SYN seen again)"
  if [ "$REPEAT_EVERY" -gt 0 ]; then
    ( end=$((SECONDS + CAPTURE_SECONDS)); while [ $SECONDS -lt $end ]; do sleep "$REPEAT_EVERY"; counters; done ) &
  fi
  timeout "$CAPTURE_SECONDS" tcpdump -n -l -tt -i any \
    "tcp port $PORT and (tcp[tcpflags] & (tcp-syn|tcp-rst) != 0)" 2>/dev/null |
  awk -v port="$PORT" '
    {
      sec = int($1)
      fl = ""; src = ""; dst = ""
      for (i = 2; i <= NF; i++) {
        if ($i == ">") { src = $(i-1); dst = $(i+1); sub(/:$/, "", dst) }
        if ($i == "Flags") { fl = $(i+1) }
      }
      if (fl == "") next
      if (sec != cur && cur != "") flush()
      cur = sec
      if (fl ~ /^\[S\],?$/) {
        key = src ">" dst
        if (key in seen) { rt++; trt++; rtdst[dst]++ } else { seen[key] = 1; syn++; tsyn++; syndst[dst]++ }
      } else if (fl ~ /^\[S\.\]/) { sa++; tsa++ }
      else if (fl ~ /^\[R/) { rst++; trst++; rstsrc[src]++ }
    }
    function flush() {
      t = cur % 86400
      printf "%02d:%02d:%02dZ syn=%d syn_retrans=%d synack=%d rst=%d\n", int(t/3600), int(t%3600/60), t%60, syn, rt, sa, rst
      fflush()
      syn = rt = sa = rst = 0
    }
    END {
      if (cur != "") flush()
      printf "\nTOTAL syn=%d syn_retrans=%d synack=%d rst=%d\n", tsyn, trt, tsa, trst
      print "new SYNs per destination:"; for (d in syndst) printf "  %-24s %d (retrans %d)\n", d, syndst[d], rtdst[d]
      if (trst > 0) { print "RST sources:"; for (s in rstsrc) printf "  %-24s %d\n", s, rstsrc[s] }
    }'
  wait
  counters
fi

cat <<'EOF'

How to read the capture:
  * syn_retrans > 0, synack < syn      -> SYNs are lost: VM backlog (H5), conntrack/SNAT (H7), policy (H1) or path (H2)
  * a burst of hundreds of syn/second  -> connection storm (H4 at startup, F1 after failover)
  * rst from the redis side            -> nothing listening / maxclients / firewall reject
  * conntrack insert_failed growing    -> SNAT port race (H7)
EOF
