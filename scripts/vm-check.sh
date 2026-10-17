#!/usr/bin/env bash
# Run on EACH Redis VM (as root or with sudo), ideally twice:
#   1) right before you restart/roll out the service
#   2) right after the service has started and errors were seen
# Diff the outputs. Counters that grew between runs are the interesting ones.
#
# Usage: REDIS_PASSWORD=... ./vm-check.sh [port ...]
set -u
PORTS=("${@:-6379}")
AUTH=()
[ -n "${REDIS_PASSWORD:-}" ] && AUTH=(-a "$REDIS_PASSWORD" --no-auth-warning)
[ -n "${REDIS_USERNAME:-}" ] && AUTH=(--user "$REDIS_USERNAME" "${AUTH[@]}")

hr() { printf '\n==== %s ====\n' "$*"; }

hr "time / host"
date -Is; hostname; uptime

hr "kernel listen/backlog sysctls (H5)"
sysctl net.core.somaxconn net.ipv4.tcp_max_syn_backlog net.ipv4.tcp_syncookies \
       net.ipv4.tcp_abort_on_overflow net.core.netdev_max_backlog 2>/dev/null

hr "listen queue overflows / SYN drops since boot (H5) - compare between runs"
if command -v nstat >/dev/null; then
  nstat -az TcpExtListenOverflows TcpExtListenDrops TcpExtTCPReqQFullDrop \
           TcpExtTCPReqQFullDoCookies TcpExtSyncookiesSent TcpExtTCPBacklogDrop 2>/dev/null
else
  netstat -s 2>/dev/null | grep -Ei 'listen|SYNs to LISTEN|overflow|cookies'
fi

for P in "${PORTS[@]}"; do
  hr "port $P: accept queue (Recv-Q = waiting to be accepted, Send-Q = backlog limit)"
  ss -lnt "sport = :$P"
  hr "port $P: connection states"
  ss -tan "sport = :$P" | awk 'NR>1{s[$1]++} END{for(k in s) print k, s[k]}'
  hr "port $P: established conns per client IP (top 15)"
  ss -tn state established "sport = :$P" | awk 'NR>1{split($4,a,":"); print a[1]}' | sort | uniq -c | sort -rn | head -15

  if command -v redis-cli >/dev/null; then
    R=(redis-cli -p "$P" "${AUTH[@]}")
    hr "port $P: redis config (H5, H6)"
    for k in maxclients tcp-backlog timeout tcp-keepalive io-threads; do
      printf '%-14s %s\n' "$k" "$("${R[@]}" CONFIG GET "$k" | tail -1)"
    done
    hr "port $P: INFO clients / stats subset (H6) - compare between runs"
    "${R[@]}" INFO clients | grep -E 'connected_clients|blocked_clients|maxclients'
    "${R[@]}" INFO stats | grep -E 'total_connections_received|rejected_connections|instantaneous_ops_per_sec|total_net_input_bytes'
    "${R[@]}" INFO cpu | grep -E 'used_cpu_sys:|used_cpu_user:'
    hr "port $P: slowlog (last 10)"
    "${R[@]}" SLOWLOG GET 10 | head -60
    hr "port $P: latency doctor"
    "${R[@]}" LATENCY DOCTOR | head -30
    PID=$("${R[@]}" INFO server | awk -F: '/process_id/{print $2}' | tr -d '\r')
    if [ -n "$PID" ] && [ -r "/proc/$PID/limits" ]; then
      hr "port $P: redis process fd limit (pid $PID) (H6)"
      grep -i 'open files' "/proc/$PID/limits"
      echo "open fds: $(ls /proc/$PID/fd 2>/dev/null | wc -l)"
    fi
  fi
done

hr "conntrack on the VM, if loaded (H7)"
cat /proc/sys/net/netfilter/nf_conntrack_count /proc/sys/net/netfilter/nf_conntrack_max 2>/dev/null
command -v conntrack >/dev/null && conntrack -S 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i ~ /^(drop|insert_failed|early_drop|search_restart)=/) printf "%s ", $i; print ""}'

hr "firewall rules touching redis ports (H2)"
(iptables-save 2>/dev/null || nft list ruleset 2>/dev/null) | grep -E "$(IFS='|'; echo "${PORTS[*]}")|conntrack|ct state" | head -30

hr "dmesg: drops / conntrack / syn flood (H5, H7)"
dmesg -T 2>/dev/null | grep -Ei 'conntrack|syn flood|possible SYN|dropping packet|nf_' | tail -20
