#!/usr/bin/env bash
# Controlled reproduction of "i/o timeouts on the new master after CLUSTER FAILOVER".
#
# Promotes the given REPLICA with CLUSTER FAILOVER and then watches it every
# second: connections, new connections per second, rejected connections,
# fork / full sync counters. Run it from any host that can reach the nodes
# with redis-cli. Keep `startup` or the real service running against the
# cluster at the same time, and run scripts/vm-check.sh on the replica's VM
# before and after.
#
# For a faithful repro the replica must be "cold": no master switches for
# longer than the clients' ConnMaxIdleTime (go-redis default 30 min).
#
# Usage: REDIS_PASSWORD=... ./failover-test.sh <replica-host:port> [watch-seconds]
#   FORCE=1 skips the confirmation prompt.
set -eu
TARGET=${1:?usage: $0 <replica-host:port> [watch-seconds]}
WATCH=${2:-120}
HOST=${TARGET%:*}
PORT=${TARGET##*:}
AUTH=()
[ -n "${REDIS_PASSWORD:-}" ] && AUTH=(-a "$REDIS_PASSWORD" --no-auth-warning)
[ -n "${REDIS_USERNAME:-}" ] && AUTH=(--user "$REDIS_USERNAME" "${AUTH[@]}")
rc() { redis-cli -h "$1" -p "$2" "${AUTH[@]}" "${@:3}"; }
field() { tr -d '\r' | awk -F: -v k="$1" '$1==k{print $2}'; }
ts() { date +%H:%M:%S; }

ROLE=$(rc "$HOST" "$PORT" ROLE | head -1)
if [ "$ROLE" != "slave" ]; then
  echo "$TARGET is '$ROLE', not a replica. Pick a replica." >&2
  exit 1
fi
MHOST=$(rc "$HOST" "$PORT" INFO replication | field master_host)
MPORT=$(rc "$HOST" "$PORT" INFO replication | field master_port)

snapshot() {
  local h=$1 p=$2
  echo "--- $h:$p ($(rc "$h" "$p" ROLE | head -1)) ---"
  rc "$h" "$p" INFO clients | tr -d '\r' | grep -E '^(connected_clients|maxclients):'
  rc "$h" "$p" INFO stats | tr -d '\r' | grep -E '^(total_connections_received|rejected_connections|sync_full|sync_partial_ok|sync_partial_err|latest_fork_usec|instantaneous_ops_per_sec):'
  rc "$h" "$p" INFO replication | tr -d '\r' | grep -E '^(role|master_repl_offset|repl_backlog_size|repl_backlog_histlen|master_replid):'
  rc "$h" "$p" CONFIG GET tcp-backlog | tail -1 | sed 's/^/tcp-backlog:/'
}

echo "== $(ts) before failover =="
echo "replica (will become master): $TARGET"
echo "current master:               $MHOST:$MPORT"
snapshot "$HOST" "$PORT"
snapshot "$MHOST" "$MPORT"

if [ "${FORCE:-0}" != "1" ]; then
  printf '\nRun CLUSTER FAILOVER on %s? This moves its slots to it. Type yes: ' "$TARGET"
  read -r ans
  [ "$ans" = "yes" ] || { echo "aborted"; exit 1; }
fi

echo "== $(ts) CLUSTER FAILOVER on $TARGET =="
rc "$HOST" "$PORT" CLUSTER FAILOVER
for i in $(seq 1 30); do
  [ "$(rc "$HOST" "$PORT" ROLE | head -1)" = "master" ] && break
  sleep 1
done
echo "$(ts) role now: $(rc "$HOST" "$PORT" ROLE | head -1)"

echo
echo "== $(ts) watching new master $TARGET for ${WATCH}s =="
printf '%-8s %8s %10s %9s %9s %8s %8s %10s\n' time clients new_conn/s rejected ops/s sync_full fork_us partial_err
prev=$(rc "$HOST" "$PORT" INFO stats | field total_connections_received)
for i in $(seq 1 "$WATCH"); do
  sleep 1
  S=$(rc "$HOST" "$PORT" INFO stats 2>&1 || true)
  C=$(rc "$HOST" "$PORT" INFO clients 2>&1 || true)
  cur=$(echo "$S" | field total_connections_received)
  if [ -z "$cur" ]; then
    printf '%-8s INFO failed: %s\n' "$(ts)" "$(echo "$S" | head -1)"
    continue
  fi
  printf '%-8s %8s %10s %9s %9s %8s %8s %10s\n' "$(ts)" \
    "$(echo "$C" | field connected_clients)" "$((cur - prev))" \
    "$(echo "$S" | field rejected_connections)" "$(echo "$S" | field instantaneous_ops_per_sec)" \
    "$(echo "$S" | field sync_full)" "$(echo "$S" | field latest_fork_usec)" \
    "$(echo "$S" | field sync_partial_err)"
  prev=$cur
done

echo
echo "== $(ts) after =="
snapshot "$HOST" "$PORT"
snapshot "$MHOST" "$MPORT"
cat <<'EOF'

How to read:
  * new_conn/s spikes to hundreds/thousands right after failover   -> F1 (connection storm)
  * new_conn/s stays high for a long time while ops/s is low        -> F2 (reconnect loop)
  * sync_full grew / latest_fork_usec large on the new master       -> F4 (full resync + fork)
  * rejected grew                                                   -> H6 (maxclients)
  * INFO itself times out from here                                 -> Redis event loop is stalled (fork, handshakes)
Also diff scripts/vm-check.sh output on the new master's VM (ListenOverflows/ListenDrops/SyncookiesSent).
EOF
