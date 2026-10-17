#!/usr/bin/env bash
# Local 6-node Redis Cluster (3 masters + 3 replicas) on 127.0.0.1:7000-7005
# to play with the tools without touching the real cluster.
#
# Make Redis deliberately "weak" to reproduce the startup storm:
#   TCP_BACKLOG=64 MAXCLIENTS=1500 ./scripts/local-cluster.sh up
#   go run ./cmd/startup -addrs 127.0.0.1:7000 -password secret -workers 3000
#
# Usage: ./scripts/local-cluster.sh up|down
set -eu
DIR=${DIR:-/tmp/redis-startup-diag-cluster}
PASS=${PASS:-secret}
TCP_BACKLOG=${TCP_BACKLOG:-511}
MAXCLIENTS=${MAXCLIENTS:-10000}
PORTS="7000 7001 7002 7003 7004 7005"

case "${1:-up}" in
up)
  for p in $PORTS; do
    mkdir -p "$DIR/$p"
    redis-server --port "$p" --cluster-enabled yes --cluster-config-file "$DIR/$p/nodes.conf" \
      --dir "$DIR/$p" --daemonize yes --logfile "$DIR/$p/log" --save '' --appendonly no \
      --requirepass "$PASS" --masterauth "$PASS" \
      --tcp-backlog "$TCP_BACKLOG" --maxclients "$MAXCLIENTS"
  done
  sleep 1
  echo yes | redis-cli -a "$PASS" --no-auth-warning --cluster create \
    $(for p in $PORTS; do printf '127.0.0.1:%s ' "$p"; done) --cluster-replicas 1 | tail -2
  echo "cluster up: REDIS_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 REDIS_PASSWORD=$PASS"
  echo "note: effective tcp-backlog is min(tcp-backlog, net.core.somaxconn=$(cat /proc/sys/net/core/somaxconn))"
  ;;
down)
  for p in $PORTS; do redis-cli -p "$p" -a "$PASS" --no-auth-warning shutdown nosave 2>/dev/null || true; done
  rm -rf "$DIR"
  ;;
esac
