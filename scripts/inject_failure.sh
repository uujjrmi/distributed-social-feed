#!/usr/bin/env bash
set -euo pipefail

failure="${1:-}"
admin_token="${ADMIN_TOKEN:-dev-admin-token}"

case "$failure" in
  redis-outage)
    docker compose stop redis
    ;;
  feed-crash)
    docker compose stop feed-service
    ;;
  notification-crash)
    docker compose stop notification-service
    ;;
  consumer-lag)
    curl -fsS -X POST http://localhost:8003/admin/consumer-pause \
      -H "content-type: application/json" \
      -H "x-admin-token: ${admin_token}" \
      -d '{"paused": true}'
    ;;
  db-latency)
    echo "db-latency is reserved for the Toxiproxy phase; use redis-outage or consumer-lag for the MVP demo." >&2
    exit 2
    ;;
  *)
    echo "usage: $0 {redis-outage|feed-crash|notification-crash|consumer-lag|db-latency}" >&2
    exit 2
    ;;
esac

