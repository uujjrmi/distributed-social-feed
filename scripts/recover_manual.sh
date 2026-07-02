#!/usr/bin/env bash
set -euo pipefail

admin_token="${ADMIN_TOKEN:-dev-admin-token}"

docker compose up -d redis feed-service notification-service

sleep 3

curl -fsS -X POST http://localhost:8003/admin/degraded-mode \
  -H "content-type: application/json" \
  -H "x-admin-token: ${admin_token}" \
  -d '{"enabled": false}' || true

curl -fsS -X POST http://localhost:8003/admin/consumer-pause \
  -H "content-type: application/json" \
  -H "x-admin-token: ${admin_token}" \
  -d '{"paused": false}' || true

echo
echo "manual recovery requested"

