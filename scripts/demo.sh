#!/usr/bin/env bash
set -euo pipefail

DEMO_URL="${DEMO_URL:-http://localhost:8080}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-70}"
BUILD_LOG="${BUILD_LOG:-/tmp/distributed-social-feed-demo-build.log}"

log() {
  printf "\n==> %s\n" "$1"
}

json_value() {
  python3 -c "import json,sys; data=json.load(sys.stdin); print($1)"
}

wait_for_demo_ui() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  until curl -fsS "${DEMO_URL}/api/healthz" >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for ${DEMO_URL}/api/healthz" >&2
      exit 1
    fi
    sleep 2
  done
}

wait_for_redis_incident() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS "${DEMO_URL}/api/incidents" | python3 -c '
import json, sys
for incident in json.load(sys.stdin):
    if incident.get("type") == "redis_outage":
        print("{} {} {}".format(incident["id"], incident["status"], incident["detected_at"]))
        raise SystemExit(0)
raise SystemExit(1)
'; then
      return 0
    fi
    sleep 4
  done
  echo "no redis_outage incident detected within ${TIMEOUT_SECONDS}s" >&2
  return 1
}

log "Starting Docker stack"
if ! docker compose up --build -d >"${BUILD_LOG}" 2>&1; then
  cat "${BUILD_LOG}" >&2
  exit 1
fi
printf "stack started; build log: %s\n" "$BUILD_LOG"
wait_for_demo_ui

log "Resetting demo controls and incident history"
curl -fsS -X POST "${DEMO_URL}/api/actions/reset-demo" | json_value "data['status']"

log "Checking seeded data"
overview="$(curl -fsS "${DEMO_URL}/api/overview")"
users="$(printf "%s" "$overview" | json_value "data['stats']['users']")"
if [ "$users" -eq 0 ]; then
  log "Seeding sample social graph"
  curl -fsS -X POST "${DEMO_URL}/api/actions/seed" | python3 -m json.tool
else
  printf "existing users: %s\n" "$users"
fi

log "Creating a live post and warm feed traffic"
user_id="$(curl -fsS "${DEMO_URL}/api/users?limit=1" | json_value "data[0]['id']")"
curl -fsS -X POST "${DEMO_URL}/api/actions/post" \
  -H "content-type: application/json" \
  -d "{\"author_id\":\"${user_id}\",\"content\":\"Reliability demo post: the feed stays useful while infra fails.\"}" \
  | python3 -m json.tool
curl -fsS -X POST "${DEMO_URL}/api/actions/traffic" | python3 -m json.tool

log "Injecting Redis outage through the UI API"
failure_started_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
curl -fsS -X POST "${DEMO_URL}/api/actions/redis-outage" | python3 -m json.tool

log "Waiting for healing agent to detect and remediate"
wait_for_redis_incident

log "Recent incidents"
curl -fsS "${DEMO_URL}/api/incidents?limit=5" | python3 -m json.tool

log "Recovering the stack"
curl -fsS -X POST "${DEMO_URL}/api/actions/recover" | python3 -m json.tool

log "Final overview"
curl -fsS "${DEMO_URL}/api/overview" | python3 -c '
import json, sys
data = json.load(sys.stdin)
summary = data["summary"]
stats = data["stats"]
print("services: {}/{} ready".format(summary["healthy_services"], summary["total_services"]))
print("availability score: {}%".format(summary["availability_score"]))
print("posts: {}  notifications: {}  incidents: {}".format(stats["posts"], stats["notifications"], stats["incidents"]))
'

printf "\nDemo URL: %s\n" "$DEMO_URL"
printf "Failure started at: %s\n" "$failure_started_at"
