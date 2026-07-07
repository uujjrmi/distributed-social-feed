#!/usr/bin/env bash
set -euo pipefail

DEMO_URL="${DEMO_URL:-http://localhost:8080}"

curl -fsS -X POST "${DEMO_URL}/api/actions/reset-demo"
echo
