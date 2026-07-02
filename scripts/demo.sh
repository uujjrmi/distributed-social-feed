#!/usr/bin/env bash
set -euo pipefail

echo "starting stack"
docker compose up --build -d

echo "seeding data"
python3 scripts/seed.py

echo "running smoke load"
k6 run load/k6-mixed.js

echo "injecting Redis outage"
./scripts/inject_failure.sh redis-outage

echo "run load again so feed-service emits Redis error metrics"
k6 run load/k6-feed-read.js

echo "recent incidents"
curl -fsS http://localhost:8005/incidents
echo

