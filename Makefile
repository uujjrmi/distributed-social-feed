.PHONY: up down logs seed smoke smoke-ui demo reset-demo load-smoke fail-redis fail-feed fail-notifications recover fmt

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=150

seed:
	python3 scripts/seed.py

smoke:
	curl -fsS http://localhost:8001/healthz
	curl -fsS http://localhost:8002/healthz
	curl -fsS http://localhost:8003/healthz
	curl -fsS http://localhost:8004/healthz
	curl -fsS http://localhost:8005/healthz

smoke-ui:
	curl -fsS http://localhost:8080/api/healthz
	curl -fsS http://localhost:8080/api/overview

demo:
	./scripts/demo.sh

reset-demo:
	./scripts/reset_demo.sh

load-smoke:
	k6 run load/k6-mixed.js

fail-redis:
	./scripts/inject_failure.sh redis-outage

fail-feed:
	./scripts/inject_failure.sh feed-crash

fail-notifications:
	./scripts/inject_failure.sh notification-crash

recover:
	./scripts/recover_manual.sh

fmt:
	gofmt -w services/post-service/cmd/server/main.go services/feed-service/cmd/server/main.go
