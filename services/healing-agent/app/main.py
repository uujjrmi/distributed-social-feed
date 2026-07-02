import asyncio
import os
import time
from typing import Any

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from .actions import ActionExecutor
from .detectors import detect_incidents
from .incident_store import IncidentStore
from .metrics_client import PrometheusClient
from .policy import PolicyEngine


SERVICE = "healing-agent"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/social")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
POLL_INTERVAL = int(os.getenv("HEALING_POLL_INTERVAL_SECONDS", "10"))
CONFIDENCE_THRESHOLD = float(os.getenv("HEALING_CONFIDENCE_THRESHOLD", "0.75"))
COOLDOWN_SECONDS = int(os.getenv("HEALING_COOLDOWN_SECONDS", "120"))

app = FastAPI(title="Healing Agent", version="0.1.0")

HTTP_REQUESTS = Counter("http_requests_total", "Total HTTP requests", ["service", "method", "path", "status"])
HTTP_ERRORS = Counter("http_errors_total", "Total HTTP errors", ["service", "method", "path", "status"])
HTTP_LATENCY = Histogram("http_request_duration_seconds", "HTTP request duration", ["service", "method", "path"])
INCIDENTS = Counter("healing_incidents_total", "Healing incidents", ["service", "type", "severity", "status"])
ACTIONS = Counter("healing_actions_total", "Healing actions", ["action_type", "target", "status"])
ACTION_LATENCY = Histogram("healing_action_duration_seconds", "Healing action duration", ["action_type", "target"])


def route_path(request: Request) -> str:
    route = request.scope.get("route")
    return getattr(route, "path", request.url.path)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    status = "500"
    path = route_path(request)
    try:
        response = await call_next(request)
        status = str(response.status_code)
        return response
    finally:
        HTTP_REQUESTS.labels(SERVICE, request.method, path, status).inc()
        HTTP_LATENCY.labels(SERVICE, request.method, path).observe(time.perf_counter() - start)
        if status.startswith("4") or status.startswith("5"):
            HTTP_ERRORS.labels(SERVICE, request.method, path, status).inc()


@app.on_event("startup")
async def startup() -> None:
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    app.state.http = httpx.AsyncClient()
    app.state.prometheus = PrometheusClient(PROMETHEUS_URL, app.state.http)
    app.state.store = IncidentStore(app.state.pool)
    app.state.policy = PolicyEngine(CONFIDENCE_THRESHOLD, COOLDOWN_SECONDS)
    app.state.executor = ActionExecutor(app.state.http)
    app.state.loop_task = asyncio.create_task(healing_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    app.state.loop_task.cancel()
    await app.state.http.aclose()
    await app.state.pool.close()


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE}


@app.get("/readyz")
async def readyz() -> dict[str, str]:
    async with app.state.pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    await app.state.prometheus.query("up")
    return {"status": "ready", "service": SERVICE}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/incidents")
async def list_incidents(limit: int = 100) -> list[dict[str, Any]]:
    return await app.state.store.list_incidents(min(limit, 500))


@app.get("/incidents/{incident_id}")
async def get_incident(incident_id: str) -> dict[str, Any]:
    incident = await app.state.store.get_incident(incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="incident not found")
    return incident


async def healing_loop() -> None:
    while True:
        try:
            snapshot = await collect_snapshot()
            for candidate in detect_incidents(snapshot):
                await handle_candidate(candidate)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(POLL_INTERVAL)
        await asyncio.sleep(POLL_INTERVAL)


async def collect_snapshot() -> dict[str, Any]:
    prom = app.state.prometheus
    return {
        "up": await prom.service_values("up", label="job"),
        "redis_errors": await prom.service_values("sum(rate(redis_errors_total[1m])) by (service)"),
        "degraded_requests": await prom.service_values("sum(rate(degraded_mode_requests_total[1m])) by (service)"),
        "http_errors": await prom.service_values("sum(rate(http_errors_total[1m])) by (service)"),
        "db_p95": await prom.service_values(
            "histogram_quantile(0.95, sum(rate(db_query_duration_seconds_bucket[1m])) by (service, le))"
        ),
        "consumer_lag": await prom.service_values("sum(kafka_consumer_lag) by (service)"),
    }


async def handle_candidate(candidate) -> None:
    decision = app.state.policy.decide(candidate)
    if not decision.allowed or decision.action_type is None:
        return

    incident_id = await app.state.store.create_incident(
        candidate.service,
        candidate.incident_type,
        candidate.severity,
        candidate.confidence,
        candidate.signals,
    )
    INCIDENTS.labels(candidate.service, candidate.incident_type, candidate.severity, "open").inc()

    result = await app.state.executor.execute(decision.action_type, candidate.target)
    await app.state.store.record_action(
        incident_id,
        decision.action_type,
        candidate.target,
        result.status,
        result.details,
    )
    ACTIONS.labels(decision.action_type, candidate.target, result.status).inc()
    ACTION_LATENCY.labels(decision.action_type, candidate.target).observe(result.duration_seconds)

    final_status = "resolved" if result.status == "success" else "action_failed"
    await app.state.store.complete_incident(incident_id, final_status)
    INCIDENTS.labels(candidate.service, candidate.incident_type, candidate.severity, final_status).inc()

