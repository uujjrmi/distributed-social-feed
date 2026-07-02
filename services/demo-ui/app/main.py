import asyncio
import os
import random
import time
from pathlib import Path
from typing import Any

import asyncpg
import docker
import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = ROOT / "static"

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/social")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "dev-admin-token")
USER_SERVICE_URL = os.getenv("USER_SERVICE_URL", "http://user-service:8001")
POST_SERVICE_URL = os.getenv("POST_SERVICE_URL", "http://post-service:8002")
FEED_SERVICE_URL = os.getenv("FEED_SERVICE_URL", "http://feed-service:8003")
NOTIFICATION_SERVICE_URL = os.getenv("NOTIFICATION_SERVICE_URL", "http://notification-service:8004")
HEALING_AGENT_URL = os.getenv("HEALING_AGENT_URL", "http://healing-agent:8005")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")

SERVICE_PROBES = [
    {"id": "user", "name": "User Service", "url": USER_SERVICE_URL, "kind": "Python API"},
    {"id": "post", "name": "Post Service", "url": POST_SERVICE_URL, "kind": "Go API"},
    {"id": "feed", "name": "Feed Service", "url": FEED_SERVICE_URL, "kind": "Go consumer"},
    {"id": "notification", "name": "Notification Service", "url": NOTIFICATION_SERVICE_URL, "kind": "Python consumer"},
    {"id": "healing", "name": "Healing Agent", "url": HEALING_AGENT_URL, "kind": "Python controller"},
]

app = FastAPI(title="Distributed Social Feed Demo UI", version="0.1.0")


class DemoPostRequest(BaseModel):
    author_id: str | None = None
    content: str = Field(default="Demo post from the ops cockpit", min_length=1, max_length=500)


class DegradedModeRequest(BaseModel):
    enabled: bool


@app.on_event("startup")
async def startup() -> None:
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(8.0, connect=2.0))
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)


@app.on_event("shutdown")
async def shutdown() -> None:
    await app.state.http.aclose()
    await app.state.pool.close()


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")


@app.get("/api/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": "demo-ui"}


@app.get("/api/overview")
async def overview() -> dict[str, Any]:
    services, stats_payload, incident_payload, user_payload, prom_up = await asyncio.gather(
        service_status(),
        get_stats(),
        list_incidents(limit=8),
        list_users(limit=24),
        prometheus_up(),
    )
    healthy = sum(1 for item in services if item["status"] == "ready")
    return {
        "services": services,
        "stats": stats_payload,
        "incidents": incident_payload,
        "users": user_payload,
        "prometheus": prom_up,
        "summary": {
            "healthy_services": healthy,
            "total_services": len(services),
            "availability_score": round((healthy / len(services)) * 100, 1) if services else 0,
        },
    }


@app.get("/api/status")
async def service_status() -> list[dict[str, Any]]:
    return await asyncio.gather(*(probe_service(service) for service in SERVICE_PROBES))


@app.get("/api/stats")
async def get_stats() -> dict[str, int]:
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              (SELECT count(*) FROM users) AS users,
              (SELECT count(*) FROM follows) AS follows,
              (SELECT count(*) FROM posts) AS posts,
              (SELECT count(*) FROM notifications) AS notifications,
              (SELECT count(*) FROM healing_incidents) AS incidents,
              (SELECT count(*) FROM healing_actions) AS actions
            """
        )
    return {key: int(row[key]) for key in row.keys()}


@app.get("/api/users")
async def list_users(limit: int = Query(default=40, ge=1, le=100)) -> list[dict[str, Any]]:
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, username, display_name, created_at
            FROM users
            ORDER BY created_at ASC
            LIMIT $1
            """,
            limit,
        )
    return [serialize_record(row) for row in rows]


@app.get("/api/feed/{user_id}")
async def feed(user_id: str, limit: int = Query(default=12, ge=1, le=100)) -> dict[str, Any]:
    response = await app.state.http.get(f"{FEED_SERVICE_URL}/feeds/{user_id}", params={"limit": limit})
    return parse_service_response(response)


@app.get("/api/incidents")
async def list_incidents(limit: int = Query(default=20, ge=1, le=100)) -> list[dict[str, Any]]:
    response = await app.state.http.get(f"{HEALING_AGENT_URL}/incidents", params={"limit": limit})
    return parse_service_response(response)


@app.get("/api/prometheus/up")
async def prometheus_up() -> list[dict[str, Any]]:
    response = await app.state.http.get(
        f"{PROMETHEUS_URL}/api/v1/query",
        params={"query": "up"},
    )
    payload = parse_service_response(response)
    results = payload.get("data", {}).get("result", [])
    return [
        {
            "job": item.get("metric", {}).get("job", "unknown"),
            "instance": item.get("metric", {}).get("instance", "unknown"),
            "value": float(item.get("value", [0, 0])[1]),
        }
        for item in results
    ]


@app.post("/api/actions/seed")
async def seed_demo_data() -> dict[str, Any]:
    prefix = f"demo_{int(time.time())}"
    created_users: list[dict[str, Any]] = []
    user_count = 24
    follows_per_user = 5
    post_count = 72

    for index in range(user_count):
        response = await app.state.http.post(
            f"{USER_SERVICE_URL}/users",
            json={
                "username": f"{prefix}_{index:02d}",
                "display_name": f"Demo User {index + 1}",
            },
        )
        created_users.append(parse_service_response(response))

    user_ids = [user["id"] for user in created_users]
    for index, follower_id in enumerate(user_ids):
        for offset in range(1, follows_per_user + 1):
            followee_id = user_ids[(index + offset) % len(user_ids)]
            await app.state.http.post(f"{USER_SERVICE_URL}/users/{follower_id}/follow/{followee_id}")

    for index in range(post_count):
        author_id = random.choice(user_ids)
        await app.state.http.post(
            f"{POST_SERVICE_URL}/posts",
            json={
                "author_id": author_id,
                "content": f"Reliability demo post {index + 1:02d} from {author_id[-6:]}",
            },
        )

    return {
        "status": "seeded",
        "users": len(created_users),
        "follows": user_count * follows_per_user,
        "posts": post_count,
    }


@app.post("/api/actions/post")
async def create_demo_post(body: DemoPostRequest) -> dict[str, Any]:
    author_id = body.author_id or await first_user_id()
    if author_id is None:
        raise HTTPException(status_code=409, detail="seed demo data before creating a post")
    response = await app.state.http.post(
        f"{POST_SERVICE_URL}/posts",
        json={"author_id": author_id, "content": body.content},
    )
    return parse_service_response(response)


@app.post("/api/actions/traffic")
async def generate_traffic() -> dict[str, Any]:
    return await run_feed_traffic(rounds=4, sample_size=24)


@app.post("/api/actions/redis-outage")
async def redis_outage() -> dict[str, Any]:
    container = await docker_action("redis", "stop")
    traffic = await run_feed_traffic(rounds=5, sample_size=24)
    return {"status": "redis_stopped", "container": container, "traffic": traffic}


@app.post("/api/actions/feed-crash")
async def feed_crash() -> dict[str, Any]:
    container = await docker_action("feed-service", "stop")
    return {"status": "feed_stopped", "container": container}


@app.post("/api/actions/notification-crash")
async def notification_crash() -> dict[str, Any]:
    container = await docker_action("notification-service", "stop")
    return {"status": "notification_stopped", "container": container}


@app.post("/api/actions/recover")
async def recover() -> dict[str, Any]:
    containers = await asyncio.gather(
        docker_action("redis", "start"),
        docker_action("feed-service", "start"),
        docker_action("notification-service", "start"),
    )
    await asyncio.sleep(3)
    degraded = await set_feed_degraded_mode(False)
    paused = await set_consumer_paused(False)
    return {"status": "recovered", "containers": containers, "degraded": degraded, "consumer": paused}


@app.post("/api/actions/degraded-mode")
async def degraded_mode(body: DegradedModeRequest) -> dict[str, Any]:
    return await set_feed_degraded_mode(body.enabled)


async def probe_service(service: dict[str, str]) -> dict[str, Any]:
    started = time.perf_counter()
    base = {
        "id": service["id"],
        "name": service["name"],
        "kind": service["kind"],
        "status": "down",
        "latency_ms": None,
        "detail": "unreachable",
    }
    try:
        response = await app.state.http.get(f"{service['url']}/readyz")
        elapsed_ms = round((time.perf_counter() - started) * 1000)
        payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        if response.status_code < 400:
            base.update({"status": payload.get("status", "ready"), "latency_ms": elapsed_ms, "detail": payload})
        else:
            base.update({"status": "degraded", "latency_ms": elapsed_ms, "detail": payload or response.text})
    except Exception as exc:
        base["detail"] = str(exc)
    return base


async def run_feed_traffic(rounds: int, sample_size: int) -> dict[str, int]:
    user_rows = await list_users(limit=sample_size)
    if not user_rows:
        return {"requests": 0, "successes": 0, "errors": 0}

    requests = 0
    successes = 0
    errors = 0
    for _ in range(rounds):
        for user in user_rows:
            requests += 1
            try:
                response = await app.state.http.get(
                    f"{FEED_SERVICE_URL}/feeds/{user['id']}",
                    params={"limit": 5},
                    timeout=4,
                )
                if response.status_code < 500:
                    successes += 1
                else:
                    errors += 1
            except Exception:
                errors += 1
    return {"requests": requests, "successes": successes, "errors": errors}


async def set_feed_degraded_mode(enabled: bool) -> dict[str, Any]:
    response = await app.state.http.post(
        f"{FEED_SERVICE_URL}/admin/degraded-mode",
        headers={"x-admin-token": ADMIN_TOKEN},
        json={"enabled": enabled},
    )
    return parse_service_response(response)


async def set_consumer_paused(paused: bool) -> dict[str, Any]:
    response = await app.state.http.post(
        f"{FEED_SERVICE_URL}/admin/consumer-pause",
        headers={"x-admin-token": ADMIN_TOKEN},
        json={"paused": paused},
    )
    return parse_service_response(response)


async def docker_action(target: str, action: str) -> dict[str, Any]:
    return await asyncio.to_thread(_docker_action_sync, target, action)


def _docker_action_sync(target: str, action: str) -> dict[str, Any]:
    client = docker.from_env()
    candidates = client.containers.list(all=True, filters={"name": target})
    if not candidates:
        raise RuntimeError(f"no container matched {target}")
    container = sorted(candidates, key=lambda item: len(item.name))[0]
    if action == "stop":
        container.stop(timeout=10)
    elif action == "start":
        container.start()
    else:
        raise RuntimeError(f"unsupported docker action {action}")
    container.reload()
    return {"name": container.name, "status": container.status, "id": container.short_id}


async def first_user_id() -> str | None:
    async with app.state.pool.acquire() as conn:
        return await conn.fetchval("SELECT id FROM users ORDER BY created_at ASC LIMIT 1")


def parse_service_response(response: httpx.Response) -> Any:
    try:
        payload = response.json()
    except ValueError:
        payload = {"raw": response.text}
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=payload)
    return payload


def serialize_record(row: asyncpg.Record) -> dict[str, Any]:
    return {
        key: value.isoformat() if hasattr(value, "isoformat") else value
        for key, value in dict(row).items()
    }
