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

HISTORY_CREATORS = [
    ("rome_daily", "Ancient Rome Daily"),
    ("archive_lens", "Archive Lens"),
    ("medieval_maps", "Medieval Maps"),
    ("space_race_room", "Space Race Room"),
    ("revolution_notes", "Revolution Notes"),
    ("artifact_lab", "Artifact Lab"),
    ("cold_war_clips", "Cold War Clips"),
    ("silk_road_stories", "Silk Road Stories"),
]

HISTORY_POSTS = [
    "A Roman road was not just pavement. It was logistics, tax collection, troop movement, and empire maintenance in one artifact.",
    "This 1520 map gets the coastline wrong, but the trade priorities exactly right. Cartography is a record of ambition.",
    "The fastest way to read an archive photo is to ask what the photographer wanted outside the frame.",
    "Medieval manuscript margins were sometimes the comment section of their day: jokes, corrections, and tiny rebellions.",
    "The space race was not only rockets. It was checklists, rooms full of operators, and reliability under impossible pressure.",
    "A museum object without provenance is a mystery with a display case. The metadata is part of the artifact.",
    "Cold War infrastructure changed everyday life: radio towers, bunkers, school drills, and supply chains all carried the politics.",
    "Silk Road history is less a single road than a protocol for exchange across languages, religions, and empires.",
    "The first newspapers scaled trust and panic at the same time. Distribution changed society before algorithms did.",
    "City walls tell you what people feared, where wealth moved, and how leaders expected conflict to arrive.",
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


@app.get("/ops")
async def ops() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/social")
async def social() -> FileResponse:
    return FileResponse(STATIC_DIR / "social.html")


@app.get("/compare")
async def compare() -> FileResponse:
    return FileResponse(STATIC_DIR / "compare.html")


@app.get("/lab")
async def lab() -> FileResponse:
    return FileResponse(STATIC_DIR / "lab.html")


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
            ORDER BY created_at DESC
            LIMIT $1
            """,
            limit,
        )
    return [serialize_record(row) for row in rows]


@app.get("/api/feed/{user_id}")
async def feed(user_id: str, limit: int = Query(default=12, ge=1, le=100)) -> dict[str, Any]:
    response = await app.state.http.get(f"{FEED_SERVICE_URL}/feeds/{user_id}", params={"limit": limit})
    return parse_service_response(response)


@app.get("/api/social/experience")
async def social_experience(
    user_id: str | None = Query(default=None),
    limit: int = Query(default=8, ge=1, le=20),
) -> dict[str, Any]:
    users, services, incidents, actions = await asyncio.gather(
        list_users(limit=40),
        service_status(),
        list_incidents(limit=8),
        recent_healing_actions(limit=8),
    )
    selected_user_id = user_id or (users[0]["id"] if users else None)
    if selected_user_id is None:
        return {
            "users": [],
            "selected_user": None,
            "notifications": {"count": 0, "items": []},
            "without_healing": empty_experience("seed demo data first"),
            "with_healing": empty_experience("seed demo data first"),
            "system": system_snapshot(services, incidents, actions, None),
            "timeline": [{"state": "pending", "label": "Seed the social graph to start the demo."}],
        }

    feed_payload = await safe_feed(selected_user_id, limit)
    feed_items = await enrich_feed_items(feed_payload.get("items", []))
    notifications = await notification_summary(selected_user_id)
    selected_user = next((user for user in users if user["id"] == selected_user_id), users[0] if users else None)
    source = feed_payload.get("source", "unavailable")
    feed_error = feed_payload.get("error")
    snapshot = system_snapshot(services, incidents, actions, source)
    outage_active = snapshot["redis_status"] != "ready" or snapshot["feed_status"] == "down"

    with_healing = {
        "label": "With Autonomic Feed Ops",
        "status": "recovering" if feed_error else ("resilient" if source == "postgres_degraded" else "ready"),
        "source": source,
        "load_ms": 420 if source == "postgres_degraded" else 96,
        "message": resilient_message(source, feed_error),
        "items": feed_items,
    }

    without_healing = {
        "label": "Without self-healing",
        "status": "failed" if outage_active else "ready",
        "source": "redis_only" if outage_active else source,
        "load_ms": 9200 if outage_active else 108,
        "message": baseline_message(outage_active, snapshot["redis_status"], snapshot["feed_status"]),
        "items": [] if outage_active else feed_items,
    }

    return {
        "users": users,
        "selected_user": selected_user,
        "notifications": notifications,
        "without_healing": without_healing,
        "with_healing": with_healing,
        "system": snapshot,
        "timeline": build_experience_timeline(snapshot, incidents, actions, source),
    }


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
                "username": f"{prefix}_{HISTORY_CREATORS[index % len(HISTORY_CREATORS)][0]}_{index:02d}",
                "display_name": HISTORY_CREATORS[index % len(HISTORY_CREATORS)][1],
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
                "content": HISTORY_POSTS[index % len(HISTORY_POSTS)],
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


@app.post("/api/actions/reset-demo")
async def reset_demo() -> dict[str, Any]:
    recovered = await recover()
    response = await app.state.http.delete(
        f"{HEALING_AGENT_URL}/incidents",
        headers={"x-admin-token": ADMIN_TOKEN},
    )
    cleared = parse_service_response(response)
    return {"status": "reset", "recovery": recovered, "incidents": cleared}


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


async def safe_feed(user_id: str, limit: int) -> dict[str, Any]:
    try:
        response = await app.state.http.get(f"{FEED_SERVICE_URL}/feeds/{user_id}", params={"limit": limit})
        return parse_service_response(response)
    except Exception as exc:
        return {"user_id": user_id, "source": "unavailable", "items": [], "error": str(exc)}


async def enrich_feed_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not items:
        return []
    author_ids = sorted({item.get("author_id") for item in items if item.get("author_id")})
    if not author_ids:
        return items
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, username, display_name
            FROM users
            WHERE id = ANY($1::text[])
            """,
            author_ids,
        )
    authors = {row["id"]: serialize_record(row) for row in rows}
    enriched = []
    for index, item in enumerate(items):
        author = authors.get(item.get("author_id"), {})
        enriched.append(
            {
                **item,
                "author": {
                    "id": item.get("author_id"),
                    "display_name": author.get("display_name", "Unknown Creator"),
                    "username": author.get("username", "unknown"),
                },
                "media_index": index % 6,
            }
        )
    return enriched


async def notification_summary(user_id: str) -> dict[str, Any]:
    async with app.state.pool.acquire() as conn:
        count = await conn.fetchval("SELECT count(*) FROM notifications WHERE user_id = $1", user_id)
        rows = await conn.fetch(
            """
            SELECT n.id, n.type, n.created_at, u.display_name AS actor_name
            FROM notifications n
            JOIN users u ON u.id = n.actor_id
            WHERE n.user_id = $1
            ORDER BY n.created_at DESC
            LIMIT 5
            """,
            user_id,
        )
    return {"count": int(count or 0), "items": [serialize_record(row) for row in rows]}


async def recent_healing_actions(limit: int = 8) -> list[dict[str, Any]]:
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              ha.id,
              ha.incident_id,
              ha.action_type,
              ha.target,
              ha.status,
              ha.details,
              ha.started_at,
              ha.completed_at,
              hi.type AS incident_type
            FROM healing_actions ha
            JOIN healing_incidents hi ON hi.id = ha.incident_id
            ORDER BY ha.started_at DESC
            LIMIT $1
            """,
            limit,
        )
    return [serialize_record(row) for row in rows]


def empty_experience(message: str) -> dict[str, Any]:
    return {
        "label": "Feed experience",
        "status": "pending",
        "source": "none",
        "load_ms": 0,
        "message": message,
        "items": [],
    }


def system_snapshot(
    services: list[dict[str, Any]],
    incidents: list[dict[str, Any]],
    actions: list[dict[str, Any]],
    source: str | None,
) -> dict[str, Any]:
    feed = next((service for service in services if service["id"] == "feed"), None)
    redis_status = "unknown"
    feed_status = "unknown"
    if feed:
        feed_status = normalize_service_status(feed.get("status"))
        detail = feed.get("detail")
        if isinstance(detail, dict):
            redis_status = detail.get("redis", "unknown")
    return {
        "services_ready": sum(1 for service in services if normalize_service_status(service.get("status")) == "ready"),
        "services_total": len(services),
        "redis_status": redis_status,
        "feed_status": feed_status,
        "feed_source": source or "unknown",
        "incidents": incidents,
        "actions": actions,
    }


def build_experience_timeline(
    snapshot: dict[str, Any],
    incidents: list[dict[str, Any]],
    actions: list[dict[str, Any]],
    source: str | None,
) -> list[dict[str, str]]:
    timeline: list[dict[str, str]] = []
    if snapshot["redis_status"] == "ready" and snapshot["feed_status"] == "ready":
        timeline.append({"state": "ready", "label": "Feed cache is healthy and serving the fast path."})
    if snapshot["redis_status"] != "ready":
        timeline.append({"state": "danger", "label": "Redis cache outage detected in the feed path."})
    if source == "postgres_degraded":
        timeline.append({"state": "resilient", "label": "Feed requests are being served from Postgres fallback."})
    for incident in incidents:
        if incident.get("type") in {"redis_outage", "service_down", "consumer_lag"}:
            timeline.append(
                {
                    "state": "resilient" if incident.get("status") == "resolved" else "danger",
                    "label": f"Agent recorded {incident.get('type')} on {incident.get('service')}.",
                }
            )
    for action in actions:
        timeline.append(
            {
                "state": "ready" if action.get("status") == "success" else "danger",
                "label": f"Action {action.get('action_type')} for {action.get('target')} ended {action.get('status')}.",
            }
        )
    if not timeline:
        timeline.append({"state": "pending", "label": "Waiting for demo traffic or a failure injection."})
    return timeline[:6]


def baseline_message(outage_active: bool, redis_status: str, feed_status: str) -> str:
    if feed_status == "down":
        return "The feed service is unavailable, so the user sees a broken timeline."
    if outage_active or redis_status != "ready":
        return "A Redis-only feed path stalls because the cache cannot answer."
    return "The fast cache path is healthy, so the feed feels normal."


def resilient_message(source: str, error: str | None) -> str:
    if error:
        return "The app is waiting for the agent to restore the feed service."
    if source == "postgres_degraded":
        return "The app is in resilient mode and serving from Postgres fallback."
    if source == "redis":
        return "The app is on the fast Redis materialized feed path."
    return "The app is loading the best available feed path."


def normalize_service_status(status: Any) -> str:
    if status in {"ready", "ok"}:
        return "ready"
    if status in {"down", "failed"}:
        return "down"
    return "degraded"


async def docker_action(target: str, action: str) -> dict[str, Any]:
    return await asyncio.to_thread(_docker_action_sync, target, action)


def _docker_action_sync(target: str, action: str) -> dict[str, Any]:
    client = docker.from_env()
    candidates = client.containers.list(all=True, filters={"name": target})
    if not candidates:
        raise RuntimeError(f"no container matched {target}")
    container = sorted(candidates, key=lambda item: len(item.name))[0]
    if action == "stop":
        if container.status == "running":
            container.stop(timeout=10)
    elif action == "start":
        if container.status != "running":
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
