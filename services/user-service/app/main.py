import os
import time
from typing import Any
from uuid import uuid4

import asyncpg
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest


SERVICE = "user-service"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/social")

app = FastAPI(title="User Service", version="0.1.0")

HTTP_REQUESTS = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["service", "method", "path", "status"],
)
HTTP_ERRORS = Counter(
    "http_errors_total",
    "Total HTTP error responses",
    ["service", "method", "path", "status"],
)
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration",
    ["service", "method", "path"],
)
DB_LATENCY = Histogram(
    "db_query_duration_seconds",
    "Database query duration",
    ["service", "operation"],
)
DB_ERRORS = Counter(
    "db_errors_total",
    "Database errors",
    ["service", "operation"],
)


class UserCreate(BaseModel):
    username: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_.-]+$")
    display_name: str = Field(min_length=1, max_length=120)


class UserOut(BaseModel):
    id: str
    username: str
    display_name: str
    created_at: str


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def route_path(request: Request) -> str:
    route = request.scope.get("route")
    return getattr(route, "path", request.url.path)


async def fetch_timed(operation: str, query: str, *args: Any) -> list[asyncpg.Record]:
    start = time.perf_counter()
    try:
        async with app.state.pool.acquire() as conn:
            return await conn.fetch(query, *args)
    except Exception:
        DB_ERRORS.labels(SERVICE, operation).inc()
        raise
    finally:
        DB_LATENCY.labels(SERVICE, operation).observe(time.perf_counter() - start)


async def fetchrow_timed(operation: str, query: str, *args: Any) -> asyncpg.Record | None:
    start = time.perf_counter()
    try:
        async with app.state.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    except Exception:
        DB_ERRORS.labels(SERVICE, operation).inc()
        raise
    finally:
        DB_LATENCY.labels(SERVICE, operation).observe(time.perf_counter() - start)


async def execute_timed(operation: str, query: str, *args: Any) -> str:
    start = time.perf_counter()
    try:
        async with app.state.pool.acquire() as conn:
            return await conn.execute(query, *args)
    except Exception:
        DB_ERRORS.labels(SERVICE, operation).inc()
        raise
    finally:
        DB_LATENCY.labels(SERVICE, operation).observe(time.perf_counter() - start)


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
        elapsed = time.perf_counter() - start
        HTTP_REQUESTS.labels(SERVICE, request.method, path, status).inc()
        HTTP_LATENCY.labels(SERVICE, request.method, path).observe(elapsed)
        if status.startswith("5") or status.startswith("4"):
            HTTP_ERRORS.labels(SERVICE, request.method, path, status).inc()


@app.on_event("startup")
async def startup() -> None:
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)


@app.on_event("shutdown")
async def shutdown() -> None:
    await app.state.pool.close()


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE}


@app.get("/readyz")
async def readyz() -> dict[str, str]:
    await fetchrow_timed("readyz", "SELECT 1")
    return {"status": "ready", "service": SERVICE}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/users", response_model=UserOut, status_code=201)
async def create_user(body: UserCreate) -> dict[str, Any]:
    user_id = new_id("usr")
    try:
        row = await fetchrow_timed(
            "create_user",
            """
            INSERT INTO users (id, username, display_name)
            VALUES ($1, $2, $3)
            RETURNING id, username, display_name, created_at
            """,
            user_id,
            body.username,
            body.display_name,
        )
    except asyncpg.UniqueViolationError as exc:
        raise HTTPException(status_code=409, detail="username already exists") from exc
    return serialize_user(row)


@app.get("/users/{user_id}", response_model=UserOut)
async def get_user(user_id: str) -> dict[str, Any]:
    row = await fetchrow_timed(
        "get_user",
        "SELECT id, username, display_name, created_at FROM users WHERE id = $1",
        user_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="user not found")
    return serialize_user(row)


@app.post("/users/{user_id}/follow/{target_user_id}", status_code=204)
async def follow_user(user_id: str, target_user_id: str) -> Response:
    if user_id == target_user_id:
        raise HTTPException(status_code=400, detail="users cannot follow themselves")
    async with app.state.pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT count(*) FROM users WHERE id = ANY($1::text[])",
            [user_id, target_user_id],
        )
        if exists != 2:
            raise HTTPException(status_code=404, detail="one or both users not found")
        await conn.execute(
            """
            INSERT INTO follows (follower_id, followee_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            """,
            user_id,
            target_user_id,
        )
    return Response(status_code=204)


@app.delete("/users/{user_id}/follow/{target_user_id}", status_code=204)
async def unfollow_user(user_id: str, target_user_id: str) -> Response:
    await execute_timed(
        "unfollow_user",
        "DELETE FROM follows WHERE follower_id = $1 AND followee_id = $2",
        user_id,
        target_user_id,
    )
    return Response(status_code=204)


@app.get("/users/{user_id}/followers")
async def list_followers(user_id: str) -> dict[str, Any]:
    rows = await fetch_timed(
        "list_followers",
        """
        SELECT u.id, u.username, u.display_name, u.created_at
        FROM follows f
        JOIN users u ON u.id = f.follower_id
        WHERE f.followee_id = $1
        ORDER BY f.created_at DESC
        """,
        user_id,
    )
    return {"user_id": user_id, "followers": [serialize_user(row) for row in rows]}


@app.get("/users/{user_id}/following")
async def list_following(user_id: str) -> dict[str, Any]:
    rows = await fetch_timed(
        "list_following",
        """
        SELECT u.id, u.username, u.display_name, u.created_at
        FROM follows f
        JOIN users u ON u.id = f.followee_id
        WHERE f.follower_id = $1
        ORDER BY f.created_at DESC
        """,
        user_id,
    )
    return {"user_id": user_id, "following": [serialize_user(row) for row in rows]}


def serialize_user(row: asyncpg.Record) -> dict[str, Any]:
    return {
        "id": row["id"],
        "username": row["username"],
        "display_name": row["display_name"],
        "created_at": row["created_at"].isoformat(),
    }

