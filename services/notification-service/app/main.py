import asyncio
import json
import os
import time
from typing import Any
from uuid import uuid4

import asyncpg
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest


SERVICE = "notification-service"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/social")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
POST_CREATED_TOPIC = os.getenv("POST_CREATED_TOPIC", "post.created.v1")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "notification-service")

app = FastAPI(title="Notification Service", version="0.1.0")

HTTP_REQUESTS = Counter("http_requests_total", "Total HTTP requests", ["service", "method", "path", "status"])
HTTP_ERRORS = Counter("http_errors_total", "Total HTTP errors", ["service", "method", "path", "status"])
HTTP_LATENCY = Histogram("http_request_duration_seconds", "HTTP request duration", ["service", "method", "path"])
DB_LATENCY = Histogram("db_query_duration_seconds", "Database query duration", ["service", "operation"])
DB_ERRORS = Counter("db_errors_total", "Database errors", ["service", "operation"])
KAFKA_CONSUMED = Counter("kafka_messages_consumed_total", "Kafka messages consumed", ["service", "topic", "status"])
KAFKA_DLQ = Counter("kafka_dlq_messages_total", "Kafka messages sent to DLQ", ["service", "topic"])


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


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
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    app.state.consumer_ready = False
    app.state.consumer_task = asyncio.create_task(consume_forever())


@app.on_event("shutdown")
async def shutdown() -> None:
    app.state.consumer_task.cancel()
    await app.state.pool.close()


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE}


@app.get("/readyz")
async def readyz() -> dict[str, Any]:
    async with app.state.pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ready", "service": SERVICE, "consumer_ready": app.state.consumer_ready}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/users/{user_id}/notifications")
async def list_notifications(user_id: str, limit: int = 50) -> dict[str, Any]:
    start = time.perf_counter()
    try:
        async with app.state.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, post_id, actor_id, type, read_at, created_at
                FROM notifications
                WHERE user_id = $1
                ORDER BY created_at DESC
                LIMIT $2
                """,
                user_id,
                min(limit, 200),
            )
    except Exception:
        DB_ERRORS.labels(SERVICE, "list_notifications").inc()
        raise
    finally:
        DB_LATENCY.labels(SERVICE, "list_notifications").observe(time.perf_counter() - start)
    return {"user_id": user_id, "notifications": [serialize_notification(row) for row in rows]}


@app.post("/users/{user_id}/notifications/read", status_code=204)
async def mark_read(user_id: str) -> Response:
    start = time.perf_counter()
    try:
        async with app.state.pool.acquire() as conn:
            await conn.execute(
                "UPDATE notifications SET read_at = now() WHERE user_id = $1 AND read_at IS NULL",
                user_id,
            )
    except Exception:
        DB_ERRORS.labels(SERVICE, "mark_read").inc()
        raise
    finally:
        DB_LATENCY.labels(SERVICE, "mark_read").observe(time.perf_counter() - start)
    return Response(status_code=204)


async def consume_forever() -> None:
    while True:
        consumer = AIOKafkaConsumer(
            POST_CREATED_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=CONSUMER_GROUP,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        try:
            await consumer.start()
            app.state.consumer_ready = True
            async for message in consumer:
                await handle_message(message.value)
                await consumer.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            app.state.consumer_ready = False
            KAFKA_CONSUMED.labels(SERVICE, POST_CREATED_TOPIC, "error").inc()
            await asyncio.sleep(5)
        finally:
            app.state.consumer_ready = False
            try:
                await consumer.stop()
            except Exception:
                pass


async def handle_message(raw: bytes) -> None:
    try:
        event = json.loads(raw.decode("utf-8"))
        payload = event["payload"]
        event_id = event["event_id"]
        async with app.state.pool.acquire() as conn:
            async with conn.transaction():
                inserted = await conn.fetchval(
                    """
                    INSERT INTO processed_events (consumer_name, event_id)
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                    RETURNING event_id
                    """,
                    CONSUMER_GROUP,
                    event_id,
                )
                if inserted is None:
                    KAFKA_CONSUMED.labels(SERVICE, POST_CREATED_TOPIC, "duplicate").inc()
                    return
                followers = await conn.fetch(
                    "SELECT follower_id FROM follows WHERE followee_id = $1",
                    payload["author_id"],
                )
                for follower in followers:
                    await conn.execute(
                        """
                        INSERT INTO notifications (id, user_id, post_id, actor_id, type)
                        VALUES ($1, $2, $3, $4, 'new_post')
                        ON CONFLICT DO NOTHING
                        """,
                        new_id("ntf"),
                        follower["follower_id"],
                        payload["post_id"],
                        payload["author_id"],
                    )
        KAFKA_CONSUMED.labels(SERVICE, POST_CREATED_TOPIC, "success").inc()
    except Exception:
        KAFKA_DLQ.labels(SERVICE, POST_CREATED_TOPIC).inc()
        KAFKA_CONSUMED.labels(SERVICE, POST_CREATED_TOPIC, "error").inc()
        raise


def serialize_notification(row: asyncpg.Record) -> dict[str, Any]:
    return {
        "id": row["id"],
        "post_id": row["post_id"],
        "actor_id": row["actor_id"],
        "type": row["type"],
        "read_at": row["read_at"].isoformat() if row["read_at"] else None,
        "created_at": row["created_at"].isoformat(),
    }

