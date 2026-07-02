import json
from typing import Any
from uuid import uuid4

import asyncpg


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


class IncidentStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def create_incident(
        self,
        service: str,
        incident_type: str,
        severity: str,
        confidence: float,
        signals: list[str],
    ) -> str:
        incident_id = new_id("inc")
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO healing_incidents
                    (id, service, type, severity, confidence, status, signals, detected_at)
                VALUES ($1, $2, $3, $4, $5, 'open', $6::jsonb, now())
                """,
                incident_id,
                service,
                incident_type,
                severity,
                confidence,
                json.dumps(signals),
            )
        return incident_id

    async def complete_incident(self, incident_id: str, status: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE healing_incidents
                SET status = $2, resolved_at = now()
                WHERE id = $1
                """,
                incident_id,
                status,
            )

    async def record_action(
        self,
        incident_id: str,
        action_type: str,
        target: str,
        status: str,
        details: dict[str, Any],
    ) -> str:
        action_id = new_id("act")
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO healing_actions
                    (id, incident_id, action_type, target, status, details, started_at, completed_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, now(), now())
                """,
                action_id,
                incident_id,
                action_type,
                target,
                status,
                json.dumps(details),
            )
        return action_id

    async def list_incidents(self, limit: int = 100) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, service, type, severity, confidence, status, signals,
                       detected_at, resolved_at
                FROM healing_incidents
                ORDER BY detected_at DESC
                LIMIT $1
                """,
                limit,
            )
        return [dict(row) for row in rows]

    async def get_incident(self, incident_id: str) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            incident = await conn.fetchrow(
                """
                SELECT id, service, type, severity, confidence, status, signals,
                       detected_at, resolved_at
                FROM healing_incidents
                WHERE id = $1
                """,
                incident_id,
            )
            if incident is None:
                return None
            actions = await conn.fetch(
                """
                SELECT id, action_type, target, status, details, started_at, completed_at
                FROM healing_actions
                WHERE incident_id = $1
                ORDER BY started_at
                """,
                incident_id,
            )
        data = dict(incident)
        data["actions"] = [dict(row) for row in actions]
        return data

