import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any

import docker
import httpx


ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "dev-admin-token")
FEED_ADMIN_URL = os.getenv("FEED_ADMIN_URL", "http://feed-service:8003")
RUNTIME_MODE = os.getenv("RUNTIME_MODE", "docker")


@dataclass
class ActionResult:
    status: str
    details: dict[str, Any]
    duration_seconds: float


class ActionExecutor:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def execute(self, action_type: str, target: str) -> ActionResult:
        start = time.perf_counter()
        try:
            if action_type == "enable_feed_degraded_mode":
                details = await self.enable_feed_degraded_mode()
            elif action_type == "restart_service":
                details = await self.restart_service(target)
            else:
                raise ValueError(f"unsupported action: {action_type}")
            return ActionResult("success", details, time.perf_counter() - start)
        except Exception as exc:
            return ActionResult("failed", {"error": str(exc)}, time.perf_counter() - start)

    async def enable_feed_degraded_mode(self) -> dict[str, Any]:
        response = await self.client.post(
            f"{FEED_ADMIN_URL}/admin/degraded-mode",
            headers={"x-admin-token": ADMIN_TOKEN},
            json={"enabled": True},
            timeout=5,
        )
        response.raise_for_status()
        return {"mode": "enabled", "target_url": FEED_ADMIN_URL}

    async def restart_service(self, target: str) -> dict[str, Any]:
        if RUNTIME_MODE != "docker":
            return {"mode": RUNTIME_MODE, "message": "restart not implemented for this runtime"}
        return await asyncio.to_thread(self._restart_docker_container, target)

    def _restart_docker_container(self, target: str) -> dict[str, Any]:
        client = docker.from_env()
        candidates = client.containers.list(all=True, filters={"name": target})
        if not candidates:
            raise RuntimeError(f"no container found for {target}")
        container = candidates[0]
        container.restart(timeout=10)
        return {"container": container.name, "container_id": container.short_id}

