from typing import Any

import httpx


class PrometheusClient:
    def __init__(self, base_url: str, client: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = client

    async def query(self, expression: str) -> list[dict[str, Any]]:
        response = await self.client.get(
            f"{self.base_url}/api/v1/query",
            params={"query": expression},
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "success":
            raise RuntimeError(f"Prometheus query failed: {payload}")
        return payload["data"]["result"]

    async def service_values(self, expression: str, label: str = "service") -> dict[str, float]:
        results = await self.query(expression)
        values: dict[str, float] = {}
        for result in results:
            metric = result.get("metric", {})
            key = metric.get(label) or metric.get("job") or "unknown"
            values[key] = float(result["value"][1])
        return values

