from dataclasses import dataclass
from typing import Any


@dataclass
class IncidentCandidate:
    service: str
    incident_type: str
    severity: str
    confidence: float
    signals: list[str]
    target: str


def detect_incidents(snapshot: dict[str, Any]) -> list[IncidentCandidate]:
    candidates: list[IncidentCandidate] = []

    for job, value in snapshot.get("up", {}).items():
        if value == 0:
            service = job_to_service(job)
            if service == "healing-agent":
                continue
            candidates.append(
                IncidentCandidate(
                    service=service,
                    incident_type="service_down",
                    severity="critical",
                    confidence=0.92,
                    signals=[f"Prometheus up{{job='{job}'}} is 0"],
                    target=service,
                )
            )

    redis_errors = snapshot.get("redis_errors", {})
    feed_redis_errors = redis_errors.get("feed-service", 0.0)
    if feed_redis_errors > 1.0:
        degraded = snapshot.get("degraded_requests", {}).get("feed-service", 0.0)
        confidence = 0.82 if degraded > 0 else 0.76
        candidates.append(
            IncidentCandidate(
                service="feed-service",
                incident_type="redis_outage",
                severity="high",
                confidence=confidence,
                signals=[
                    f"feed-service Redis error rate is {feed_redis_errors:.2f}/s",
                    f"feed-service degraded request rate is {degraded:.2f}/s",
                ],
                target="feed-service",
            )
        )

    for service, value in snapshot.get("http_errors", {}).items():
        if value > 5.0:
            candidates.append(
                IncidentCandidate(
                    service=service,
                    incident_type="service_errors",
                    severity="high",
                    confidence=0.80,
                    signals=[f"{service} HTTP error rate is {value:.2f}/s"],
                    target=service,
                )
            )

    for service, value in snapshot.get("db_p95", {}).items():
        if value > 0.5:
            candidates.append(
                IncidentCandidate(
                    service=service,
                    incident_type="db_latency_spike",
                    severity="medium",
                    confidence=0.78,
                    signals=[f"{service} DB p95 latency is {value:.3f}s"],
                    target=service,
                )
            )

    for service, value in snapshot.get("consumer_lag", {}).items():
        if value > 1000:
            candidates.append(
                IncidentCandidate(
                    service=service,
                    incident_type="consumer_lag",
                    severity="high",
                    confidence=0.80,
                    signals=[f"{service} Kafka consumer lag is {value:.0f} messages"],
                    target=service,
                )
            )

    return candidates


def job_to_service(job: str) -> str:
    if job in {"user-service", "post-service", "feed-service", "notification-service", "healing-agent"}:
        return job
    return job.replace("_", "-")
