import time
from dataclasses import dataclass

from .detectors import IncidentCandidate


@dataclass
class PolicyDecision:
    allowed: bool
    action_type: str | None
    reason: str


class PolicyEngine:
    def __init__(self, confidence_threshold: float, cooldown_seconds: int) -> None:
        self.confidence_threshold = confidence_threshold
        self.cooldown_seconds = cooldown_seconds
        self.last_action_at: dict[str, float] = {}

    def decide(self, candidate: IncidentCandidate) -> PolicyDecision:
        if candidate.confidence < self.confidence_threshold:
            return PolicyDecision(False, None, "confidence below threshold")

        key = f"{candidate.service}:{candidate.incident_type}"
        now = time.time()
        last = self.last_action_at.get(key, 0)
        if now - last < self.cooldown_seconds:
            return PolicyDecision(False, None, "cooldown active")

        action = self.action_for(candidate.incident_type)
        if action is None:
            return PolicyDecision(False, None, "no policy action")

        self.last_action_at[key] = now
        return PolicyDecision(True, action, "allowed")

    def action_for(self, incident_type: str) -> str | None:
        if incident_type == "redis_outage":
            return "enable_feed_degraded_mode"
        if incident_type in {"service_down", "service_errors", "consumer_lag"}:
            return "restart_service"
        return None

