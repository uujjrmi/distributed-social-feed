# Benchmark Results

This file records real local validation runs. It intentionally separates demo evidence from larger-scale load claims.

## Demo Validation Run

- Date: 2026-07-06 21:07 CDT
- Machine: Apple M1 Pro
- Memory: 16.0 GiB
- Docker: Docker version 28.0.4, Docker Compose v2.34.0-desktop.1
- Command: `make demo`

## Observed Results

- Warm feed traffic before failure: 96 requests, 96 successes, 0 errors
- Redis-outage feed traffic: 120 requests, 120 successes, 0 errors
- Failure injected at: 2026-07-07T02:09:23Z
- Redis outage detected at: 2026-07-07T02:09:37.099284Z
- Incident resolved at: 2026-07-07T02:09:37.102977Z
- Detection time: about 14.1 seconds
- Remediation completion time: about 14.1 seconds
- Final service readiness: 5 / 5 services ready
- Final demo availability score: 100.0%

## Incident Evidence

The run produced a single demo incident:

```json
{
  "service": "feed-service",
  "type": "redis_outage",
  "severity": "high",
  "confidence": 0.82,
  "status": "resolved",
  "signals": [
    "feed-service Redis error rate is 2.18/s",
    "feed-service degraded request rate is 3.93/s"
  ]
}
```

## Notes

- This is not a production-scale benchmark.
- It validates the demo claim that feed reads remain successful during a Redis outage through Postgres fallback.
- It validates the self-healing claim that Redis degradation is detected and remediated in under a minute in the local Docker demo.
- Do not publish the 1,000 RPS / 5,000 concurrent-user claim until this file contains matching k6 output from `load/`.
