# 90-Second Demo Script

Use this for Palantir prompt 1: "Show us something you've built that matters to you."

## Recording Setup

Start from the repo root:

```bash
docker compose up --build -d
make demo
```

Open the cockpit:

```text
http://localhost:8080
```

For the recording, keep the browser on the cockpit and a terminal beside it running `make demo`.

## Narrative

**0-15s: Problem**

"I built this because smaller social platforms live or die by reliability. If a feed takes 5-10 seconds to load, users experience it as failure and switch back to the incumbent. Smaller teams usually do not have giant ops teams watching every dependency."

**15-35s: What It Is**

"This is a distributed social feed backend. Users and follows are served by a Python service, posts are written through a Go service, Kafka carries post-created events, Redis stores materialized feeds, and Prometheus watches the system. The demo UI is an operations cockpit that shows service health, feed data, incidents, and remediation actions."

**35-65s: Failure Demo**

"Here I inject a Redis outage. Redis is now down, but feed reads still succeed because feed-service falls back to Postgres. The healing agent sees the Redis error rate in Prometheus and enables degraded feed mode automatically."

**65-85s: Why It Matters**

"The important part is not that Redis failed. The important part is that the user-facing feed stayed usable and the system recorded what happened without a human jumping in. That is the reliability gap I wanted to close for smaller teams."

**85-90s: Close**

"This matters to me because I like building systems that keep working under real-world failure, not just happy-path demos."

## What To Show On Screen

1. The service mesh: all services ready.
2. Feed preview: real materialized feed items.
3. Click or run Redis outage.
4. Feed-service still returns successful reads.
5. Incident timeline shows `redis_outage`.
6. Recovery restores Redis and clears degraded mode.

## Commands Worth Having Ready

```bash
make smoke
make smoke-ui
make demo
make reset-demo
```

## Claims To Keep Honest

- Safe to say: the local demo detects Redis degradation and remediates in under a minute.
- Safe to say: feed reads continue during Redis outage through Postgres fallback.
- Avoid saying: production scale, unless you add deployment evidence.
- Avoid saying: 5,000 concurrent users, unless you attach matching k6 output.
