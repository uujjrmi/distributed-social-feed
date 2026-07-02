# Architecture Notes

The system is intentionally split by ownership:

- User Service owns identity and follow relationships.
- Post Service owns durable post creation and publishes `post.created.v1`.
- Feed Service owns feed materialization and read behavior.
- Notification Service owns notification rows.
- Healing Agent owns incident detection, policy, and remediation.

Kafka keeps write-time post creation decoupled from read-time feed materialization. Redis gives fast feed reads, while Postgres remains the fallback source of truth during a cache outage.

The first self-healing path is Redis outage recovery:

1. Redis is stopped or becomes unreachable.
2. Feed reads emit `redis_errors_total` and fall back to Postgres.
3. Prometheus scrapes the Feed Service.
4. Healing Agent detects the Redis error rate.
5. Healing Agent calls `POST /admin/degraded-mode` on Feed Service.
6. Feed Service bypasses Redis immediately and serves feeds from Postgres.
7. Incident and action rows are written to Postgres for auditability.

This is intentionally policy-constrained. The agent is not allowed to run arbitrary commands; it chooses from a small action matrix.

