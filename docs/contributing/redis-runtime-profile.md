# Durable Redis Runtime Profile

Rustvello's Redis backend can share a Redis server with an application while
remaining an independent client. Use a distinct ACL user and the
`rustvello:{app_id}:` key namespace; logical database numbers alone do not
isolate authorization or memory pressure.

`RedisOptions` bounds connection/operation time, delivery leases, queue rows and
payload bytes. Set `require_durable_server` for task runtimes that must reject
startup unless Redis reports `maxmemory-policy noeviction` and either AOF or RDB
persistence. That check does not configure Redis, prove backups, or establish HA.

`RedisTlsOptions::private_ca_pem` accepts a bounded private CA for `rediss://` and
retains normal hostname verification. There is no insecure TLS mode.

Submission uses one atomic publication for invocation, call, initial status and
queue visibility. Exact replay is idempotent; the same invocation ID with changed
content or lineage fails. Dequeue creates an expiring lease, and expired leases
are returned to their original queue. Attempt and completion publication retain
worker ownership fences.

Run the focused fault and transport profiles with a local Redis image:

```bash
cargo test --offline -p rustvello-redis --test suite \
  crash_consistent_publication_and_delivery_lease -- --ignored --exact --test-threads=1
cargo test --offline -p rustvello-redis --test suite \
  tls_authentication_and_transport_rejections -- --ignored --exact --test-threads=1
```

Applications still own idempotency for side effects outside Rustvello. Redis
persistence cannot make an atomic transaction with an application's separate
Redis client, even when both clients use the same server.
