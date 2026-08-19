---
name: stack
description: Run the demo stack under podman-compose — bring it up, get a code change into a running service, reconnect the dashboard after a restart, or wipe data. Use when a container is unhealthy, when an edit to backend/ or frontend/ needs to reach the running stack, when queries fail with connection errors, or when every container has died at once.
user-invocable: true
allowed-tools:
  - Bash
  - Read
---

# Running the stack

Eight containers: `cassandra`, `kafka`, `presto`, `spark`, `backend`, `frontend`, `data-producer`, `data-cassandra-sink`.  `compose.yml` is a symlink to `podman-compose.yml`; edit the latter.

## Up

```bash
podman compose -f podman-compose.yml up -d
```

The podman machine needs more than 12 GB; Cassandra, Presto and Spark each want a JVM heap.  No host port conflicts with macOS ControlCenter any more: internode gossip, 7000, is no longer published, so the untracked `podman-compose.local.yml` override that used to drop that mapping is not needed.

## Getting a change into a running service

```bash
podman compose -f podman-compose.yml build backend frontend
podman compose -f podman-compose.yml up -d --no-deps backend frontend
```

`up -d` on its own recreates the container from the **old image**; it looks like your change had no effect.  Always build first.  `--no-deps` keeps compose from restarting Cassandra and Kafka underneath you.

`frontend` serves a build baked into the image, so a change under `frontend/src/` needs the build.  For iteration, `cd frontend && npm run dev` proxies `/api` to `localhost:8000` and reloads.

## Waiting

Poll; do not sleep.

```bash
until curl -sf -m 5 localhost:8000/api/health > /dev/null; do sleep 5; done
```

The backend has taken **290 s** to report healthy when Cassandra was starting beside it, because it retries the driver's contact points rather than failing.  A `sleep 60` that usually works is how a check becomes flaky.

## After restarting a service

The dashboard is a container beside the others and cannot restart them; that is deliberate.  Restarting a service leaves the backend holding a dead session, so rebuild the clients:

```bash
podman restart cassandra          # or spark, presto
curl -s -X POST localhost:8000/api/platform/reconnect \
  -H 'Content-Type: application/json' -d '{"target":"all"}' | jq .
curl -s localhost:8000/api/platform/health | jq '.services[] | {name, status, detail}'
```

`target` may be one of the client names instead of `"all"`.  Restarting `spark` restarts the master, worker and Thrift Server together, and clears a wedged HiveServer2 session.

## Wiping

```bash
./scripts/cleanup-data.sh                 # truncate the demo tables, stack stays up
./stop-and-clean-data-and-schema.sh       # stop everything and delete cassandra-data/
```

The first is what you want between measurements.  The second changes the schema question as well as the data; see the `schema` skill before reaching for it.

## Reading a failure

- **Every container exited 137 at once** — the podman machine or the host stopped.  Check `podman logs --tail 5 cassandra`: a healthy probe as the last line means it was killed, not that it crashed.  Do not go looking for the change that "broke" it.
- **`Cassandra not connected` in the backend log** — expected while Cassandra is still starting.  The endpoints degrade rather than fail; `/api/query/window` reports `closed: false` instead of erroring.
- **A Spark query hangs, then times out** — usually the connector's schema refresh.  Restart `spark`, then reconnect.  `podman logs --tail 100 spark` shows the Thrift Server's own complaint.
- **The sink's progress line** is the quickest sign of life: `podman logs --tail 2 data-cassandra-sink` prints `total_inserted=… (~1990/s)`.
