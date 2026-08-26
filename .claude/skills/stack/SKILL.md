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

Nothing in the `backend` image compiles.  The cqlite reader arrives as a prebuilt wheel from `backend/dist/`, so the context is `./backend` like every other service.  Measured on darwin/arm64: 33.1 s with `--no-cache`, 4.9 s after a new wheel, 2.9 s for a Python-only change, and 1.6 s with nothing changed.  The cold figure was 9 min 25 s while the Rust was compiled here, and CI paid it on every run because a runner keeps no cargo cache mount.

A reader change now costs more than a build here, not less: a commit in the cqlite fork, then `scripts/build-cqlite-wheel.sh` once per architecture, then a commit of `backend/dist/`.  `backend/dist/VENDOR.md` names the fork commit each wheel came from.

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

## An unclean stop stops Accord, and Accord stops the node

Accord writes a `started` marker into `cassandra-data/accord_journal/` and a `stopped` marker when it shuts down cleanly.  A `started` with no `stopped` means the node was killed, and `AccordService.localStartup()` treats that as fatal:

```
Stop marker is older than start marker (-1<1787604329607), so cannot assume we have a
complete log of our votes in any consensus groups. Exiting.
```

Every table's data is intact; the node just will not open.  `podman machine stop`, a sleeping laptop or an out-of-memory kill each cause it, and this stack has taken all three.

`cassandra/entrypoint.sh` sets `accord.journal.stop_marker_failure_policy: ALLOW_UNSAFE_STARTUP`, so the condition now warns and startup continues.  What that gives up is the guarantee that this node knows every vote it cast; at RF=1 there is no peer to hold a conflicting one, so it gives up nothing here.  **A multi-node cluster must not carry that setting.**  The recovery without it is `rm -rf cassandra-data/accord_journal/`, which discards the vote log and keeps the tables.

Three things made this hard to see, and two of them are fixed:

- The container reported `Up (starting)` with nothing listening, because the entrypoint's `until cqlsh …` loop kept polling a daemon that had died.  It now tests `kill -0` on the backgrounded pid and exits, so `podman ps` says `Exited (1)` and `podman logs cassandra` ends at the cause.
- **`spark` never started at all**, and looked like a second, unrelated failure.  Its `depends_on` is `condition: service_healthy` on cassandra, so compose created the container and never ran it: `podman inspect` showed `State=initialized` and `StartedAt=0001-01-01`, and `podman logs spark` was empty.  An empty log and that date mean *not started*, not *crashed*.  Start it with `podman compose -f podman-compose.yml up -d spark` once cassandra is healthy.
- **The sink does not recover.**  Ten hours after Cassandra died it was still printing `batch write failed, will retry from the last commit: ('Unable to complete the operation against any hosts', {})`, with an empty error map, meaning the driver had no host left to try.  `podman restart data-cassandra-sink` fixes it, and the sink then drains the Kafka backlog at about twice the producer's rate.  The backend needs the same treatment; `/api/platform/reconnect` above is the lighter form of it.

A drained backlog writes into **past** buckets, because `event_bucket` comes from the event's own time.  So the current window can be empty while old windows are still growing, and no window is safely closed until the lag reaches zero.  Check it before quoting any figure:

```bash
podman exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 \
  --describe --group demo-cassandra-sink | awk 'NR>1 && NF>5 {n++; t+=$6} END {print n" partitions, total lag "t}'
```

## Drain before you stop Cassandra, or it will not come back

With CDC on, a node that still has commit log segments to replay **exits during startup**:

```
ERROR [main] DefaultDiskErrorsHandler.java:177 - Exiting due to error while processing commit log during initialization.
java.io.IOException: Bad file descriptor
	at org.apache.cassandra.db.commitlog.CommitLogSegment.writeCDCIndexFile(CommitLogSegment.java:388)
	at org.apache.cassandra.db.commitlog.CommitLogReplayer.handleCDCReplayCompletion(CommitLogReplayer.java:234)
```

Every table's data is intact.  `handleCDCReplayCompletion` runs once per replayed segment that held a CDC mutation, and the 18-byte index file it writes into `cdc_raw` fails on the flush.  It is not disk space and it is not the bind mount; the same write from a shell into the same directory succeeds.  Measured on darwin/arm64.

So make the drain part of stopping:

```bash
podman exec cassandra nodetool drain      # leaves one commit log segment, seven files in cdc_raw
podman stop cassandra
podman compose -f podman-compose.yml up -d --no-deps cassandra   # CQL back in ~55 s
```

If the node is already refusing to start, bring it up **with CDC off**, which skips the path because `sawCDCMutation` gates the call, then drain and recreate normally:

```bash
CASSANDRA_CDC_ENABLED=false podman compose -f podman-compose.yml up -d --no-deps --force-recreate cassandra
podman exec cassandra nodetool drain
podman stop cassandra && podman compose -f podman-compose.yml up -d --no-deps cassandra
```

What a drain gives up is the mutations sitting in `cdc_raw` that the publisher had not yet read.  Restart `data-cassandra-sink` and `backend` afterwards; neither recovers a dead session on its own.

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
- **Cassandra is `Up (starting)` and nothing answers on 9042, or `spark` has an empty log** — read "An unclean stop stops Accord" above.  Both are the same cause.
- **Cassandra exits during startup with `Bad file descriptor` after "Finished reading … CommitLog-9-…"** — read "Drain before you stop Cassandra" above.  This one is CDC, not Accord, and the two look alike from `podman ps`.
- **The Streaming page shows an age in minutes** — the publisher is behind the writer, which happens whenever the sink is draining a Kafka backlog.  Check the sink's lag first; the 8.0 s figure is a floor and not a promise.
- **`/api/sql-console/status` reports `connected: false` while `accord-sql` looks fine** — the Spring context died and the JVM did not.  Read the container's log to its end: `Cannot connect to Cassandra`, caused by `DriverTimeoutException: Query timed out after PT2S` at `CassandraExecutor.init:65`, is the cold-start failure.  `podman compose up -d --no-deps accord-sql` answers "Running" and changes nothing; `podman restart accord-sql` is the recovery.  The image's entrypoint now exits the container when nothing opens 5432 within `ACCORD_SQL_STARTUP_TIMEOUT_S`, 300 by default, so `restart: unless-stopped` retries; a container that is genuinely stuck predates that entrypoint, so rebuild it.
- **`accord-sql` serves but its five SQL tables are missing** (`Table does not exist: OPERATORS`) — Cassandra's data was wiped under it.  `curl -X POST http://localhost:8000/api/sql-console/reset` rebuilds and reseeds; expect 2 errors, both `DROP TYPE`, and judge the `CREATE` and `INSERT` statements instead.
