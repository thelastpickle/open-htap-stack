# Porting the Python services to Java

tl;dr: three of this stack's ten services are Python: the dashboard backend, the synthetic producer and the Kafka sink. &emsp;9,851 lines in all. &emsp;They are being rewritten in Java 25, the backend on Quarkus, on the `main-java` branch. &emsp;This document is the review that preceded the rewrite: fifteen findings about the design as `trunk` leaves it, and which commit answers each. &emsp;The findings are ordered by what a miss costs, not by where the code lives.

## Contents

- [What was reviewed](#what-was-reviewed)
- [The findings](#the-findings)
  - [F1: one process is a requirement recorded only in a Dockerfile comment](#f1-one-process-is-a-requirement-recorded-only-in-a-dockerfile-comment)
  - [F2: ten places rest on the backend having no JVM](#f2-ten-places-rest-on-the-backend-having-no-jvm)
  - [F3: the access-path list is a dict literal, and the frontend declares it a second time](#f3-the-access-path-list-is-a-dict-literal-and-the-frontend-declares-it-a-second-time)
  - [F4: there is no test suite](#f4-there-is-no-test-suite)
  - [F5: six engine clients reimplement connecting, and no two support the same set of operations](#f5-six-engine-clients-reimplement-connecting-and-no-two-support-the-same-set-of-operations)
  - [F6: the one-run-at-a-time idiom is written thirteen times](#f6-the-one-run-at-a-time-idiom-is-written-thirteen-times)
  - [F7: the DataFusion version pin is a three-way agreement enforced by a document](#f7-the-datafusion-version-pin-is-a-three-way-agreement-enforced-by-a-document)
  - [F8: the sink duplicates the backend's geometry, and the duplication is undefended](#f8-the-sink-duplicates-the-backends-geometry-and-the-duplication-is-undefended)
  - [F9: query.py is 1,054 lines holding four separable concerns](#f9-querypy-is-1054-lines-holding-four-separable-concerns)
  - [F10: CI read the backend's settings out of the process](#f10-ci-read-the-backends-settings-out-of-the-process)
  - [F11: 51 settings sit in one flat class and a typo is silent](#f11-51-settings-sit-in-one-flat-class-and-a-typo-is-silent)
  - [F12: models.py is 781 lines and 56 classes, and one request model lives elsewhere](#f12-modelspy-is-781-lines-and-56-classes-and-one-request-model-lives-elsewhere)
  - [F13: compute_bearing_deg has no caller in the backend](#f13-compute_bearing_deg-has-no-caller-in-the-backend)
  - [F14: the backend's healthcheck is a Python one-liner](#f14-the-backends-healthcheck-is-a-python-one-liner)
  - [F15: the window endpoint opens two Kafka clients per request](#f15-the-window-endpoint-opens-two-kafka-clients-per-request)
- [What the port must not change](#what-the-port-must-not-change)
- [What must be measured rather than carried over](#what-must-be-measured-rather-than-carried-over)
- [Which commit answers which finding](#which-commit-answers-which-finding)

## What was reviewed

`trunk` at `e17ad33`, 55 commits, and the Python it contains:

| Service | Files | Lines |
| --- | --- | --- |
| `backend/app` | 27 | 7,949 |
| `ingress/consumer/consumer.py` | 1 | 1,123 |
| `ingress/producer/producer.py` | 1 | 779 |

Repository tooling stays Python and is out of scope: `scripts/*.py`, the workflow's inline `python3 -` blocks, and `cassandra/` shell.

The frontend is React and TypeScript and is not being ported. &emsp;That is the tightest constraint the review found, and it is stated before the findings because several of them are shaped by it. &emsp;Eleven frontend files call 44 distinct `/api` paths as bare same-origin paths, and `frontend/src/lib/api.ts:79` reads every failure out of a `detail` field, accepting either a string or an array. &emsp;No Java compiler can protect any of that.

## The findings

### F1: one process is a requirement recorded only in a Dockerfile comment

Nineteen module-level names in `backend/app` hold mutable state, and `backend/Dockerfile:40`'s `--workers 1` is the whole of what keeps them coherent. &emsp;The comment above it says why. &emsp;Nothing fails loudly if a second worker appears; the dashboard would simply report two different health scores and permit two comparisons at once, each timing the other's contention.

The nineteen, grouped by what they are:

| What | Where |
| --- | --- |
| six engine-client singletons | `cassandra_client.py:401`, `presto_client.py:156`, `spark_client.py:540,541`, `cqlite_client.py:444`, `accord_sql_client.py:158` |
| two loop objects | `streaming.py:354`, `vector.py:406` |
| the comparison's lock, its in-flight record and its cancel event | `query.py:241,242,246` |
| the health score and its lock | `health.py:69,70` |
| the demo settings and their lock | `settings.py:22,35` |
| two one-run gates | `transactions.py:68`, `sql_console.py:108` |
| the memoised probe entity | `demo.py:124` |
| the snapshot counter | `spark_client.py:53` |

Quarkus removes half the hazard by construction: there is no worker fork, and a second process on the same port fails to bind fatally where uvicorn's second worker did not. &emsp;What remains is "do not run two backend containers", which `container_name: backend` already makes impossible under podman-compose. &emsp;The port makes each of the nineteen an `@ApplicationScoped` bean and keeps statics for immutable things only.

Rejected: a Cassandra singleton claim table. &emsp;It would add a table the sink owns, and it buys nothing the port binding does not already give.

### F2: ten places rest on the backend having no JVM

Seven say it outright. &emsp;Five of those are prose and UI copy: `README.md:332`, `CLAUDE.md:19`, `docs/MISSION-CONTROL.md:93`, `frontend/src/pages/Explore.tsx:148` and `frontend/src/pages/Health.tsx:135`. &emsp;Two are in the Python the port replaces: `backend/app/db/cqlite_client.py:4` and `backend/app/models.py:254`.

Three more say it by other words, calling Spark and the bulk reader "the JVM paths" and putting cqlite outside that group: `frontend/src/pages/Explore.tsx:198` and `:272`, and `.claude/skills/measure/SKILL.md:78`. &emsp;Each is a sentence about a draining Kafka backlog taking CPU the two Spark paths want, which is a real finding; the grouping is what a Java backend breaks, since every path then runs in a JVM and cqlite's is the dashboard's own.

Two of the ten matter out of proportion to their size: `cqlite_client.py:4` and `models.py:254`, because a module docstring and a field comment are what a port carries across verbatim while translating the code beneath them. &emsp;Restate those two rather than translating them.

What the path demonstrates is unchanged: no snapshot, no Sidecar, no coordinator, and the parse and the SQL running inside the dashboard's own process rather than in a separate engine. &emsp;So the distinction to restate is another container against the dashboard's own process, and not a JVM against no JVM. &emsp;The claim has to be restated rather than quietly dropped, because "no JVM" was never the property; it was one way of saying the property.

`README.md:152` is not one of the ten and must not be swept with them: it says the connector's reads and writes go through Cassandra's own JVM, which the port does not touch.

### F3: the access-path list is a dict literal, and the frontend declares it a second time

`query.py:135`'s `ENGINES` decides which path ids exist and which client and SQL dialect each one maps to. &emsp;`health.py:29` imports it and `health.py:150` rebuilds a second dict from it. &emsp;Seven sites in `query.py` read it, at `:147, 216, 330, 331, 342, 499, 1049`.

Its insertion order reaches two things, and the Python's own comment at `:130-134` names both. &emsp;One is the result list of a comparison made in one request, at `:330` for a request that names no path and `:342` for one that does, which the docstring at `:323` says is so the columns do not move about. &emsp;The other is the order a sequential benchmark runs the paths in, because `:456` hands `_run_sequentially` that same list; so the order decides which path runs first against a stack the other four have not yet touched, which makes it a property of the measurement and not only of the display. &emsp;The compare page's own order is neither of them. &emsp;`frontend/src/pages/Explore.tsx` declares the five paths again, four times over: the `Engine` union at `:90`, an `ENGINES` array carrying label, role and colour at `:120`, `DEFAULT_RUN_ORDER` at `:218`, and a per-preset `order` at `:237, 247, 276, 299`. &emsp;A parallel run takes its column order from that array at `:802` and a sequential run takes it from arrival at `:803`.

So the coupling with nothing to enforce it is the TypeScript list, and the port cannot fix it, because the frontend is not being ported. &emsp;What the port can do is stop the Python side being implicit: an `EnginePathId` enum whose declaration order is the one-request result order, carrying an explicit id string so the JSON keys stay `spark_bulk` and the rest, and an `EnumMap` registry bean the health resource injects rather than imports. &emsp;Adding a sixth path still means editing two languages, and that is worth saying out loud rather than discovering.

### F4: there is no test suite

`CLAUDE.md` states it plainly: "Backend: no test suite", and correctness is verified by running queries against the stack. &emsp;That is the right primary check for this repository and it is not sufficient, because a good deal of the Python is pure logic that needs no stack at all: WKT parsing and polygon distance, the bucket and shard arithmetic, the SQL rewriting, percentiles, the Confluent five-byte Avro framing, the nested-prefix error tidying, the anomaly-rate solution.

The port introduces JUnit 6, and one standing rule governs it. &emsp;**Every commit that adds a Java class adds that class's tests in the same commit.** &emsp;No commit is exempt and no test is deferred, because a commit is the review unit here and an untested class reviewed alone gives a reviewer nothing to check the behaviour against. &emsp;Where behaviour genuinely needs a running stack, a real Presto cancel or a snapshot expiring mid-scan, the commit still tests what is testable without one and its message names the CI step that covers the rest.

### F5: six engine clients reimplement connecting, and no two support the same set of operations

Each of the six writes its own connect and its own lock, and beyond that they differ in ways that are each justified and nowhere collected. &emsp;Assembling the table is the finding:

| Client | Reconnect throttle | `busy` | Query lock | Abort |
| --- | --- | --- | --- | --- |
| `cassandra_client` | 10 s, `:52` | — | connect only, `:60` | — |
| `presto_client` | — | `:35` | `:31` | `kill_query(query_id)`, `:143` |
| `spark_client`, connector | — | `:108` | `:95` | `:163` |
| `spark_client`, bulk | — | `:357` | `:342` | `:397` |
| `cqlite_client` | — | `:143` | `:94`, beside a connect lock at `:90` | `:357` |
| `accord_sql_client` | — | `:55` | `:51` | — |

Two of the six have no abort at all, one throttles reconnects and five do not, and Cassandra alone has no `busy`. &emsp;So `busy` and abort are optional in the port and not universal: a `GatePolicy` demanding an abort style for all six would have to invent behaviour for two of them, and an invented value is worse than the absence it replaces. &emsp;Nor are the four aborts one mechanism. &emsp;Presto's is an HTTP `DELETE` to the coordinator naming a query id, which needs no session and works while this client's connection is busy with the query being killed. &emsp;Spark's closes the socket under a blocked PyHive read, taking no lock because the thread holding it is the one being interrupted, and the bulk client's abort delegates to that same one. &emsp;cqlite's sets a flag the merge polls, so the scan stops at its next partition and nothing is torn down or rebuilt.

The port composes rather than inherits: one `ConnectionGate` holding the lock, plus a `GatePolicy` record naming the reconnect throttle, whether an error drops the connection, whether a batch is preceded by a probe, and an abort style that may be absent. &emsp;Six policy values then read side by side, which is the property the Python lacks.

Three details the port must not get wrong, each established by reading the Python rather than assumed:

- `cassandra_client.execute_query` at `:123` takes **no** lock. &emsp;`CqlSession` is likewise multiplexed, so Cassandra's gate is a connect gate only, and adding a query lock would serialise the one path the demo measures point reads on.
- `_prepare_lock` and `_prepared` at `cassandra_client.py:71,72` disappear rather than being ported, and the reason is narrower than a registry the driver happens to have. &emsp;`:105` clears the map inside `connect()`, and the Python's own comment says why: a prepared statement belongs to the session that prepared it, so a reconnect leaves the old ones invalid. &emsp;That is a session lifetime and not a schema change. &emsp;A Java reconnect builds a new `CqlSession` with its own registry, and `CqlSession.prepare` on a string already prepared answers from that registry without a second round trip, so both the map and its lock are the driver's.
- The consistency level on those statements must survive the move. &emsp;`:196` assigns `ConsistencyLevel.QUORUM` to each statement as it prepares it, because Accord refuses the driver's LOCAL_ONE default outright, and QUORUM is what the sink already writes at. &emsp;A Java `PreparedStatement` carries no consistency level, so the port sets it per bound statement or gives the transaction path its own execution profile. &emsp;Dropped, every Accord transaction fails at the driver.

### F6: the one-run-at-a-time idiom is written thirteen times

`acquire(blocking=False)`, followed by a 409 when it fails, appears at thirteen call sites over three lock objects: five in `sql_console.py`, seven in `transactions.py`, one in `query.py:477`. &emsp;The comments are emphatic that it must not queue, and they are right: a queued caller would be timed while the run ahead of it finished, so the number it reported would be the wrong number rather than a late one.

The port takes one `SingleRunGate` over `ReentrantLock`, using **`tryLock()` with no arguments**, and the reason is narrower than it first looks. &emsp;`tryLock(0, TimeUnit.SECONDS)` does not block: measured on Zulu 25.0.2, it returned false in 8 to 254 µs while another thread held the lock. &emsp;What it does instead is honour fairness. &emsp;Over 2,000 rounds with a waiter queued and the lock then released, on a fair lock `tryLock()` got in ahead of the waiter 457 times and `tryLock(0, SECONDS)` never once; on the default unfair lock both barged, 344 and 253 times. &emsp;It also throws `InterruptedException`, which the caller must then decide something about.

So on `new ReentrantLock()` the two behave alike, and the argument for the no-argument form is that it cannot be turned into a queue by a later edit adding `true` to the constructor. &emsp;The test asserts the property the demo needs rather than the mechanism: a second caller arriving while the first holds the gate gets 409 and is not timed waiting.

### F7: the DataFusion version pin is a three-way agreement enforced by a document

`backend/dist/VENDOR.md:37` records it: the `datafusion` crate the wheel was compiled against, `datafusion-ffi`, and the `datafusion` wheel in `requirements.txt` must all be 54, because `FFI_TableProvider` is `#[repr(C)]` and each side reads the capsule as the struct its own build declared. &emsp;Raising one is not a type error; it is a segfault. &emsp;The pin exists only because Python owns the DataFusion `SessionContext`, and `VENDOR.md:39` says outright that a prebuilt wheel has no lockfile here to hold the Rust side.

Moving the boundary removes the class of failure rather than restating it. &emsp;The Arrow C Data Interface **is** a stable specification with a documented struct layout and a versioning rule, where `FFI_TableProvider` is an internal type that happens to be `repr(C)`. &emsp;With rows crossing as `ArrowArrayStream`, the crate's Arrow version and Arrow Java's need not match, and the three-way pin becomes one runtime assertion: `cqlite_abi_version()` checked at library load, with registration refused on a mismatch. &emsp;That is a stronger guarantee than the wheel had, and the new `VENDOR.md` should say so in those words.

### F8: the sink duplicates the backend's geometry, and the duplication is undefended

Five functions exist twice, identically named: `parse_wkt_polygon`, `haversine_distance_m`, `compute_bearing_deg`, `point_in_polygon` and `distance_to_polygon_m`, at `backend/app/utils/geometry.py:17-99` and `ingress/consumer/consumer.py:606-662`. &emsp;The bucket arithmetic is duplicated too, and `query.py:818` has a comment admitting it: "The same arithmetic as event_bucket() in ingress/consumer/consumer.py".

The duplication is deliberate and it protects two things worth protecting: the sink's image does not carry the backend's package, and a change made for the dashboard does not silently change what the sink writes. &emsp;What it does not protect against is the two drifting, and they have already drifted once: `parse_wkt_polygon` strips `(wkt or "")` in the sink and `wkt` in the backend, so a null answers an empty ring on one side and raises on the other. &emsp;The sink is where that matters, because `consumer.py:811` passes a nullable `polygon_wkt` column straight into it. &emsp;If they drift again then live alerts and the dashboard's what-if page disagree with nothing to notice it.

A zero-dependency `htap-common` module keeps both properties and removes the drift, because the sink depends on the library and not on the backend. &emsp;One rule holds it, enforced in the POM rather than asked for in a comment: **the module may declare no dependency outside test scope, transitively included.** &emsp;Stated as an allowlist because that is how the enforcer states it: naming the scopes to ban lets `system` through, and a rule short of one scope is a rule that passes the case it was written for. &emsp;Test scope holds `junit-jupiter` and reaches neither the sink's image nor the backend's. &emsp;`java.lang.Math`, `java.time` and `java.util.zip.CRC32` are enough, and `CRC32` is zlib's crc32, so `event_shard` ports exactly. &emsp;The moment that module wants Jackson or a driver, the coupling the duplication avoided has come back.

### F9: query.py is 1,054 lines holding four separable concerns

SQL validation and rewriting; the engine registry; the comparison orchestrator with its lock and its four cancel mechanisms; and the natural-language translator. &emsp;They share a file and almost nothing else. &emsp;`transactions.py` at 1,021 lines and `sql_console.py` at 701 are large for the same reason, though less separably.

Four Java classes, and the orchestrator gets the tests, since it is the one holding state that a wrong edit corrupts silently.

### F10: CI read the backend's settings out of the process

Two dashboard assertions ran `podman exec backend python -c 'from app.config import settings; …'` to learn the data directory the cqlite reader parses and the socket timeout the bulk reader derives its snapshot TTL from. &emsp;Reading the running configuration is right. &emsp;Reaching into the process to do it ties the test suite to the language the backend happens to be written in, and a port would have had to rewrite an assertion that has nothing to do with the port.

Answered before any Java landed, so that it stands alone: `SPARK_QUERY_TIMEOUT_S` is now declared in compose beside `CQLITE_DATA_DIR`, and both reads are `podman exec backend printenv`. &emsp;Running the step against the live stack found a second defect in the same lines, recorded under [F10 in the table below](#which-commit-answers-which-finding).

### F11: 51 settings sit in one flat class and a typo is silent

`backend/app/config.py` holds 51 fields in one `Settings` class with no grouping, environment names implicit in the upper-cased field name, and `extra="ignore"` at `:182`. &emsp;So `SPARK_QUERY_TIMOUT_S` in a compose file is accepted, ignored, and takes the default; nothing reports it.

The port takes `@ConfigMapping` groups with explicit names. &emsp;Whether that is enough to catch the typo is a measurement and not a certainty: every setting here arrives as an environment variable, and an environment-sourced name has to be normalised back into a dotted key before anything can judge it unknown, which is the weakest case for SmallRye Config's unknown-property validation. &emsp;So set `SPARK_QUERY_TIMOUT_S` against the Quarkus backend, record whether the container starts, and say which in the commit that lands the config classes.

### F12: models.py is 781 lines and 56 classes, and one request model lives elsewhere

No grouping by route, and `SqlRequest` sits at `routes/sql_console.py:116` rather than with the other 56.

Of the 57 route decorators, 24 declare no `response_model`. &emsp;Seven of those return a model by annotation, at `transactions.py:377, 410, 820, 829, 845, 875, 938`, so their field names are still written down somewhere. &emsp;Two are not JSON: the CSV `Response` at `overview.py:40` and the NDJSON `StreamingResponse` at `query.py:515`. &emsp;That leaves **fifteen JSON routes whose field names exist only in the body of the function**, and they are the capture list, not a number:

| File | Route |
| --- | --- |
| `demo.py` | `/trigger-breach-scenario`, `/latency` |
| `map.py` | `/drone/{entity_id}/nearby` |
| `overview.py` | `/ingestion-history`, `/resync` |
| `query.py` | `/window`, `/engines` |
| `settings.py` | `/demo/cleanup` |
| `sql_console.py` | `/status` |
| `transactions.py` | `/session/schema`, `/session/{user_id}/{session_id}`, `/session/open`, `/clearance/reset` |
| `vector.py` | `/index-all` |
| `zones.py` | `` (the router's own prefix) |

Capture each from the running Python service before porting it, and diff the Java answer's keys against the capture. &emsp;An undercount here is the one mistake in this document that would leave a route's shape unprotected, which is why the list replaces the count.

Records, one package per route group, all inside the backend module since nothing else reads them.

### F13: compute_bearing_deg has no caller in the backend

`backend/app/utils/geometry.py:99` is dead. &emsp;The sink's copy at `consumer.py:635` is live, called at `:741` to derive `heading_deg`. &emsp;Nothing in the frontend or the backend calls the backend's.

Under F8 it moves to `htap-common`, where the sink's caller keeps it live. &emsp;The finding is recorded because it is evidence for F8 rather than against it: a function copied into two places and used in one is what drift looks like at its earliest stage.

### F14: the backend's healthcheck is a Python one-liner

`services.backend.healthcheck.test` in `podman-compose.yml` runs `python -c "import urllib.request; …"`. &emsp;The key path rather than a line number, because this branch moves that line and a stale number sends a reader to the wrong service. &emsp;A temurin base carries no HTTP client at all, so the runtime stage must install one and the check must change in the same commit as the image. &emsp;Probed rather than assumed, on 2026-08-28: `podman run --rm docker.io/library/eclipse-temurin:25-jre sh -c 'for b in curl wget python3; do command -v $b || echo absent; done'` answers `absent` three times, and `:25-jdk` answers the same; both are Ubuntu 26.04. &emsp;Worth naming separately because it is the one part of the port that fails as a healthcheck timeout rather than as an error, and a timeout is the least informative failure this stack produces.

### F15: the window endpoint opens two Kafka clients per request

`e17ad33` added `_sink_consumed_past` at `query.py:849`, which builds a `KafkaConsumer` and a `KafkaAdminClient` on the request thread. &emsp;Its own commit message measures the cost: `/api/query/window` goes from about 20 ms to 550 ms, of which 520 ms is opening the two clients, 109 ms for the consumer and 310 ms for the admin client, against 1 to 3 ms for the queries.

That trade was made deliberately and correctly: the endpoint is read once per page load, beside comparisons taking 8 to 40 s, and one client cannot do both jobs, since a consumer carrying the sink's group refuses a request timeout below its 10 s session timeout and then blocks past 74 s inside `committed()` with no assignment. &emsp;The finding is not that the trade is wrong. &emsp;It is that a Java port should not reproduce the per-request construction unexamined: two long-lived clients on the backend's own group would remove 520 ms from an endpoint the Explore page polls, and whether they can be long-lived without committing over the sink's progress is a question to settle with a measurement rather than by translating the Python. &emsp;Until that measurement exists, the port copies the Python's arrangement, including its reason for not sharing one client.

## What the port must not change

Captured here because most of these break a page with no compile error anywhere. &emsp;Two bullets name routes nothing calls, and each says so: there the capture-and-diff below is the only check that exists, and a reader should not spend effort defending a shape no caller reads.

- The 44 route paths the frontend calls, exactly. &emsp;The backend declares more than that: 57 decorators over 55 distinct paths. &emsp;Declaration order is free, and that was checked rather than assumed: no two of the 57 share a method and a segment count with a literal in one slot and a parameter in the other, so no pattern can swallow a literal. &emsp;`/session/schema` and `/session/{user_id}/{session_id}` differ in segment count and cannot collide whichever is declared first.
- Seven POST routes take **query parameters and no body**: `session/step`, `session/open`, `session/demo`, `clearance/grant`, `clearance/release`, `clearance/contend`, `clearance/demo`. &emsp;A natural port invents request bodies the frontend does not send. &emsp;Three have a caller: `session/demo`, `clearance/contend` and `clearance/demo`, each from `frontend/src/pages/transactions/AccordPanel.tsx` and each from one step of the compose workflow. &emsp;The other four appear in no file under `frontend/src` and in no step of `.github/workflows/test-podman-compose.yaml`, so a changed shape breaks nothing and the capture is the whole protection.
- `/drone/{entity_id}/nearby` likewise has no caller anywhere outside `backend/`, and F12 lists it as a shape to capture. &emsp;Five uncalled routes is the same evidence F13 records against one uncalled helper, so read this bullet as a reason to check the shape cheaply rather than as a page to protect.
- Errors as `{"detail": "..."}`, from an exception mapper, since `api.ts:79` reads that field and nothing else.
- `POST /api/query/benchmark/stream`: newline-delimited JSON, `X-Accel-Buffering: no`, `Cache-Control: no-store`, flushed per line, and the one-run gate released when the client disconnects. &emsp;`postNdjson`, `api.ts:32`, reads it with `response.body.getReader()` at `api.ts:50` and holds a partial line across chunks, so a line split anywhere is already handled; what it cannot survive is a response buffered to the end.
- `GET /api/overview/ingestion-history/csv`: `text/csv` with the same `Content-Disposition`.
- Every JSON field name in the 56 models, and in the fifteen routes F12 tabulates, whose names exist only in the body of the function.

The mechanical protection is cheap and is the only one available: capture all 44 endpoints from the Python backend, then diff the Java answers with `jq -S`. &emsp;Field names and types must match; values differ wherever the data moved.

## What must be measured rather than carried over

Every number this repository states is a measurement from a real run, and the port invalidates several of them. &emsp;Re-measure or delete; do not carry over.

| Figure | Why the port changes it |
| --- | --- |
| the cqlite timings, per split and per key chunk | the rows now cross a different FFI boundary |
| the memory figures, and the `anon` guidance | a JVM heap is anonymous memory in the same cgroup, so `anon` no longer means what `CLAUDE.md` says it means |
| the CDC delay figures | they depend on the sink's write rate, and the sink is being rewritten |
| the Accord medians | they depend on the driver |
| the producer's sustained rate | 2,000 events a second is what `producer.py`'s send loop reached, and that loop is per-event Python around a vectorised numpy batch: one dict, one timeuuid, one `orjson.dumps` and one `send` for each event of a 50 ms window. &emsp;A Java loop is not that loop, and the requested rate was never the achieved one: `CLAUDE.md` records a runner producing 1,899/s of the 2,000 asked for |

Three things the port might get wrong in a way no existing assertion would catch, so each needs its own measurement:

- **The v1 timeuuid.** &emsp;`Uuids.startOf(millis)` fixes clock_seq and node to constants, so two events in the same millisecond mint the *same* UUID. &emsp;The primary key is `((event_bucket, shard), event_id)`, so that is a silently overwritten row, and at 2,000 events a second the producer stamps them 0.5 ms apart. &emsp;`htap-common` mints its own, matching `cassandra.util.uuid_from_time`: a 60-bit count of 100 ns intervals since 1582-10-15, the version and variant bits, and a **random** 14-bit clock_seq and 48-bit node. &emsp;The node's multicast bit is left as drawn, which RFC 9562 would have set, for the one reason that `uuid_from_time` leaves it as drawn: it forces no bit, measured on driver 3.30.1 in the running backend as 1,994 of 4,000 draws with bit 40 of the node set, that bit being the least significant of the node's first octet and so the one the RFC names. &emsp;Every reference assertion binds the node explicitly, six of them over five distinct UUIDs, so none of those would have caught a Java that forced the bit; `theMulticastBitIsLeftAsDrawn` is the assertion that does, requiring both values of the bit over a thousand draws. &emsp;Assert a million distinct UUIDs at the producer's cadence, and assert the intended millisecond round-trips, since the sink derives `event_time` from `event_id`.
- **BLAKE2b.** &emsp;`vector.py` uses `hashlib.blake2b(digest_size=8)`, which the JDK does not have. &emsp;A different function turns cosine similarity into noise, and the CI vector assertion then passes or fails arbitrarily. &emsp;Check a fixed corpus and its Python-produced vectors into a test resource and assert byte equality.
- **Cross-path value spelling.** &emsp;The workflow asserts the five paths agree value for value. &emsp;A JDBC driver returning `java.sql.Timestamp` and `BigDecimal` where the Python client returned natives will spell those differently in JSON. &emsp;Run one row through all five Java paths and assert the JSON strings are identical.

Three places Java offers a better mechanism than the Python had. &emsp;Each is an improvement to measure, not to assume:

- **Presto.** &emsp;`Statement.cancel()` issues the coordinator's DELETE for one query, where `query.py` kills every query belonging to its user and so can kill an unrelated one. &emsp;Keep the REST listing, which the Health page needs because it shows other callers' queries too.
- **Spark.** &emsp;`Statement.cancel()` reaches HiveServer2's `CancelOperation` and is callable from another thread, where PyHive had nothing and the Python resorts to `shutdown(SHUT_RDWR)`. &emsp;It *should* make HiveServer2 cancel the job group itself. &emsp;Keep `spark_ui.kill_jobs_for` and assert whether any job survives a cancel; delete it only when a measurement shows zero.
- **cqlite.** &emsp;Cancel per statement, where the Python cancels every provider including one another thread is scanning. &emsp;Moving the per-scan figures from the provider to the statement also removes the substring test that guesses which table a statement touched by looking for its name in the SQL.

## Which commit answers which finding

| Finding | Answered by |
| --- | --- |
| F1 one process | |
| F2 the no-JVM claim | |
| F3 the engine list | |
| F4 no test suite | every commit that adds a class, by the standing rule. &emsp;`Add a Maven reactor, a zero-dependency common module, and its tests` is where the first suite exists and where `.github/workflows/java-tests.yaml` starts running it on every push, so a later commit's tests fail a build rather than waiting for someone to run them |
| F5 six clients | |
| F6 the one-run idiom | |
| F7 the version pin | |
| F8 duplicated geometry | `Add a Maven reactor, a zero-dependency common module, and its tests`, in part: `htap-common` holds the five geometry functions and the bucket and shard arithmetic, and its POM refuses any dependency outside test scope, by an allowlist rather than by naming scopes, so the module stays reachable from every service. &emsp;Open until the sink and the backend each read them from it instead of from a copy |
| F9 query.py | |
| F10 CI reading the process | `Read the backend's two settings from its environment, not its internals`, which also fixed a guard in those lines that could not fire: `ls "$DIR" \| head -5 \|\| exit 1` takes its status from `head`, so the step printed "cannot access" and reported PASS. &emsp;Verified both ways on the running stack |
| F11 flat settings | |
| F12 models.py | |
| F13 the dead helper | |
| F14 the healthcheck | |
| F15 two Kafka clients per request | copied as it stands; the measurement that would justify sharing them is not done |

The table is filled in as the branch lands, and the last commit closes it.
