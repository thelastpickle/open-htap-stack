# Mission Control — the dashboard

A web dashboard over the running stack, at <http://localhost:4000>. It exists to make one claim
visible: that the transactional store, the analytical engine and the batch engine are reading the
same rows, at the same moment, with nothing copied between them.

Everything on every page is a query against the running stack. There are no fixtures, no seeded
screenshots and no invented numbers. Where a figure cannot be measured the page shows a dash.

```
                 browser :4000
                       │
                  nginx │ serves the bundle, proxies /api
                       ▼
              FastAPI backend :8000
                 │       │       │
      CQL ───────┘       │       └─────── HiveServer2
                    Presto HTTP                 │
         ┌─────────────┐ │ ┌───────────────┐    │
         │  Cassandra  │◄┴─┤    Presto     │    │
         │             │◄──┤ Spark (Thrift)│◄───┘
         └─────────────┘   └───────────────┘
```

## The pages

| Page         | What it shows                                                              | Where the data comes from                                                        |
| ------------ | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| **Overview** | Fleet KPIs, ingestion volume, service health, the latest alerts            | One bounded scan of `drone_latest_status`, plus the `ingestion_counts` counters    |
| **Map**      | Live positions, restricted airspace, and an asset's recorded flight path    | `drone_latest_status` for positions; `drone_events_by_entity` for the path         |
| **Alerts**   | Zone-proximity and breach alerts, newest first                             | `alerts_by_bucket`, read one hourly partition at a time                           |
| **Explore**  | SQL console, vector search, and the three-engine comparison                 | Whichever engine you pick; all three read the same Cassandra tables               |
| **Health**   | Per-service reachability and latency by access path                        | A TCP probe per service, and one timed query per path                             |
| **Settings** | Fleet size, event rate, outlier share, pause, and the breach scenario      | Held in the backend; the data producer polls and adopts them                      |

## The comparison that matters

Explore → **Three-engine compare** runs one statement on all three engines and reports what each
took. The statement is rewritten per dialect, and the rewrite is shown above each result, so the
comparison is inspectable rather than asserted.

The engines are not interchangeable, and that is the point:

- **Cassandra** answers by partition. A point read is a few milliseconds; it has no joins, no
  ordering on arbitrary columns and no aggregates beyond counting.
- **Presto** plans a distributed scan over the same rows through its Cassandra connector. Full SQL,
  a couple of hundred milliseconds for this data.
- **Spark** starts a job. Full SQL again, seconds rather than milliseconds for a query this small,
  and the engine you want when the query is large rather than quick.

Change the query and the ordering changes with it. That is the honest result, and a more useful one
than a single number.

## Vector search

Each asset carries a snippet of prose on some unrelated subject, sampled by the producer from
`ingress/producer/wikipedia.txt`. Explore → **Vector search** embeds your phrase, asks Cassandra's
SAI index for the nearest neighbours from `drone_text_embeddings`, scores each with
`similarity_cosine`, then point-reads each matching asset for its live position. One search
therefore exercises the analytical index and the transactional path together.

Press **Build embeddings** once to populate the table; nothing is indexed until then.

With `OPENAI_API_KEY` set the backend embeds through that endpoint. Without one it uses a local
hashing embedder — no key, no network, and matching that is lexical rather than semantic, but real,
ranked and reproducible.

### Why embeddings live in their own table

`drone_text_embeddings` is separate from `drone_latest_status` for two reasons:

1. PrestoDB's bundled Cassandra driver cannot parse the CQL `vector` type, and drops the metadata
   for the whole table when it meets one. A vector column on the live-status table would make that
   table invisible to Presto, taking the analytical half of the demo with it.
2. An embedding is 1536 floats. Keeping it out of the row the map reads every few seconds keeps that
   read small.

## Demo controls

The Settings page writes to the backend's memory; the data producer polls
`GET /api/settings/demo` every ten seconds and adopts what it finds. Every control there changes
what the stack generates:

- **Fleet size** — assets emitting telemetry, up to the producer's `MAX_ENTITIES`.
- **Events per second** — total ingest rate across the fleet.
- **Overheating assets** — the share of the fleet running an anomalous internal temperature, so the
  outlier queries on Explore have something to find.
- **Pause** — stops generation; stored data stays put.
- **Trigger breach scenario** — flags a real airborne asset as breaching and writes a matching
  alert, which the map, the KPIs and the alert feed then pick up through their ordinary queries.
- **Truncate `drone_latest_status`** — after reducing the fleet size, retired assets keep their last
  row and the KPIs keep counting them. This clears them; history and the zones are untouched.

Nothing here is persisted. Restarting the backend returns the demo to the values the compose file
declares, and the producer follows within a poll cycle.

## Running the dashboard from source

The compose file builds and serves both halves, so this is only for working on them.

```shell
# Backend.  Reaching Cassandra from the host means the driver discovers the node's
# in-network broadcast address, so tell it to use the published port instead.
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
CASSANDRA_HOST=localhost CASSANDRA_TRANSLATE_ADDRESSES_TO=127.0.0.1 \
  PRESTO_HOST=localhost PRESTO_PORT=8088 \
  SPARK_THRIFT_HOST=localhost KAFKA_HOST=localhost KAFKA_PORT=9092 SPARK_UI_HOST=localhost \
  .venv/bin/uvicorn app.main:app --reload --port 8000
```

```shell
# Frontend.  Vite proxies /api to localhost:8000, so no CORS and no compiled-in host.
cd frontend && npm install && npm run dev
```

The API documents itself at <http://localhost:8000/docs>.

## Resetting the data

```shell
scripts/cleanup-data.sh              # truncate the generated tables, keep the stack up
./stop-and-clean-data-and-schema.sh  # stop everything and clear the data directories
```
