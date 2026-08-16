# Data model

The schema is defined in one place, `ensure_schema()` in
[ingress/consumer/consumer.py](../ingress/consumer/consumer.py). That process is the only one that
has to exist for data to flow, so putting the definitions there means there is no separate migration
step and no second copy to drift out of step.

| Table                     | Shape                                                   | Read by                                      |
| ------------------------- | ------------------------------------------------------- | -------------------------------------------- |
| `events`                  | One row per event, partitioned by `event_id`             | Presto, the Spark bulk reader                |
| `drone_latest_status`     | One row per asset, the current state                     | The dashboard's map and KPIs; all three engines |
| `drone_events_by_entity`  | Per-asset history, clustered by `event_time DESC`        | Flight paths, per-asset analysis             |
| `drone_text_embeddings`   | One row per asset: its text snippet and a 1536-float vector | Vector search, through a SAI index        |
| `alerts_by_bucket`        | Alerts, partitioned by hour                              | The dashboard's alert feed                   |
| `ingestion_counts`        | Counter per 30-minute bucket                             | The ingestion volume chart                   |
| `restricted_zones`        | Zone polygons as WKT; reference data                     | The map, and the sink's proximity checks     |
| `sessions_open`, `session_seq_applied`, `session_timeline` | Supporting tables for the Accord transaction demo | See the `mck/cassandra-6` branch |

`drone_latest_status` holds one row per asset, so a full scan of it is bounded by fleet size rather
than by how long the demo has been running. That is what lets the dashboard scan it for KPIs several
times a minute without pretending Cassandra enjoys table scans.

Embeddings live in their own table deliberately: PrestoDB's bundled Cassandra driver cannot parse the
CQL `vector` type and drops the metadata for any table carrying one, so a vector column on
`drone_latest_status` would hide that table from Presto entirely. Keeping 1536 floats out of the row
the map reads every few seconds also keeps that read small.
