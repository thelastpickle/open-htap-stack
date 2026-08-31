"""Backend configuration, read from the environment (see podman-compose.yml)."""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Cassandra
    cassandra_host: str = "cassandra"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "demo"
    # Named in the bulk reader's options; it addresses nodes by datacenter.
    cassandra_datacenter: str = "datacenter1"
    # The Sidecar runs beside Cassandra, on the same host as far as this backend is
    # concerned.  Used to size the snapshot a bulk read is about to stream, so the
    # result can say how much data it went through.
    sidecar_port: int = 9043

    # How demo.events is partitioned: a window of this many minutes, spread over
    # this many shards.  The sink writes with these values and the compare page has
    # to name them, so compose sets both services from one declaration.  Read here
    # rather than inferred from the data, because inferring the shard count from
    # what happens to be present would silently narrow a query.
    event_bucket_minutes: int = 15
    event_shards: int = 16
    # When the backend runs on the host rather than inside the compose network,
    # the driver discovers the node's broadcast address (172.20.0.10) and cannot
    # reach it.  Set this to 127.0.0.1 to rewrite every discovered address to the
    # published port instead.  Empty means "no translation", which is correct
    # in-network.
    cassandra_translate_addresses_to: str = ""

    # Presto
    presto_host: str = "presto"
    presto_port: int = 8080
    presto_user: str = "htap-mission-control"
    presto_catalog: str = "cassandra"
    presto_schema: str = "demo"

    # Spark Thrift Server (HiveServer2).  The client speaks the plain transport,
    # matching the hive.server2.authentication=NOSASL the spark service starts
    # with; that needs no SASL library on either side.
    spark_thrift_host: str = "spark"
    spark_thrift_port: int = 10000
    # A Spark job has no other deadline, so this socket timeout is what stops a
    # stuck query from hanging the dashboard.  It bounds how long the server may go
    # without answering, which is not the same as how long the query may take: it is
    # a threshold for "nothing is coming back", not a budget.  Set for the contended
    # case rather than the typical one, because a scan of the whole history that
    # answers in 113s alone was still working after 180s with three other paths
    # beside it.  The bulk reader's snapshot TTL is derived from this value (see
    # db/spark_client.py), and nginx allows longer still in front of it.
    spark_query_timeout_s: int = 900

    # The cqlite reader, which is a library in this process rather than a service.
    # It parses the SSTable files under this directory in place, so the path must
    # be the one compose mounts the Cassandra data directory at, read-only.
    cqlite_data_dir: str = "/var/lib/cassandra/data"
    # How many slices of the token ring a full scan divides into.  One, because
    # most of a slice is work every other slice repeats.  cqlite's BTI route drains
    # the data section sequentially with no partition-index seek, so each slice
    # re-reads and re-parses the whole file and only the row decode divides.
    # Swept once per SSTable version, in CPU time because the host was too loaded
    # for a wall clock: two "ea" generations of 180,672,491 bytes took 9.53 s,
    # 14.05 s, 23.83 s and 38.68 s at one, two, four and seven slices, and one
    # 203.7 MB "da" generation of 1,102,576 rows took 11.79 s, 22.05 s, 40.33 s
    # and 73.01 s.  Solving N*P + R from the two- and four-slice points puts the
    # repeated part at 53% and 71%, so the best a split can do is under 2x wall
    # clock for N times the CPU; on seven shared cores the wall clock in fact
    # rose, 14.05 to 17.34 s on "ea" and 6.0-7.4 to 11.2-12.4 s on "da".  Memory
    # does not bind: the walk merger streams, and peak resident stayed at 35 to
    # 39 MB at every "da" slice count.  Raise this only when the walk seeks to
    # its slice through Partitions.db rather than draining past it.
    cqlite_splits: int = 1
    # Rows per Arrow record batch handed to DataFusion.
    cqlite_batch_rows: int = 8192
    # How many of the partitions a query names are read at a time.  One, because
    # cqlite's seek merger decodes every row of every partition it is given before
    # the merge starts, at about 3.9 GB of anonymous memory per million rows on
    # either SSTable version: a 16-partition window of 1.78M "da" rows held
    # 6.83 GB read together, and a second such query crossed the container's 8 GB
    # limit, where the kernel killed this process; 1.25M "ea" rows held 4.84 GB.
    # One partition at a time held 1.41 GB and 1.09 GB, and was the faster of the
    # two in both sweeps.  Raise it only to measure that again.
    cqlite_key_chunk: int = 1

    # cassandra-sql, which speaks the Postgres wire protocol.  5432 is not a
    # setting on either side: it is a private static final in the service's
    # PostgresProtocolServer, so it is named here only to be addressed.  The
    # database and user names go unchecked by the service, and psycopg insists on
    # sending both.  A short connect timeout because this is a demo panel and the
    # service is either up or it is not.
    accord_sql_host: str = "accord-sql"
    accord_sql_port: int = 5432
    accord_sql_database: str = "cassandra_sql"
    accord_sql_user: str = "htap-mission-control"
    accord_sql_connect_timeout_s: float = 5.0

    # Kafka — the platform health probe, and the CDC tail below
    kafka_host: str = "kafka"
    kafka_port: int = 19092

    # The topic the producer writes and the group the sink consumes it under.  The
    # window's "settled" flag reads both: whether the sink has consumed everything
    # that could still land in a closed window is a question only Kafka can answer,
    # since the sink files each event under the event's own timestamp.  Declared in
    # compose beside the sink that uses them, so the two cannot name different ones.
    events_topic: str = "demo-events"
    sink_group_id: str = "demo-cassandra-sink"
    # How long the settled check waits on Kafka before it gives up and reports that
    # it could not tell.  The compare page blocks on this while it loads, so a
    # broker that has stopped answering must cost a second and not a page.
    kafka_offsets_timeout_s: float = 5.0

    # Change Data Capture.  The Sidecar publishes demo.drone_latest_status mutations
    # to this topic as Confluent-framed Avro, and the Streaming page shows what
    # arrives.  The registry is Apicurio's Confluent-compatible endpoint, and the
    # backend needs it for the same reason any consumer does: a record carries the id
    # of its schema, not the schema.
    cdc_topic: str = "cdc-mutations"
    cdc_schema_registry_url: str = "http://apicurio:8080/apis/ccompat/v7"
    # How many mutations the tail keeps.  A ring buffer, so watching the stream costs
    # a fixed amount of memory however long the page is left open; the point of the
    # page is the latest mutations, and a log of every mutation is what the topic
    # already is.
    cdc_buffer_size: int = 200
    # Seconds the consumer waits for records before it loops.  It runs in a thread,
    # so this is only how promptly the loop notices a shutdown.
    cdc_poll_timeout_s: float = 1.0

    # Spark master UI — used by the platform health probe only
    spark_ui_host: str = "spark"
    spark_ui_port: int = 8080
    # The Spark *application* UI, which is a different server on a different port:
    # the master at 8080 knows which applications exist, and this one knows what
    # the Thrift Server's application is doing.  The Health page reads running jobs
    # from its REST API and cancels them through it.
    spark_app_ui_port: int = 4040

    # Optional: an OpenAI-compatible embeddings endpoint for vector search.
    # Without a key the backend falls back to a local hashing embedder, which
    # keeps the demo self-contained (see app/routes/vector.py).
    openai_api_key: str = ""
    openai_base_url: str = "https://api.openai.com/v1"
    embedding_model: str = "text-embedding-3-small"

    # Live embedding: keep drone_text_embeddings following the snippets the sink
    # writes, instead of embedding once on demand.  Off at startup, because it is a
    # claim the demo should make deliberately rather than by default: the work runs
    # behind the writes and never in them, so an operator turning it on should see
    # the point-read latency stay where it was.
    vector_live_embeddings: bool = False
    # How long the loop waits between passes.  The producer rotates each asset's
    # snippet every 5 to 30 seconds, so a shorter interval would mostly find
    # nothing, and a much longer one would let the index fall behind visibly.
    vector_live_interval_s: float = 5.0
    # Most assets embedded in one pass.  A bound rather than the whole fleet, so a
    # pass stays short at 2,000 assets and the loop keeps reporting; whatever it
    # defers is embedded by the next pass, and the status says how much that is.
    vector_live_max_per_cycle: int = 64

    # Optional: an OpenAI-compatible chat endpoint for natural-language → SQL.
    openrouter_api_key: str = ""
    openrouter_model: str = "openai/gpt-4o-mini"

    # Demo defaults.  Compose sets these from the same variables it passes to the
    # data producer, so the Settings page opens showing what is actually running.
    #
    # Five a second, matching what the producer is given.  The figures in the docs were
    # measured at 2,000 and the Settings page reaches 5,000: the demo starts as a
    # trickle and is turned up for the run it is being shown in, because nothing
    # downstream of the generator bounds the data and a stack left at 2,000 fills a
    # laptop's disk in an afternoon.
    demo_events_per_sec: int = 5
    demo_n_entities: int = 100
    demo_max_entities: int = 2000
    demo_outlier_percent: float = 5.0

    # Which container command the dashboard should tell an operator to run.  The Health
    # page renders a restart and two log commands as copyable text, and this repository
    # runs podman, so that is the default.  A workshop attendee runs docker, and a
    # dashboard that hands them a command their machine does not have is the one failure
    # they cannot diagnose: the product told them the wrong thing.  A setting rather than
    # a build argument, because the alternative is two frontend images.
    container_cli: str = "podman"

    # API
    allowed_origins: str = "*"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
