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
    # more is measured as a loss on Cassandra 5 files: cqlite 0.16 filters a token
    # bound after its BTI walk rather than pushing the bound into it, so N slices
    # read the whole ring N times.  Raise it only to measure that.
    cqlite_splits: int = 1
    # Rows per Arrow record batch handed to DataFusion.
    cqlite_batch_rows: int = 8192

    # Kafka — used by the platform health probe only
    kafka_host: str = "kafka"
    kafka_port: int = 19092

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

    # Optional: an OpenAI-compatible chat endpoint for natural-language → SQL.
    openrouter_api_key: str = ""
    openrouter_model: str = "openai/gpt-4o-mini"

    # Demo defaults.  Compose sets these from the same variables it passes to the
    # data producer, so the Settings page opens showing what is actually running.
    demo_events_per_sec: int = 2000
    demo_n_entities: int = 100
    demo_max_entities: int = 2000
    demo_outlier_percent: float = 5.0

    # API
    allowed_origins: str = "*"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
