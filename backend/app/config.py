"""Backend configuration, read from the environment (see podman-compose.yml)."""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Cassandra
    cassandra_host: str = "cassandra"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "demo"
    # Named in the bulk reader's options; it addresses nodes by datacenter.
    cassandra_datacenter: str = "datacenter1"
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

    # Kafka — used by the platform health probe only
    kafka_host: str = "kafka"
    kafka_port: int = 19092

    # Spark master UI — used by the platform health probe only
    spark_ui_host: str = "spark"
    spark_ui_port: int = 8080

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
