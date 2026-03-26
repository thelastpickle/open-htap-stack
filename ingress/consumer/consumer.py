import json
import os
import time
import uuid
from datetime import datetime

from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import datetime_from_uuid1
from kafka import KafkaConsumer


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def connect_cassandra(host: str, port: int):
    # No auth by default; if you add auth later, extend via env vars.
    cluster = Cluster([host], port=port)
    session = cluster.connect()
    return cluster, session


def ensure_schema(session, keyspace: str, table: str):
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': 1 }};
        """
    )
    session.set_keyspace(keyspace)

    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          entity_id text,
          event_day date,
          event_id timeuuid,
          event_time timestamp,
          event_type text,
          observer_id text,
          latitude double,
          longitude double,
          altitude_m float,
          temp_external_c float,
          temp_internal_c float,
          text_payload text,
          PRIMARY KEY (event_id)
        );
        """
        # TODO: PRIMARY KEY ((entity_id, event_day), event_id)
        #
        # WITH transactional_mode = 'full';
    )
    # These are for the Accord transactions the demo "exactly-once in-order session timeline projections"
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS demo.sessions_open (
          user_id text,
          session_id uuid,
          PRIMARY KEY ((user_id), session_id)
        );
        """
        # WITH transactional_mode = 'full';
    )
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS demo.session_seq_applied (
          user_id text,
          session_id uuid,
          seq bigint,
          PRIMARY KEY ((user_id, session_id), seq)
        );
        """
        # WITH transactional_mode = 'full';
    )
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS demo.session_timeline (
          user_id text,
          session_id uuid,
          seq bigint,
          event_id timeuuid,
          event_time timestamp,
          event_type text,
          payload text,
          PRIMARY KEY ((user_id, session_id), seq)
        );
        """
        # WITH transactional_mode = 'full';
    )


def parse_ts(ts: str):
    # Expect ISO 8601 with timezone (producer emits UTC ISO)
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return datetime.utcnow()


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
    topic = os.getenv("TOPIC", "demo-events")
    group_id = os.getenv("GROUP_ID", "demo-cassandra-sink")

    cass_host = os.getenv("CASSANDRA_HOST", "cassandra")
    cass_port = env_int("CASSANDRA_PORT", 9042)
    keyspace = os.getenv("KEYSPACE", "demo")
    table = os.getenv("TABLE", "events")

    batch_size = max(1, env_int("BATCH_SIZE", 200))
    log_every = max(100, env_int("LOG_EVERY", 2000))

    print(
        f"[sink] kafka={bootstrap} topic={topic} group_id={group_id} "
        f"cassandra={cass_host}:{cass_port} {keyspace}.{table} batch_size={batch_size}"
    )

    # Wait/retry Cassandra until it's ready
    cluster = None
    session = None
    while True:
        try:
            cluster, session = connect_cassandra(cass_host, cass_port)
            ensure_schema(session, keyspace, table)
            print("[sink] cassandra connected and schema ensured")
            break
        except Exception as e:
            print(f"[sink] cassandra not ready yet: {e}")
            time.sleep(5)

    insert_cql = session.prepare(
        f"INSERT INTO {table} (entity_id, event_day, event_id, event_time, event_type, observer_id, latitude, longitude, altitude_m, temp_external_c, temp_internal_c, text_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    insert_cql.consistency_level = ConsistencyLevel.QUORUM

    # Kafka consumer
    consumer = None
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                consumer_timeout_ms=0,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                max_poll_records=batch_size,
            )
            print("[sink] kafka consumer started")
            break
        except Exception as e:
            print(f"[sink] kafka not ready yet: {e}")
            time.sleep(5)

    buffered = 0
    total = 0
    window_count = 0
    last_report = time.time()

    while True:
        records = consumer.poll(timeout_ms=1000, max_records=batch_size)
        if not records:
            continue

        for _, msgs in records.items():
            for msg in msgs:
                evt = msg.value
                try:
                    event_id = uuid.UUID(evt.get("event_id"))
                    # Extract timestamp from timeuuid
                    event_time = datetime_from_uuid1(event_id)
                except Exception:
                    event_id = uuid.uuid4()
                    event_time = datetime.utcnow()

                event_day = event_time.date()
                entity_id = str(evt.get("entity_id", ""))
                event_type = str(evt.get("event_type", ""))
                observer_id = str(evt.get("observer_id", ""))
                pos = evt.get("position", {})
                latitude = float(pos.get("lat", 0.0))
                longitude = float(pos.get("lon", 0.0))
                altitude_m = float(evt.get("z_m", 0.0))
                temp_external_c = float(evt.get("temp_external_c", 0.0))
                temp_internal_c = float(evt.get("temp_internal_c", 0.0))
                text_payload = str(evt.get("text", ""))

                session.execute_async(insert_cql, (entity_id, event_day, event_id, event_time, event_type, observer_id, latitude, longitude, altitude_m, temp_external_c, temp_internal_c, text_payload))
                buffered += 1
                total += 1
                window_count += 1

        # Commit offsets after successful writes
        consumer.commit()
        buffered = 0

        # Report every 5 seconds (like producer)
        now = time.time()
        if now - last_report >= 5.0:
            elapsed = now - last_report
            rate = window_count / elapsed if elapsed > 0 else 0
            print(f"[sink] total_inserted={total} (~{rate:.1f}/s)")
            window_count = 0
            last_report = now


if __name__ == "__main__":
    main()
