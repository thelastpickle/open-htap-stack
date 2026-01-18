import json
import os
import random
import string
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def rand_payload(n: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))


def try_create_topic(bootstrap: str, topic: str, partitions: int = 6, replication: int = 1) -> None:
    # Best-effort: if auto-create is enabled, this is optional, but creating explicitly helps.
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="topic-bootstrap")
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=replication)])
        admin.close()
        print(f"[producer] created topic={topic} partitions={partitions} rf={replication}")
    except TopicAlreadyExistsError:
        print(f"[producer] topic already exists: {topic}")
    except Exception as e:
        print(f"[producer] topic create skipped/failed (ok for demo): {e}")


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
    topic = os.getenv("TOPIC", "demo-events")
    eps = max(1, env_int("EVENTS_PER_SEC", 500))
    payload_bytes = max(0, env_int("PAYLOAD_BYTES", 256))
    client_id = os.getenv("PRODUCER_CLIENT_ID", "demo-producer")

    print(f"[producer] bootstrap={bootstrap} topic={topic} events_per_sec={eps} payload_bytes={payload_bytes}")

    # create topic if possible
    try_create_topic(bootstrap, topic)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        client_id=client_id,
        acks=1,
        linger_ms=10,
        batch_size=128 * 1024,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
        retries=10,
    )

    interval = 1.0 / float(eps)
    sent = 0
    last_report = time.time()

    while True:
        now = datetime.now(timezone.utc)
        event_id = str(uuid.uuid4())
        user_id = f"user-{random.randint(1, 100_000)}"
        event_type = random.choice(["click", "view", "purchase", "signup", "heartbeat"])
        payload = rand_payload(payload_bytes) if payload_bytes > 0 else ""

        evt = {
            "event_id": event_id,
            "ts": now.isoformat(),
            "user_id": user_id,
            "event_type": event_type,
            "payload": payload,
        }

        # Key by user_id to improve locality (optional)
        producer.send(topic, key=user_id, value=evt)
        sent += 1

        # lightweight rate control
        time.sleep(interval)

        # report every ~5 seconds
        t = time.time()
        if t - last_report >= 5.0:
            print(f"[producer] sent_total={sent}")
            last_report = t


if __name__ == "__main__":
    main()
