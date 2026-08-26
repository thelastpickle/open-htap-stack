"""Change Data Capture routes — a live tail of what the Sidecar publishes to Kafka.

Cassandra hard-links each commit log segment into ``cdc_raw`` as it is discarded.  The
Sidecar beside the node reads those segments, deserializes the mutations of a
CDC-enabled table, and publishes them to a Kafka topic as Confluent-framed Avro: one
magic byte, the four-byte id of the schema it registered, then the record.  So this
module is an ordinary Kafka consumer with a schema lookup, and nothing here touches
Cassandra: the mutations arrive from the commit log, not from a query.

Two things bound what the page can cost.  The tail keeps a fixed number of records, so
a page left open overnight holds no more memory than one just opened; and it consumes
whether or not anybody is watching, so what the page shows is what the topic did rather
than what it did since somebody looked.
"""
import asyncio
import base64
import io
import json
import time
import uuid
from collections import deque
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Deque, Dict, List, Optional

import fastavro
import httpx
from fastapi import APIRouter, Query
from kafka import KafkaConsumer, TopicPartition

from app.config import settings
from app.models import CdcRecord, CdcSchemaView, CdcStreamResponse, CdcStreamStatus

router = APIRouter(prefix="/api/streaming", tags=["streaming"])

# The Confluent wire format: byte 0 is a magic byte, then the schema id, big-endian.
_MAGIC_BYTE = 0
_HEADER_BYTES = 5
# Latency samples kept for the p50, over live records only.
_LATENCY_SAMPLES = 200
# How long the loop waits before trying again when the topic does not exist yet or the
# broker is unreachable.  The topic appears when the Sidecar publishes its first
# mutation, which on a fresh stack is after the sink has created the CDC table.
_RETRY_DELAY_S = 5.0
# The envelope the publisher wraps every row in, from cdc_generic_record.avsc.  Named
# here so that a record which carries no `payload` can still be shown as columns.
_ENVELOPE_FIELDS = frozenset(
    {
        "timestampMicros",
        "sourceTable",
        "sourceKeyspace",
        "truncatedFields",
        "version",
        "operationType",
        "isPartial",
        "updateFields",
        "range",
        "ttl",
        "payload",
    }
)


def _json_safe(value: Any) -> Any:
    """Make one decoded Avro value safe to put in a JSON response.

    Avro carries bytes, and Cassandra's blob and inet columns arrive as bytes here.
    Base64 rather than a hex string or a lossy decode, so what the page shows is
    reversible and it is obvious that it is not text.
    """
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (Decimal, uuid.UUID)):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return str(value)


def _payload_fields(avro_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The table's own columns, out of the envelope's nested ``payload`` record.

    Each column is a union of one type and null, and the publisher writes the CQL type
    it converted from onto the Avro type as a ``cqlType`` property.  Reading it back is
    what lets the page say `timestamp` where Avro says `long`.
    """
    for field in avro_schema.get("fields", []):
        if field.get("name") != "payload":
            continue
        nested = field.get("type")
        if not isinstance(nested, dict):
            return []
        columns = []
        for column in nested.get("fields", []):
            branches = column.get("type")
            branches = branches if isinstance(branches, list) else [branches]
            declared = next((b for b in branches if isinstance(b, dict)), {})
            columns.append(
                {
                    "name": column.get("name"),
                    "avro_type": declared.get("type") or declared.get("logicalType"),
                    "cql_type": declared.get("cqlType"),
                }
            )
        return columns
    return []


class CdcTail:
    """The consumer behind the Streaming page: one thread-bound poll, one ring buffer.

    kafka-python is blocking, so each poll runs in a worker thread and the decode runs
    on the event loop, where the schema lookups already are.  The buffer is a deque
    with a maximum length, which is the whole of the "no logging forever" property:
    the oldest record leaves as the newest arrives, and nothing grows.
    """

    def __init__(self) -> None:
        self._buffer: Deque[CdcRecord] = deque(maxlen=settings.cdc_buffer_size)
        self._consumer: Any = None
        self._partitions: List[int] = []
        self._state = "starting"
        self._error: Optional[str] = None
        self._seq = 0
        self._consumed = 0
        self._decode_failures = 0
        self._schemas: Dict[int, Dict[str, Any]] = {}
        # Per partition, the offset that was the end of the log when the tail attached.
        # Anything below it was read to fill the buffer rather than seen arrive.
        self._backfill_until: Dict[Any, int] = {}
        self._latencies: Deque[float] = deque(maxlen=_LATENCY_SAMPLES)
        self._last_record_at_ms: Optional[int] = None
        # Rate over the window between two polls that both saw records, rather than
        # since startup: the demo changes its write rate from the Settings page, and an
        # average since startup would hide that.
        self._rate = 0.0
        self._rate_marker_at = time.monotonic()
        self._rate_marker_count = 0

    @property
    def bootstrap(self) -> str:
        return f"{settings.kafka_host}:{settings.kafka_port}"

    # ──────────────────────── the loop ────────────────────────

    async def run(self) -> None:
        """Started once at startup and cancelled at shutdown.

        A broker that is not up yet, a topic that does not exist yet and a registry
        that is not answering are all ordinary states on a stack that is minutes old,
        so each is reported and retried rather than raised.
        """
        while True:
            try:
                if self._consumer is None:
                    await asyncio.to_thread(self._attach)
                batches = await asyncio.to_thread(self._poll)
                await self._ingest(batches)
                self._error = None
            except asyncio.CancelledError:
                await asyncio.to_thread(self._close)
                raise
            except Exception as exc:  # noqa: BLE001 — the loop must outlive any one failure
                self._error = f"{type(exc).__name__}: {exc}"
                self._state = "error"
                await asyncio.to_thread(self._close)
                await asyncio.sleep(_RETRY_DELAY_S)

    def _attach(self) -> None:
        """Open a consumer and place it at the end of every partition, less a bufferful.

        No consumer group and no committed offsets: this is a tail, and a restarted
        backend should show what is arriving now rather than replay from where the last
        one stopped.  It reads back one buffer's worth so that a page opened after the
        fact still has something to show; those records are flagged, because their age
        measures the backlog rather than the pipeline.
        """
        consumer = KafkaConsumer(
            bootstrap_servers=[self.bootstrap],
            client_id="htap-cdc-tail",
            enable_auto_commit=False,
            group_id=None,
            # Bounded so one poll cannot hand the decode more than the buffer holds.
            max_poll_records=settings.cdc_buffer_size,
            consumer_timeout_ms=0,
        )
        partitions = consumer.partitions_for_topic(settings.cdc_topic)
        if not partitions:
            consumer.close()
            self._state = "waiting_for_topic"
            raise RuntimeError(
                f"topic {settings.cdc_topic} does not exist yet; the Sidecar creates it "
                "with its first published mutation"
            )

        assignment = [TopicPartition(settings.cdc_topic, p) for p in sorted(partitions)]
        consumer.assign(assignment)
        ends = consumer.end_offsets(assignment)
        begins = consumer.beginning_offsets(assignment)
        backfill_each = max(1, settings.cdc_buffer_size // len(assignment))
        for tp in assignment:
            consumer.seek(tp, max(begins[tp], ends[tp] - backfill_each))
        self._backfill_until = dict(ends)

        self._consumer = consumer
        self._partitions = [tp.partition for tp in assignment]
        self._state = "tailing"

    def _poll(self) -> Dict[Any, List[Any]]:
        return self._consumer.poll(
            timeout_ms=int(settings.cdc_poll_timeout_s * 1000),
            max_records=settings.cdc_buffer_size,
        )

    def _close(self) -> None:
        consumer, self._consumer = self._consumer, None
        if consumer is not None:
            try:
                consumer.close(autocommit=False)
            except Exception:  # noqa: BLE001 — a close that fails leaves nothing to do
                pass

    async def _ingest(self, batches: Dict[Any, List[Any]]) -> None:
        received = 0
        for topic_partition, messages in batches.items():
            for message in messages:
                received += 1
                self._consumed += 1
                self._buffer.append(await self._decode(topic_partition, message))
        if received:
            now = time.monotonic()
            elapsed = now - self._rate_marker_at
            if elapsed >= 1.0:
                self._rate = (self._consumed - self._rate_marker_count) / elapsed
                self._rate_marker_at = now
                self._rate_marker_count = self._consumed

    async def _decode(self, topic_partition: Any, message: Any) -> CdcRecord:
        self._seq += 1
        backfill = message.offset < self._backfill_until.get(topic_partition, 0)
        record = CdcRecord(
            seq=self._seq,
            partition=message.partition,
            offset=message.offset,
            key=message.key.decode("utf-8", "replace") if message.key else "",
            kafka_at_ms=message.timestamp or 0,
            backfill=backfill,
        )
        # The key is keyspace:table:hash, and it is the one field that needs no schema,
        # so a record whose value cannot be decoded still says what it touched.
        parts = record.key.split(":")
        if len(parts) >= 2:
            record.keyspace, record.table = parts[0], parts[1]

        try:
            payload = message.value or b""
            if len(payload) < _HEADER_BYTES or payload[0] != _MAGIC_BYTE:
                raise ValueError(
                    f"not Confluent-framed: {len(payload)} bytes beginning "
                    f"{payload[:1].hex() or 'nothing'}"
                )
            schema_id = int.from_bytes(payload[1:_HEADER_BYTES], "big")
            record.schema_id = schema_id
            schema = await self._schema(schema_id)
            decoded = fastavro.schemaless_reader(io.BytesIO(payload[_HEADER_BYTES:]), schema)
        except Exception as exc:  # noqa: BLE001 — one bad record must not stop the tail
            self._decode_failures += 1
            record.decode_error = f"{type(exc).__name__}: {exc}"
            return record

        record.operation = str(decoded.get("operationType") or "")
        record.keyspace = str(decoded.get("sourceKeyspace") or record.keyspace)
        record.table = str(decoded.get("sourceTable") or record.table)
        record.partial = bool(decoded.get("isPartial") or False)
        micros = decoded.get("timestampMicros") or 0
        record.mutation_at_ms = int(micros // 1000)
        record.update_fields = [str(name) for name in (decoded.get("updateFields") or [])]
        # The row itself is one nested Avro record, `payload`, whose own fields are the
        # table's columns; the ten fields beside it are the CDC envelope.  Flattened
        # here, because a column is what the page shows and `payload.speed_mps` would be
        # this envelope's shape rather than the table's.  A record with no `payload`
        # keeps whatever else it carried, so a schema change shows rather than hides.
        payload = decoded.get("payload")
        if isinstance(payload, dict):
            record.columns = {name: _json_safe(value) for name, value in payload.items()}
        else:
            record.columns = {
                name: _json_safe(value)
                for name, value in decoded.items()
                if name not in _ENVELOPE_FIELDS
            }

        if record.mutation_at_ms:
            age_ms = time.time() * 1000 - record.mutation_at_ms
            if not backfill:
                record.age_ms = round(age_ms, 1)
                self._latencies.append(age_ms)
        self._last_record_at_ms = record.kafka_at_ms or record.mutation_at_ms
        return record

    async def _schema(self, schema_id: int) -> Dict[str, Any]:
        """The Avro schema a record names, fetched once per id and kept.

        A schema id is immutable in the registry, so this needs no expiry; the Sidecar
        registers a new id when the table's columns change.
        """
        cached = self._schemas.get(schema_id)
        if cached is not None:
            return cached
        url = f"{settings.cdc_schema_registry_url.rstrip('/')}/schemas/ids/{schema_id}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            body = response.json()
        parsed = fastavro.parse_schema(json.loads(body["schema"]))
        self._schemas[schema_id] = parsed
        return parsed

    # ──────────────────────── what the page reads ────────────────────────

    def status(self) -> CdcStreamStatus:
        live = sorted(self._latencies)
        return CdcStreamStatus(
            state=self._state,
            topic=settings.cdc_topic,
            bootstrap=self.bootstrap,
            registry=settings.cdc_schema_registry_url,
            partitions=self._partitions,
            buffer_size=settings.cdc_buffer_size,
            buffered=len(self._buffer),
            consumed=self._consumed,
            decode_failures=self._decode_failures,
            rate_per_sec=round(self._rate, 1),
            latency_p50_ms=round(live[len(live) // 2], 1) if live else None,
            latency_max_ms=round(live[-1], 1) if live else None,
            schema_ids=sorted(self._schemas),
            last_record_at_ms=self._last_record_at_ms,
            error=self._error,
        )

    def records(self, limit: int, since: Optional[int]) -> List[CdcRecord]:
        newest_first = list(self._buffer)[::-1]
        if since is not None:
            newest_first = [r for r in newest_first if r.seq > since]
        return newest_first[:limit]


cdc_tail = CdcTail()


@router.get("/cdc", response_model=CdcStreamResponse)
async def cdc_stream(
    limit: int = Query(50, ge=1, le=500),
    since: Optional[int] = Query(None, description="Return only records after this seq"),
):
    """The latest mutations, newest first, with what the tail is doing.

    Poll it with ``since`` to receive only what is new; poll it without to receive the
    latest window whatever has been seen before.
    """
    return CdcStreamResponse(status=cdc_tail.status(), records=cdc_tail.records(limit, since))


@router.get("/cdc/status", response_model=CdcStreamStatus)
async def cdc_status():
    """The tail alone, for a caller that wants the counters and not the records."""
    return cdc_tail.status()


@router.get("/cdc/schema", response_model=CdcSchemaView)
async def cdc_schema():
    """The Avro schema the topic's records are written against.

    Read from the registry rather than from a record, because the point is that the
    contract lives there: ``{topic}-value`` is the subject name Confluent's serializer
    uses, and Apicurio serves it under its compatibility endpoint.
    """
    registry = settings.cdc_schema_registry_url.rstrip("/")
    subject = f"{settings.cdc_topic}-value"
    view = CdcSchemaView(subject=subject, registry=registry)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{registry}/subjects/{subject}/versions/latest")
            if response.status_code == 404:
                view.error = (
                    f"no schema registered for {subject} yet; the Sidecar registers one "
                    "with its first published mutation"
                )
                return view
            response.raise_for_status()
            body = response.json()
        view.schema_id = body.get("id")
        view.version = body.get("version")
        avro_schema = json.loads(body["schema"])
        view.avro_schema = avro_schema
        view.fields = [
            {"name": field.get("name"), "type": field.get("type")}
            for field in avro_schema.get("fields", [])
        ]
        view.payload_fields = _payload_fields(avro_schema)
    except Exception as exc:  # noqa: BLE001 — the page reports a registry it cannot reach
        view.error = f"{type(exc).__name__}: {exc}"
    return view
