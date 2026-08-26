#!/usr/bin/env python3
"""
Kafka sink: writes the event stream into Cassandra and derives what the dashboard
reads.

Per event it:
1. appends to the raw event table, which Presto and the Spark bulk reader query
2. appends to drone_events_by_entity, the per-asset history behind flight trails
3. upserts drone_latest_status, the one-row-per-asset table behind the live map
4. derives speed, heading and flight state from the previous event for that asset
5. scores the position against the restricted zones and writes alerts

It also owns the demo schema: see ensure_schema below.
"""

import json
import math
import os
import time
import uuid
import zlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.util import datetime_from_uuid1
from kafka import KafkaConsumer


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


# How the raw event table is partitioned.  Both values are part of the data model
# rather than tuning: change either one and existing rows keep the buckets and
# shards they were written with, so queries naming the new ones stop matching the
# old rows.  They come from the environment because the dashboard's backend has to
# name the same buckets and shards when it queries, and compose sets both services
# from one declaration so they cannot drift apart.
#
# Fifteen minutes is short enough that "recently" costs one or two partitions and
# long enough that an hour is four of them.  Sixteen shards keeps a bucket at the
# demo's default rate near 110k rows per partition, and is few enough that naming
# every shard in a query stays readable.
EVENT_BUCKET_MINUTES = env_int("EVENT_BUCKET_MINUTES", 15)
EVENT_SHARDS = env_int("EVENT_SHARDS", 16)

# Whether Accord is enabled on the node, read from the same declaration in
# podman-compose.yml that the node itself reads.  It is here because a table opts
# into Accord at CREATE TABLE, and the node refuses the option outright when the
# subsystem is off: "Cannot create table demo.x with transactional mode full with
# accord.enabled set to false".  So the sink cannot carry the option
# unconditionally; with Accord off the schema step would fail and the sink would
# never start.  Only the three session tables below use this.
ACCORD_ENABLED = os.getenv("CASSANDRA_ACCORD_ENABLED", "true").lower() == "true"

# What the three session tables are created WITH.  Measured on 6.0-alpha2: a table
# must be *born* transactional, because neither route into an existing one works on
# a single node.  ALTER TABLE ... WITH transactional_mode='full' is accepted, but it
# only starts a migration: the table then reports transactional_migration_from='off'
# with its whole range in repairPendingRanges, and every transaction against it is
# refused with "Transaction Statement is unsupported when ... before migration to
# Accord is complete for a range".  Finishing that migration needs a repair, and at
# replication factor 1 nodetool repair declines with "No repair is needed", while
# nodetool consensus_admin finish-migration fails in its own JMX return path with
# NotSerializableException: java.util.ArrayList$SubList.  CREATE TABLE with the
# option needs no migration at all and works at once.  The consequence for anyone
# changing this: a data directory whose session tables already exist without the
# option cannot be fixed in place, and needs ./stop-and-clean-data-and-schema.sh.
TRANSACTIONAL = " WITH transactional_mode='full'" if ACCORD_ENABLED else ""

# Which table the Sidecar's Change Data Capture publisher follows.  One table opts in,
# drone_latest_status: it is the fleet's live state, one row per asset, so a mutation of
# it is the event a downstream consumer would want, and at a hundred assets its rate is
# the demo's whole write rate against a hundred keys.  events deliberately stays out.
# Opting it in would put every raw row through the publisher as well, and the demo's
# claim about the raw table is that the analytical paths read it without a copy.
#
# Unlike transactional_mode, cdc can be turned on with ALTER TABLE, so a table that
# already exists is one statement away rather than a wipe away.  The option is still
# written here, because the sink owns the schema and CREATE TABLE IF NOT EXISTS will not
# apply it to a table that exists.
CDC_ENABLED = os.getenv("CASSANDRA_CDC_ENABLED", "true").lower() == "true"
CDC = " WITH cdc = true" if CDC_ENABLED else ""


def event_bucket(event_time: datetime) -> str:
    """The window an event belongs to, as "YYYY-MM-DDTHH:MM" in UTC.

    Text rather than a timestamp on purpose.  This value is written by hand into
    queries that four different engines have to parse, and a quoted string means
    the same thing in CQL, Presto SQL and Spark SQL, where a timestamp literal does
    not.  It also sorts lexicographically, so a range predicate still reads
    naturally on the paths that cannot prune on it.
    """
    # Converted before the minute is read, not after: an offset like +05:30 would
    # otherwise floor the UTC hour using a local minute.
    utc = event_time.astimezone(timezone.utc)
    floored = utc.replace(
        minute=(utc.minute // EVENT_BUCKET_MINUTES) * EVENT_BUCKET_MINUTES,
        second=0,
        microsecond=0,
    )
    return floored.strftime("%Y-%m-%dT%H:%M")


def event_shard(event_id: uuid.UUID) -> int:
    """Which partition of a bucket this event goes to.

    Taken from the event's own id, so the spread is even whatever the fleet size:
    deriving it from the asset would put every row of a one-asset demo in a single
    partition.  Deterministic, unlike Python's salted hash of a string, so a
    restarted sink keeps writing the same event to the same place.

    Hashed rather than taken modulo the id itself, which measured as every event
    landing in one shard: these are version-1 UUIDs, whose low bits are the node
    field, and that is constant for a given host.
    """
    return zlib.crc32(event_id.bytes) % EVENT_SHARDS


def connect_cassandra(host: str, port: int):
    cluster = Cluster([host], port=port)
    session = cluster.connect()
    return cluster, session


# ──────────────────────────────────────────────────────────────
# Schema
#
# This sink owns the demo schema: it is the only process that has to exist for
# data to flow, so putting the definitions here means there is no separate
# migration step to forget and no second copy to drift.
# ──────────────────────────────────────────────────────────────

# Dimensions of the text embedding the dashboard's AI search writes.  Matches
# EMBEDDING_DIMS in backend/app/routes/vector.py.
EMBEDDING_DIMS = 1536

# Restricted airspace around Oslo, matching the producer's default fleet area.
# Reference data, so it is seeded with IF NOT EXISTS and never truncated.
#
# The last element is how many drones the zone will clear at once, and it is here
# rather than in the clearance tables so that a zone's definition and its limit
# cannot disagree.  The numbers are small on purpose: the transaction demo has to
# be able to exhaust a zone within a handful of steps, and a capacity of 200 would
# make the interesting refusal unreachable.  A tighter limit on the two critical
# zones than on the warning one is the only realism claimed.
DEMO_ZONES = (
    (
        "zone-oslo-airport",
        "Oslo Lufthavn Gardermoen",
        "POLYGON((11.05 60.18, 11.15 60.18, 11.15 60.22, 11.05 60.22, 11.05 60.18))",
        "critical",
        2,
    ),
    (
        "zone-royal-palace",
        "Det Kongelige Slott",
        "POLYGON((10.72 59.91, 10.74 59.91, 10.74 59.92, 10.72 59.92, 10.72 59.91))",
        "critical",
        3,
    ),
    (
        "zone-fornebu",
        "Fornebu Tech Park",
        "POLYGON((10.62 59.88, 10.66 59.88, 10.66 59.90, 10.62 59.90, 10.62 59.88))",
        "warning",
        5,
    ),
)


def ensure_schema(session, keyspace: str, table: str):
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': 1 }};
        """
    )
    session.set_keyspace(keyspace)

    # Raw event stream, one row per event.  Presto and the Spark bulk reader read
    # this table; the mission-control tables below are projections of it.
    #
    # Partitioned by a time bucket rather than by event_id, so that a question
    # about a period of time is a question about particular partitions.  Keyed on
    # the event alone, every path had to read the whole table to answer "the last
    # fifteen minutes", because event_time is not part of the key and a token is a
    # hash: there was nothing to prune on.  With the bucket in the key, Cassandra
    # reads those partitions and needs no ALLOW FILTERING, Presto and the
    # spark-cassandra-connector push the predicate down, and the bulk reader turns
    # it into a partition-key filter and skips the SSTables that cannot hold them.
    #
    # The shard is what keeps those partitions a sane size.  One bucket at the
    # demo's default rate is about 1.8M rows, which is far too much for a single
    # partition; spread over SHARDS it is nearer 110k, a few tens of megabytes.  It
    # comes from the event's own id rather than from the asset, so the spread does
    # not collapse when the fleet is small.  A query for a whole window therefore
    # names every shard, which is the cost of the arrangement and the reason to
    # keep the count modest.
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
          event_bucket text,
          shard int,
          event_id timeuuid,
          entity_id text,
          event_day date,
          event_time timestamp,
          event_type text,
          observer_id text,
          latitude double,
          longitude double,
          altitude_m float,
          temp_external_c float,
          temp_internal_c float,
          text_payload text,
          PRIMARY KEY ((event_bucket, shard), event_id)
        );
        """
        # WITH transactional_mode = 'full';
    )

    # Latest state per asset — the live map and the fleet KPIs.  One row per
    # asset, so a full scan of it is bounded by fleet size.
    #
    # This is also the one CDC table: see CDC above, and the Streaming page that reads
    # what the Sidecar publishes.  An existing table can be opted in with
    # ALTER TABLE demo.drone_latest_status WITH cdc = true.
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.drone_latest_status (
          entity_id text PRIMARY KEY,
          event_id timeuuid,
          event_time timestamp,
          event_type text,
          observer_id text,
          latitude double,
          longitude double,
          altitude_m float,
          temp_external_c float,
          temp_internal_c float,
          speed_mps double,
          heading_deg double,
          is_flying boolean,
          telemetry_age_s int,
          near_restricted_zone boolean,
          predicted_zone_breach boolean,
          risk_score double,
          text_payload text,
          updated_at timestamp
        ){CDC};
        """
    )

    # Text embeddings for the dashboard's vector search, in their own table for
    # two reasons.  PrestoDB's Cassandra connector cannot parse the CQL vector
    # type and drops the metadata for any table carrying one, which would hide
    # the live-status table from Presto entirely.  And an embedding is 1536
    # floats: keeping it out of the table the map reads on every refresh keeps
    # that row small.  text_payload is stored alongside the vector so a search
    # result shows the snippet the vector was actually built from, even after the
    # producer has moved that asset on to another one.
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.drone_text_embeddings (
          entity_id text PRIMARY KEY,
          text_payload text,
          payload_vector vector<float, {EMBEDDING_DIMS}>,
          updated_at timestamp
        );
        """
    )
    # No WITH OPTIONS, so the index takes the server's default similarity
    # function.  Stated because it means a Cassandra release that changes that
    # default changes which neighbours this index returns, silently rather than
    # by failing.  Checked on 6.0-alpha2: system_schema.indexes carries
    # class_name and target only, and a search answers.  What the check cannot
    # do is notice a changed default, because without an embedding key the
    # backend uses a local hashing embedder, whose neighbours are not ordered by
    # meaning; measured, five hits over one query spanned 0.550 to 0.602 cosine
    # and the nearest was unrelated prose.  Naming the function here would fix
    # the demo to one metric, which is a decision worth taking deliberately.
    # Murmur3 only, and the stack has no way to relax that.  Every non-Murmur3
    # partitioner is refused here by name — "Storage-attached index does not support
    # the following IPartitioner implementations: [OrderPreservingPartitioner,
    # LocalPartitioner, ByteOrderedPartitioner, RandomPartitioner]" — and since this
    # is the schema step, the refusal stops the sink and with it every table after
    # it.  Measured: on ByteOrderedPartitioner the sink looped here forever having
    # created three of the eleven tables.  SAI is also the only vector index
    # Cassandra has, so there is no weaker index to fall back to; a partitioner
    # change means losing the vector search page outright.
    session.execute(
        f"""
        CREATE CUSTOM INDEX IF NOT EXISTS payload_vector_idx
        ON {keyspace}.drone_text_embeddings (payload_vector)
        USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';
        """
    )

    # Per-asset history — flight trails and per-asset analysis.
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.drone_events_by_entity (
          entity_id text,
          event_time timestamp,
          event_id timeuuid,
          event_type text,
          observer_id text,
          latitude double,
          longitude double,
          altitude_m float,
          temp_external_c float,
          temp_internal_c float,
          speed_mps double,
          heading_deg double,
          zone_id text,
          text_payload text,
          PRIMARY KEY ((entity_id), event_time, event_id)
        ) WITH CLUSTERING ORDER BY (event_time DESC, event_id DESC);
        """
    )

    # Restricted airspace definitions.
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.restricted_zones (
          zone_id text PRIMARY KEY,
          zone_name text,
          polygon_wkt text,
          severity text,
          enabled boolean,
          updated_at timestamp
        );
        """
    )

    # Alerts, partitioned by hour so the dashboard reads whole partitions.
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.alerts_by_bucket (
          bucket text,
          alert_time timestamp,
          entity_id text,
          alert_id timeuuid,
          alert_type text,
          severity text,
          zone_id text,
          latitude double,
          longitude double,
          altitude_m float,
          message text,
          risk_score double,
          PRIMARY KEY ((bucket), alert_time, entity_id, alert_id)
        ) WITH CLUSTERING ORDER BY (alert_time DESC, entity_id ASC, alert_id DESC);
        """
    )

    # Ingestion volume, in 30-minute buckets, for the dashboard's throughput chart.
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.ingestion_counts (
            bucket text PRIMARY KEY,
            record_count counter
        );
        """
    )

    # The three tables behind the Accord transaction demo, "exactly-once in-order
    # session timeline projections".  Together they let one transaction decide
    # whether an event may join a session's timeline: sessions_open says the session
    # exists, session_seq_applied is the record of which sequence numbers have been
    # applied, and session_timeline is the projection itself.  A single transaction
    # reads all three and writes two, which is what no batch or lightweight
    # transaction can do: a batch is not conditional across partitions, and a
    # lightweight transaction conditions on one partition only.  Here the three
    # tables have three different partition keys.
    #
    # These are the only tables that opt into Accord.  demo.events deliberately does
    # not: it takes 2,000 events/s and putting a consensus protocol in front of that
    # write path is the opposite of what this stack is for.
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.sessions_open (
          user_id text,
          session_id uuid,
          PRIMARY KEY ((user_id), session_id)
        ){TRANSACTIONAL};
        """
    )
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.session_seq_applied (
          user_id text,
          session_id uuid,
          seq bigint,
          PRIMARY KEY ((user_id, session_id), seq)
        ){TRANSACTIONAL};
        """
    )
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.session_timeline (
          user_id text,
          session_id uuid,
          seq bigint,
          event_id timeuuid,
          event_time timestamp,
          event_type text,
          payload text,
          PRIMARY KEY ((user_id, session_id), seq)
        ){TRANSACTIONAL};
        """
    )
    # The reference table for the transaction measurement: the same columns and the
    # same key as session_timeline, and deliberately not transactional.  A figure for
    # an Accord transaction means nothing on its own, so the demo writes the same row
    # three ways on the same node in the same run: through the transaction above,
    # through a plain INSERT here, and through an IF NOT EXISTS lightweight
    # transaction here.  Same table for the latter two so the comparison is not also
    # a comparison of two table definitions.
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.session_timeline_plain (
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
    )
    # The three tables behind the airspace-clearance transaction, which is the
    # second Accord demonstration and shows a different thing from the first.  The
    # session timeline above shows exactly-once and in-order; this shows mutual
    # exclusion and a bounded resource, which is the shape of every reservation
    # problem: a drone may hold one clearance and a zone will clear only so many at
    # once.
    #
    # What makes it need Accord is that the two facts live in different partitions.
    # A lightweight transaction conditions on one partition, so it can enforce
    # "this drone holds nothing" or "this zone has room" but never both together;
    # two of them in sequence can interleave, and the zone ends up over capacity.
    #
    # The counter is `remaining` and not `granted`, and that is forced rather than
    # chosen.  Accord's CQL will compare a LET reference to a literal but not to
    # another LET reference: `IF occ.granted < occ.capacity` is refused outright
    # with "IllegalArgumentException null", where `IF occ.remaining > 0` is
    # accepted.  Counting down against zero therefore keeps the whole test inside
    # the transaction; counting up would mean the caller reading the capacity first
    # and binding it, and a concurrent change of capacity would not be serialised
    # against the grant.  `capacity` is kept only so a reader can see the limit.
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.zone_occupancy (
          zone_id text PRIMARY KEY,
          zone_name text,
          severity text,
          capacity bigint,
          remaining bigint
        ){TRANSACTIONAL};
        """
    )
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.zone_clearance (
          zone_id text,
          entity_id text,
          granted_at timestamp,
          PRIMARY KEY ((zone_id), entity_id)
        ){TRANSACTIONAL};
        """
    )
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.drone_clearance (
          entity_id text PRIMARY KEY,
          zone_id text,
          granted_at timestamp
        ){TRANSACTIONAL};
        """
    )
    print(f"[sink] session tables transactional_mode: {'full' if ACCORD_ENABLED else 'off'}")
    ensure_cdc(session, keyspace)

    seed_zones(session, keyspace)
    seed_zone_occupancy(session, keyspace)
    print("[sink] schema ensured")


def ensure_cdc(session, keyspace: str) -> None:
    """Bring drone_latest_status's cdc option to what CASSANDRA_CDC_ENABLED asks for.

    The CREATE TABLE above carries the option, which covers a fresh keyspace and nothing
    else: CREATE TABLE IF NOT EXISTS applies none of its options to a table that already
    exists, so a stack that has been running since before CDC was added would keep a
    table with the option off and a Sidecar with nothing to publish.  Read the current
    value and alter only on a difference, so a sink restart against an already-correct
    table issues no schema change.
    """
    want = "true" if CDC_ENABLED else "false"
    row = session.execute(
        "SELECT cdc FROM system_schema.tables "
        "WHERE keyspace_name = %s AND table_name = 'drone_latest_status'",
        (keyspace,),
    ).one()
    have = "true" if (row and row.cdc) else "false"
    if have == want:
        print(f"[sink] drone_latest_status cdc: {have}")
        return
    session.execute(f"ALTER TABLE {keyspace}.drone_latest_status WITH cdc = {want};")
    print(f"[sink] drone_latest_status cdc: {have} -> {want} (altered)")


def seed_zones(session, keyspace: str) -> None:
    """Insert the demo zones if they are absent, leaving any edits in place."""
    for zone_id, name, polygon_wkt, severity, _capacity in DEMO_ZONES:
        try:
            session.execute(
                f"INSERT INTO {keyspace}.restricted_zones "
                "(zone_id, zone_name, polygon_wkt, severity, enabled, updated_at) "
                "VALUES (%s, %s, %s, %s, true, toTimestamp(now())) IF NOT EXISTS",
                (zone_id, name, polygon_wkt, severity),
            )
        except Exception as e:
            print(f"[sink] could not seed zone {zone_id}: {e}")


def seed_zone_occupancy(session, keyspace: str) -> None:
    """Give each zone its clearance limit, without disturbing one already in use.

    Three things here are not the obvious spelling, and each is forced by the table
    being transactional.

    QUORUM, because transactional_mode='full' routes even a plain INSERT through
    Accord, and Accord refuses the driver's default: "ConsistencyLevel LOCAL_ONE is
    unsupported with Accord for write/commit".

    A read before the write rather than IF NOT EXISTS, because a lightweight
    transaction and Accord are two different consensus paths over the same row and
    there is no reason to ask a table in Accord's care to run one.  Only this sink
    seeds, so the read and the write need not be one operation.

    And `remaining` is left alone once the row exists.  Restarting the sink must not
    hand back clearances the zone has granted, which resetting the count would do
    while leaving every zone_clearance row in place.
    """
    for zone_id, name, _polygon_wkt, severity, capacity in DEMO_ZONES:
        try:
            existing = session.execute(
                SimpleStatement(
                    f"SELECT zone_id FROM {keyspace}.zone_occupancy WHERE zone_id = %s",
                    consistency_level=ConsistencyLevel.QUORUM,
                ),
                (zone_id,),
            ).one()
            if existing:
                continue
            session.execute(
                SimpleStatement(
                    f"INSERT INTO {keyspace}.zone_occupancy "
                    "(zone_id, zone_name, severity, capacity, remaining) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    consistency_level=ConsistencyLevel.QUORUM,
                ),
                (zone_id, name, severity, capacity, capacity),
            )
        except Exception as e:
            print(f"[sink] could not seed zone capacity {zone_id}: {e}")


def _thirty_min_bucket(dt: datetime) -> str:
    """Generate a 30-minute bucket key like '2026-04-08T14:00' or '2026-04-08T14:30'."""
    minute_bucket = 0 if dt.minute < 30 else 30
    return f"{dt.strftime('%Y-%m-%dT%H')}:{minute_bucket:02d}"


# ──────────────────────────────────────────────────────────────
# Geometry helpers
#
# Deliberately duplicated from backend/app/utils/geometry.py: this container has
# no dependency on the backend and must keep running without it.  The algorithms
# match so that the alerts written here agree with the what-if simulation the
# dashboard runs.
# ──────────────────────────────────────────────────────────────

EARTH_RADIUS_M = 6_371_000.0
M_PER_DEG_LAT = 111_320.0


def parse_wkt_polygon(wkt: str) -> List[Tuple[float, float]]:
    """Parse ``POLYGON((lon lat, ...))`` into a list of (lon, lat) pairs."""
    text = (wkt or "").strip()
    if not text.upper().startswith("POLYGON"):
        return []
    start, end = text.find("(("), text.rfind("))")
    if start == -1 or end == -1:
        return []

    ring: List[Tuple[float, float]] = []
    for pair in text[start + 2:end].split(","):
        parts = pair.split()
        if len(parts) >= 2:
            try:
                ring.append((float(parts[0]), float(parts[1])))
            except ValueError:
                continue
    return ring


def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in metres."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = rlat2 - rlat1
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


def compute_bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing from point 1 to point 2, in degrees clockwise from north."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(rlat2)
    y = math.cos(rlat1) * math.sin(rlat2) - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def point_in_polygon(lat: float, lon: float, polygon: List[Tuple[float, float]]) -> bool:
    """Ray-casting containment test against a (lon, lat) ring."""
    if len(polygon) < 3:
        return False

    inside = False
    j = len(polygon) - 1
    for i, (xi, yi) in enumerate(polygon):
        xj, yj = polygon[j]
        # Only edges straddling the ray's latitude can cross it, which also
        # guarantees yj != yi below.
        if (yi > lat) != (yj > lat):
            if lon < xi + (xj - xi) * (lat - yi) / (yj - yi):
                inside = not inside
        j = i
    return inside


def distance_to_polygon_m(lat: float, lon: float, polygon: List[Tuple[float, float]]) -> float:
    """Shortest distance in metres to the polygon's boundary, 0.0 if inside.

    Measured to the nearest point on each edge, not merely the nearest vertex: an
    asset alongside a long edge is close to the zone even when it is far from
    either corner.  Distances are computed on a local projection in metres, which
    at zone scale is accurate to well under a metre.
    """
    if len(polygon) < 3:
        return float("inf")
    if point_in_polygon(lat, lon, polygon):
        return 0.0

    m_per_deg_lon = M_PER_DEG_LAT * max(math.cos(math.radians(lat)), 0.01)
    nearest = float("inf")
    for i, (vlon, vlat) in enumerate(polygon):
        nlon, nlat = polygon[(i + 1) % len(polygon)]
        ax, ay = (vlon - lon) * m_per_deg_lon, (vlat - lat) * M_PER_DEG_LAT
        bx, by = (nlon - lon) * m_per_deg_lon, (nlat - lat) * M_PER_DEG_LAT
        dx, dy = bx - ax, by - ay
        length_sq = dx * dx + dy * dy
        t = 0.0 if length_sq == 0.0 else max(0.0, min(1.0, -(ax * dx + ay * dy) / length_sq))
        nearest = min(nearest, math.hypot(ax + t * dx, ay + t * dy))
    return nearest


# ──────────────────────────────────────────────────────────────
# Derived field computation
# ──────────────────────────────────────────────────────────────

class DroneTracker:
    """In-memory state tracker for deriving fields from sequential events."""

    # Altitude alone decides whether an asset counts as flying: with a variable
    # gap between an entity's events, a derived speed is too noisy to threshold.
    FLYING_ALTITUDE_THRESHOLD_M = 10.0
    # Above this, a derived speed is a position glitch rather than movement.
    MAX_PLAUSIBLE_SPEED_MPS = 100.0

    def __init__(self):
        # entity_id → {lat, lon, alt, time, speed, heading}
        self._state: Dict[str, dict] = {}

    def update(
        self,
        entity_id: str,
        lat: float,
        lon: float,
        alt: float,
        event_time: datetime,
    ) -> Tuple[float, float, bool]:
        """
        Derive speed_mps, heading_deg, is_flying for this event.
        Returns (speed_mps, heading_deg, is_flying).
        """
        prev = self._state.get(entity_id)

        if prev is None:
            # First event for this drone — can't derive speed/heading yet
            self._state[entity_id] = {
                "lat": lat,
                "lon": lon,
                "alt": alt,
                "time": event_time,
            }
            return (0.0, 0.0, alt > self.FLYING_ALTITUDE_THRESHOLD_M)

        dt = (event_time - prev["time"]).total_seconds()
        if dt <= 0:
            dt = 0.001  # Avoid division by zero

        # Distance and speed
        dist_m = haversine_distance_m(prev["lat"], prev["lon"], lat, lon)
        speed_mps = dist_m / dt

        if speed_mps > self.MAX_PLAUSIBLE_SPEED_MPS:
            speed_mps = prev.get("speed", 0.0)

        # Heading
        heading_deg = compute_bearing_deg(prev["lat"], prev["lon"], lat, lon)

        is_flying = alt > self.FLYING_ALTITUDE_THRESHOLD_M

        self._state[entity_id] = {
            "lat": lat,
            "lon": lon,
            "alt": alt,
            "time": event_time,
            "speed": speed_mps,
            "heading": heading_deg,
        }

        return (speed_mps, heading_deg, is_flying)

    def get_state(self, entity_id: str) -> Optional[dict]:
        return self._state.get(entity_id)


# ──────────────────────────────────────────────────────────────
# Alert generation
# ──────────────────────────────────────────────────────────────

class AlertGenerator:
    """Flags zone proximity on each event and writes alert rows."""

    # Distance at which proximity is flagged.  The dashboard's what-if simulation
    # uses the same figure, so its answer matches the live alerting.
    WARNING_DISTANCE_M = 500.0
    # Proximity risk above this counts as a predicted breach.
    BREACH_RISK_THRESHOLD = 0.7
    # Risk below this is not worth an alert row.
    ALERT_RISK_THRESHOLD = 0.5
    # One alert per asset per zone per this many seconds.  Without it, every
    # message from a loitering asset would write another row.
    ALERT_COOLDOWN_S = 60

    def __init__(self, session, keyspace: str):
        self.session = session
        self.keyspace = keyspace
        self._zones: List[dict] = []
        self._loaded = False
        self._last_alert: Dict[Tuple[str, str], float] = {}
        self._insert_alert = session.prepare(
            f"""
            INSERT INTO {keyspace}.alerts_by_bucket
                (bucket, alert_time, entity_id, alert_id, alert_type, severity,
                 zone_id, latitude, longitude, altitude_m, message, risk_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

    def load_zones(self) -> None:
        """Refresh the zone cache, parsing each polygon once.

        The WKT is parsed here rather than per event: at demo throughput that is
        the difference between parsing three polygons a minute and thousands a
        second.  ``enabled`` is filtered in Python so the table needs no index.
        """
        self._loaded = True
        try:
            rows = self.session.execute(
                f"SELECT zone_id, zone_name, polygon_wkt, severity, enabled "
                f"FROM {self.keyspace}.restricted_zones"
            )
            self._zones = [
                {
                    "zone_id": r.zone_id,
                    "zone_name": r.zone_name,
                    "severity": r.severity,
                    "ring": parse_wkt_polygon(r.polygon_wkt),
                }
                for r in rows
                if r.enabled
            ]
            unparsed = [z["zone_id"] for z in self._zones if len(z["ring"]) < 3]
            self._zones = [z for z in self._zones if len(z["ring"]) >= 3]
            if unparsed:
                print(f"[alert] ignoring zones with unusable polygons: {', '.join(unparsed)}")
            print(f"[alert] loaded {len(self._zones)} restricted zones")
        except Exception as e:
            print(f"[alert] could not load zones: {e}")

    def check_proximity(
        self,
        entity_id: str,
        lat: float,
        lon: float,
        alt: float,
        alert_time: datetime,
    ) -> Tuple[bool, bool, float, Optional[str]]:
        """Score this position against every zone, writing alerts as warranted.

        Returns (near_zone, predicted_breach, risk_score, nearest_zone_id).
        """
        if not self._loaded:
            self.load_zones()

        near_zone = False
        predicted_breach = False
        risk_score = 0.0
        nearest_zone_id = None

        for zone in self._zones:
            distance = distance_to_polygon_m(lat, lon, zone["ring"])
            if distance >= self.WARNING_DISTANCE_M:
                continue

            inside = distance == 0.0
            zone_risk = 0.95 if inside else 1.0 - (distance / self.WARNING_DISTANCE_M)
            near_zone = True
            if zone_risk > risk_score:
                risk_score = zone_risk
                nearest_zone_id = zone["zone_id"]
            if inside or zone_risk > self.BREACH_RISK_THRESHOLD:
                predicted_breach = True

            if zone_risk >= self.ALERT_RISK_THRESHOLD and self._cooldown_elapsed(entity_id, zone["zone_id"]):
                if inside:
                    message = f"{entity_id} is inside restricted zone {zone['zone_name']}"
                    alert_type, severity = "zone_breach", "critical"
                else:
                    message = (
                        f"{entity_id} is {distance:.0f}m from restricted zone {zone['zone_name']}"
                    )
                    alert_type = "zone_proximity"
                    severity = "high" if zone_risk > 0.8 else "warning"
                self._write_alert(
                    entity_id=entity_id,
                    alert_time=alert_time,
                    alert_type=alert_type,
                    severity=severity,
                    zone_id=zone["zone_id"],
                    lat=lat,
                    lon=lon,
                    alt=alt,
                    message=message,
                    risk_score=zone_risk,
                )

        return (near_zone, predicted_breach, risk_score, nearest_zone_id)

    def _cooldown_elapsed(self, entity_id: str, zone_id: str) -> bool:
        key = (entity_id, zone_id)
        now = time.monotonic()
        if now - self._last_alert.get(key, float("-inf")) < self.ALERT_COOLDOWN_S:
            return False
        self._last_alert[key] = now
        return True

    def _write_alert(
        self,
        entity_id: str,
        alert_time: datetime,
        alert_type: str,
        severity: str,
        zone_id: Optional[str],
        lat: float,
        lon: float,
        alt: float,
        message: str,
        risk_score: float,
    ) -> None:
        try:
            self.session.execute_async(
                self._insert_alert,
                (
                    alert_time.strftime("%Y-%m-%dT%H"),
                    alert_time,
                    entity_id,
                    uuid.uuid1(),
                    alert_type,
                    severity,
                    zone_id,
                    lat,
                    lon,
                    alt,
                    message,
                    risk_score,
                ),
            )
        except Exception as e:
            print(f"[alert] could not write alert for {entity_id}: {e}")


# ──────────────────────────────────────────────────────────────
# Main consumer
# ──────────────────────────────────────────────────────────────

def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
    topic = os.getenv("TOPIC", "demo-events")
    group_id = os.getenv("GROUP_ID", "demo-cassandra-sink")

    cass_host = os.getenv("CASSANDRA_HOST", "cassandra")
    cass_port = env_int("CASSANDRA_PORT", 9042)
    keyspace = os.getenv("KEYSPACE", "demo")
    table = os.getenv("TABLE", "events")

    # A batch fans out to three writes per event, all in flight at once, so the
    # batch has to be small enough that the driver's request queue holds them;
    # 200 events is 600 requests.  Each request's own timeout comes from the
    # driver's execution profile.
    batch_size = max(1, env_int("BATCH_SIZE", 200))
    report_every_s = env_float("REPORT_EVERY_S", 5.0)
    zone_reload_s = env_float("ZONE_RELOAD_S", 60.0)

    print(
        f"[sink] kafka={bootstrap} topic={topic} group_id={group_id} "
        f"cassandra={cass_host}:{cass_port} {keyspace}.{table} batch_size={batch_size}"
    )

    cluster, session = None, None
    while True:
        try:
            cluster, session = connect_cassandra(cass_host, cass_port)
            ensure_schema(session, keyspace, table)
            print("[sink] cassandra connected and schema ensured")
            break
        except Exception as e:
            print(f"[sink] cassandra not ready yet: {e}")
            time.sleep(5)

    insert_raw = session.prepare(
        f"INSERT INTO {keyspace}.{table} (event_bucket, shard, event_id, entity_id, event_day, "
        "event_time, event_type, observer_id, latitude, longitude, altitude_m, "
        "temp_external_c, temp_internal_c, text_payload) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    insert_raw.consistency_level = ConsistencyLevel.QUORUM

    insert_history = session.prepare(
        f"""
        INSERT INTO {keyspace}.drone_events_by_entity
            (entity_id, event_time, event_id, event_type, observer_id,
             latitude, longitude, altitude_m, temp_external_c, temp_internal_c,
             speed_mps, heading_deg, zone_id, text_payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    insert_history.consistency_level = ConsistencyLevel.QUORUM

    upsert_latest = session.prepare(
        f"""
        UPDATE {keyspace}.drone_latest_status SET
            event_id = ?, event_time = ?, event_type = ?, observer_id = ?,
            latitude = ?, longitude = ?, altitude_m = ?,
            temp_external_c = ?, temp_internal_c = ?,
            speed_mps = ?, heading_deg = ?, is_flying = ?, telemetry_age_s = ?,
            near_restricted_zone = ?, predicted_zone_breach = ?, risk_score = ?,
            text_payload = ?, updated_at = ?
        WHERE entity_id = ?
        """
    )
    upsert_latest.consistency_level = ConsistencyLevel.QUORUM

    count_ingested = session.prepare(
        f"UPDATE {keyspace}.ingestion_counts SET record_count = record_count + ? WHERE bucket = ?"
    )

    tracker = DroneTracker()
    alerts = AlertGenerator(session, keyspace)

    consumer = None
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                max_poll_records=batch_size,
            )
            print("[sink] kafka consumer started")
            break
        except Exception as e:
            print(f"[sink] kafka not ready yet: {e}")
            time.sleep(5)

    total_inserted = 0
    window_inserted = 0
    last_report = time.monotonic()
    last_zone_reload = time.monotonic()

    while True:
        records = consumer.poll(timeout_ms=1000, max_records=batch_size)
        if not records:
            continue

        now_monotonic = time.monotonic()
        if now_monotonic - last_zone_reload > zone_reload_s:
            alerts.load_zones()
            last_zone_reload = now_monotonic

        # Writes are issued asynchronously across the batch and collected here,
        # so the batch overlaps in the cluster but the offsets are only committed
        # once every write in it has been acknowledged.  Committing first, as a
        # fire-and-forget loop must, would silently drop the batch on any write
        # failure — the offsets would already say it had been handled.
        pending = []
        buffered = 0

        for _, messages in records.items():
            for message in messages:
                event = message.value
                try:
                    event_id = uuid.UUID(event["event_id"])
                    event_time = datetime_from_uuid1(event_id)
                except Exception:
                    event_id = uuid.uuid1()
                    event_time = datetime.now(timezone.utc)
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)

                entity_id = str(event.get("entity_id", ""))
                position = event.get("position") or {}
                latitude = float(position.get("lat", 0.0))
                longitude = float(position.get("lon", 0.0))
                altitude_m = float(event.get("z_m", 0.0))
                event_type = str(event.get("event_type", ""))
                observer_id = str(event.get("observer_id", ""))
                temp_external_c = float(event.get("temp_external_c", 0.0))
                temp_internal_c = float(event.get("temp_internal_c", 0.0))
                text_payload = str(event.get("text", ""))

                speed_mps, heading_deg, is_flying = tracker.update(
                    entity_id, latitude, longitude, altitude_m, event_time
                )
                near_zone, predicted_breach, risk_score, zone_id = alerts.check_proximity(
                    entity_id, latitude, longitude, altitude_m, event_time
                )
                now_utc = datetime.now(timezone.utc)
                telemetry_age_s = max(0, int((now_utc - event_time).total_seconds()))

                pending.append(session.execute_async(insert_raw, (
                    event_bucket(event_time), event_shard(event_id), event_id,
                    entity_id, event_time.date(), event_time, event_type,
                    observer_id, latitude, longitude, altitude_m,
                    temp_external_c, temp_internal_c, text_payload,
                )))
                pending.append(session.execute_async(insert_history, (
                    entity_id, event_time, event_id, event_type, observer_id,
                    latitude, longitude, altitude_m, temp_external_c, temp_internal_c,
                    speed_mps, heading_deg, zone_id, text_payload,
                )))
                pending.append(session.execute_async(upsert_latest, (
                    event_id, event_time, event_type, observer_id,
                    latitude, longitude, altitude_m, temp_external_c, temp_internal_c,
                    speed_mps, heading_deg, is_flying, telemetry_age_s,
                    near_zone, predicted_breach, risk_score, text_payload, now_utc,
                    entity_id,
                )))
                buffered += 1

        try:
            for future in pending:
                future.result()
        except Exception as e:
            # Leave the offsets where they are: the batch is redelivered and
            # replayed.  Every write here is an idempotent upsert, so a replay
            # costs duplicate work but no duplicate data.
            print(f"[sink] batch write failed, will retry from the last commit: {e}")
            continue

        consumer.commit()
        if buffered:
            session.execute_async(
                count_ingested, (buffered, _thirty_min_bucket(datetime.now(timezone.utc)))
            )
        total_inserted += buffered
        window_inserted += buffered

        elapsed = time.monotonic() - last_report
        if elapsed >= report_every_s:
            print(f"[sink] total_inserted={total_inserted} (~{window_inserted / elapsed:.0f}/s)")
            window_inserted = 0
            last_report = time.monotonic()


if __name__ == "__main__":
    main()
