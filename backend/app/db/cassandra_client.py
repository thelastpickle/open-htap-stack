"""Cassandra client — the OLTP side of the demo.

Every read here is either a point read or a bounded scan of a table that holds
one row per asset, so the dashboard stays honest about what Cassandra is good at.
"""
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import AddressTranslator
from cassandra.query import SimpleStatement

from app.config import settings

# One row per asset, so a full scan of drone_latest_status is bounded by fleet
# size.  Keep it in step with MAX_ENTITIES in the producer.
FLEET_SCAN_LIMIT = 5000

# Rows read from an asset's history to build a flight path.  Sized so the path
# covers a useful stretch of time at demo ingest rates while staying one bounded
# single-partition read.
TRAIL_SCAN_ROWS = 2000

LATEST_STATUS_COLUMNS = (
    "entity_id, event_time, latitude, longitude, altitude_m, speed_mps, "
    "heading_deg, is_flying, temp_internal_c, temp_external_c, event_type, "
    "observer_id, telemetry_age_s, near_restricted_zone, predicted_zone_breach, "
    "risk_score"
)


class FixedAddressTranslator(AddressTranslator):
    """Rewrites every address the driver discovers to a single host.

    Needed only when the backend runs outside the container network and reaches
    Cassandra through a published port.
    """

    def __init__(self, target: str):
        self._target = target

    def translate(self, addr):
        return self._target


class CassandraClient:
    """Lazily-connected Cassandra session with a retry throttle."""

    RECONNECT_INTERVAL_S = 10
    # Shortest gap between ingest-counter samples worth differencing.  Below this
    # the counter's own write latency dominates the arithmetic.
    RATE_MIN_INTERVAL_S = 2.0

    def __init__(self):
        self._cluster: Optional[Cluster] = None
        self._session: Optional[Session] = None
        self._lock = threading.Lock()
        self._last_attempt = 0.0
        self.connected = False
        self._rate_lock = threading.Lock()
        self._rate_sample: Optional[Tuple[float, int]] = None  # (monotonic, total_events)
        self._last_rate = 0.0
        # Prepared statements, held by statement text, for execute_transaction.  Its
        # own lock rather than _lock above, which connect() holds while it builds a
        # session: a thread preparing a statement has no business waiting on that.
        # Emptied on every connect, because a prepared statement belongs to the
        # session that prepared it and a reconnect leaves the old ones invalid.
        self._prepare_lock = threading.Lock()
        self._prepared: Dict[str, Any] = {}

    def connect(self, force: bool = False) -> None:
        """Connect if not already connected.

        Attempts are throttled so that a dashboard polling every few seconds
        cannot turn an outage into a connection storm.  Raises only when
        ``force`` is set; otherwise failures leave ``connected`` False and are
        reported by the endpoints as an unavailable engine.
        """
        with self._lock:
            if self.connected and not force:
                return
            now = time.monotonic()
            if not force and (now - self._last_attempt) < self.RECONNECT_INTERVAL_S:
                return
            self._last_attempt = now

            try:
                profile = ExecutionProfile(request_timeout=15)
                translator = (
                    FixedAddressTranslator(settings.cassandra_translate_addresses_to)
                    if settings.cassandra_translate_addresses_to
                    else None
                )
                self._cluster = Cluster(
                    contact_points=[settings.cassandra_host],
                    port=settings.cassandra_port,
                    address_translator=translator,
                    execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                )
                self._session = self._cluster.connect(settings.cassandra_keyspace)
                with self._prepare_lock:
                    self._prepared.clear()
                self.connected = True
                print(f"[db] Cassandra connected: {settings.cassandra_host}:{settings.cassandra_port}")
            except Exception as e:
                self.connected = False
                self._session = None
                print(f"[db] Cassandra connection failed: {e}")
                if force:
                    raise

    @property
    def session(self) -> Session:
        if not self.connected:
            self.connect()
        if self._session is None:
            raise RuntimeError("Cassandra not connected")
        return self._session

    def execute_query(self, cql: Any, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        """Run CQL and return rows as dicts, with timestamps as ISO-8601 strings.

        Takes a string or any statement object the driver accepts, so a caller that
        needs its own consistency level can pass a SimpleStatement.
        """
        rows = self.session.execute(cql, params)
        columns = rows.column_names
        result = []
        for row in rows:
            record = {}
            for name in columns:
                value = getattr(row, name, None)
                record[name] = value.isoformat() if hasattr(value, "isoformat") else value
            result.append(record)
        return result

    def execute_write(self, cql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        """Run a statement that writes, at QUORUM, and return whatever rows it reports.

        Named apart from execute_query because this file's promise is that every
        read through it is a point read or a bounded scan, and a method that writes
        should not hide behind a name that says query.  Used only by the transaction
        demo, whose whole subject is writes; nothing on the dashboard's read paths
        calls it.  A lightweight transaction returns an [applied] row, so the return
        type matches execute_query rather than being None.

        QUORUM, not the profile's default, and the reason is Accord rather than
        durability.  transactional_mode='full' routes *every* write to the table
        through Accord, not only a BEGIN TRANSACTION, so a plain INSERT into one of
        the session tables is refused at LOCAL_ONE exactly as a transaction is:
        "ConsistencyLevel LOCAL_ONE is unsupported with Accord for write/commit".
        Setting it here also keeps the demo's two reference writes at the same
        consistency as the transaction they are compared against, which is the only
        way that comparison means anything.
        """
        statement = SimpleStatement(cql, consistency_level=ConsistencyLevel.QUORUM)
        return self.execute_query(statement, params)

    def execute_transaction(self, cql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
        """Run one Accord transaction and return its SELECT projection as a dict.

        The statement is prepared, and prepared for two reasons rather than one.
        A transaction is written with ? placeholders, which the driver binds only on
        a prepared statement; a simple statement takes %s and would have the driver
        substitute the values into the text instead.  And the statements this demo
        runs are a handful of fixed texts run many times, so preparing each once is
        what the driver is for.  The prepared statements are held per statement text
        under the same lock the connection uses, since a demo run and a dashboard
        poll can arrive together.

        An Accord transaction reports differently from a lightweight transaction: it
        returns no [applied] column, only the row its own SELECT projects, and an
        empty result when it projects nothing.  So a caller cannot ask the server
        whether the IF fired; it has to read the guard values back out of the
        projection and decide.  Returning the single projected row, rather than a
        list, is what makes that legible at the call site.

        The statement must be deterministic.  now() and toTimestamp(now()) inside a
        transaction would each be evaluated per replica, so every timeuuid and
        timestamp this demo writes is bound from the caller instead.
        """
        session = self.session
        with self._prepare_lock:
            prepared = self._prepared.get(cql)
            if prepared is None:
                prepared = session.prepare(cql)
                # The driver's default is LOCAL_ONE, which Accord refuses outright:
                # "ConsistencyLevel LOCAL_ONE is unsupported with Accord for
                # write/commit, supported are [ANY, ONE, QUORUM, ALL, SERIAL]".
                # QUORUM of the five, because it is what the sink already writes at,
                # so a transaction here is not quietly held to a weaker standard than
                # an ordinary write in this stack.
                prepared.consistency_level = ConsistencyLevel.QUORUM
                self._prepared[cql] = prepared
        rows = session.execute(prepared, params)
        columns = rows.column_names
        # zip rather than getattr, which execute_query above uses: a transaction
        # projects columns named session_ok.session_id, and a dot cannot be a
        # namedtuple field, so the driver's row object does not carry that name.
        # The values are in projection order either way.
        for row in rows:
            return {
                name: (value.isoformat() if hasattr(value, "isoformat") else value)
                for name, value in zip(columns, row)
            }
        return {}

    # ──────────────────────── Overview / KPI queries ────────────────────────

    def get_overview_kpis(self) -> Dict[str, Any]:
        """Derive every fleet KPI from a single scan of drone_latest_status.

        The previous shape issued six separate ``ALLOW FILTERING`` aggregates
        over the same partition set; one scan gives identical numbers from a
        consistent snapshot for a fraction of the coordinator work.
        """
        rows = self.execute_query(
            f"SELECT entity_id, is_flying, speed_mps, altitude_m, near_restricted_zone, "
            f"predicted_zone_breach FROM drone_latest_status LIMIT {FLEET_SCAN_LIMIT}"
        )

        flying = [r for r in rows if r.get("is_flying")]
        speeds = [r["speed_mps"] for r in flying if r.get("speed_mps") is not None]
        altitudes = [r["altitude_m"] for r in flying if r.get("altitude_m") is not None]

        def stats(values: List[float]) -> Dict[str, float]:
            if not values:
                return {"max": 0.0, "min": 0.0, "avg": 0.0}
            return {
                "max": round(max(values), 1),
                "min": round(min(values), 1),
                "avg": round(sum(values) / len(values), 1),
            }

        speed, altitude = stats(speeds), stats(altitudes)
        total_events = self.get_total_events()
        return {
            "total_drones": len(rows),
            "active_flying_drones": len(flying),
            "grounded_drones": len(rows) - len(flying),
            "max_speed_mps": speed["max"],
            "min_speed_mps": speed["min"],
            "avg_speed_mps": speed["avg"],
            "max_altitude_m": altitude["max"],
            "min_altitude_m": altitude["min"],
            "avg_altitude_m": altitude["avg"],
            "near_zone_count": sum(1 for r in rows if r.get("near_restricted_zone")),
            "predicted_breach_count": sum(1 for r in rows if r.get("predicted_zone_breach")),
            "total_events": total_events,
            "ingestion_rate_per_sec": self.observe_ingestion_rate(total_events),
        }

    def get_drone_count(self) -> int:
        """Fleet size.  A count aggregate, so the coordinator returns one row."""
        rows = self.execute_query("SELECT count(*) AS cnt FROM drone_latest_status")
        return rows[0]["cnt"] if rows else 0

    def get_total_events(self) -> int:
        """Sum every ingestion bucket.  Returns 0 before the sink has written any."""
        try:
            rows = self.execute_query("SELECT record_count FROM ingestion_counts")
            return sum((r["record_count"] or 0) for r in rows)
        except Exception:
            return 0

    # ──────────────────────── Map / fleet queries ────────────────────────

    def get_drones(self, limit: int = FLEET_SCAN_LIMIT, flying_only: bool = False) -> List[Dict[str, Any]]:
        """Latest state per asset, for the map and the polygon tools."""
        limit = max(1, min(limit, FLEET_SCAN_LIMIT))
        where = "WHERE is_flying = true " if flying_only else ""
        suffix = " ALLOW FILTERING" if flying_only else ""
        return self.execute_query(
            f"SELECT {LATEST_STATUS_COLUMNS} FROM drone_latest_status {where}LIMIT {limit}{suffix}"
        )

    def get_drone_detail(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Single-partition point read — the query Cassandra is here for."""
        rows = self.execute_query(
            f"SELECT {LATEST_STATUS_COLUMNS} FROM drone_latest_status WHERE entity_id = %s",
            (entity_id,),
        )
        return rows[0] if rows else None

    def get_drone_trail(self, entity_id: str, points: int = 60) -> List[Dict[str, Any]]:
        """A thinned flight path for one asset, newest first.

        A single-partition range scan of drone_events_by_entity, which is
        clustered by event_time DESC — a sequential read, not a filtered scan.

        The scan is far denser than a path needs: at demo rates each asset emits
        tens of readings a second, so drawing every row would be a smudge rather
        than a track.  Reading a fixed window and keeping every nth row gives a
        path spanning real time for one bounded read.
        """
        points = max(2, min(points, 500))
        rows = self.execute_query(
            "SELECT event_time, latitude, longitude, altitude_m, speed_mps, heading_deg "
            f"FROM drone_events_by_entity WHERE entity_id = %s LIMIT {TRAIL_SCAN_ROWS}",
            (entity_id,),
        )
        stride = max(1, len(rows) // points)
        return rows[::stride][:points]

    def get_zones(self) -> List[Dict[str, Any]]:
        """Every enabled restricted zone.

        ``enabled`` is filtered in Python so the table needs no index; it holds
        a handful of rows of reference data seeded by the ingest sink.
        """
        rows = self.execute_query(
            "SELECT zone_id, zone_name, polygon_wkt, severity, enabled FROM restricted_zones"
        )
        return [r for r in rows if r.get("enabled", True)]

    # ──────────────────────── Alerts ────────────────────────

    def get_alerts(self, limit: int = 50, hours: int = 6) -> List[Dict[str, Any]]:
        """Recent alerts, newest bucket first.

        alerts_by_bucket is partitioned by hour, so this walks back one
        partition at a time until it has enough rows.
        """
        alerts: List[Dict[str, Any]] = []
        bucket_time = datetime.now(timezone.utc)
        for _ in range(max(1, hours)):
            remaining = limit - len(alerts)
            if remaining <= 0:
                break
            alerts.extend(
                self.execute_query(
                    "SELECT alert_id, alert_time, entity_id, alert_type, severity, zone_id, "
                    "latitude, longitude, altitude_m, message, risk_score "
                    f"FROM alerts_by_bucket WHERE bucket = %s LIMIT {remaining}",
                    (bucket_time.strftime("%Y-%m-%dT%H"),),
                )
            )
            bucket_time -= timedelta(hours=1)
        return alerts[:limit]

    # ──────────────────────── Ingestion volume ────────────────────────

    @staticmethod
    def _bucket_key(t: datetime) -> str:
        """The 30-minute bucket key the ingest sink counts into."""
        return f"{t.strftime('%Y-%m-%dT%H')}:{0 if t.minute < 30 else 30:02d}"

    def _bucket_count(self, t: datetime) -> int:
        try:
            rows = self.execute_query(
                "SELECT record_count FROM ingestion_counts WHERE bucket = %s", (self._bucket_key(t),)
            )
            return (rows[0]["record_count"] or 0) if rows else 0
        except Exception:
            return 0

    def get_ingestion_history(self, hours: int = 8) -> List[Dict[str, Any]]:
        """Ingestion counts in 30-minute buckets, oldest first."""
        now = datetime.now(timezone.utc)
        buckets = []
        for i in range(hours * 2 - 1, -1, -1):
            t = now - timedelta(minutes=30 * i)
            key = self._bucket_key(t)
            buckets.append({"time": key[11:], "timestamp": key, "count": self._bucket_count(t)})
        return buckets

    def observe_ingestion_rate(self, total_events: int) -> float:
        """Events per second, from the change in the ingest counter between calls.

        Dividing the counter by elapsed bucket time would understate the rate
        whenever the stack was started mid-bucket, and would keep understating it
        for up to half an hour.  Differencing consecutive observations measures
        what is arriving now, which is what a live dashboard is claiming to show.

        Returns the previous figure when called again too soon to difference
        meaningfully, and 0.0 until there are two observations to compare.
        """
        now = time.monotonic()
        with self._rate_lock:
            previous = self._rate_sample
            self._rate_sample = (now, total_events)

            if previous is None:
                return 0.0
            elapsed_s = now - previous[0]
            if elapsed_s < self.RATE_MIN_INTERVAL_S:
                self._rate_sample = previous  # keep the older baseline to difference from
                return self._last_rate
            delta = total_events - previous[1]
            if delta < 0:
                # The counters were truncated; start again from here.
                self._last_rate = 0.0
            else:
                self._last_rate = round(delta / elapsed_s, 1)
            return self._last_rate


cassandra_client = CassandraClient()
