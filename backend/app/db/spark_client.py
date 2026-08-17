"""Spark Thrift Server clients — the two batch paths into Cassandra.

Spark can reach the same rows two ways, and the dashboard offers both because the
difference between them is the architectural claim this stack makes:

``spark_client``
    The spark-cassandra-connector, reading through Cassandra's CQL request path.
    Good for per-partition work; shares the coordinator with the OLTP traffic.

``spark_bulk_client``
    The Cassandra Analytics bulk reader, reading SSTable files directly from a
    coordinated snapshot via the Sidecar.  It never touches the request path, so a
    scan here does not contend with OLTP latency — the "resource isolation by
    construction" the README describes.  Its rows are consistent as of the
    snapshot rather than as of now.

Both talk to the same Thrift Server, but each holds its own connection, and so its
own HiveServer2 session.  One connection would be enough to serve them in turn;
two is what lets them run at the same time, which the comparison offers on purpose
so the contention between paths can be seen rather than described.  Each session
carries only the views its own path needs.
"""
import itertools
import math
import re
import socket as stdlib_socket
import threading
import time
from typing import Any, Dict, List, Optional, Sequence

from thrift.transport import TSocket, TTransport
from thrift.transport.TTransport import TTransportException

from app.config import settings

# Tables the dashboard queries.  Both clients register a view per table: the
# connector under the table's own name, the bulk reader under a bulk_ prefix, so
# one statement can be aimed at either path by name alone.
REGISTERED_TABLES = ("drone_latest_status", "drone_events_by_entity", "events")
BULK_VIEW_PREFIX = "bulk_"

# Errors that mean the connector views need rebuilding — the session was
# recycled, or the table behind one was replaced since it was registered.
_STALE_VIEW_MARKERS = ("TABLE_OR_VIEW_NOT_FOUND", "cannot be found", "does not exist", "UNRESOLVED_")

# PyHive surfaces a server-side failure as the whole Thrift response object: a
# wall of JVM stack frames with the actual message buried in it.  These pull out
# the part a person can act on.
_HIVE_MESSAGE_RE = re.compile(r'errorMessage="((?:[^"\\]|\\.)*)"')
_HIVE_EXCEPTION_RE = re.compile(r"\*?(?:[\w.]+\.)?(\w+(?:Exception|Error)):([^:\']{0,300})")

_bulk_snapshot_counter = itertools.count(1)


def readable_error(error: Exception) -> str:
    """Reduce a HiveServer2 failure to its message."""
    text = str(error)
    match = _HIVE_MESSAGE_RE.search(text)
    if match:
        return match.group(1).replace("\\n", " ").strip()
    match = _HIVE_EXCEPTION_RE.search(text)
    if match:
        return f"{match.group(1)}: {match.group(2).strip()}"
    return text[:400]


class SparkThriftClient:
    """Thin HiveServer2 wrapper.  One connection, serialised by a lock.

    ``register_connector_views`` builds the spark-cassandra-connector views on
    connect.  The bulk reader holds its own instance of this class and does not
    want them: it registers its own views per query, and a session carrying views
    it never reads would take a table definition through the CQL path for nothing.

    ``name`` only labels the log lines, so the two connections can be told apart.
    """

    # What a query reports when abort() cut its connection.  A cancelled query and
    # a server that stopped answering arrive here as the same transport error, and
    # only this class knows which of the two happened.
    CANCELLED_MESSAGE = (
        "Cancelled: the connection was taken away, so Spark stopped work on this "
        "statement.  It reconnects on the next query."
    )

    def __init__(self, name: str = "spark", register_connector_views: bool = True):
        self._name = name
        self._register_connector_views = register_connector_views
        self._conn: Optional[Any] = None
        self._socket: Optional[Any] = None
        # Set by abort() so the interrupted query can say it was cancelled rather
        # than blaming the server for going quiet.
        self._aborted = False
        self._lock = threading.Lock()
        self.connected = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def was_aborted(self) -> bool:
        """Whether the last failure on this connection was a cancellation."""
        return self._aborted

    @property
    def busy(self) -> bool:
        """True while a statement is in flight on this connection.

        Worth asking before reconnecting: connect() takes the same lock a query
        holds, so it would otherwise wait out the whole query rather than doing
        anything, and a control that hangs for a quarter of an hour is worse than
        one that explains itself.
        """
        return self._lock.locked()

    def connect(self) -> None:
        with self._lock:
            try:
                from pyhive import hive

                # The transport is built here rather than left to PyHive so it can
                # carry a socket timeout.  PyHive offers no per-query timeout, and
                # without one a Spark job that never finishes would hang the
                # dashboard rather than reporting a failure.  This is the plain
                # transport, matching the hive.server2.authentication=NOSASL the
                # spark service starts its Thrift Server with.
                transport_socket = TSocket.TSocket(
                    settings.spark_thrift_host, settings.spark_thrift_port
                )
                transport_socket.setTimeout(settings.spark_query_timeout_s * 1000)
                self._conn = hive.connect(
                    thrift_transport=TTransport.TBufferedTransport(transport_socket),
                    database="default",
                )
                # Kept so abort() can reach it without the lock (see below).
                self._socket = transport_socket
                self._aborted = False
                self.connected = True
                print(
                    f"[db] Spark Thrift Server connected ({self._name}): "
                    f"{settings.spark_thrift_host}:{settings.spark_thrift_port}"
                )
            except Exception as e:
                self._close_locked()
                print(f"[db] Spark Thrift Server connection failed ({self._name}): {e}")
                return
        if self._register_connector_views:
            self._register_views()

    def _close_locked(self) -> None:
        """Drop the connection.  Callers must hold the lock."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = None
        self._socket = None
        self.connected = False

    def abort(self) -> bool:
        """Cut the connection from under a query that is still running.

        This deliberately does not take the lock, because the thread holding it is
        the one being interrupted: PyHive gives no way to cancel a statement it is
        already waiting on, so the way to stop waiting is to close the socket
        underneath it.  Its blocked read then raises, the query reports a failure
        like any other, and HiveServer2 cancels the statement's job group when it
        notices the session has gone.

        Returns False if there was nothing open to close.  Closing an idle
        connection is harmless, since the next use reconnects.
        """
        transport_socket = self._socket
        if transport_socket is None:
            return False
        self._aborted = True
        self.connected = False
        # Shut the connection down before closing it.  Closing alone is not enough:
        # a close in this thread does not reliably wake a recv already blocked in
        # another, so the query would keep waiting out its whole timeout.  A
        # shutdown ends the connection itself, so the blocked read returns at once.
        #
        # This frees the dashboard, not the cluster: measured here, Spark keeps
        # running a job whose session has gone, so whoever cancels has to kill the
        # job as well (see db/spark_ui.kill_jobs_for).
        handle = getattr(transport_socket, "handle", None)
        if handle is not None:
            try:
                handle.shutdown(stdlib_socket.SHUT_RDWR)
            except OSError:
                pass  # already gone, which is the outcome being asked for
        try:
            transport_socket.close()
        except Exception as e:
            print(f"[db] Spark abort ({self._name}) could not close the socket: {e}")
        print(f"[db] Spark connection aborted ({self._name}); it will reconnect on next use")
        return True

    def _register_views(self) -> None:
        """Point Spark at the Cassandra tables through the CQL connector.

        Best effort: if the connector or the keyspace is not ready, the views are
        missing and queries say so, rather than the dashboard claiming Spark is
        healthy and returning nothing.
        """
        for table in REGISTERED_TABLES:
            ddl = (
                f"CREATE OR REPLACE TEMP VIEW {table} "
                "USING org.apache.spark.sql.cassandra "
                f"OPTIONS (keyspace '{settings.cassandra_keyspace}', table '{table}')"
            )
            try:
                self.execute_ddl(ddl)
                print(f"[db] Spark connector view registered: {table}")
            except Exception as e:
                print(f"[db] Spark view registration failed for {table}: {readable_error(e)}")

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """Run a query, rebuilding the views once if they have gone stale."""
        try:
            return self.execute(sql)
        except TTransportException as e:
            aborted = self._aborted
            with self._lock:
                self._close_locked()
            if aborted:
                raise RuntimeError(self.CANCELLED_MESSAGE) from e
            raise RuntimeError(
                f"Spark stopped answering for {settings.spark_query_timeout_s}s ({e}). "
                "Either the job is starved — comparing the paths all at once leaves it "
                "and the poll that watches it competing with every other path for the "
                "same cores — or the connector is parked resolving a table definition. "
                "The Thrift Server log tells them apart: a starved job is still "
                "finishing tasks."
            ) from e
        except Exception as e:
            if any(marker in str(e) for marker in _STALE_VIEW_MARKERS):
                # Only a missing view is worth re-registering for.  Retrying
                # registration after any other failure re-enters the code path
                # that resolves a table definition, which is where the failures
                # come from in the first place.
                print("[db] Spark views look stale; re-registering and retrying")
                self._register_views()
                try:
                    return self.execute(sql)
                except Exception as retry_error:
                    raise RuntimeError(readable_error(retry_error)) from retry_error
            raise RuntimeError(readable_error(e)) from e

    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Run one statement and return its rows, holding the connection lock."""
        with self._lock:
            if self._conn is None:
                raise RuntimeError("Spark Thrift Server not connected")
            cur = self._conn.cursor()
            try:
                cur.execute(sql)
                # Hive qualifies result columns as "view.column"; the dashboard
                # wants the bare name so results line up with the other engines.
                columns = [d[0].split(".")[-1] for d in cur.description or []]
                rows = cur.fetchall() if columns else []
                return [dict(zip(columns, row)) for row in rows]
            finally:
                cur.close()

    def execute_ddl(self, ddl: str) -> None:
        """Run a statement for its effect, without reading a result set.

        Spark answers DDL with a column named "Result" and no rows, which PyHive
        cannot fetch — it assumes a described column implies fetchable data.  So
        DDL is issued and never fetched.
        """
        with self._lock:
            if self._conn is None:
                raise RuntimeError("Spark Thrift Server not connected")
            cur = self._conn.cursor()
            try:
                cur.execute(ddl)
            finally:
                cur.close()


class SparkBulkClient:
    """The Analytics bulk reader, over its own Thrift Server connection.

    Each query takes a fresh coordinated snapshot of just the tables it reads, so
    the answer is current and the timing includes what the mechanism costs.  The
    snapshot is released when the read completes, so snapshots do not accumulate.
    """

    # Options confirmed against org.apache.cassandra.spark.data.ClientConfig in
    # the vendored analytics jar, which expects "{strategy [ttl]}" with the
    # strategy spelled exactly as its enum: an unrecognised value is not an error,
    # it silently falls back to keeping the snapshot for ever.
    #
    # OnCompletionOrTTL rather than OnCompletion: the completion hook does not fire
    # for a query issued through the Thrift Server, so the TTL is what actually
    # releases the snapshot, and Cassandra enforces it whatever this process does.
    #
    # A snapshot hard-links live SSTables, so until it expires it keeps them from
    # being compacted away: the TTL wants to be short.  But it must outlast the
    # read, and Cassandra will clear it mid-read if it does not — the components
    # vanish and the read fails with "Required 1 replicas but only 0 responded",
    # which is what a fixed 15 minutes did to a 16-minute contended run.  So it is
    # derived from the socket timeout rather than guessed: that timeout bounds any
    # read the dashboard is still waiting on, so a TTL beyond it cannot expire under
    # a read whose answer anybody expects.  Doubled, for the snapshot this query
    # takes before that clock starts and for slack.
    SNAPSHOT_TTL_MINUTES = max(15, math.ceil(settings.spark_query_timeout_s / 60) * 2)
    CLEAR_SNAPSHOT_STRATEGY = f"OnCompletionOrTTL {SNAPSHOT_TTL_MINUTES}m"
    NUM_CORES = 4

    def __init__(self, thrift: SparkThriftClient):
        self._thrift = thrift

    @property
    def connected(self) -> bool:
        return self._thrift.connected

    @property
    def busy(self) -> bool:
        return self._thrift.busy

    def connect(self) -> None:
        self._thrift.connect()

    def abort(self) -> bool:
        return self._thrift.abort()

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """Snapshot what the statement reads, then run it."""
        for table in self._tables_in(sql):
            self._register_bulk_view(table)
        try:
            return self._thrift.execute(sql)
        except TTransportException as e:
            if self._thrift.was_aborted:
                raise RuntimeError(SparkThriftClient.CANCELLED_MESSAGE) from e
            raise RuntimeError(
                f"The bulk reader stopped answering for {settings.spark_query_timeout_s}s ({e}). "
                "Reading the whole history off SSTables is minutes of work, and with the "
                "other paths beside it the job outlasts this guard.  Spark carries on "
                "with it after this gives up, so cancel it from the Health page rather "
                "than leaving it to compete with the next run.  For a figure, run the "
                "paths one at a time; to wait it out under contention, raise "
                "SPARK_QUERY_TIMEOUT_S."
            ) from e
        except Exception as e:
            raise RuntimeError(readable_error(e)) from e

    @staticmethod
    def _tables_in(sql: str) -> Sequence[str]:
        """Which tables this statement reads.

        The statement arrives with its names already rewritten to the bulk views,
        so matching them needs no SQL parsing, and only the tables actually read
        are snapshotted.
        """
        lowered = sql.lower()
        return [t for t in REGISTERED_TABLES if f"{BULK_VIEW_PREFIX}{t}" in lowered]

    def _register_bulk_view(self, table: str) -> None:
        """Re-create the view, which takes the snapshot it will read.

        Snapshot names carry the clock as well as a counter, so a restarted
        backend cannot ask for a name that an earlier run took and has not yet
        released.
        """
        snapshot = f"htap_dashboard_{table}_{int(time.time())}_{next(_bulk_snapshot_counter)}"
        ddl = (
            f"CREATE OR REPLACE TEMP VIEW {BULK_VIEW_PREFIX}{table} "
            "USING org.apache.cassandra.spark.sparksql.CassandraDataSource "
            "OPTIONS ("
            f"  sidecar_contact_points '{settings.cassandra_host}',"
            f"  keyspace '{settings.cassandra_keyspace}',"
            f"  table '{table}',"
            f"  DC '{settings.cassandra_datacenter}',"
            "  createSnapshot 'true',"
            f"  snapshotName '{snapshot}',"
            f"  clearSnapshotStrategy '{self.CLEAR_SNAPSHOT_STRATEGY}',"
            f"  numCores '{self.NUM_CORES}'"
            ")"
        )
        self._thrift.execute_ddl(ddl)


spark_client = SparkThriftClient(name="connector")
spark_bulk_client = SparkBulkClient(
    SparkThriftClient(name="bulk-reader", register_connector_views=False)
)
