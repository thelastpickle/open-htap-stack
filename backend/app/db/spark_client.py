"""Spark Thrift Server client — the batch-analytics side of the demo.

Spark reads Cassandra directly through the spark-cassandra-connector, using the
same ``USING org.apache.spark.sql.cassandra`` data source the repository's CI
exercises.  Nothing is copied or bridged through another engine, so a timing
measured here is Spark's own.
"""
import re
import threading
from typing import Any, Dict, List, Optional

from thrift.transport import TSocket, TTransport
from thrift.transport.TTransport import TTransportException

from app.config import settings

# Tables the dashboard queries, registered as temp views on connect.  TEMP views
# keep the Derby metastore out of it: they live and die with the Thrift Server
# session, so a restart cannot leave a stale definition behind.
REGISTERED_VIEWS = ("drone_latest_status", "drone_events_by_entity")

# Errors that mean the views need rebuilding — the session was recycled, or the
# underlying table was replaced since they were registered.
_STALE_VIEW_MARKERS = ("TABLE_OR_VIEW_NOT_FOUND", "cannot be found", "does not exist", "UNRESOLVED_")

# PyHive surfaces a server-side failure as the whole Thrift response object: a
# wall of JVM stack frames with the actual message buried in it.  These pull out
# the part a person can act on.
_HIVE_MESSAGE_RE = re.compile(r'errorMessage="((?:[^"\\]|\\.)*)"')
_HIVE_EXCEPTION_RE = re.compile(r"\*?(?:[\w.]+\.)?(\w+(?:Exception|Error)):([^:\']{0,300})")


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
    """Thin HiveServer2 wrapper.  One connection, serialised by a lock."""

    def __init__(self):
        self._conn: Optional[Any] = None
        self._lock = threading.Lock()
        self.connected = False

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
                socket = TSocket.TSocket(settings.spark_thrift_host, settings.spark_thrift_port)
                socket.setTimeout(settings.spark_query_timeout_s * 1000)
                self._conn = hive.connect(
                    thrift_transport=TTransport.TBufferedTransport(socket),
                    database="default",
                )
                self.connected = True
                print(
                    f"[db] Spark Thrift Server connected: "
                    f"{settings.spark_thrift_host}:{settings.spark_thrift_port}"
                )
            except Exception as e:
                self._close_locked()
                print(f"[db] Spark Thrift Server connection failed: {e}")
                return
        self._register_views()

    def _close_locked(self) -> None:
        """Drop the connection.  Callers must hold the lock."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = None
        self.connected = False

    def _register_views(self) -> None:
        """Point Spark at the Cassandra tables.

        Best effort: if the connector or the keyspace is not ready, the views are
        missing and queries say so, rather than the dashboard claiming Spark is
        healthy and returning nothing.
        """
        for view in REGISTERED_VIEWS:
            ddl = (
                f"CREATE OR REPLACE TEMP VIEW {view} "
                "USING org.apache.spark.sql.cassandra "
                f"OPTIONS (keyspace '{settings.cassandra_keyspace}', table '{view}')"
            )
            try:
                self._execute_ddl(ddl)
                print(f"[db] Spark view registered: {view}")
            except Exception as e:
                print(f"[db] Spark view registration failed for {view}: {readable_error(e)}")

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """Run a query, rebuilding the views once if they have gone stale."""
        try:
            return self._execute(sql)
        except TTransportException as e:
            # The socket timed out or the server went away: this connection is
            # not reusable, so drop it and let the next call reconnect.
            with self._lock:
                self._close_locked()
            raise RuntimeError(
                f"Spark did not answer within {settings.spark_query_timeout_s}s: {e}"
            ) from e
        except Exception as e:
            if any(marker in str(e) for marker in _STALE_VIEW_MARKERS):
                # The session lost its temp views, or the table behind one was
                # replaced.  Re-register and give the query one more go.
                print("[db] Spark views look stale; re-registering and retrying")
                self._register_views()
                try:
                    return self._execute(sql)
                except Exception as retry_error:
                    raise RuntimeError(readable_error(retry_error)) from retry_error
            raise RuntimeError(readable_error(e)) from e

    def _execute(self, sql: str) -> List[Dict[str, Any]]:
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

    def _execute_ddl(self, ddl: str) -> None:
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


spark_client = SparkThriftClient()
