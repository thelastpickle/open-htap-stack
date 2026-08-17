"""PrestoDB client — the OLAP side of the demo.

Presto reads the same live Cassandra tables through its Cassandra connector
(see presto/etc/catalog/cassandra.properties), so an analytical query here and
a point read on the Cassandra client see the same data with no ETL between them.
That is the whole point of the stack, so the dashboard runs both and shows both.
"""
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
import prestodb

from app.config import settings

# States the coordinator uses for a query that has stopped.  Anything else is
# still costing the cluster something and belongs on the Health page.
_SETTLED_STATES = ("FINISHED", "FAILED", "CANCELED")

# The Health page polls the query list, so a coordinator that is slow to answer
# should be reported as such rather than delay the whole page.
_REST_TIMEOUT_S = 3.0


class PrestoClient:
    """Thin DBAPI wrapper.  One connection, serialised by a lock."""

    def __init__(self):
        self._conn: Optional[Any] = None
        self._lock = threading.Lock()
        self.connected = False

    @property
    def busy(self) -> bool:
        """True while a query is in flight on this connection.

        Worth asking before reconnecting: connect() takes the same lock a query
        holds, so it would otherwise wait the query out rather than doing anything.
        """
        return self._lock.locked()

    def connect(self) -> None:
        """Open a connection and prove it works.

        The DBAPI connect() is lazy, so an unreachable coordinator would
        otherwise look connected until the first real query.
        """
        with self._lock:
            try:
                conn = prestodb.dbapi.connect(
                    host=settings.presto_host,
                    port=settings.presto_port,
                    user=settings.presto_user,
                    catalog=settings.presto_catalog,
                    schema=settings.presto_schema,
                )
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchall()
                cur.close()
                self._conn = conn
                self.connected = True
                print(f"[db] Presto connected: {settings.presto_host}:{settings.presto_port}")
            except Exception as e:
                self._conn = None
                self.connected = False
                print(f"[db] Presto connection failed: {e}")

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        with self._lock:
            if self._conn is None:
                raise RuntimeError("Presto not connected")
            cur = self._conn.cursor()
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description or []]
                return [dict(zip(columns, row)) for row in rows]
            except Exception:
                # Could be a bad query or a coordinator that went away; drop the
                # connection either way so the next call re-probes it.  connect()
                # is a single SELECT 1, so re-proving costs little.
                self._conn = None
                self.connected = False
                raise
            finally:
                cur.close()

    # ── Seeing and stopping work, over the coordinator's REST API ──
    #
    # Not over the connection above, and not as SQL against system.runtime: the
    # connection is serialised by a lock, so the one query worth asking about is
    # the one holding the lock that would answer.  REST needs no connection, and
    # a listing that is not itself a query does not appear in its own results.

    @staticmethod
    def _rest_url(path: str) -> str:
        return f"http://{settings.presto_host}:{settings.presto_port}{path}"

    def running_queries(self) -> List[Dict[str, Any]]:
        """Every query the coordinator has not finished with, oldest first.

        Unfiltered, and then filtered here.  The coordinator can select by state,
        but only one state per request, and there are seven a query can be in
        without having finished — including PLANNING, which is exactly where a
        query worth noticing gets stuck.  So this fetches the coordinator's whole
        recent history, a couple of hundred kilobytes over the compose network to
        be sure the answer is complete, rather than seven requests or a partial one.
        """
        response = httpx.get(self._rest_url("/v1/query"), timeout=_REST_TIMEOUT_S)
        response.raise_for_status()
        now = datetime.now(timezone.utc)
        running = []
        for query in response.json():
            if query.get("state") in _SETTLED_STATES:
                continue
            stats = query.get("queryStats") or {}
            created = stats.get("createTime")
            elapsed_s = 0.0
            if created:
                # Timed here rather than read from queryStats.elapsedTime, which
                # the coordinator formats for people ("17.44m") and would have to
                # be parsed back.
                try:
                    started = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    elapsed_s = max(0.0, (now - started).total_seconds())
                except ValueError:
                    pass
            session = query.get("session") or {}
            running.append(
                {
                    "id": query.get("queryId", ""),
                    "state": (query.get("state") or "").lower(),
                    "sql": " ".join((query.get("query") or "").split())[:300],
                    "running_s": round(elapsed_s, 1),
                    "user": session.get("user") or "",
                    "source": session.get("source") or "",
                }
            )
        return sorted(running, key=lambda q: -q["running_s"])

    def kill_query(self, query_id: str) -> None:
        """Ask the coordinator to cancel one query.

        A DELETE is the coordinator's own cancel, so it needs no session and works
        while this client's connection is busy with the very query being killed.
        """
        response = httpx.delete(self._rest_url(f"/v1/query/{query_id}"), timeout=_REST_TIMEOUT_S)
        if response.status_code >= 400:
            raise RuntimeError(
                f"Presto refused to cancel {query_id} (HTTP {response.status_code})"
            )


presto_client = PrestoClient()
