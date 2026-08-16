"""PrestoDB client — the OLAP side of the demo.

Presto reads the same live Cassandra tables through its Cassandra connector
(see presto/etc/catalog/cassandra.properties), so an analytical query here and
a point read on the Cassandra client see the same data with no ETL between them.
That is the whole point of the stack, so the dashboard runs both and shows both.
"""
import threading
from typing import Any, Dict, List, Optional

import prestodb

from app.config import settings


class PrestoClient:
    """Thin DBAPI wrapper.  One connection, serialised by a lock."""

    def __init__(self):
        self._conn: Optional[Any] = None
        self._lock = threading.Lock()
        self.connected = False

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


presto_client = PrestoClient()
