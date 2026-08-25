"""cassandra-sql — Postgres-dialect SQL, and serializable transactions, over Accord.

GEICO's cassandra-sql speaks the Postgres wire protocol and stores SQL rows in
Cassandra as an ordered key-value encoding of its own, in its own keyspaces.  So
this is not a sixth way to read demo.events: it cannot read a table the sink
wrote, and it appears nowhere in the five-path comparison.  What it demonstrates
instead is the thing none of those five can do, over SQL rather than over CQL:
a multi-statement transaction that commits or does not.

Three things measured against the running service shape this client, and each is
a defect rather than a preference:

- **A bound parameter silently returns no rows.**  `WHERE customer_id = 1001`
  returns the row; the same statement binding the integer 1001 returns `[]`, with
  no error.  Binding the *string* "1001" returns the row, so the comparison is a
  text one and a typed bind misses it.  So execute_query takes one complete SQL
  string and never binds, and the console route must not offer parameters.
- **Every value arrives as text.**  The server sends no type OIDs worth the name,
  so psycopg does no conversion and every column comes back as a `str`.  Rows are
  passed on as they arrive; presenting them as numbers here would be inventing a
  type the server did not send.  What the text says is the server's own: an
  `INT` column reads back "49" as inserted and "48.0" after an `UPDATE` that
  subtracted from it, though "50" again after one that assigned a literal, so it
  is the arithmetic that promotes it.  `DECIMAL(10,2)` is a double too: 99.00
  reads back "99.0".
- **A statement may be a whole transaction.**  `BEGIN; INSERT ...; UPDATE ...;
  COMMIT;` in one execute() works and returns no result set, which is how the
  transaction preset runs.  psycopg is held in autocommit so that the SQL's own
  BEGIN is the only transaction there is.
"""
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg

from app.config import settings

# A statement that the parser accepts and that touches no table, for proving the
# connection.  `SELECT 1 AS one` is not it: the alias is rejected with "Table does
# not exist: unknown", which is a parser quirk about that identifier and not about
# the protocol, since `SELECT 1 AS ok` and a bare `SELECT 1` both answer.
_PROBE_SQL = "SELECT 1"


class AccordSqlClient:
    """Thin psycopg wrapper.  One connection, serialised by a lock."""

    def __init__(self):
        self._conn: Optional[psycopg.Connection] = None
        self._lock = threading.Lock()
        self.connected = False

    @property
    def busy(self) -> bool:
        """True while a statement is in flight.  Worth asking before reconnecting,
        which takes the same lock and would otherwise wait the statement out."""
        return self._lock.locked()

    def _dsn(self) -> str:
        return (
            f"host={settings.accord_sql_host} port={settings.accord_sql_port} "
            f"dbname={settings.accord_sql_database} user={settings.accord_sql_user} "
            f"connect_timeout={int(settings.accord_sql_connect_timeout_s)}"
        )

    def _open(self) -> None:
        """Open a connection and prove it answers.  Raises; the lock is held."""
        conn = psycopg.connect(self._dsn(), autocommit=True)
        with conn.cursor() as cur:
            cur.execute(_PROBE_SQL)
            cur.fetchall()
        self._conn = conn
        self.connected = True
        print(f"[db] cassandra-sql connected: {settings.accord_sql_host}:{settings.accord_sql_port}")

    def connect(self) -> None:
        """Open a connection at startup, and never fatally.

        Expected to fail on a cold stack, because the backend and cassandra-sql start
        together and this service creates three keyspaces and thirteen tables before
        it answers: 36.3 s on its first start after the image was built, and 3.7 to
        3.8 s on a restart afterwards.  ensure_ready() opens it on first use instead.
        """
        with self._lock:
            try:
                self._open()
            except Exception as e:
                self._conn = None
                self.connected = False
                print(f"[db] cassandra-sql connection failed: {e}")

    def ensure_ready(self) -> bool:
        """Prove the connection before a caller runs a statement over it.

        Worth a round trip, and this was measured rather than assumed: without it,
        every statement of the first batch after the service restarted failed with
        "not connected", because a dead socket is only discovered by the statement
        that uses it and the statements after that one found no connection at all.
        SELECT 1 answers in about 2 ms, so the batch starts on a connection that
        has just answered rather than on a flag that was true earlier.
        """
        with self._lock:
            if self._conn is not None:
                try:
                    with self._conn.cursor() as cur:
                        cur.execute(_PROBE_SQL)
                        cur.fetchall()
                    return True
                except Exception:
                    self._conn = None
                    self.connected = False
            try:
                self._open()
                return True
            except Exception as e:
                self._conn = None
                self.connected = False
                print(f"[db] cassandra-sql connection failed: {e}")
                return False

    def execute(self, sql: str) -> Tuple[List[str], List[List[Any]], float]:
        """Run one SQL string; return its columns, its rows and its duration in ms.

        The string may hold several statements separated by semicolons, which is
        how a BEGIN/COMMIT transaction is sent.  A statement that returns nothing
        gives no columns and no rows, which is not an error: an INSERT succeeding
        is the result.
        """
        with self._lock:
            if self._conn is None:
                # Opened here rather than refused, so that one failed statement does
                # not fail every statement after it in the same batch.
                self._open()
            started = time.perf_counter()
            try:
                with self._conn.cursor() as cur:
                    cur.execute(sql)
                    if cur.description is None:
                        return [], [], (time.perf_counter() - started) * 1000.0
                    columns = [d.name for d in cur.description]
                    rows = [list(row) for row in cur.fetchall()]
                    return columns, rows, (time.perf_counter() - started) * 1000.0
            except Exception:
                # Could be a rejected statement or a service that went away; drop
                # the connection either way so the next call re-probes it.  A
                # reconnect is one SELECT 1, so re-proving costs little.
                self._conn = None
                self.connected = False
                raise

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """The shape the other clients expose, for the Health page's probe."""
        columns, rows, _ = self.execute(sql)
        return [dict(zip(columns, row)) for row in rows]


accord_sql_client = AccordSqlClient()
