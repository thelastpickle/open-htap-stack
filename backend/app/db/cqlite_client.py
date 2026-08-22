"""cqlite client — SQL over the live SSTable files, in this process.

The fifth access path, and the only one that reads Cassandra's data files where
they lie.  There is no snapshot, no Sidecar and no JVM: the `cqlite_datafusion`
extension module opens the `Data.db` files a flush or a compaction has already
written, k-way merges the generations so each row is resolved once, and hands the
rows to DataFusion, which plans and executes the SQL.

Two consequences the dashboard has to state rather than hide.  A query answers as
of the last flush, so rows still in a memtable are invisible and `data_age_s`
says how stale the answer was.  And a table Cassandra has never flushed has no
files to read, so the path declines instead of returning an empty answer, which
is the state a stack that started minutes ago is in.

The reader cannot contend with the request path, because it never enters it: no
coordinator, no read repair and no page cache of Cassandra's is involved.  That
is the same claim the bulk reader makes, without the snapshot.
"""
import glob
import os
import threading
import time
from typing import Any, Dict, List, Optional

from app.config import settings
from app.db.cassandra_client import cassandra_client

# The tables the path offers.  drone_text_embeddings is left out on purpose: it
# holds a vector<float, n> column, which this reader has no Arrow type for, so
# opening it would fail rather than answering.
CQLITE_TABLES = ("events", "drone_latest_status", "drone_events_by_entity")

# How long an error message may be before it is cut.  A DataFusion failure
# carries the whole plan, and the first sentence is the part a viewer can act on.
_MESSAGE_LIMIT = 400

# How often registration is retried while nothing is registered.  See
# `ensure_registered`; the interval is what keeps a Health page poll from
# re-reading three schemas every few seconds.
_RETRY_AFTER_S = 30.0


def readable_error(error: Exception) -> str:
    """Reduce a DataFusion or reader failure to its first sentence."""
    text = " ".join(str(error).split())
    # Each layer the failure crossed adds its own prefix, and they nest: a reader
    # error arrives as "FFI error: External error: cqlite: ...".  Strip repeatedly
    # rather than once, so what is left is the sentence the reader wrote.
    prefixes = (
        "FFI error: ",
        "DataFusion error: ",
        "Execution error: ",
        "External error: ",
        "cqlite: ",
    )
    stripped = True
    while stripped:
        stripped = False
        for prefix in prefixes:
            if text.startswith(prefix):
                text = text[len(prefix):]
                stripped = True
    return text[:_MESSAGE_LIMIT]


class CqliteClient:
    """One DataFusion session over the SSTable directories of one keyspace.

    Duck-types to the same contract the other four engines answer: ``connected``,
    a ``connect`` that reports rather than raises, ``execute_query``, ``busy`` and
    ``abort``.  It is not a service and holds no connection; connecting here means
    resolving each table's directory and its `CREATE TABLE` statement, and
    registering a provider per table.
    """

    # There is no snapshot to reuse, so the comparison must not offer to.
    SUPPORTS_SNAPSHOT_REUSE = False

    CANCELLED_MESSAGE = (
        "Cancelled: the scan was stopped, so the reader abandoned the merge.  "
        "The next query starts a new one."
    )

    def __init__(self):
        self._ctx: Optional[Any] = None
        self._providers: Dict[str, Any] = {}
        # Why each table that is not registered is not registered, for the Health
        # page: an unflushed table and a missing mount are different problems.
        self._declined: Dict[str, str] = {}
        self._connect_lock = threading.Lock()
        # Re-discovery and execution are one operation, as they are for the bulk
        # reader: a provider lists its directory again inside the scan, and two
        # statements interleaving would report each other's figures.
        self._query_lock = threading.Lock()
        # What the last query on *this thread* read.  Thread-local, because each
        # path of a parallel comparison runs on its own thread.
        self._measured = threading.local()
        # When registration was last attempted, so `ensure_registered` can retry
        # without a poll turning into a schema read every few seconds.
        self._attempted_at = 0.0
        # Set by abort() and read by the statement that was running, which is how a
        # cancelled scan is told from one that failed: both arrive as an error from
        # the reader, and only the operator's intent distinguishes them.
        self._aborted = False
        self.connected = False

    # ──────────────────────── What the last query read ────────────────────────

    @property
    def last_sstable_files(self) -> Optional[int]:
        """Live Data.db files this thread's last query merged."""
        return getattr(self._measured, "files", None)

    @property
    def last_sstable_bytes(self) -> Optional[int]:
        """Their total size.

        The counterpart of the bulk reader's snapshot_bytes, and it carries the
        same caveat: it is the size of the files the scan opened, which is not
        what it read when the statement named a partition.
        """
        return getattr(self._measured, "bytes", None)

    @property
    def last_reader_open_ms(self) -> Optional[float]:
        """What listing the directories and opening the files cost.

        This is the whole of the path's fixed cost, and it is the figure to
        compare against the bulk reader's snapshot_ms.
        """
        return getattr(self._measured, "reader_open_ms", None)

    @property
    def last_data_age_s(self) -> Optional[int]:
        """Seconds since the newest file the last query read was written.

        So: how stale the answer was.  Rows written since are in a memtable and
        were not read.  A path that takes no snapshot still owes a viewer this.
        """
        return getattr(self._measured, "data_age_s", None)

    @property
    def busy(self) -> bool:
        """True while a statement is in flight."""
        return self._query_lock.locked()

    @property
    def tables(self) -> Dict[str, str]:
        """The directory registered for each table, for the Health page."""
        return {
            table: provider.directory
            for table, provider in sorted(self._providers.items())
        }

    @property
    def declined(self) -> Dict[str, str]:
        """Why each table that could not be registered could not be."""
        return dict(self._declined)

    def files_now(self) -> Dict[str, int]:
        """What the registered directories hold at this moment.

        Reads the directories rather than any SSTable, so it is cheap enough for a
        health probe to call.  A table Cassandra has not flushed counts as nothing
        rather than as a failure: it is a state the stack passes through, and a
        probe that raised there would report the whole path down.
        """
        files = 0
        size = 0
        for provider in self._providers.values():
            try:
                found = provider.discover()
            except Exception:
                continue
            files += found["files"]
            size += found["bytes"]
        return {"tables": len(self._providers), "files": files, "bytes": size}

    # ──────────────────────── Connecting ────────────────────────

    def connect(self) -> None:
        """Register a provider per table.  Reports failures rather than raising.

        Cassandra is asked for each table's `CREATE TABLE` statement, so the
        schema this reader parses the files with cannot drift from the schema that
        wrote them.  That makes this path depend on the CQL path being up, once,
        to connect; a query then needs nothing but the files.
        """
        with self._connect_lock:
            self._attempted_at = time.monotonic()
            self._declined = {}
            try:
                from datafusion import SessionContext
                from cqlite_datafusion import SSTableProvider
            except ImportError as e:
                self.connected = False
                print(f"[db] cqlite reader unavailable: {e}")
                return

            if not cassandra_client.connected:
                cassandra_client.connect()
            if not cassandra_client.connected:
                self.connected = False
                print("[db] cqlite reader needs Cassandra once, for the schema; "
                      "it is not connected")
                return

            ctx = SessionContext()
            providers: Dict[str, Any] = {}
            for table in CQLITE_TABLES:
                try:
                    directory = self._table_directory(table)
                    ddl = self._create_table_cql(table)
                    provider = SSTableProvider.open(
                        directory,
                        ddl,
                        splits=settings.cqlite_splits,
                        batch_rows=settings.cqlite_batch_rows,
                    )
                    ctx.register_table(table, provider)
                    providers[table] = provider
                    print(f"[db] cqlite provider registered: {table} at {directory}")
                except Exception as e:
                    self._declined[table] = readable_error(e)
                    print(f"[db] cqlite provider failed for {table}: {readable_error(e)}")

            self._ctx = ctx
            self._providers = providers
            # Connected if anything is readable.  One unflushed table must not
            # take the path away from the statements that name the others.
            self.connected = bool(providers)

    def ensure_registered(self) -> bool:
        """Register whatever is not registered yet, at most every 30s.

        The other four paths are probed by opening a socket, so a service that
        arrives late is noticed on the next poll.  This path has no socket, and
        registration needs the keyspace to exist: a backend that started before
        the sink created the schema registers nothing, and nothing afterwards
        would look again.  The Health page's probe calls this so the path recovers
        on its own rather than at the first query.

        The partial case is retried as well, and separately, because it is the one
        a clean start actually produces: the sink creates the three tables one
        statement at a time, so a backend registering between two of them gets
        `events` and neither of the others, and a policy that only retried the
        all-or-nothing case left those two missing for the life of the process.
        Measured: this failed the CI dashboard step at its first comparison, where
        cqlite reported "table 'drone_latest_status' not found".

        A missing table is added to the session that is already there, rather than
        the session being rebuilt.  That is what keeps the retry safe: a provider
        another thread is scanning is never replaced, which is the reason the
        all-or-nothing restriction existed.
        """
        if len(self._providers) == len(CQLITE_TABLES):
            return True
        if time.monotonic() - self._attempted_at < _RETRY_AFTER_S:
            return bool(self._providers)
        if not self._providers:
            self.connect()
            return bool(self._providers)
        self._register_missing()
        return bool(self._providers)

    def _register_missing(self) -> None:
        """Add the tables that are not registered to the existing session.

        Reports each failure and keeps going, as `connect` does: a table that is
        still unflushed is expected on a young stack, and must not take the path
        away from the tables that are readable.
        """
        with self._connect_lock:
            self._attempted_at = time.monotonic()
            ctx = self._ctx
            if ctx is None:
                return
            try:
                from cqlite_datafusion import SSTableProvider
            except ImportError as e:
                print(f"[db] cqlite reader unavailable: {e}")
                return
            if not cassandra_client.connected:
                cassandra_client.connect()
            if not cassandra_client.connected:
                return
            for table in CQLITE_TABLES:
                if table in self._providers:
                    continue
                try:
                    directory = self._table_directory(table)
                    ddl = self._create_table_cql(table)
                    provider = SSTableProvider.open(
                        directory,
                        ddl,
                        splits=settings.cqlite_splits,
                        batch_rows=settings.cqlite_batch_rows,
                    )
                    # register_table refuses a name the session already holds, so
                    # the name is taken away first.  It should not be held: a
                    # table absent from _providers failed to register.  Doing it
                    # anyway makes the retry idempotent rather than permanently
                    # stuck on "the table already exists", and it cannot disturb a
                    # scan: DataFusion clones the provider into the plan, so a
                    # scan already running keeps the one it started with.
                    try:
                        ctx.deregister_table(table)
                    except Exception:
                        pass
                    ctx.register_table(table, provider)
                    self._providers[table] = provider
                    self._declined.pop(table, None)
                    print(f"[db] cqlite provider registered late: {table} at {directory}")
                except Exception as e:
                    self._declined[table] = readable_error(e)

    def _table_directory(self, table: str) -> str:
        """The directory Cassandra keeps this table's SSTable files in.

        The name is ``<table>-<id>`` with the table's UUID stripped of its
        dashes, and the id changes when a table is dropped and recreated, so it is
        taken from the cluster rather than remembered.  A directory left behind by
        an earlier incarnation of the table is still on disk, and reading it would
        answer from data the cluster has forgotten.
        """
        keyspace = settings.cassandra_keyspace
        root = os.path.join(settings.cqlite_data_dir, keyspace)
        metadata = cassandra_client.session.cluster.metadata
        table_id = getattr(metadata.keyspaces[keyspace].tables[table], "id", None)
        if table_id:
            directory = os.path.join(root, f"{table}-{str(table_id).replace('-', '')}")
            if os.path.isdir(directory):
                return directory
        # No id in the metadata: fall back to the one directory that matches, and
        # refuse to guess between several.
        matches = sorted(glob.glob(os.path.join(root, f"{table}-*")))
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise FileNotFoundError(
                f"no directory for {keyspace}.{table} under {root}; the data "
                "directory is mounted read-only at CQLITE_DATA_DIR"
            )
        raise RuntimeError(
            f"{len(matches)} directories match {keyspace}.{table} under {root} "
            "and the cluster did not say which is current"
        )

    def _create_table_cql(self, table: str) -> str:
        """This table's own `CREATE TABLE`, from the cluster's schema metadata."""
        keyspace = settings.cassandra_keyspace
        metadata = cassandra_client.session.cluster.metadata
        return metadata.keyspaces[keyspace].tables[table].as_cql_query()

    def abort(self) -> bool:
        """Stop every scan that is running.

        Cancellation is cooperative: the merge polls a flag and gives up at its
        next partition, so a scan stops in a fraction of a second without the
        connection being torn down.  Nothing else has to be rebuilt afterwards,
        which is why this path needs no reconnect after a cancel.

        Returns False when nothing was running.
        """
        running = self.busy
        if running:
            self._aborted = True
        for provider in self._providers.values():
            provider.cancel()
        if running:
            print("[db] cqlite scans cancelled")
        return running

    # ──────────────────────── Running a statement ────────────────────────

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """Plan and run one statement, then report what it read."""
        self._reset_measured()
        with self._query_lock:
            if self._ctx is None:
                raise RuntimeError("cqlite reader not connected")
            self._aborted = False
            try:
                batches = self._ctx.sql(sql).collect()
            except Exception as e:
                # The figures are still worth having: a cancelled or failed scan
                # opened its files, and how many it opened is part of the story.
                self._collect_measured(sql)
                message = readable_error(e)
                # A cancelled merge reports itself as an error like any other, so
                # say which it was: the reader's own wording is accurate and
                # unhelpful, and an operator who pressed stop should read that they
                # stopped it.
                if self._aborted or "cancelled" in message.lower():
                    raise RuntimeError(self.CANCELLED_MESSAGE) from e
                raise RuntimeError(message) from e
            self._collect_measured(sql)

        rows: List[Dict[str, Any]] = []
        for batch in batches:
            rows.extend(batch.to_pylist())
        return [
            {name: _plain(value) for name, value in row.items()} for row in rows
        ]

    def _reset_measured(self) -> None:
        self._measured.files = None
        self._measured.bytes = None
        self._measured.reader_open_ms = None
        self._measured.data_age_s = None

    def _collect_measured(self, sql: str) -> None:
        """Add up what each provider the statement named reported."""
        lowered = sql.lower()
        scans = [
            provider.last_scan
            for table, provider in self._providers.items()
            if table in lowered
        ]
        if not scans:
            return
        self._measured.files = sum(scan["files"] for scan in scans)
        self._measured.bytes = sum(scan["bytes"] for scan in scans)
        self._measured.reader_open_ms = round(
            sum(scan["reader_open_ms"] for scan in scans), 1
        )
        ages = [scan["data_age_s"] for scan in scans if scan["data_age_s"] is not None]
        # The oldest, because that is the age of the least current thing read.
        self._measured.data_age_s = max(ages) if ages else None


def _plain(value: Any) -> Any:
    """Render a value the way the other paths render it.

    Arrow gives back datetimes and dates as Python objects; the CQL path sends
    them as ISO-8601 strings, and a comparison whose paths disagree about how a
    timestamp is spelled cannot be checked row against row.
    """
    return value.isoformat() if hasattr(value, "isoformat") else value


cqlite_client = CqliteClient()
