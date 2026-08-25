"""The two schemas this stack holds, read from the engines that own them.

Nothing on the dashboard answered "what is the data model" before this.  The demo
keyspace is described in docs/DATA-MODEL.md and in a comment in the sink, and neither
is visible from a browser; cassandra-sql's own tables were described nowhere.

Two routes rather than one, because the two engines fail apart: Cassandra can be up
while cassandra-sql is down, and a page that read both in one call would blank the half
it could still answer.

Where each fact comes from, and why, since every obvious source here is wrong in some
way.

**``transactional_mode`` comes from ``DESCRIBE KEYSPACE``.**  It is not a column of
``system_schema.tables`` -- ``SELECT transactional_mode FROM system_schema.tables`` is
refused with "Undefined column name" -- and nothing else there distinguishes a
transactional table from a plain one: ``session_timeline`` and its non-transactional
twin have identical flags, extensions and fast_path.  ``DESCRIBE`` is server-side from
Cassandra 4.0, so the driver can run it: one ``DESCRIBE KEYSPACE demo`` returns 16 rows
-- the keyspace, 14 tables and one index -- each with a ``create_statement``, and
``transactional_mode`` is in that text.  Six tables read ``full`` and eight read
``off``.  This replaces the behavioural probe in transactions.py, which needed one
transaction per table and could only ask about tables whose key it knew how to bind.

**The keys come from ``system_schema.columns``**, which does carry ``kind``,
``position`` and ``clustering_order`` as columns, so they need no parsing.  It is a
bounded read by ``keyspace_name``, which keeps cassandra_client.py's promise that every
read through it is a point read or a bounded scan.

**On the SQL side the catalog is partly stale, so the source matters.**  Measured at
cassandra-sql revision a0257ec9a22ff84daaf6f529ae8b523fdc45b431:

- ``pg_catalog.pg_tables`` is **stale after a DROP**.  It listed ten tables when five
  existed, four of them from a schema this page had already dropped; ``SELECT COUNT(*)
  FROM products`` answered "Table does not exist: PRODUCTS" while ``pg_tables`` still
  named it.  So it is not read here.
- ``pg_class WHERE relkind = 'r'`` is accurate: exactly the five live tables, with an
  ``oid`` and a column count.  ``pg_attribute WHERE attrelid = <oid> ORDER BY attnum``
  is accurate too, and a WHERE and an ORDER BY both work there because the statement is
  ungrouped.
- **``pg_attribute`` does not exist until one CREATE TABLE has run in that JVM.**
  ``PgCatalogManager`` logs "tables will be created on first use", and a cassandra-sql
  that has restarted answers "Table does not exist: PG_ATTRIBUTE" to every read of it
  while ``pg_class`` still answers its five rows from the key-value store.  One CREATE
  TABLE registers the view and a DROP TABLE does not remove it, so this route reports the
  refusal per table rather than treating a five-column table as a zero-column one.  The
  rows of a dropped table stay as well: the five tables created twice held 80 attribute
  rows for their 40 live columns, which the per-``oid`` read is indifferent to because
  each new table gets a new ``oid``.
- **The two index catalogs disagree with each other.**  ``pg_indexes`` held 14 rows
  including one on a dropped table; ``pg_class WHERE relkind = 'i'`` held 2, missing
  four of the five primary-key indexes.  Neither is reported as fact: the route says
  what each said and warns that they differ.
- ``information_schema`` does not exist.  ``pg_constraint`` exists and is empty, so the
  one constraint this engine enforces -- UNIQUE -- is the one its catalog does not
  report.  ``pg_enum`` and ``pg_sequence`` do not exist, though the schema declares two
  ENUMs and a sequence.

The joins are done in Python and not in SQL, and that is not a preference: a column
name appearing in two joined tables resolves to the wrong table here, and arithmetic
across a join returns one of its operands.  Joining ``pg_class`` to ``pg_attribute`` in
this engine would be using the defect the SQL subtab exists to document.
"""
import re
from typing import Any, Dict, List

from fastapi import APIRouter

from app.config import settings
from app.db.accord_sql_client import accord_sql_client
from app.db.cassandra_client import cassandra_client
from app.models import SchemaColumn, SchemaIndex, SchemaTable, SchemaView

router = APIRouter(prefix="/api/schema", tags=["schema"])

# The three keyspaces cassandra-sql encodes its SQL rows into.  Named as a constant
# rather than discovered, so the route can say so even when Cassandra is unreachable.
SQL_STORAGE_KEYSPACES = ("cassandra_sql", "cassandra_sql_internal", "pg_catalog")

# transactional_mode as DESCRIBE prints it: `AND transactional_mode = 'full'`.
_MODE = re.compile(r"transactional_mode\s*=\s*'(\w+)'")

# The index class and target, from system_schema.indexes options.
_SAI = "StorageAttachedIndex"

# pg_type is read once and mapped by oid.  Seven types exist there, and the mapping is
# read rather than hard-coded because the set is the engine's to change.
_UNKNOWN_TYPE = "unknown"


# ──────────────────────── The CQL side ────────────────────────


def _describe_keyspace(keyspace: str) -> Dict[str, Dict[str, str]]:
    """Every object in the keyspace, by type and name, with its CREATE statement.

    One statement for the whole keyspace: DESCRIBE is server-side, so this is one
    round trip rather than one per table.
    """
    described: Dict[str, Dict[str, str]] = {}
    for row in cassandra_client.execute_query(f"DESCRIBE KEYSPACE {keyspace}"):
        described.setdefault(row.get("type") or "", {})[row.get("name") or ""] = (
            row.get("create_statement") or ""
        )
    return described


def _cql_columns(keyspace: str) -> Dict[str, List[SchemaColumn]]:
    """Columns per table, in the order that identifies a row.

    Partition key first by position, then the clustering columns by position, then the
    rest by name.  That is the order a reader needs: it is the order the key is written
    in, and it is what makes a partition-key column visibly different from a column that
    merely comes first alphabetically.
    """
    rows = cassandra_client.execute_query(
        "SELECT table_name, column_name, kind, position, clustering_order, type "
        "FROM system_schema.columns WHERE keyspace_name = %s",
        (keyspace,),
    )
    ranks = {"partition_key": 0, "clustering": 1, "static": 2, "regular": 3}
    by_table: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        by_table.setdefault(row["table_name"], []).append(row)
    result: Dict[str, List[SchemaColumn]] = {}
    for table, columns in by_table.items():
        columns.sort(
            key=lambda c: (ranks.get(c["kind"], 4), c["position"], c["column_name"])
        )
        result[table] = [
            SchemaColumn(
                name=column["column_name"],
                type=column["type"],
                kind=column["kind"],
                position=column["position"],
                clustering_order=column["clustering_order"],
            )
            for column in columns
        ]
    return result


@router.get("/cql", response_model=SchemaView)
def cql_schema() -> SchemaView:
    """The demo keyspace: every table, its key, and whether Accord fronts it."""
    keyspace = settings.cassandra_keyspace
    view = SchemaView(engine="cassandra", keyspace=keyspace, storage_keyspaces=[keyspace])
    try:
        described = _describe_keyspace(keyspace)
        columns = _cql_columns(keyspace)
    except Exception as exc:
        view.error = str(exc)
        return view

    for name, create in sorted(described.get("table", {}).items()):
        mode = _MODE.search(create)
        view.tables.append(
            SchemaTable(
                name=name,
                columns=columns.get(name, []),
                transactional_mode=mode.group(1) if mode else "",
                create_statement=create,
            )
        )

    # The index's class and target come from system_schema.indexes, which reports both
    # as an options map; the CREATE statement DESCRIBE gives says the same thing in one
    # line, and the map is the easier of the two to render.
    try:
        for row in cassandra_client.execute_query(
            "SELECT table_name, index_name, kind, options "
            "FROM system_schema.indexes WHERE keyspace_name = %s",
            (keyspace,),
        ):
            options = row.get("options") or {}
            class_name = options.get("class_name", row.get("kind") or "")
            view.indexes.append(
                SchemaIndex(
                    name=row["index_name"],
                    table=row["table_name"],
                    detail=class_name.rsplit(".", 1)[-1] if class_name else "",
                    target=options.get("target", ""),
                )
            )
    except Exception as exc:
        view.warnings.append(f"the index list could not be read: {exc}")

    # Said here rather than in the UI copy, because it is the fact that decides whether
    # a table can carry a transaction and the page should not have to know it.
    accord = [table.name for table in view.tables if table.transactional_mode == "full"]
    view.warnings.append(
        f"{len(accord)} of {len(view.tables)} tables route reads and writes through "
        "Accord; the rest, events included, do not, so a transaction against one of "
        "them is refused."
    )
    return view


# ──────────────────────── The SQL side ────────────────────────


def _sql_types() -> Dict[str, str]:
    """oid to type name, read from pg_type rather than assumed."""
    try:
        rows = accord_sql_client.execute_query("SELECT oid, typname FROM pg_catalog.pg_type")
    except Exception:
        return {}
    return {str(row["oid"]): str(row["typname"]) for row in rows}


@router.get("/sql", response_model=SchemaView)
def sql_schema() -> SchemaView:
    """cassandra-sql's own tables, and the keyspaces they are encoded into."""
    view = SchemaView(
        engine="cassandra-sql",
        keyspace=settings.accord_sql_database,
        storage_keyspaces=list(SQL_STORAGE_KEYSPACES),
    )
    if not accord_sql_client.ensure_ready():
        view.error = (
            f"cassandra-sql is not reachable at {settings.accord_sql_host}:"
            f"{settings.accord_sql_port}"
        )
        return view

    types = _sql_types()
    try:
        relations = accord_sql_client.execute_query(
            "SELECT oid, relname, relnatts FROM pg_catalog.pg_class WHERE relkind = 'r'"
        )
    except Exception as exc:
        view.error = str(exc)
        return view

    for relation in sorted(relations, key=lambda r: str(r["relname"])):
        table = SchemaTable(name=str(relation["relname"]))
        try:
            attributes = accord_sql_client.execute_query(
                "SELECT attname, attnum, atttypid, attnotnull FROM pg_catalog.pg_attribute "
                f"WHERE attrelid = {int(relation['oid'])} ORDER BY attnum"
            )
        except Exception as exc:
            # The one recorded cause is the lazy catalog, so the recovery goes in the
            # note: a restarted cassandra-sql refuses pg_attribute until a CREATE TABLE
            # has run in that JVM, and /reset is that CREATE.
            table.note = (
                f"columns could not be read: {exc}."
                "  pg_attribute is created on first use, so a restarted cassandra-sql"
                " refuses it until one CREATE TABLE has run;"
                " POST /api/sql-console/reset is that CREATE."
            )
            view.tables.append(table)
            continue
        for attribute in attributes:
            # attnotnull is true for the primary key alone here, and a declared
            # `TEXT UNIQUE NOT NULL` reads false, so it is reported as the key it
            # marks rather than as a nullability the engine does not hold.
            primary = str(attribute["attnotnull"]).lower() == "true"
            table.columns.append(
                SchemaColumn(
                    name=str(attribute["attname"]),
                    type=types.get(str(attribute["atttypid"]), _UNKNOWN_TYPE),
                    kind="primary key" if primary else "regular",
                    position=0 if primary else -1,
                )
            )
        # COUNT(*) raises over an empty table here rather than answering zero, so the
        # count is left absent and the reason recorded against that table.
        try:
            counted = accord_sql_client.execute_query(f"SELECT COUNT(*) AS n FROM {table.name}")
            table.row_count = int(float(counted[0]["n"])) if counted else None
        except Exception as exc:
            table.note = f"no count: {exc}"
        view.tables.append(table)

    # Both index catalogs are reported, and the disagreement with them.  pg_indexes
    # carries the CREATE statement, which is the more useful of the two, but it lists
    # indexes on tables that no longer exist; pg_class carries fewer than exist.  So the
    # live tables filter the list and the warning says what was dropped.
    live = {table.name for table in view.tables}
    try:
        listed = accord_sql_client.execute_query(
            "SELECT indexname, tablename, indexdef FROM pg_catalog.pg_indexes"
        )
        stale = sorted({str(row["tablename"]) for row in listed} - live)
        for row in listed:
            if str(row["tablename"]) in live:
                view.indexes.append(
                    SchemaIndex(
                        name=str(row["indexname"]),
                        table=str(row["tablename"]),
                        detail=str(row["indexdef"]),
                    )
                )
        if stale:
            view.warnings.append(
                "pg_indexes still lists indexes on "
                + ", ".join(stale)
                + ", which no longer exist; this catalog is not cleared by DROP TABLE, and "
                "DROP INDEX is not implemented in either form."
            )
    except Exception as exc:
        view.warnings.append(f"the index list could not be read: {exc}")

    view.warnings.append(
        "There is no information_schema here, and pg_constraint is empty, so UNIQUE -- "
        "the one constraint this engine enforces -- is the one its catalog does not "
        "report.  pg_enum and pg_sequence do not exist, though the schema declares two "
        "ENUMs and a sequence."
    )
    return view
