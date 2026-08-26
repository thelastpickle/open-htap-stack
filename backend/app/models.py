"""Request and response contracts for the dashboard API."""
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

# ──────────────────────── Overview / KPIs ────────────────────────


class AlertSummary(BaseModel):
    alert_id: str
    alert_time: str
    entity_id: str
    alert_type: str
    severity: str
    message: str
    risk_score: float


class OverviewKPIs(BaseModel):
    active_flying_drones: int = 0
    grounded_drones: int = 0
    total_drones: int = 0
    max_speed_mps: float = 0.0
    min_speed_mps: float = 0.0
    avg_speed_mps: float = 0.0
    max_altitude_m: float = 0.0
    min_altitude_m: float = 0.0
    avg_altitude_m: float = 0.0
    near_zone_count: int = 0
    predicted_breach_count: int = 0
    total_events: int = 0
    ingestion_rate_per_sec: float = 0.0
    # Fraction of the stack's services reachable, from the health probe.
    platform_health_score: float = 0.0
    latest_alerts: List[AlertSummary] = []


class IngestionBucket(BaseModel):
    time: str  # display label, "14:30"
    timestamp: str  # bucket key, "2026-08-16T14:30"
    count: int = 0


# ──────────────────────── Map / fleet ────────────────────────


class DronePosition(BaseModel):
    entity_id: str
    event_time: str
    latitude: float
    longitude: float
    altitude_m: float
    speed_mps: float
    heading_deg: float
    is_flying: bool
    temp_internal_c: float
    temp_external_c: float
    near_restricted_zone: bool = False
    predicted_zone_breach: bool = False
    risk_score: float = 0.0


class RestrictedZone(BaseModel):
    zone_id: str
    zone_name: str
    polygon_wkt: str
    severity: str
    enabled: bool = True


class MapLiveResponse(BaseModel):
    drones: List[DronePosition] = []
    zones: List[RestrictedZone] = []
    timestamp: str


class TrailPoint(BaseModel):
    event_time: str
    latitude: float
    longitude: float
    altitude_m: float = 0.0
    speed_mps: float = 0.0


class DroneTrail(BaseModel):
    entity_id: str
    points: List[TrailPoint] = []


class NearbyDroneResult(BaseModel):
    entity_id: str
    event_time: str
    latitude: float
    longitude: float
    altitude_m: float
    distance_m: float


class PolygonStatsRequest(BaseModel):
    polygon_wkt: str


class PolygonStatsResponse(BaseModel):
    drone_count: int = 0
    avg_speed_mps: float = 0.0
    max_speed_mps: float = 0.0
    avg_altitude_m: float = 0.0
    max_altitude_m: float = 0.0
    avg_temp_internal_c: float = 0.0


class WhatIfZoneRequest(BaseModel):
    polygon_wkt: str
    zone_name: str = "What-if zone"
    severity: str = "warning"


class WhatIfZoneResponse(BaseModel):
    zone: RestrictedZone
    drones_inside: int = 0
    drones_nearby: int = 0
    affected_drone_ids: List[str] = []


# ──────────────────────── Alerts ────────────────────────


class AlertRecord(AlertSummary):
    zone_id: Optional[str] = None
    latitude: float = 0.0
    longitude: float = 0.0
    altitude_m: float = 0.0


class AlertsResponse(BaseModel):
    alerts: List[AlertRecord] = []
    # Alerts found in the window, before any severity filter, so the UI's
    # per-severity counts add up to it.
    total_count: int = 0


# ──────────────────────── Query ────────────────────────


# Whether the bulk reader may read the snapshot the last query took instead of
# taking another.  Taking one is a hardlink pass over every SSTable, so it is a fixed
# cost per read that a bounded query pays in full; reusing skips it, and answers as
# of when that snapshot was taken rather than now.  Off by default, because "the same
# rows at the same moment" is the claim the comparison exists to make.
_REUSE_SNAPSHOT_FIELD = Field(
    default=False,
    description=(
        "Let the bulk reader re-read its last snapshot rather than take a new one. "
        "Faster, but the rows are as of that snapshot; the response says how old it was."
    ),
)


class SQLQueryRequest(BaseModel):
    sql: str
    limit: int = Field(default=10, ge=1, le=1000)
    engine: str = "cassandra"
    reuse_snapshot: bool = _REUSE_SNAPSHOT_FIELD


class SQLQueryResult(BaseModel):
    columns: List[str] = []
    rows: List[List[Any]] = []
    row_count: int = 0
    query_time_ms: float = 0.0
    # The statement as actually issued, after per-engine rewriting.
    sql: Optional[str] = None


class BenchmarkRequest(BaseModel):
    sql: str
    limit: int = Field(default=10, ge=1, le=1000)
    # Which access paths to compare.  None means all of them.  Naming a subset is
    # how a viewer asks a narrower question: two paths against each other, or one
    # path on its own as a reference.
    engines: Optional[List[str]] = None
    # "sequential" times each path alone, which is the only way a timing means
    # what it appears to.  "parallel" runs them at once, so the paths contend and
    # the figures show what that costs.  Both are legitimate; they answer
    # different questions, and the response says which was asked.
    mode: Literal["sequential", "parallel"] = "sequential"
    reuse_snapshot: bool = _REUSE_SNAPSHOT_FIELD


class OltpImpact(BaseModel):
    """What a single-partition read cost while something else was running.

    Sampled by the comparison endpoint, so the effect of an analytical query on
    the transactional path is shown rather than asserted.
    """

    p50_ms: float = 0.0
    p95_ms: float = 0.0
    max_ms: float = 0.0
    samples: int = 0
    # Point reads that did not come back at all during the window.
    failures: int = 0


class EngineResult(SQLQueryResult):
    # available=False means the engine could not be reached at all, which the UI
    # distinguishes from a query that reached it and failed.
    available: bool = True
    error: Optional[str] = None
    # The size of the snapshot a bulk read was taken over, which only the bulk
    # reader can report because only it reads files.  Reported because a scan of the
    # whole history is slow for the honest reason that it is large, and a demo table
    # that grows by tens of megabytes a minute otherwise looks like something
    # degrading.
    #
    # It is the volume the read had available, not always the volume it consumed: a
    # statement that names partitions reads only those, so the rate this implies is
    # only a throughput for a query that scans the lot.
    snapshot_bytes: Optional[int] = None
    # What preparing that snapshot cost, and how current it was.  Taking one is a
    # hardlink pass over every SSTable, so it costs the same however small the query
    # is; these three say what that came to, whether an older snapshot was read
    # instead, and how old the answer therefore is.
    snapshot_ms: Optional[float] = None
    snapshot_reused: bool = False
    snapshot_age_s: Optional[float] = None
    # The same four questions asked of the cqlite path, which reads the live files
    # and takes no snapshot: how many it merged, how big they were, what opening
    # them cost, and how stale the answer is.  Named for the files rather than for
    # a snapshot, because there is none to name.
    sstable_files: Optional[int] = None
    # Carries the snapshot_bytes caveat above for the same reason: it is what the
    # scan opened, not what it read when the statement named partitions.
    sstable_bytes: Optional[int] = None
    reader_open_ms: Optional[float] = None
    # Seconds since the newest file read was written, so how far behind the answer
    # is.  Rows still in a memtable are not in any file and were not read; this is
    # the counterpart of snapshot_age_s, and the path's one real limitation.
    data_age_s: Optional[float] = None
    # Absent unless the result came from the comparison endpoint, which is the
    # only caller that probes the OLTP path while a query runs.
    oltp: Optional[OltpImpact] = None


class BenchmarkResponse(BaseModel):
    # A path the request did not ask for is absent rather than empty, so a partial
    # comparison cannot be mistaken for five paths of which some failed.
    cassandra: Optional[EngineResult] = None
    presto: Optional[EngineResult] = None
    spark: Optional[EngineResult] = None
    # The Analytics bulk reader: same rows, read from a snapshot's SSTables.
    spark_bulk: Optional[EngineResult] = None
    # The cqlite reader: the same rows again, read from the live SSTables in place,
    # in this process, with no snapshot and no JVM.
    cqlite: Optional[EngineResult] = None
    # Which of the two run modes produced these figures.  Without it a parallel
    # run and a sequential one are indistinguishable, and they are not comparable.
    mode: str = "sequential"
    # The same point read measured before any path ran, so each path's figure has
    # something to be compared against.
    oltp_baseline: Optional[OltpImpact] = None
    # Set only for a parallel run: one sample covering the whole window, because
    # while the paths overlap the cost cannot be attributed to any one of them.
    oltp_combined: Optional[OltpImpact] = None
    # True when the run was stopped from the Health page.  Paths that had not
    # started are absent rather than reported as failures, since they never ran.
    cancelled: bool = False


class NLQueryRequest(BaseModel):
    prompt: str


class NLQueryResponse(BaseModel):
    generated_sql: Optional[str] = None
    engine: Optional[str] = None
    result: Optional[SQLQueryResult] = None
    error: Optional[str] = None
    render_hint: str = "table"  # table, map, chart, kpi


# ──────────────────────── Vector search ────────────────────────


class VectorSearchRequest(BaseModel):
    query: str
    limit: int = Field(default=5, ge=1, le=50)


class VectorSearchResponse(BaseModel):
    results: List[Dict[str, Any]] = []
    query_time_ms: float = 0.0


class LiveEmbeddingRequest(BaseModel):
    enabled: bool


class LiveEmbeddingStatus(BaseModel):
    """What the live embedder is doing, in the terms the Explore page shows.

    Each figure names what it measures.  ``pending`` is what the last pass had to
    defer and ``behind_s`` is how long ago that pass ran, so the two together say
    whether the index is following the writes or falling behind them.
    """

    enabled: bool = False
    embedder: str = "local"
    interval_s: float = 0.0
    # Totals since this backend started, not since the loop was last enabled: the
    # loop keeps what it has embedded across a disable, so resetting the counts
    # would misreport the work already done.
    embedded: int = 0
    failed: int = 0
    passes: int = 0
    # The last completed pass.
    last_embedded: int = 0
    last_pass_ms: float = 0.0
    pending: int = 0
    behind_s: Optional[float] = None
    tracked: int = 0
    error: Optional[str] = None


# ──────────────────────── Demo controls ────────────────────────


class DemoSettings(BaseModel):
    drones_enabled: int = Field(default=100, ge=1, le=100_000)
    events_per_sec: int = Field(default=2000, ge=1, le=1_000_000)
    # Percentage of telemetry readings carrying an anomalous internal
    # temperature, so the outlier queries on the Explore page have something to
    # find.
    outlier_percent: float = Field(default=5.0, ge=0.0, le=100.0)
    paused: bool = False


class DemoSettingsResponse(BaseModel):
    settings: DemoSettings
    success: bool = True
    message: str = ""


# ──────────────────────── Platform health ────────────────────────


class ServiceHealth(BaseModel):
    name: str
    status: str = "unknown"  # up, down, unknown
    endpoint: str = ""


class PlatformHealthResponse(BaseModel):
    services: List[ServiceHealth] = []
    overall_health_score: float = 0.0
    total_drones: int = 0


# ──────────────────── Work in flight, and stopping it ────────────────────


class RunningQuery(BaseModel):
    """One query an engine is still working on, whoever submitted it.

    The engines are asked directly, so this includes work the dashboard knows
    nothing about: a presto-cli session in the container shows up here too, which
    is usually what you want to know when the dashboard looks slow.
    """

    engine: str
    # The engine's own handle, and what a kill request has to name: a Presto
    # query_id, or a Spark job id.
    id: str
    state: str = ""
    running_s: float = 0.0
    sql: str = ""
    # Who submitted it, where the engine records that.  Empty when it does not.
    submitter: str = ""
    # Progress, for engines that report it.  Spark counts tasks; Presto does not
    # expose anything this simple, so both stay 0 there.
    tasks_done: int = 0
    tasks_total: int = 0


class ComparisonRun(BaseModel):
    """The comparison currently holding the one-at-a-time lock."""

    running_for_s: float = 0.0
    mode: str = "sequential"
    engines: List[str] = []
    sql: str = ""
    # Paths that have already answered, so a long run shows progress rather than
    # only an age.
    done: List[str] = []


class RunningWorkResponse(BaseModel):
    comparison: Optional[ComparisonRun] = None
    queries: List[RunningQuery] = []
    # Engines whose running work could not be read, with the reason.  Reported
    # rather than silently omitted: an empty list and an unreachable engine are
    # very different answers to "what is running?".
    unreadable: Dict[str, str] = {}


class KillQueryRequest(BaseModel):
    engine: Literal["presto", "spark"]
    id: str


class ReconnectRequest(BaseModel):
    # The dashboard's client for one path, or every one of them.  Reconnecting the
    # cqlite path re-reads the schema and re-resolves the table directories, which
    # is what picks up a table that has since been flushed or recreated.
    target: Literal["cassandra", "presto", "spark", "spark_bulk", "cqlite", "all"]


class OperationResult(BaseModel):
    """What an operator control actually did, in words the page can show."""

    ok: bool = True
    # One line per thing done, so a control that does several things says so
    # instead of reporting a bare success.
    actions: List[str] = []


# ──────────────────────── Accord transactions ────────────────────────


class TransactionStep(BaseModel):
    """One step of the session-timeline demo, and what the server made of it."""

    # What the step was asked to do, e.g. "apply seq=1" or "replay seq=0".
    action: str
    # The statement that ran, so the page shows the reader the real CQL rather
    # than a description of it.
    cql: str = ""
    # Whether the transaction's IF fired.  Derived from the projection, not read
    # from an [applied] column: an Accord transaction has none.
    applied: bool = False
    # Why it did not fire, in the words of the guard that stopped it, or "" when
    # it did fire.  This is the field the demo exists to show.
    reason: str = ""
    # The guard values the transaction projected, verbatim.
    projection: Dict[str, Any] = {}
    # Server-side latency of the one statement, timed at the backend.
    duration_ms: float = 0.0
    # Rows in session_timeline for this session after the step, so a refused step
    # is visibly a step that changed nothing.
    timeline_rows: int = 0
    # The same idea for a demo whose "changed nothing" is not a row count: the
    # clearance demo puts the zone's remaining slots and its holders here.  A step
    # is only convincing if the reader can see the state it did or did not move.
    state: Dict[str, Any] = {}
    error: Optional[str] = None


class TransactionTimelineRow(BaseModel):
    seq: int
    event_id: str
    event_time: str
    event_type: str
    payload: str


class TransactionDemoResult(BaseModel):
    """The whole scripted sequence, and the projection it left behind."""

    user_id: str
    session_id: str
    steps: List[TransactionStep] = []
    timeline: List[TransactionTimelineRow] = []
    # The two references, on the same row shape in a non-transactional twin table:
    # a plain INSERT and an IF NOT EXISTS lightweight transaction.  A transaction
    # latency means nothing without them.
    reference_ms: Dict[str, float] = {}
    # p50 and max of an applied transaction over repeats, when asked for.
    repeats: int = 0
    applied_p50_ms: Optional[float] = None
    applied_max_ms: Optional[float] = None
    # What the OLTP point read was doing while the transactions ran, and over an
    # idle window just before, so the claim that Accord stayed off the request path
    # is a difference the reader can see rather than one asserted.
    oltp_probe: Dict[str, Any] = {}
    oltp_baseline: Dict[str, Any] = {}


# ──────────────────────── Accord airspace clearance ────────────────────────


class ClearanceZone(BaseModel):
    """One restricted zone's clearance ledger, read from both sides."""

    zone_id: str
    zone_name: str = ""
    severity: str = ""
    capacity: int = 0
    # Slots left.  Held as a count-down rather than a count of grants because that is
    # what Accord can decrement in one statement: SET remaining -= 1 needs no
    # capacity to compare against, where a count-up would need the transaction to
    # compare two LET references, which Accord refuses.
    remaining: int = 0
    # Drones cleared into the zone, from the zone's own clearance partition.
    holders: List[str] = []
    # capacity == remaining + len(holders): the semaphore's whole invariant, and the
    # thing the demo exists to keep true.  Reported rather than asserted, because a
    # broken one is the interesting result and hiding it would defeat the point.
    consistent: bool = True


class ClearanceState(BaseModel):
    zones: List[ClearanceZone] = []
    # Holders whose own drone_clearance row names a different zone, or none at all.
    # The two tables are written by one transaction, so this must stay empty; it is
    # the cross-partition half of the invariant above.
    mismatched: List[str] = []


class ClearanceContentionResult(BaseModel):
    """What happened when many drones asked for the same zone at once.

    This is the claim the whole schema exists to make, so it is measured rather than
    described: ``granted`` must equal the zone's capacity however many asked, and the
    ledger must still add up afterwards.  A count-and-write done outside consensus
    would oversubscribe here, and the number would say so.
    """

    zone_id: str
    capacity: int = 0
    askers: int = 0
    granted: int = 0
    refused: int = 0
    # Who won, which differs between runs: that is what shows the asks genuinely
    # contended rather than being serialised by the client.
    winners: List[str] = []
    # Transactions that raised rather than being refused.  A refusal is the expected
    # outcome for a loser; an error is not, and the two must not be conflated.
    errors: List[str] = []
    # Wall clock for the whole overlapping set, not per ask.
    duration_ms: float = 0.0
    zone: Optional[ClearanceZone] = None


class ClearanceDemoResult(BaseModel):
    """The scripted clearance sequence, and the ledger it left behind."""

    zone_id: str
    entity_ids: List[str] = []
    steps: List[TransactionStep] = []
    state: ClearanceState
    # A grant and a release timed over repeats.  Two figures rather than one, because
    # they are two different transactions: a grant reads two partitions and writes
    # three, a release reads one and writes three.
    repeats: int = 0
    grant_p50_ms: Optional[float] = None
    grant_max_ms: Optional[float] = None
    release_p50_ms: Optional[float] = None
    release_max_ms: Optional[float] = None


# ──────────────────────── cassandra-sql console ────────────────────────


class SqlPreset(BaseModel):
    """One statement the cassandra-sql page offers, with what it demonstrates."""

    id: str
    title: str
    description: str
    sql: str


class SqlStatementResult(BaseModel):
    """One statement's outcome.  A statement is a whole SQL string, which may hold
    several statements separated by semicolons: that is how a BEGIN/COMMIT
    transaction is sent, and cassandra-sql executes the string as one unit."""

    sql: str
    # Column names as the server named them.  Empty for a statement that returns
    # no result set, which includes every DDL statement and every transaction.
    columns: List[str] = []
    # Rows as they arrived.  Every value is a string, because the server sends no
    # usable type OIDs; converting here would invent a type it did not send.
    rows: List[List[Any]] = []
    row_count: int = 0
    duration_ms: float = 0.0
    error: Optional[str] = None


class SqlConsoleResult(BaseModel):
    engine: str = "cassandra-sql"
    statements: List[SqlStatementResult] = []
    duration_ms: float = 0.0
    # Statements that raised.  Reported as a count as well as per statement, so a
    # seed run that is mostly "already exists" reads at a glance.
    error_count: int = 0


class SqlQuirk(BaseModel):
    """One measured cassandra-sql defect, with the control that isolates it.

    A defect is only a defect if something nearby works, so each carries both: the
    probe over a join, and the same expression over one table, which is exact.  The
    two are separate statements because a multi-statement string returns only its
    last result set here, so a paired demonstration cannot be one string.

    ``expected`` is what a correct engine would answer, written out rather than
    computed.  Without it a viewer sees two numbers and no reason to prefer either.
    """

    id: str
    title: str
    # What goes wrong, in one sentence.
    summary: str
    expected: str
    probe: SqlStatementResult
    control: SqlStatementResult


# ──────────────────────── Schema explorer ────────────────────────
#
# Two schemas that share no row: Cassandra's demo keyspace, and the SQL tables
# cassandra-sql keeps under an encoding of its own.  One model serves both, because
# what a reader wants of each is the same: what the columns are, what identifies a
# row, and what the engine will and will not promise about it.


class SchemaColumn(BaseModel):
    """One column, as the engine that owns it describes it."""

    name: str
    type: str
    # partition_key / clustering / regular / static on the CQL side.  On the SQL side
    # only "primary key" and "column" are distinguishable, because pg_attribute reports
    # attnotnull true for the primary key alone: `zone_code TEXT UNIQUE NOT NULL` reads
    # false, which agrees with NOT NULL going unenforced.
    kind: str = "regular"
    # Position within the partition key or the clustering key, -1 for neither.
    position: int = -1
    # asc / desc for a clustering column, none otherwise.
    clustering_order: str = "none"


class SchemaIndex(BaseModel):
    name: str
    table: str
    # The index class on the CQL side; the CREATE statement on the SQL side.
    detail: str
    target: str = ""


class SchemaTable(BaseModel):
    name: str
    columns: List[SchemaColumn] = []
    # off / mixed_reads / full on the CQL side, parsed out of the DESCRIBE statement
    # because system_schema.tables carries no such column.  Empty on the SQL side,
    # where the question does not arise: cassandra-sql's own tables are all Accord
    # tables and it is the engine, not the table, that decides.
    transactional_mode: str = ""
    # Rows, where the engine will answer cheaply.  Absent rather than zero when it
    # will not: COUNT(*) over an empty table raises on the SQL side.
    row_count: Optional[int] = None
    # The whole CREATE statement, for a reader who wants the options as well.
    create_statement: str = ""
    note: str = ""


class SchemaView(BaseModel):
    """One engine's whole schema, and what it could not answer."""

    engine: str
    keyspace: str
    tables: List[SchemaTable] = []
    indexes: List[SchemaIndex] = []
    # Keyspaces the rows are physically encoded into.  On the SQL side these are the
    # three cassandra-sql owns, which is how the page shows that this is SQL over
    # Cassandra rather than a second database.
    storage_keyspaces: List[str] = []
    # Read fresh on every request and never held, because a catalog here can go stale:
    # pg_tables still lists tables that were dropped.
    warnings: List[str] = []
    error: Optional[str] = None


# ──────────────────────── Change Data Capture ────────────────────────


class CdcRecord(BaseModel):
    """One mutation, as it arrived on the CDC topic.

    ``columns`` is what the Sidecar's Avro record carries beside its own header
    fields, so it is the table's own columns for the mutation that touched them.  An
    UPDATE names only what it wrote, which is why a row here can be sparse.
    """

    # Monotonic within this backend's run, so the page can ask for what it has not
    # seen without trusting Kafka offsets across a partition.
    seq: int
    partition: int
    offset: int
    # keyspace:table:hash, written by the publisher as a plain UTF-8 string.
    key: str = ""
    keyspace: str = ""
    table: str = ""
    operation: str = ""
    # The mutation's own write time, from the record, and the broker's, from the
    # message.  Both in milliseconds since the epoch, so the page can show either.
    mutation_at_ms: int = 0
    kafka_at_ms: int = 0
    # How old the mutation was when this backend decoded it: the demo's end-to-end
    # latency for that record, Cassandra write to dashboard.  Absent on a record read
    # from before the tail attached, which would measure the backlog instead.
    age_ms: Optional[float] = None
    # A record the tail read to fill its buffer on attach, rather than one it saw
    # arrive.  Excluded from the latency figures for that reason.
    backfill: bool = False
    partial: bool = False
    columns: Dict[str, Any] = {}
    # The columns the mutation itself named, from the envelope's updateFields.  The
    # publisher's Avro record has a field per column of the table and fills the rest
    # with null, so this is what distinguishes a column written as null from one the
    # mutation never touched.
    update_fields: List[str] = []
    # The Avro schema id the record named, and the header fields that are not
    # columns.  Kept because the demo's claim is about the wire format as much as
    # the data.
    schema_id: Optional[int] = None
    decode_error: Optional[str] = None


class CdcStreamStatus(BaseModel):
    """What the tail is doing, in the terms the Streaming page shows."""

    # starting / waiting_for_topic / tailing / error
    state: str = "starting"
    topic: str = ""
    bootstrap: str = ""
    registry: str = ""
    partitions: List[int] = []
    buffer_size: int = 0
    buffered: int = 0
    # Records this backend has consumed since it started, and how many of those it
    # could not decode.  A decode failure is kept in the buffer with its reason
    # rather than dropped, because a record the dashboard cannot read is a finding.
    consumed: int = 0
    decode_failures: int = 0
    # Records a second, over the last measured interval.
    rate_per_sec: float = 0.0
    # End-to-end latency over the records seen live, mutation write to decode here.
    latency_p50_ms: Optional[float] = None
    latency_max_ms: Optional[float] = None
    schema_ids: List[int] = []
    last_record_at_ms: Optional[int] = None
    error: Optional[str] = None


class CdcStreamResponse(BaseModel):
    status: CdcStreamStatus
    # Newest first.
    records: List[CdcRecord] = []


class CdcSchemaView(BaseModel):
    """The Avro schema the topic's records are written against."""

    subject: str = ""
    schema_id: Optional[int] = None
    version: Optional[int] = None
    # The registry's own reply, parsed.  Field names and their Avro types, in
    # declaration order, so the page can show the contract rather than a blob.
    fields: List[Dict[str, Any]] = []
    # The columns of the table, from the nested `payload` record: the name, the Avro
    # type the publisher chose and the CQL type it chose it for.  Separate from the
    # ten envelope fields, because the two answer different questions.
    payload_fields: List[Dict[str, Any]] = []
    registry: str = ""
    avro_schema: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
