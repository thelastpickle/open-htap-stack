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
