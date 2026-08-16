"""Request and response contracts for the dashboard API."""
from typing import Any, Dict, List, Optional

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


class SQLQueryRequest(BaseModel):
    sql: str
    limit: int = Field(default=10, ge=1, le=1000)
    engine: str = "cassandra"


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


class EngineResult(SQLQueryResult):
    # available=False means the engine could not be reached at all, which the UI
    # distinguishes from a query that reached it and failed.
    available: bool = True
    error: Optional[str] = None


class BenchmarkResponse(BaseModel):
    cassandra: EngineResult
    presto: EngineResult
    spark: EngineResult
    # The Analytics bulk reader: same rows, read straight from SSTables.
    spark_bulk: EngineResult


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
