"""Map routes — live fleet positions, zone polygons, spatial questions."""
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException

from app.db.cassandra_client import cassandra_client
from app.models import (
    DronePosition,
    DroneTrail,
    MapLiveResponse,
    NearbyDroneResult,
    PolygonStatsRequest,
    PolygonStatsResponse,
    RestrictedZone,
    TrailPoint,
)
from app.utils.geometry import haversine_distance_m, parse_wkt_polygon, point_in_polygon

router = APIRouter(prefix="/api/map", tags=["map"])

DEFAULT_MAP_LIMIT = 2000
NEARBY_MAX_METRES = 5000


def _to_drone(row: Dict[str, Any]) -> DronePosition:
    return DronePosition(
        entity_id=str(row.get("entity_id", "")),
        event_time=str(row.get("event_time") or ""),
        latitude=float(row.get("latitude") or 0.0),
        longitude=float(row.get("longitude") or 0.0),
        altitude_m=float(row.get("altitude_m") or 0.0),
        speed_mps=float(row.get("speed_mps") or 0.0),
        heading_deg=float(row.get("heading_deg") or 0.0),
        is_flying=bool(row.get("is_flying")),
        temp_internal_c=float(row.get("temp_internal_c") or 0.0),
        temp_external_c=float(row.get("temp_external_c") or 0.0),
        near_restricted_zone=bool(row.get("near_restricted_zone")),
        predicted_zone_breach=bool(row.get("predicted_zone_breach")),
        risk_score=float(row.get("risk_score") or 0.0),
    )


def _to_zone(row: Dict[str, Any]) -> RestrictedZone:
    return RestrictedZone(
        zone_id=str(row.get("zone_id", "")),
        zone_name=str(row.get("zone_name", "")),
        polygon_wkt=str(row.get("polygon_wkt", "")),
        severity=str(row.get("severity") or "warning"),
        enabled=bool(row.get("enabled", True)),
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.get("/live", response_model=MapLiveResponse)
def get_map_live(limit: int = DEFAULT_MAP_LIMIT) -> MapLiveResponse:
    """Latest position per asset plus the restricted zones, for the live map."""
    if not cassandra_client.connected:
        cassandra_client.connect()
    if not cassandra_client.connected:
        return MapLiveResponse(drones=[], zones=[], timestamp=_now())
    try:
        return MapLiveResponse(
            drones=[_to_drone(r) for r in cassandra_client.get_drones(limit=limit)],
            zones=[_to_zone(r) for r in cassandra_client.get_zones()],
            timestamp=_now(),
        )
    except Exception as e:
        print(f"[map] /live failed: {e}")
        return MapLiveResponse(drones=[], zones=[], timestamp=_now())


@router.post("/polygon-stats", response_model=PolygonStatsResponse)
def get_polygon_stats(req: PolygonStatsRequest) -> PolygonStatsResponse:
    """Aggregate the fleet inside an arbitrary polygon.

    Cassandra has no spatial predicate, so containment is evaluated here over the
    bounded latest-state table.  For the same question over history, the Explore
    page runs it on Presto instead.
    """
    if not cassandra_client.connected:
        return PolygonStatsResponse()
    polygon = parse_wkt_polygon(req.polygon_wkt)
    if not polygon:
        raise HTTPException(status_code=400, detail="Could not parse polygon_wkt")
    try:
        inside = [
            d
            for d in cassandra_client.get_drones()
            if d.get("latitude") is not None
            and d.get("longitude") is not None
            and point_in_polygon(float(d["latitude"]), float(d["longitude"]), polygon)
        ]
    except Exception as e:
        print(f"[map] polygon-stats failed: {e}")
        return PolygonStatsResponse()

    def values(column: str) -> List[float]:
        return [float(d[column]) for d in inside if d.get(column) is not None]

    def mean(vs: List[float]) -> float:
        return round(sum(vs) / len(vs), 1) if vs else 0.0

    speeds, altitudes, temps = values("speed_mps"), values("altitude_m"), values("temp_internal_c")
    return PolygonStatsResponse(
        drone_count=len(inside),
        avg_speed_mps=mean(speeds),
        max_speed_mps=round(max(speeds), 1) if speeds else 0.0,
        avg_altitude_m=mean(altitudes),
        max_altitude_m=round(max(altitudes), 1) if altitudes else 0.0,
        avg_temp_internal_c=mean(temps),
    )


@router.get("/drone/{entity_id}", response_model=DronePosition)
def get_drone_detail(entity_id: str) -> DronePosition:
    if not cassandra_client.connected:
        raise HTTPException(status_code=503, detail="Cassandra unavailable")
    row = cassandra_client.get_drone_detail(entity_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"No such asset: {entity_id}")
    return _to_drone(row)


@router.get("/drone/{entity_id}/trail", response_model=DroneTrail)
def get_drone_trail(entity_id: str, points: int = 60) -> DroneTrail:
    """The asset's recent flight path, read from the event history table.

    This is the real recorded track, not an extrapolation from the current
    heading — the history is already in Cassandra, so the map may as well show it.
    """
    if not cassandra_client.connected:
        raise HTTPException(status_code=503, detail="Cassandra unavailable")
    try:
        rows = cassandra_client.get_drone_trail(entity_id, points=points)
    except Exception as e:
        print(f"[map] trail for {entity_id} failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    # The table is clustered newest-first; a path reads better oldest-first.
    path = [
        TrailPoint(
            event_time=str(r.get("event_time") or ""),
            latitude=float(r["latitude"]),
            longitude=float(r["longitude"]),
            altitude_m=float(r.get("altitude_m") or 0.0),
            speed_mps=float(r.get("speed_mps") or 0.0),
        )
        for r in reversed(rows)
        if r.get("latitude") is not None and r.get("longitude") is not None
    ]
    return DroneTrail(entity_id=entity_id, points=path)


@router.get("/drone/{entity_id}/nearby")
def get_nearby_drones(entity_id: str, meters: int = 500) -> Dict[str, Any]:
    """Other assets within a radius of this one."""
    meters = max(1, min(meters, NEARBY_MAX_METRES))
    if not cassandra_client.connected:
        return {"drones": []}
    target = cassandra_client.get_drone_detail(entity_id)
    if not target or target.get("latitude") is None:
        return {"drones": []}

    tlat, tlon = float(target["latitude"]), float(target["longitude"])
    nearby = []
    for d in cassandra_client.get_drones(flying_only=True):
        if d.get("entity_id") == entity_id or d.get("latitude") is None:
            continue
        distance = haversine_distance_m(tlat, tlon, float(d["latitude"]), float(d["longitude"]))
        if distance <= meters:
            nearby.append(
                NearbyDroneResult(
                    entity_id=str(d["entity_id"]),
                    event_time=str(d.get("event_time") or ""),
                    latitude=float(d["latitude"]),
                    longitude=float(d["longitude"]),
                    altitude_m=float(d.get("altitude_m") or 0.0),
                    distance_m=round(distance, 1),
                ).model_dump()
            )
    nearby.sort(key=lambda d: d["distance_m"])
    return {"drones": nearby}
