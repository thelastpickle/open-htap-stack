"""Zone routes — restricted airspace and what-if simulation."""
from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from app.db.cassandra_client import cassandra_client
from app.models import RestrictedZone, WhatIfZoneRequest, WhatIfZoneResponse
from app.utils.geometry import distance_to_polygon_m, parse_wkt_polygon, point_in_polygon

router = APIRouter(prefix="/api/zones", tags=["zones"])

# Distance at which the ingest sink starts flagging proximity; the what-if
# simulation uses the same figure so its answer matches the live alerting.
WARNING_DISTANCE_M = 500.0


@router.get("")
def get_zones() -> Dict[str, Any]:
    """Every enabled restricted zone."""
    if not cassandra_client.connected:
        return {"zones": []}
    try:
        zones = cassandra_client.get_zones()
    except Exception as e:
        print(f"[zones] query failed: {e}")
        return {"zones": []}
    return {
        "zones": [
            RestrictedZone(
                zone_id=str(z.get("zone_id", "")),
                zone_name=str(z.get("zone_name", "")),
                polygon_wkt=str(z.get("polygon_wkt", "")),
                severity=str(z.get("severity") or "warning"),
                enabled=bool(z.get("enabled", True)),
            ).model_dump()
            for z in zones
        ]
    }


@router.post("/what-if", response_model=WhatIfZoneResponse)
def what_if_zone(req: WhatIfZoneRequest) -> WhatIfZoneResponse:
    """Score a hypothetical zone against the live fleet, without persisting it."""
    polygon = parse_wkt_polygon(req.polygon_wkt)
    if not polygon:
        raise HTTPException(status_code=400, detail="Could not parse polygon_wkt")

    zone = RestrictedZone(
        zone_id="what-if",
        zone_name=req.zone_name,
        polygon_wkt=req.polygon_wkt,
        severity=req.severity,
        enabled=True,
    )
    if not cassandra_client.connected:
        return WhatIfZoneResponse(zone=zone)

    try:
        drones = cassandra_client.get_drones()
    except Exception as e:
        print(f"[zones] what-if failed: {e}")
        return WhatIfZoneResponse(zone=zone)

    inside, nearby = [], []
    for d in drones:
        if d.get("latitude") is None or d.get("longitude") is None:
            continue
        lat, lon = float(d["latitude"]), float(d["longitude"])
        entity_id = str(d.get("entity_id", ""))
        if point_in_polygon(lat, lon, polygon):
            inside.append(entity_id)
        elif distance_to_polygon_m(lat, lon, polygon) < WARNING_DISTANCE_M:
            nearby.append(entity_id)

    return WhatIfZoneResponse(
        zone=zone,
        drones_inside=len(inside),
        drones_nearby=len(nearby),
        affected_drone_ids=inside + nearby,
    )
